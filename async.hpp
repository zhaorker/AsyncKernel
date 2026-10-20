/**
 * @file async.hpp
 * @brief AsyncKernel 协程框架核心头文件。
 * SPDX-License-Identifier: GPL-3.0-or-later
 * SPDX-FileCopyrightText: Copyright (c) 2026 zhaorker kevin081006@outlook.com
 */

#pragma once

#include <coroutine>
#include<expected>
#include<type_traits>
#include<stdexcept>
#include<concepts>
#include<cstdint>
#include<cassert>
#include<memory>
#include<atomic>
//这在调试期能提供一些类型安全检查
#ifndef NDEBUG
    #include<typeinfo>
#endif

struct _base_promise;
struct _base_task;
template<typename T>
struct _promise_type;//这行，非常关键，不然编译报大量错
template<typename T>
class Task;

/**
 * @brief 错误码，用于 std::expected 返回。
 */
enum class Promise_error{
    NO_value,//该协程暂时没有返回或yield值
    Invalid_task//无效的Task(_handle=nullptr)
};

/**
 *@brief 用于表示协程状态的平凡结构体
 *
 *由一个int64_t字段和一个bool字段组成，bool代表是否是内核态的状态码
 *目前使用的kernel态状态码有：
 *-1：代表调用链的彻底退出，不会保留协程帧
 *0：调用链自然执行结束，协程帧会保留
 *1：协程resume前会把状态码设为1,这意味着没有使用状态挂起的话，状态码一律为1
 *2：该协程正在co_await子协程，get_state无法获取此状态码，因为_in_active_promise_ptr指向的协程帧一定没有子协程
 *3：该协程因co_yield挂起
 *@warning 库作者可以按需使用kernel态状态码，但不要与-1,0,1,2,3重复
 *@note 普通用户一定要使用用户态状态码（is_kernel==false），可以安全地把任意数据向外传递，甚至是指针
 */
struct State_code_package
{
    bool operator==(const State_code_package&other)const noexcept
    {
        return user_msg==other.user_msg && is_kernel==other.is_kernel;
    }
    bool operator!=(const State_code_package&other)const noexcept
    {
        return user_msg!=other.user_msg || is_kernel!=other.is_kernel;
    }
    std::int64_t user_msg;
    bool is_kernel;
};
static constexpr inline State_code_package Kernel_State[]{{-1,true},{0,true},{1,true},{2,true},{3,true}};

/**
 *@brief 用于通知协程完成的令牌
 *
 *Task::get_token返回此类型的shared_ptr，允许外部轮询_ready字段
 *并通过_value获取协程的返回值
 *
 *@tparam T作为返回值结果类型，void特化没有_value字段
 *
 *Complete_token是线程安全的，用_ready的原子操作和内存序保证结束通知和返回值可见性
 *
 *@note 对于非void的Task,只能获取一次完成令牌，而void版本允许有多个观察者
 *@note 如果你的场景只是在协程内部 co_await 子协程，则不需要此令牌，
 *       直接 co_await task 即可。
 *@warning 一旦一个Task被获取了观察者令牌，那么该Task的get_value就直接失效了，因为值不会再存在promise_type::_value里
 *
 *@par 典型场景：
 * - 你需要在一个非协程函数中等待多个协程完成 → 用 get_token() 轮询 _ready
 * - 你需要实现自定义的 when_all/when_any → Complete_token 是统一的状态载体(不过通常库自带的readyable够用)
 * - 你需要将协程的完成状态传递给另一个线程 → token 是线程安全的
 *@see Task::get_token ，When_all_readyable ,When_any_readyable
 */

template<typename T>
struct Complete_token
{
    struct
    {
        T _value{};//值
        bool _has_value{false};//有值标记
    }_value;
    std::atomic<bool> _ready{false};//协程结束标记
};
template<>
struct Complete_token<void>
{
    std::atomic<bool> _ready{false};
};

/**
 *@brief 所有Task::final_suspend的返回值类型
 *
 *该awaitable总是会将当前协程挂起，并执行：
 *如果是没有父协程的根协程，设置该帧状态码为0,若存在观察者则置位_ready表示结束，然后挂起
 *若是有父协程的子协程，纠正根Task的_in_active为父协程，纠正父帧的_owner，将父协程状态码设为1（活跃中），然后用对称转移立刻恢复父协程句柄
 *await_suspend因为是模板函数，总是能精准获取将要挂起的协程的确切类型并获取promise()
 *@note await_resume永远不会被调用
 */
struct final_awaitable//final_suspend返回的awaiter的作用是让子协程结束后直接跳转到父协程继续执行
{
    bool await_ready()const noexcept{return false;}
    //作为即将结束的类型T协程，它收到的self就是自身T类型的句柄
    /**
     * @brief 挂起当前协程，并根据父子关系决定下一步。
     * @tparam 当前将要挂起的协程的类型，编译器自动提供T （通过 _promise_type<T> 推导）。
     * @param self 当前协程的句柄，编译器自动提供
     * @return 要恢复的协程句柄（若为 noop_coroutine 则直接结束）。
     * @par 设计决策：为什么使用对称转移而非非对称？
     * 传统协程在子协程结束后需要层层返回（子->父->祖父...），
     * 每层都产生不必要的栈帧保留。本库采用对称转移，子协程结束
     * 时直接跳转到父协程的恢复点，避免中间帧的保留和恢复开销，
     * 特别适合深度嵌套的异步调用链。
     */
    template<typename T>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<T>>self)noexcept
    {
        auto &father_handle=self.promise()._parent_handle;
        if (father_handle) {
            auto *top_task_ptr=self.promise()._owner;
            /*self是正在结束中的子协程，它有根task的指针*/
            top_task_ptr->_in_active_handle=father_handle;//转移活动协程指向
            top_task_ptr->_in_active_promise_ptr=self.promise()._parent_promise_ptr;

            self.promise()._parent_promise_ptr->_owner=self.promise()._owner;//校准父协程的_owner,因为自己一定是准的（Task移动coro_repoint）

            self.promise()._parent_promise_ptr->_state_code={1,true};
            return father_handle;//不逗留，对称转移立刻resume父协程
        }
        self.promise()._state_code={0,true};
        if(self.promise()._complete_flag)
            self.promise()._complete_flag->_ready.store(true,std::memory_order_release);
        return std::noop_coroutine();
    }
    void await_resume()const noexcept{}//根本不会被调用
};

/** @internal */
struct _base_promise
{

    std::coroutine_handle<> _parent_handle{nullptr};        //父协程句柄
    _base_promise *_parent_promise_ptr{nullptr};            //父协程的promise_type指针

    _base_task* _owner{nullptr};                            //管理改协程链的根Task

    State_code_package _state_code{1,true};

    std::exception_ptr _exp{};                              //协程内部未捕获的异常
    #ifndef NDEBUG
    const std::type_info *type;
    #endif

    std::suspend_always initial_suspend()const noexcept{ return {}; }
    final_awaitable final_suspend()const noexcept{return {};}

    void unhandled_exception()noexcept
    {
        _exp=std::current_exception();
    }

    _base_promise()noexcept
    {
        #ifndef NDEBUG
        type=&typeid(_base_promise);
        #endif
    }
};
/** @internal */
struct _base_task
{
    //掌握所有权关系的协程帧
    std::coroutine_handle<> _handle{nullptr};
    _base_promise *_promise_ptr{nullptr};
    //观察非拥有的活跃协程帧
    std::coroutine_handle<> _in_active_handle{nullptr};
    _base_promise *_in_active_promise_ptr{nullptr};//两者共同合作处理一个协程帧
    //要使用_in_active_handle激活底部协程
    //要使用_in_active_promise_ptr修改_owner(在Task被移动时)
    #ifndef NDEBUG
        const std::type_info *type;
    #endif
protected:
    _base_task()noexcept
    {
        #ifndef NDEBUG
        type=&typeid(_base_task);
        #endif
    }
    _base_task(std::coroutine_handle<>handle,_base_promise *promise_ptr)noexcept:
        _handle{handle},_promise_ptr{promise_ptr},
        _in_active_handle{handle},_in_active_promise_ptr{promise_ptr}
    {
        #ifndef NDEBUG
        type=&typeid(_base_task);
        #endif
        promise_ptr->_owner=this;
    }
    _base_task(const _base_task&)=delete;
    _base_task& operator=(const _base_task&)=delete;
    _base_task(_base_task&&other)noexcept:
        _handle{other._handle},_promise_ptr{other._promise_ptr},
        _in_active_handle{other._in_active_handle},_in_active_promise_ptr{other._in_active_promise_ptr}
    {
        other._handle=
        other._in_active_handle=nullptr;
        other._promise_ptr=
        other._in_active_promise_ptr=nullptr;
        coro_repoint();
    }
    _base_task& operator=(_base_task&&other)noexcept
    {
        if(&other==this)
            return *this;
        std::coroutine_handle<> handle=_handle;

        _handle=other._handle;
        _in_active_handle=other._in_active_handle;
        _promise_ptr=other._promise_ptr;
        _in_active_promise_ptr=other._in_active_promise_ptr;

        other._handle=
        other._in_active_handle=nullptr;
        other._promise_ptr=
        other._in_active_promise_ptr=nullptr;

        coro_repoint();

        if(handle)
            handle.destroy();
        return *this;
    }
    /**
     *@brief 在移动操作过后调用该函数纠正活跃协程的_owner
     *@note 不需要全部纠正，因为子协程结束会自动纠正父
     *@see final_awaitable::await_suspend
     */
    void coro_repoint()noexcept
    {
        if(_in_active_promise_ptr)
            _in_active_promise_ptr->_owner=this;
    }
public:
    ~_base_task()
    {
        if(!_handle)
            return;
        std::coroutine_handle<> handle=_handle;
        _handle=
        _in_active_handle=nullptr;
        _promise_ptr=
        _in_active_promise_ptr=nullptr;
        handle.destroy();
    }
    /**
     *@brief 用于获取协程的返回值，可以是任意深度的yield,或者根协程的return
     *
     *@tparam 想要获取的返回值类型U
     *
     *@return std::expected<U,Promise_error>
     *
     *@warning 如果没开启NDEBUG,会用RTTI检测当前活跃协程的类型是否是_promise_type<U>，请自己确保传入的类型是正确的
     *
     *@note 取值操作是一次性的，连续多次get_value只有第一次能成功
     */
    template<typename U>
    requires (!std::is_same_v<U ,void>)
    std::expected<U,Promise_error> get_value()noexcept
    {
        if(!_in_active_handle)
            return std::unexpected(Promise_error::Invalid_task);
        assert(_in_active_promise_ptr->type==&typeid(_promise_type<U>));
        auto real_type_handle=std::coroutine_handle<_promise_type<U>>::from_address(
            _in_active_handle.address()
        );
        if(!real_type_handle.promise()._value._has_value)
            return std::unexpected(Promise_error::NO_value);
        real_type_handle.promise()._value._has_value=false;
        return std::move(real_type_handle.promise()._value._value);
    }

    /**
     *@brief 获取当前协程的状态码
     *@return const State_code_package&，即内部_state_code字段的引用，若帧销毁返回全局的-1状态码引用
     */
    const State_code_package& get_state()const noexcept
    {
        if(!_in_active_handle || !_in_active_promise_ptr)
            return Kernel_State[0];//协程已销毁肯定是发生了co_exit,因为正常结束是不会立刻销毁协程帧的
        return _in_active_promise_ptr->_state_code;
    }
    /**
     *@brief 查询协程状态
     *@return bool 若为true则代表可以继续resume
     */
    bool valid()const noexcept
    {
        if(!_in_active_handle || _in_active_handle.done())
        //  相当于state_code=-1  相当于state_code=0（结束）
            return false;
        //依旧不可能获得2
        return true;
    }
    /**
     *@brief 查询协程状态
     *@return bool 若为true代表协程已经自然执行结束（而非co_exit），协程帧有效
     */
    bool done()const noexcept
    {
        if(_in_active_handle)
            return _in_active_handle.done();
        return false;
    }
    /**
     *@brief 恢复协程的执行
     *@return bool 若为true表示本次恢复成功，false说明协程早就失效（valid()==false）
     *@throw 在整个调用链里都没有被截获的异常会在reusme处抛出
     *@note 自动检测状态码若为-1则直接销毁协程帧
     *@par 恢复协程前总是要先把协程状态设为内核1,目的是保证在协程co_await_suspend_always{};之后挂起的状态码是规定的“无状态的挂起：1”
     *@code
     * //最方便的用法是
     * while(handle.resume())
     * {
     *      //do something
     * }
     *@endcode
     */
    bool resume()
    {
        if(!valid())
            return false;
        _in_active_promise_ptr->_state_code={1,true};
        _in_active_handle.resume();
        if(_in_active_promise_ptr->_exp)
            std::rethrow_exception(_in_active_promise_ptr->_exp);
        if(_in_active_promise_ptr->_state_code==State_code_package{-1,true})
        {
            auto handle=_handle;
            _in_active_handle=_handle=nullptr;
            _in_active_promise_ptr=nullptr;
            handle.destroy();
        }
        return true;
    }

    //接下来是作为awaitable的通用成员
    //永远先触发父协程挂起，然后建立父子联系然后恢复子协程执行
    bool await_ready()const noexcept{return false;}

    /**
     * @brief 挂起父协程，并将本 Task 作为子协程执行。
     * @tparam T 父协程的返回类型。
     * @param _father 父协程的句柄。
     * @return 要恢复的子协程句柄。
     * @note 此函数建立父子关系，设置协程状态码，并更新根 Task 的活动指针。
     * @warning 不要co_await外部传入，不由协程内掌握生命周期的Task,除非你清楚调度逻辑
     */
    template<typename T>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<T>> _father)noexcept
    {
        if(!valid())//当前task持有的不是一个有效的协程桟（-1或者已经结束）
            return std::noop_coroutine();

        _promise_ptr->_parent_handle = _father;
        _promise_ptr->_parent_promise_ptr = &_father.promise();

        //只让底部对准_owner,对称转移的时候才纠正父协程的_owner
        auto* top_Task_ptr=_in_active_promise_ptr->_owner=_father.promise()._owner;
        
        //并且通过_owner赋值链一直传递到底部
        top_Task_ptr->_in_active_handle=_in_active_handle;//修改活跃协程为本桟的活跃协程
        top_Task_ptr->_in_active_promise_ptr=_in_active_promise_ptr;

        _father.promise()._state_code={2,true};
        _in_active_promise_ptr->_state_code={1,true};
        return _in_active_handle;
    }
    //await_resume因为要返回expected,没法泛型化了
};

/**
 * @brief 用于在协程内部挂起并设置状态码的 awaitable。
 *
 * 用法：`co_await suspend_state_code(value, is_kernel);` 或 `co_await co_exit;`。
 * 该 awaitable 总会挂起当前协程，并将状态码设置为指定值。
 *
 * @code
 * // 协程内部挂起并通知外部进度
 * co_await suspend_state_code(42, false);  // 42 表示"已处理第42个数据块"
 * 
 * // 外部轮询进度
 * auto code = task.get_state();
 * if (!code.is_kernel) {
 *     printf("进度: %lld%%\n", code.user_msg);
 * }
 * @endcode

 * @note 内核状态码 -1（`co_exit`）会触发协程帧在恢复后立即销毁。见_base_task::resume
 */
class suspend_state_code
{
    State_code_package _value;
public:
    constexpr suspend_state_code(std::int64_t value,bool is_kernel=false)noexcept:_value{value,is_kernel}{}
    bool await_ready()const noexcept{return false;}

    template<typename T>
    void await_suspend(std::coroutine_handle<_promise_type<T>>handle)const noexcept
    {
        handle.promise()._state_code=_value;
    }

    void await_resume()const noexcept{}
};
static constexpr inline suspend_state_code co_exit{-1,true};


/**
 * @brief 非 void 返回类型的 promise 特化。
 * @tparam T 返回值类型。
 */
template<typename T>
struct _promise_type:public _base_promise
{
    struct Optional_package{
        T _value{};
        bool _has_value{false};
    }_value;

    //给When_xxx_readyable用的完成令牌
    mutable std::shared_ptr<Complete_token<T>> _complete_flag{nullptr};

    Task<T> get_return_object()noexcept
    {
        return  std::coroutine_handle<_promise_type>::from_promise(*this);
    }
    _promise_type()noexcept:_base_promise{}
    {
        #ifndef NDEBUG
        type=&typeid(_promise_type);
        #endif
    }
    ~_promise_type()
    {
        if(_complete_flag)
            _complete_flag->_ready.store(true,std::memory_order_relaxed);
    } 

    suspend_state_code yield_value(const T &value)noexcept(
        noexcept(T(value)) &&
        noexcept(value=value)
    )//yield值之后应当到根部的resumer
    {
        _value._has_value=true;
        _value._value=value;
        return{3,true};
    }
    suspend_state_code yield_value(T &&value)noexcept(
        noexcept(T(std::move(value))) &&
        noexcept(value=std::move(value))
    )
    {
        _value._has_value=true;
        _value._value=std::move(value);
        return{3,true};
    }
    void return_value(T &value)noexcept(
        noexcept(value=value) &&
        noexcept(T(value))
    )
    {
        if(_complete_flag)
        {
            _complete_flag->_value._has_value=true;
            _complete_flag->_value._value=value;
            return;
        }
        _value._has_value=true;
        _value._value=value;
    }
    void return_value(T &&value)noexcept(
        noexcept(value=std::move(value)) &&
        noexcept(T(std::move(value)))
    )
    {
        if(_complete_flag)
        {
            _complete_flag->_value._has_value=true;
            _complete_flag->_value._value=std::move(value);
            return;
        }
        _value._has_value=true;
        _value._value=std::move(value);
    }
};

/**
 * @brief 协程任务类型，管理一个协程的生命周期和执行。
 * 作为一个协程句柄，Task有以下特性：
 *  - 极其自然的嵌套写法 co_await child_coro_func();等待子协程结束再继续执行父协程
 *  - 极其自然的异常传播，类似桟展开的机制，你完全可以用try-catch管理调用链里的异常流
 *  - 灵活的向外传递状态码机制
 * 所有Task都是unique语义的RAII句柄，不需要手动destroy,如果真的要置空销毁帧，可以handle={};
 * Task默认非线程安全，如果需要，自行用shared_ptr和mutex包装
 * @tparam T 协程的返回值类型（void 允许特化）。
 * @par 为什么不默认共享，不默认线程安全
 * 库的首要原则是灵活性和零开销抽象，默认shared会引入某些时候不必要的计数开销，mutex的开销更大
 * 作为完全由程序员掌握生命周期的协程句柄，本身默认unique语义能提供默认的最大安全，同时，unique也暗示并不提倡多线程共享Task
 * unique语义让移动、对称转移、结构化同步等设施实现起来更直观简介
 * 如果真的有特殊需求，自行包装和扩展并不困难
 *@code
 * //使用协程句柄只需要将该类型作为函数返回值，就可以在内部co_await,co_return,co_yield
 * Task<int> range(int a,int b,int c)
 * {
 *      while(int i=a;i<b;i+=c)
 *          co_yield i;
 * }
 * auto handle=range(0,10,2);
 * while(handle.resume())
 * {
 *      if(handle.valid())
 *          printf("%d ",handle.get_value().value());
 * }
 */
template<typename T>
class Task:public _base_task
{
    template<std::invocable<Task<void>&,std::coroutine_handle<>>>friend class _swap_switch_to;
    template<typename U> friend class _promise_type; 
public:
    using promise_type=_promise_type<T>;
    auto get_value()noexcept{return _base_task::get_value<T>();}
    Task()noexcept:_base_task{}
    {
        #ifndef NDEBUG
        type=&typeid(Task);
        #endif
    }
    Task(std::coroutine_handle<promise_type> handle)noexcept:_base_task{handle,&handle.promise()}
    {
        #ifndef NDEBUG
        type=&typeid(Task);
        #endif
    }
    Task(Task&&)noexcept=default;
    Task(const Task&)=delete;
    Task& operator=(Task&&)noexcept=default;
    Task& operator=(const Task&)=delete;
    ~Task()=default;

    /**
     *@brief 子协程结束，转移恢复父协程时调用
     *@return 包含子协程返回值的expected<T,Promise_error>
     *@throw 如果子协程有未捕获的异常，通过该函数重新抛进父协程空间
     *@note 返回值用std::move返回
     *@note 无效句柄的行为是：无条件挂起，状态码为1,然后恢复时返回unexpected(Invalid_task)
     *@see Task<void>::await_resume
     */
    std::expected<T,Promise_error> await_resume()const
    {
        if(!_promise_ptr)
            return std::unexpected(Promise_error::Invalid_task);
        if(_promise_ptr->_exp)
            std::rethrow_exception(static_cast<_promise_type<T>*>(_promise_ptr)->_exp);
        if(!static_cast<_promise_type<T>*>(_promise_ptr)->_value._has_value)
            return std::unexpected(Promise_error::NO_value);
        return std::move(static_cast<_promise_type<T>*>(_promise_ptr)->_value._value);
    }
    /**
     * @brief 获取完成令牌（用于外部观察）。
     * @return std::shared_ptr<Complete_token<T>>，若已存在观察者或内存不足则返回 nullptr。
     * @note 对于非 void 类型，每个 Task 只能成功获取一次令牌。
     * @par 过去曾把get_token写作原子操作实现线程安全，但由于其它成员全部非线程安全，加锁的隐式开销过大，因而get_token也非线程安全
     * @warning get_token要和其他成员一样，保证串行化执行
     * @code //如果不使用When_xxx_readyable,可以这样判断协程完成情况
     *  //协程内代码
     *  auto token=handle.get_token();
     *  //假设有协程actor循环器
     *  Actor.join_run(std::move(handle));
     *  while(!token->_ready.load())
     *      co_await_suspend_always{};
     * @endcode
     * @see When_all_readyable,When_any_readyable,Task<void>::get_token
     */
    std::shared_ptr<Complete_token<T>> get_token()const noexcept
    {
        if(!_promise_ptr)
            return nullptr;
        promise_type* ptr=static_cast<promise_type*>(_promise_ptr);
        //说明已经有人观察了这个Task,那就直接放弃获取观察
        if(ptr->_complete_flag && ptr->_complete_flag.use_count()>1)
            return nullptr;
        if(ptr->_complete_flag.use_count()==1)
            return ptr->_complete_flag;
        std::shared_ptr<Complete_token<T>> shared(new (std::nothrow) Complete_token<T>);
        if(!shared)
            return nullptr;//内存分配失败那就算了
        ptr->_complete_flag=shared;
        if(done())
        {
            shared->_value._value=std::move(ptr->_value._value);
            shared->_value._has_value=ptr->_value._has_value;
            ptr->_value._has_value=false;
            shared->_ready.store(true,std::memory_order_relaxed);
        }
        return shared;
    }
};

/*=========================然后做一下void特化=========================*/
template<>class Task<void>;
template<>struct _promise_type<void>;
template<>
class Task<void>:public _base_task
{
    template<std::invocable<Task<void>&,std::coroutine_handle<>>>friend class _swap_switch_to;
    template<typename U> friend class _promise_type;
public:
    using promise_type=_promise_type<void>;
    Task()noexcept:_base_task{}
    {
        #ifndef NDEBUG
        type=&typeid(Task);
        #endif
    }
    //这两个不得不延迟实现
    std::shared_ptr<Complete_token<void>> get_token()const noexcept;
    Task(std::coroutine_handle<promise_type> handle)noexcept;
    Task(Task&&other)noexcept=default;
    Task(const Task&)=delete;
    Task& operator=(Task&& other)noexcept=default;
    Task& operator=(const Task&)=delete;
    ~Task()=default;
    /**
     * @brief 作为子协程被 co_await 后恢复时调用。
     * @return true 表示协程正常完成，false 表示无效。
     * @throws 如果子协程抛出异常未捕获则重新抛出。
     */
    bool await_resume()const
    {
        if(!_promise_ptr)
            return false;
        if(_promise_ptr->_exp)
            std::rethrow_exception(_promise_ptr->_exp);
        return true;
    }
};
template<>
struct _promise_type<void>:public _base_promise
{
    mutable std::shared_ptr<Complete_token<void>> _complete_flag{nullptr};

    Task<void> get_return_object()noexcept
    {
        return  std::coroutine_handle<_promise_type>::from_promise(*this);
    }
    _promise_type()noexcept:_base_promise{}
    {
        #ifndef NDEBUG
        type=&typeid(_promise_type);
        #endif
    }
    ~_promise_type()
    {
        if(_complete_flag)
            _complete_flag->_ready.store(true,std::memory_order_relaxed);
    }

    void return_void()const noexcept{}
};
Task<void>::Task(std::coroutine_handle<promise_type> handle)noexcept:_base_task{handle,&handle.promise()}
{
    #ifndef NDEBUG
    type=&typeid(Task);
    #endif
}
/**
 * @brief 获取完成令牌（void 版本允许多个观察者）。
 * @return 现有的或新创建的 shared_ptr<Complete_token<void>>。
 */
std::shared_ptr<Complete_token<void>> Task<void>::get_token()const noexcept
{
    if(!_promise_ptr)
        return nullptr;
    promise_type *ptr=static_cast<promise_type*>(_promise_ptr);
    if(ptr->_complete_flag)
        return ptr->_complete_flag;
    std::shared_ptr<Complete_token<void>> shared(new (std::nothrow)Complete_token<void>);
    if(!shared)
        return nullptr;
    //current_ptr=nullptr
    ptr->_complete_flag=shared;
    if(done())
    {
        shared->_ready.store(true,std::memory_order_relaxed);
    }
    return shared;
}

template class Task<void>;
using async=Task<void>;

/**
 * @brief 将任何 Task<T> 包装为 Task<void>，便于统一根协程类型。
 * 
 * @par 为什么需要它？
 * `swap_switch_to` 要求两个 Task 的根类型均为 Task<void>。
 * 如果你的协程链中会用到对称转移，用 `start()` 包装最外层 Task，
 * 可以确保类型统一，避免未定义行为。
 * 
 * @param root 要包装的协程任务。
 * @return async（即 Task<void>）类型。
 */
template<typename T>
async start(Task<T> root)noexcept
{
    co_await root;
}
//用这个包装任意协程桟可保证根协程类型一致，这能让swap_switch_to更安全地运行


//黑魔法类，直接获取根task指针
/**
 *@brief 极其危险的库作者专属工具
 *假如你希望Task类本身与某些上下文对象绑定在一起，可以继承Task类，然后在协程内部获取根Task指针以直接窃取上下文
 *@code
 * class My_task:public Task<void>
 * {Context context_handle;...};
 * //协程内部，或者说某工具协程函数内部
 * auto ptr=co_await _get_top_task_ptr{};
 * My_task *task_ptr=static_cast<My_task*>(ptr);
 * //访问task_ptr->context_handle
 *@endcode
 *@warning 如果你是库作者，强烈建议用本库类似手法RTTI验证类型，如果你是写业务代码的用户，不要使用此工具
 */
class _get_top_task_ptr
{
public:
    bool await_ready()noexcept{return false;}
    template<typename T>
    bool await_suspend(std::coroutine_handle<_promise_type<T>> this_coro)noexcept
    {
        _top_task=this_coro.promise()._owner;
        return false;
    }
    _base_task* await_resume()noexcept
    {
        return _top_task;
    }
private:
    _base_task* _top_task;
};

/**
 * @brief 对称转移工具，允许在两个同类型 Task<void> 之间交换执行权。
 * @tparam 钩子函数的可调用类型Callable,用invocable作约束
 *
 * 使用方式：co_await swap_switch_to(destination, hook);
 * 会交换 origin 和 destination 的内容，然后恢复 destination 的协程。
 * hook以原协程帧和指向原协程帧的Task<void>为参数
 * 如果 hook 返回 false，则原协程帧会被销毁。
 *
 * @warning 要求两个 Task 的根Task类型均为 Task<void>，否则行为未定义。
 */
bool Do_nothing(Task<void>&,std::coroutine_handle<>)noexcept{return true;}
bool Delete_origin(Task<void>&,std::coroutine_handle<>)noexcept{return false;}

template<std::invocable<Task<void>&,std::coroutine_handle<>> Callable>
class _swap_switch_to
{
    template<std::invocable<Task<void>&,std::coroutine_handle<>> F> friend
    _swap_switch_to<F> swap_switch_to(Task<void> &des,F&&hook)noexcept;
    template<std::invocable<Task<void>&,std::coroutine_handle<>> F> friend
    _swap_switch_to<F> swap_switch_to(Task<void> &&des,F&&hook)noexcept;

    template<typename F>
    _swap_switch_to(Task<void> *des,F&&hook)noexcept:
        _destination{des},if_success{false},_hook{std::forward<F>(hook)}{}
public:
    _swap_switch_to(const _swap_switch_to&)=delete;
    _swap_switch_to& operator=(const _swap_switch_to&)=delete;
    _swap_switch_to(_swap_switch_to&&other)noexcept:
        _destination{other._destination},if_success{false},_hook{std::move(other._hook)}
    {other._destination=nullptr;}
    _swap_switch_to& operator=(_swap_switch_to&&)=delete;

    bool await_ready()noexcept{return false;}
    template<typename U>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<U>> _origin)noexcept
    {
        assert(_origin.promise()._owner->type==&typeid(Task<void>));
        if(!_destination || !_destination->valid())
            return std::noop_coroutine();
        Task<void> *origin_task=static_cast<Task<void>*>(_origin.promise()._owner);
        if(origin_task==_destination)
        {
            return _origin;
        }
        std::swap(*origin_task,*_destination);//移动赋值自动处理了所有子协程帧的_owner
        if_success=true;
        if(!static_cast<bool>(_hook(*_destination,_origin)))
        {
            //这里需要考虑一种极端情况
            /*
            在某个协程的内部，启动了一个新的协程，存在局部变量里
            async test()
            {
                async task=async_step();
                co_await swap_switch_to(task,Do_nothing);
            }
            经过这一步转移，导致test的生命周期由自己的局部变量task掌控，如果丢失其句柄会彻底造成内存泄漏
            而如果使用co_await swap_switch_to(task,Delete_origin);
            如果直接task._handle.destroy,会导致协程帧销毁->task析构->再destroy一次，之后发生什么就很难想象了

            所以要想删除原协程帧，必须先解除task的所有权，然后destroy原协程
            */
            auto _handle=_destination->_handle;
            _destination->_handle=nullptr;
            _destination->_in_active_handle=nullptr;
            _destination->_in_active_promise_ptr=nullptr;
            _destination->_promise_ptr=nullptr;
            _handle.destroy();
            //虽然销毁帧，并不会影响await_suspend的执行，因为接下来不会再访问原协程的任何东西了
            //也不会再访问_swap_switch_to的任何成员，origin_task是该函数的局部变量
        }
        origin_task->_in_active_promise_ptr->_state_code={1,true};
        return origin_task->_in_active_handle;//其实现在_origin_task已经指向整个destination桟了
        /*注意这会直接改变整个Task的指向，所以如果转移到的目标桟发生了co_exit并不会丢失-1码
          因为在Task::resume内部_handle_resume结束后，再检查_in_active_promise_ptr->_state_code正是destination协程上的-1
          对应的协程会按预期销毁*/
    }
    bool await_resume()noexcept{return if_success;}
private:
    Task<void> *_destination;
    bool if_success;
    //给用户一个钩子可以操作切换后的原协程
    std::decay_t<Callable> _hook;
    //返回false意味着“抛弃”原协程调用栈，await_suspend内部会将原协程销毁，目标协程的原主Task会被置空；
};
/**
 **
 *@brief 返回_swap_switch_to的工厂函数
 *@tparam 钩子的可调用对象类型Callable
 *@param 转移到的目标的根Task和钩子函数
 *@return _swap_switch_to<Callable>
 */
template<std::invocable<Task<void>&,std::coroutine_handle<>> Callable>
_swap_switch_to<Callable> swap_switch_to(Task<void> &des,Callable&&hook)noexcept
{
    return {&des,std::forward<Callable>(hook)};
}
template<std::invocable<Task<void>&,std::coroutine_handle<>> Callable>
_swap_switch_to<Callable> swap_switch_to(Task<void> &&des,Callable&&hook)noexcept
{
    return {&des,std::forward<Callable>(hook)};
}

//when_all 和 when_any接口，结构化并发的同步设施
//when_all和when_any的职责很简单：判断是否达成条件。至于如何调度句柄，不在该接口的功能范围内（在并发状态下安全取到Task里的值极其困难，且可能有违unique语义
enum class When_error{
    NOT_YET,//尚未结束的任务
    INVALID_HANDLE,//在get_token时失败，没获取到有效token指针
    VOID_TASK,//async句柄，没有返回值
    NO_VALUE//有T类型的Task却没有co_return
};
namespace detail
{
constexpr inline auto check_one_element=[]<typename T>(const std::shared_ptr<Complete_token<T>> element)->bool
{
    if(!element)
        return true;
    return element->_ready.load(std::memory_order_acquire);
};
constexpr inline auto get_one_result=[]<typename T>(const std::shared_ptr<Complete_token<T>> element)->std::expected<T,When_error>
{
    if(!element)
        return std::unexpected(When_error::INVALID_HANDLE);
    if(element->_ready.load(std::memory_order_acquire))
    {
        if constexpr(std::is_same_v<T,void>)
            return std::unexpected(When_error::VOID_TASK);
        else{
            if(element->_value._has_value)
            {
                element->_value._has_value=false;
                return std::move(element->_value._value);
            }
            return std::unexpected(When_error::NO_VALUE);

        }
    }
    return std::unexpected(When_error::NOT_YET);
};
}
/**
 * @brief 等待所有任务完成的类
 * 用ready()成员检验是否全完成，用get_result获取协程返回值
 * 没成功获取token的句柄返回INVALID_HANDLE
 *  未结束的协程一律NOT_YET
 *  已结束的协程根据类型可能返回VOID_TASK
 *      结束了没有co_return的返回NO_VALUE
 *      返回value（expected.has_value()==true）
 * @tparam ...Argt是所有要等待的Task<Argt>的参数包
 *
 * @warning readyable也都是非线程安全的类型
 * @note 如果观察的调用链内部发生对称转移，那么观察方向保持指向原来的协程帧，不会跟随Task转变观察方向
 */
template<typename ...Argt>
struct When_all_readyable
{
    /**
     *@brief When_xxx_readyable的构造函数
     *@param 必须接收已经存在的Task的左值引用
     */
    When_all_readyable(Task<Argt>& ...args)noexcept:
        _tasks_waiting{std::make_tuple(args.get_token() ...)}
    {}
    /**
     *@brief 检查所有任务是否结束
     *@return bool true代表全部结束 
     */
    bool ready()const noexcept
    {
        return check_tuple_and(detail::check_one_element,N);
    }
    /**
     *@brief 获取所有等待的任务的返回值
     *@return 包含所有任务的返回值的tuple<expected<Argt,When_error>...>
     *@note 内部用move返回协程的结果，因此已经获取过返回值的任务无法再次获取，多次调用会返回NO_VALUE
     */
    auto get_result()const noexcept
    {
        return get_tuple_and(detail::get_one_result,N);
    }
    When_all_readyable(const When_all_readyable&)=delete;
    When_all_readyable& operator=(const When_all_readyable&)=delete;
    When_all_readyable(When_all_readyable&&)=delete;
    When_all_readyable& operator=(When_all_readyable&&)=delete;
private:
    std::tuple<std::shared_ptr<Complete_token<Argt>> ...> _tasks_waiting;
    static constexpr inline auto N=std::make_index_sequence<std::tuple_size_v<decltype(_tasks_waiting)>>{};
    template<typename F,std::size_t ...Index>
    bool check_tuple_and(F &&func,std::index_sequence<Index...>)const noexcept
    {
        return (func(std::get<Index>(_tasks_waiting)) && ...);
    }
    template<typename F,std::size_t ...Index>
    auto get_tuple_and(F &&func,std::index_sequence<Index...>)const noexcept
    {
        return std::tuple_cat(std::make_tuple(func(std::get<Index>(_tasks_waiting))) ...);
    }
};
/**
 * @brief 等待任一任务完成的类
 *
 * @tparam ...Argt是所有要等待的Task<Argt>的参数包
 *
 * @warning readyable也都是非线程安全的类型
 */
template<typename ...Argt>
struct When_any_readyable
{
    When_any_readyable(Task<Argt>& ...args)noexcept:
        _tasks_waiting{std::make_tuple(args.get_token() ...)}
    {}
    bool ready()const noexcept
    {
        return check_tuple_or(detail::check_one_element,N);
    }
    auto get_result()const noexcept
    {
        return get_tuple_and(detail::get_one_result,N);
    }
    When_any_readyable(const When_any_readyable&)=delete;
    When_any_readyable& operator=(const When_any_readyable&)=delete;
    When_any_readyable(When_any_readyable&&)=delete;
    When_any_readyable& operator=(When_any_readyable&&)=delete;
private:
    std::tuple<std::shared_ptr<Complete_token<Argt>> ...> _tasks_waiting;
    static constexpr inline auto N=std::make_index_sequence<std::tuple_size_v<decltype(_tasks_waiting)>>{};
    template<typename F,std::size_t ...Index>
    bool check_tuple_or(F &&func,std::index_sequence<Index...>)const noexcept
    {
        return (func(std::get<Index>(_tasks_waiting)) || ...);
    }
    template<typename F,std::size_t ...Index>
    auto get_tuple_and(F &&func,std::index_sequence<Index...>)const noexcept
    {
        return std::tuple_cat(std::make_tuple(func(std::get<Index>(_tasks_waiting))) ...);
    }
};
