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
#ifndef NDEBUG
    #include<typeinfo>
#endif

struct _base_promise;
struct _base_task;
template<typename T>
struct _promise_type;//这行，非常关键，不然编译报大量错
template<typename T>
class Task;

enum class Promise_error{
    NO_value,Invalid_task
};
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
/*
目前使用的kernel态状态码有：
-1：代表调用链的彻底退出，不会保留协程帧
0：调用链自然执行结束，协程帧会保留
1：协程resume前会把状态码设为1,这意味着没有使用状态挂起的话，状态码一律为1
2：该协程正在co_await子协程，get_state无法获取此状态码，因为_in_active_promise_ptr指向的协程帧一定没有子协程
3：该协程因co_yield挂起
库作者可以按需使用kernel态状态码，但不要与-1,0,1,2,3重复
普通用户一定要使用用户态状态码，可以安全地把任意数据向外传递，甚至是指针
*/
};
static constexpr inline State_code_package Kernel_State[]{{-1,true},{0,true},{1,true},{2,true},{3,true}};



struct final_awaitable//final_suspend返回的awaiter的作用是让子协程结束后直接跳转到父协程继续执行
{
    bool await_ready()const noexcept{return false;}
    //作为即将结束的类型T协程，它收到的self就是自身T类型的句柄
    template<typename T>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<T>>self)noexcept
    {
        auto &father_handle=self.promise()._parent_handle;
        if (father_handle) {
            auto *top_task_ptr=self.promise()._owner;
            /*self是正在结束中的子协程，它有顶层task的指针*/
            top_task_ptr->_in_active_handle=father_handle;//转移活动协程指向
            top_task_ptr->_in_active_promise_ptr=self.promise()._parent_promise_ptr;

            self.promise()._parent_promise_ptr->_owner=self.promise()._owner;//校准父协程的_owner,因为自己一定是准的（Task移动coro_repoint）

            self.promise()._parent_promise_ptr->_state_code={1,true};
            return father_handle;//不逗留，对称转移立刻resume父协程
        }
        self.promise()._state_code={0,true};
        return std::noop_coroutine();
    }
    void await_resume()const noexcept{}//根本不会被调用
};

struct _base_promise
{

    std::coroutine_handle<> _parent_handle{nullptr};
    _base_promise *_parent_promise_ptr{nullptr};

    _base_task* _owner{nullptr};

    State_code_package _state_code{1,true};

    std::exception_ptr _exp{};
    #ifndef NDEBUG
    const std::type_info *type;
    #endif

    std::suspend_always initial_suspend()const noexcept{ return {}; }
    final_awaitable final_suspend()const noexcept{return {};}

    void unhandled_exception()noexcept
    {
        _exp=std::current_exception();
    }
    //用于实现结构化并发的观察令牌机制
    // std::shared_ptr<std::atomic_flag> _complete_flag{nullptr};
    _base_promise()noexcept
    {
        #ifndef NDEBUG
        type=&typeid(_base_promise);
        #endif
    }
};
struct _base_task
{
    //所有权关系
    std::coroutine_handle<> _handle{nullptr};
    _base_promise *_promise_ptr{nullptr};
    //观察非拥有
    std::coroutine_handle<> _in_active_handle{nullptr};
    _base_promise *_in_active_promise_ptr{nullptr};//两者共同合作处理一个协程帧
    //要使用_in_active_handle激活底部协程
    //要使用_in_active_handle修改_owner(在Task被移动时)
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
    _base_task(_base_task&&other)noexcept:_base_task{other._handle,other._promise_ptr}
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
    template<typename U>
    std::expected<U,Promise_error> get_value()noexcept
    {
        if( _in_active_handle)
        {
            assert(_in_active_promise_ptr->type==&typeid(_promise_type<U>));
            auto real_type_handle=std::coroutine_handle<_promise_type<U>>::from_address(
                _in_active_handle.address()
            );
            if(!real_type_handle.promise()._value._has_value)
                return std::unexpected(Promise_error::NO_value);
            real_type_handle.promise()._value._has_value=false;
            return std::move(real_type_handle.promise()._value._value);
        }
        return std::unexpected(Promise_error::Invalid_task);
    }

    const State_code_package& get_state()const noexcept
    {
        if(!_in_active_handle || !_in_active_promise_ptr)
            return Kernel_State[0];//协程已销毁肯定是发生了co_exit,因为正常结束是不会立刻销毁协程帧的
        return _in_active_promise_ptr->_state_code;
    }
    //代表可reume的状态
    bool valid()const noexcept
    {
        if(!_in_active_handle || _in_active_handle.done())
        //  相当于state_code=-1  相当于state_code=0（结束）
            return false;
        //依旧不可能获得2
        return true;
    }
    //代表不可resume中的执行结束，（另一种不可恢复是无效Task）
    bool done()const noexcept
    {
        if(_in_active_handle)
            return _in_active_handle.done();
        return false;
    }

    bool resume()
    {
        if(!_in_active_handle || _in_active_handle.done())
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
    // std::weak_ptr<std::atomic_flag> get_token()const noexcept;

    //接下来是awaitable成员
    //永远先触发父协程挂起，然后建立父子联系然后恢复子协程执行
    bool await_ready()const noexcept{return false;}

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

/*
在coroutine内部使用co_await suspend_state_code{value};挂起当前协程并设置状态码
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



template<typename T>
struct _promise_type:public _base_promise
{
    struct Optional_package{
        T _value{};
        bool _has_value{false};
    }_value;
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
    ~_promise_type(){} 

    suspend_state_code yield_value(const T &value)noexcept(
        noexcept(T(value)) &&
        noexcept(value=value)
    )//yield值之后应当到最顶层的resumer
    {
        _value._has_value=true;
        _value._value=value;
        return{3,true};
    }
    suspend_state_code yield_value(T &&value)noexcept(
        noexcept(T(std::move(value))) &&
        noexcept(value=std::move(value))
    )//yield值之后应当到最顶层的resumer
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
        _value._has_value=true;
        _value._value=value;
    }
    void return_value(T &&value)noexcept(
        noexcept(value=std::move(value)) &&
        noexcept(T(std::move(value)))
    )
    {
        _value._has_value=true;
        _value._value=std::move(value);
    }
};


template<typename T>
class Task:public _base_task
{
    template<std::invocable<Task<void>&,std::coroutine_handle<>>>friend class _swap_switch_to;
    template<typename U> friend class _promise_type; 
public:
    using promise_type=_promise_type<T>;
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

    //子协程里的异常将通过这个抛进父协程空间
    //因为发生异常时还是会调用final_suspend,所以如果子协程异常结束，按照设计的final_awaitable,还是会恢复父协程
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
    Task(std::coroutine_handle<promise_type> handle)noexcept;
    Task(Task&&other)noexcept=default;
    Task(const Task&)=delete;
    Task& operator=(Task&& other)noexcept=default;
    Task& operator=(const Task&)=delete;
    ~Task()=default;

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
    ~_promise_type(){}

    void return_void()const noexcept{}
};
Task<void>::Task(std::coroutine_handle<promise_type> handle)noexcept:_base_task{handle,&handle.promise()}
{
    #ifndef NDEBUG
    type=&typeid(Task);
    #endif
}


//定义顶层协程包装器
template class Task<void>;
using async=Task<void>;
template<typename T>
async start(Task<T> root)noexcept
{
    co_await root;
}
//用这个包装任意协程桟可保证顶协程类型一致，这能让switch_to更安全地运行
//黑魔法类，直接获取顶部task指针
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

//对称转移工具，co_await Task属于非对称转移
//_switch_to最好配合start包装器使用，因为_switch_to会假设原协程顶Task的类型和目标Task类型一样并static_cast
//如果顶部Task类型不是Task<void>，会导致UB
//这是类型擦除的_base_task本身的限制，目前并没有更好的解决方案，
//本库的目的是简洁高效和灵活，所以不会引入RTTI和虚函数等机制，类型安全部分交给程序员保证
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
                co_await swap_switch_to(task,1,Do_nothing);
            }
            经过这一步转移，导致test的生命周期由自己的局部变量task掌控，如果丢失其句柄会彻底造成内存泄漏
            而如果使用co_await swap_switch_to(task,1,Delete_origin);
            如果直接task._handle.destroy,会导致协程帧销毁->task析构->再destroy一次，之后发生什么就很难想象了

            所以要想删除原协程帧，必须先解除task的所有权，然后destroy原协程
            */
            auto _handle=_destination->_handle;
            _destination->_handle=nullptr;
            _destination->_in_active_handle=nullptr;
            _destination->_in_active_promise_ptr=nullptr;
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
