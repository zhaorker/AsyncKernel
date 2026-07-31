#pragma once

#include <coroutine>
#include<optional>
#include<functional>
#include<new>


struct _base_promise;
template<typename T>
struct _promise_type;//这行，非常关键，不然编译报大量错

//触发子协程结束时，父协程状态一定是2,自身则一定是1,所以需要设置父协程状态为运行时（1）
struct final_awaitable//final_suspend返回的awaiter的作用是让子协程结束后直接跳转到父协程继续执行
{
    bool await_ready()noexcept{return false;}
    //作为即将结束的类型T协程，它收到的self就是自身T类型的句柄
    template<typename T>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<T>>)noexcept;
    void await_resume()noexcept{}//根本不会被调用
};

struct _base_task
{
    std::coroutine_handle<> _in_active_handle{nullptr};
    _base_promise *_in_active_promise_ptr{nullptr};//两者共同合作处理一个协程帧
    //要使用_in_active_handle激活底部协程
    //要使用_in_active_handle修改_owner(在Task被移动时)
    template<typename T>
    std::optional<T> get_value()noexcept;

    void coro_repoint()noexcept;

    int get_state()noexcept;
    bool valid()noexcept
    {
        if(!_in_active_handle || _in_active_handle.done())
        //  相当于state_code=-1  相当于state_code=0（结束）
            return false;
        //依旧不可能获得2
        return true;
    }
    //awaitable
    //永远先触发父协程挂起，然后建立父子联系然后恢复子协程执行
    bool await_ready()noexcept{return false;}
};

struct _base_promise
{
    std::coroutine_handle<> _parent_handle{nullptr};
    _base_promise *_parent_promise_ptr{nullptr};//同理

    _base_task* _owner{nullptr};

    //设置一个int字段，可供用户随意设值以表示当前“调用栈”状态，（内部向外部单向的信息传递）
    //-1,0,1,2,3,4被库本身限制使用，用户不应把自己的状态码设为{-1,0,1,2,3}
    /*
    -1表示调用栈已exit,不可再尝试恢复
    0整个协程桟自然执行结束（刚创建还未resume过的协程也是0）
    1可以说明不代表任何状态的挂起，比如co_await suspend_always
      协程还处在执行中时状态总是为1
    2说明该协程存在并正在等待子协程，用户态代码通常无法获取到此状态码
    3说明协程因为yield挂起，应当取值后再resume
    */
    int _state_code{0};

    std::suspend_always initial_suspend()noexcept{ return {}; }
    final_awaitable final_suspend()noexcept{return {};}

    //异常不好，所以不要在协程内抛异常
    void unhandled_exception()noexcept{}
};
int _base_task::get_state()noexcept
{
    if(!_in_active_handle || !_in_active_promise_ptr)
        return -1;//协程已销毁肯定是发生了co_exit,因为正常结束是不会立刻销毁协程帧的
    return _in_active_promise_ptr->_state_code;
}

/*
在coroutine内部使用co_await suspend_state_code{value};挂起当前协程并设置状态码
*/
class suspend_state_code
{
    int _value;
public:
    constexpr suspend_state_code(int value)noexcept:_value{value}{}
    bool await_ready()const noexcept{return false;}

    template<typename T>
    void await_suspend(std::coroutine_handle<_promise_type<T>>)const noexcept;

    void await_resume()const noexcept{}
};
static constexpr inline suspend_state_code co_exit{-1};


template<typename T>
class Task:public _base_task
{
    friend class _swap_switch_to;
public:
    Task()noexcept=default;
    template<typename U> friend class _promise_type; 
    using promise_type=_promise_type<T>;
    Task(std::coroutine_handle<promise_type>)noexcept;
    Task(Task&&other)noexcept:_handle(other._handle),_base_task(other)
    {
        other._handle=nullptr;
        other._in_active_handle=nullptr;
        other._in_active_promise_ptr=nullptr;
        coro_repoint();
    }
    Task(const Task&)=delete;
    Task& operator=(Task&& other)noexcept
    {
        if(&other==this)
            return *this;
        auto handle=_handle;

        _handle=other._handle;
        _in_active_handle=other._in_active_handle;
        _in_active_promise_ptr=other._in_active_promise_ptr;

        other._handle=nullptr;
        other._in_active_handle=nullptr;
        other._in_active_promise_ptr=nullptr;

        coro_repoint();
        if(handle)
            handle.destroy();
        return *this;
    }
    Task& operator=(const Task&)=delete;

    //现在要顾及特殊状态码-1和0
    //返回值含义是本次恢复执行是否成功了
    bool resume()noexcept
    {
        if(!_in_active_handle || _in_active_handle.done())//当done为真，只有可能是对直属协程的,那就意味着整个桟结束了
            return false;
        _in_active_promise_ptr->_state_code=1;//正在运行的协程状态必须是1
        _in_active_handle.resume();
        if(_in_active_promise_ptr->_state_code==-1)//内部使用了co_await co_exit;此时应当直接析构整个协程桟
        {
            _handle.destroy();//可以放心调用，毕竟如果父协程不在了，_in_active_handle一定是个失效的句柄
            _in_active_handle=_handle=nullptr;
            _in_active_promise_ptr=nullptr;
        }
        return true;
    }
    ~Task()
    {
        if(_handle)
        {
            _handle.destroy();
        }
    }

    template<typename Father_T>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept;

    std::optional<T> await_resume()noexcept;
private:
    std::coroutine_handle<promise_type> _handle;//所有权
};

template<typename T>
struct _promise_type:public _base_promise
{
    Task<T> get_return_object()noexcept
    {
        return  std::coroutine_handle<_promise_type>::from_promise(*this);
    }
    ~_promise_type()
    {
        if(_value)
            delete _value;
    } 

    suspend_state_code yield_value(T value)noexcept//yield值之后应当到最顶层的resumer
    {
        if(!_value)
            return {1};
        *_value=value;
        return{3};
    }
    void return_value(T value)noexcept{
        if(_value)
            *_value=value;
    }

    std::optional<T> *_value{new (std::nothrow) std::optional<T>{}};
};

template<typename T>
template<typename Father_T>
std::coroutine_handle<> Task<T>::await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept {
    if(!valid())//当前task持有的不是一个有效的协程桟（-1或者已经结束）
        return std::noop_coroutine();

    _handle.promise()._parent_handle = _father;
    _handle.promise()._parent_promise_ptr = &_father.promise();

    //只让底部对准_owner,对称转移的时候才纠正父协程的_owner
    auto* top_Task_ptr=_in_active_promise_ptr->_owner=_father.promise()._owner;
    
    //并且通过_owner赋值链一直传递到底部
    top_Task_ptr->_in_active_handle=_in_active_handle;//修改活跃协程为本桟的活跃协程
    top_Task_ptr->_in_active_promise_ptr=_in_active_promise_ptr;

    _father.promise()._state_code=2;
    _in_active_promise_ptr->_state_code=1;
    return _in_active_handle;
}
template<typename T>
std::optional<T> Task<T>::await_resume()noexcept
{
    return *(_handle.promise()._value);//返回的是结束的子协程的co_return值
}
template<typename T>
Task<T>::Task(std::coroutine_handle<promise_type> handle)noexcept:_handle{handle}
{
    _in_active_handle=handle;
    _handle.promise()._owner=this;
    _in_active_promise_ptr=&handle.promise();//指向实在的promise,用基类指针
}

/*=========================然后做一下void特化=========================*/
template<>
struct _promise_type<void>;
template<>
class Task<void>:public _base_task
{
    friend class _swap_switch_to;
public:
    Task()noexcept=default;
    template<typename U> friend class _promise_type; 
    using promise_type=_promise_type<void>;
    Task(std::coroutine_handle<promise_type>)noexcept;
    Task(Task&&other)noexcept:_handle(other._handle),_base_task(other)
    {
        other._handle=nullptr;
        other._in_active_handle=nullptr;
        other._in_active_promise_ptr=nullptr;
        coro_repoint();
    }
    Task(const Task&)=delete;
    Task& operator=(Task&& other)noexcept
    {
        if(&other==this)
            return *this;

        auto handle=_handle;

        _handle=other._handle;
        _in_active_handle=other._in_active_handle;
        _in_active_promise_ptr=other._in_active_promise_ptr;

        other._handle=nullptr;
        other._in_active_handle=nullptr;
        other._in_active_promise_ptr=nullptr;

        if(handle)
            handle.destroy();

        coro_repoint();
        return *this;
    }
    Task& operator=(const Task&)=delete;
    bool resume()noexcept
    {
        if(!_in_active_handle || _in_active_handle.done())
            return false;
        _in_active_promise_ptr->_state_code=1;
        _in_active_handle.resume();
        if(_in_active_promise_ptr->_state_code==-1)
        {
            _handle.destroy();
            _in_active_handle=_handle=nullptr;
            _in_active_promise_ptr=nullptr;
        }
        return true;
    }
    ~Task()
    {
        if(_handle)
        {
            _handle.destroy();
        }
    }

    template<typename Father_T>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept;

    void await_resume()noexcept{}
private:
    std::coroutine_handle<promise_type> _handle;
};
template<>
struct _promise_type<void>:public _base_promise
{
    Task<void> get_return_object()noexcept
    {
        return  std::coroutine_handle<_promise_type>::from_promise(*this);
    }
    ~_promise_type(){}

    void return_void()noexcept{}

    int*_value_unused{nullptr};//只是为了让所有_promise_type内存布局一样
};
template<typename Father_T>
std::coroutine_handle<> Task<void>::await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept {
    if(!valid())
        return std::noop_coroutine();
    _handle.promise()._parent_handle = _father;
    _handle.promise()._parent_promise_ptr = &_father.promise();

    auto* top_Task_ptr=_in_active_promise_ptr->_owner=_father.promise()._owner;
    
    top_Task_ptr->_in_active_handle=_in_active_handle;
    top_Task_ptr->_in_active_promise_ptr=_in_active_promise_ptr;

    _father.promise()._state_code=2;
    _in_active_promise_ptr->_state_code=1;
    return _in_active_handle;
}
Task<void>::Task(std::coroutine_handle<promise_type> handle)noexcept:_handle{handle}
{
    _in_active_handle=handle;
    _handle.promise()._owner=this;
    _in_active_promise_ptr=&handle.promise();
}

//_base_task的循环依赖让成员不得不在这里写实现
void _base_task::coro_repoint()noexcept
{
    _in_active_promise_ptr->_owner=this;
}
template<typename U>
std::optional<U> _base_task::get_value()noexcept
{
    if( _in_active_handle)
    {
        auto real_type_handle=std::coroutine_handle<_promise_type<U>>::from_address(
            _in_active_handle.address()
        );
        return *(real_type_handle.promise()._value);
    }
    return std::nullopt;
}
//补上最开始的final_awaitable实现，依赖promise_type具体成员
template<typename T>
std::coroutine_handle<> final_awaitable::await_suspend(std::coroutine_handle<_promise_type<T>> self)noexcept {
    auto &father_handle=self.promise()._parent_handle;
    if (father_handle) {
        auto *top_task_ptr=self.promise()._owner;
        /*self是正在结束中的子协程，它有顶层task的指针*/
        top_task_ptr->_in_active_handle=father_handle;//转移活动协程指向
        top_task_ptr->_in_active_promise_ptr=self.promise()._parent_promise_ptr;

        self.promise()._parent_promise_ptr->_owner=self.promise()._owner;//校准父协程的_owner,因为自己一定是准的（Task移动coro_repoint）

        self.promise()._parent_promise_ptr->_state_code=1;
        return father_handle;//不逗留，对称转移立刻resume父协程
    }
    self.promise()._state_code=0;
    return std::noop_coroutine();
}
//定义顶层协程包装器
template class Task<void>;
using async=Task<void>;
template<typename T>
async start(Task<T> &root)noexcept
{
    co_await root;
}
//用这个包装任意协程桟可保证顶协程类型一致，这能让switch_to更安全地运行

//补上suspend_state_code和switch_to的实现函数
template<typename T>
void suspend_state_code::await_suspend(std::coroutine_handle<_promise_type<T>> this_coroutine)const noexcept
{
    this_coroutine.promise()._state_code=_value;
}
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
//如果顶部Task类型不是Task<void>
//这是类型擦除的_base_task本身的限制，目前并没有更好的解决方案，
//本库的目的是简洁高效和灵活，所以不会引入RTTI和虚函数等机制，类型安全部分交给程序员保证
bool Do_nothing(Task<void>&,std::coroutine_handle<>)noexcept{return true;}
bool Delete_origin(Task<void>&,std::coroutine_handle<>)noexcept{return false;}
class _swap_switch_to
{
    friend _swap_switch_to swap_switch_to(std::coroutine_handle<>,int,
        const std::function<bool(Task<void>&,std::coroutine_handle<>)>&)noexcept;
    friend _swap_switch_to swap_switch_to(Task<void>&,int,
        const std::function<bool(Task<void>&,std::coroutine_handle<>)>&)noexcept;
    _swap_switch_to(Task<void> *des,int code,
        const std::function<bool(Task<void>&,std::coroutine_handle<>)>&hook)noexcept:
        _destination{des},_code{code},if_success{false},_hook{std::move(hook)}{}
    //要考虑一种传noop_coroutine的情况
    _swap_switch_to(std::coroutine_handle<>des,int code,
        const std::function<bool(Task<void>&,std::coroutine_handle<>)>&hook)noexcept:
        _destination{nullptr},_code{code},if_success{false},_hook{std::move(hook)}
    {
        if(!des || des==std::noop_coroutine() || des.done())
            return;
        std::coroutine_handle<_promise_type<void>> _des=std::coroutine_handle<_promise_type<void>>::from_address(
            des.address()
        );
        //只允许底协程恢复，中途恢复执行只会引发UB
        if(_des.promise()._state_code==2)
            return;
        _destination=static_cast<Task<void>*>(_des.promise()._owner);
    }
public:
    _swap_switch_to(const _swap_switch_to&)=delete;
    _swap_switch_to& operator=(const _swap_switch_to&)=delete;
    _swap_switch_to(_swap_switch_to&&other)noexcept:
        _destination{other._destination},_code{other._code},if_success{false},_hook{std::move(other._hook)}
    {other._destination=nullptr;}
    _swap_switch_to& operator=(_swap_switch_to&&)=delete;
    bool await_ready()noexcept{return false;}
    template<typename U>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<U>> _origin)noexcept
    {
        if(!_destination || !_destination->valid())
            return std::noop_coroutine();
        Task<void> *origin_task=static_cast<Task<void>*>(_origin.promise()._owner);
        std::swap(*origin_task,*_destination);//移动赋值自动处理了所有子协程帧的_owner
        if_success=true;
        if(!_hook(*_destination,_origin))
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
        }
        origin_task->_in_active_promise_ptr->_state_code=1;
        return origin_task->_in_active_handle;//其实现在_origin_task已经指向整个destination桟了
        /*注意这会直接改变整个Task的指向，所以如果转移到的目标桟发生了co_exit并不会丢失-1码
          因为在Task::resume内部_handle_resume结束后，再检查_in_active_promise_ptr->_state_code正是destination协程上的-1
          对应的协程会按预期销毁*/
    }
    bool await_resume()noexcept{return if_success;}
private:
    Task<void> *_destination;
    int _code;
    bool if_success;
    //给用户一个钩子可以操作切换后的原协程
    std::function<bool(Task<void>&,std::coroutine_handle<>)> _hook;
    //返回false意味着“抛弃”原协程调用栈，await_suspend内部会将原协程销毁，目标协程的原主Task会被置空；
};
_swap_switch_to swap_switch_to(Task<void> &des,int code=1,
        const std::function<bool(Task<void>&,std::coroutine_handle<>)>&hook=Do_nothing)noexcept
{
    return {&des,code,hook};
}
_swap_switch_to swap_switch_to(std::coroutine_handle<>des,int code=1,
        const std::function<bool(Task<void>&,std::coroutine_handle<>)>&hook=Do_nothing)noexcept
{
    return {des,code,hook};
}