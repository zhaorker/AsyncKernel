#include <coroutine>
#include<optional>


struct _base_promise;
template<typename T>
struct _promise_type;//这行，非常关键，不然编译报大量错

//触发子协程结束时，父协程状态一定是2,自身则一定是1,所以需要设置父协程状态为运行时（1）
struct final_awaitable//final_suspend返回的awaiter的作用是让子协程结束后直接跳转到父协程继续执行
{
    // std::coroutine_handle<_promise_type> _father_handle;//这个应当是结束的协程自身还是其父协程呢？
    //回应：应当传父亲的句柄，尽管可能为空
    //但是现在不需要了
    bool await_ready()const noexcept
    {
        //所以永远先挂起？
        return false;//留住这个协程帧，指不定还有用
    }
    //作为即将结束的类型T协程，它收到的self就是自身T类型的句柄
    template<typename T>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<T>>)const  noexcept;
    void await_resume()const noexcept{}//都final_waiter了怎么还会resume呢
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

    int get_state()const noexcept;

    //awaitable
    bool await_ready()const noexcept{return false;}

    bool valid()const noexcept
    {
        if(!_in_active_handle || _in_active_handle.done())
        //  相当于state_code=-1  相当于state_code=0（结束）
            return false;
        //依旧不可能获得2
        return true;
    }
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

    //当这个被调用，程序一定co_return了，那么return了之后该：
    //1.是顶层的协程，直接返回resumer
    //2.不是顶层协程，返回父
    final_awaitable final_suspend()noexcept{return {};}
};
int _base_task::get_state()const noexcept
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
public:
    template<typename U> friend class _promise_type; 
    using promise_type=_promise_type<T>;
    Task(std::coroutine_handle<promise_type>)noexcept;
    Task(Task&&other)noexcept:_handle(other._handle),_base_task(other/*可以隐式转换上去吧*/)
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
        //先要确保原先的协程不泄漏
        if(_handle)
        {
            _handle.destroy();
            // _handle=nullptr;
        }
        _handle=other._handle;
        _in_active_handle=other._in_active_handle;
        _in_active_promise_ptr=other._in_active_promise_ptr;
        other._handle=nullptr;
        other._in_active_handle=nullptr;
        other._in_active_promise_ptr=nullptr;
        coro_repoint();
        return *this;
    }
    Task& operator&&(const Task&)=delete;

    //现在要顾及特殊状态码-1和1
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
            //然后通过Task析构链全数销毁协程帧，不过这要求内部全部RAII
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
    std::coroutine_handle<promise_type> _handle;//你只管理你亲儿子的命
    //这个需要成为泛型句柄
    // std::coroutine_handle</*_promise_type*/> _in_active_handle;//更像个weak,不管理生命周期
};
//外界如何使用Task？
//首先Task互相之间不存在操作空间，不需要为之特地泛型化
// _promise_type会使用Task的：
    //Task*解引用其_in_active_handle成员

//外界如何使用_promise_type?
//首先promise_type之间会形成父子协程关系——用_parent
    //使用_parent时会用到_parent自身的赋值，operatorbool,
//然后Task持有的_in_active_handle会在get_value时试图访问promise_type的_value成员
template<typename T>
struct _promise_type:public _base_promise
{
    Task<T> get_return_object()noexcept
    {
        return  std::coroutine_handle<_promise_type>::from_promise(*this);
    }
    ~_promise_type(){} 

    suspend_state_code yield_value(T value)noexcept//yield值之后当然是回到最顶层的resumer啦
    {
        _value=value;
        return{3};
    }
    void return_value(T value)noexcept{ _value=value; }
    //我希望用户有noexcept的自觉
    void unhandled_exception()noexcept{}
    // std::coroutine_handle</*_promise_type不使用_parent.promise()，不需要类型*/> _parent{nullptr};
    // _base_task *_owner;//谁拥有着这个协程帧？
    //还是说并不是拥有语义，而是：谁是整个“调用栈”的顶层句柄
    //就是顶层句柄，这样能把回退的复杂度将为O(1)

    std::optional<T> _value{std::nullopt};
};

template<typename T>
template<typename Father_T>
std::coroutine_handle<> Task<T>::await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept {
    if(!valid())//当前task持有的不是一个有效的协程桟（-1或者已经结束）
        return std::noop_coroutine();
    //接下来的情况分为2是个有儿子的协程桟；0是个刚刚建立还没有开始执行的单协层程；134正常的挂起状态的单层协程
    // // 先更新直接父 Task
    // auto* father_task = _father.promise()._owner;
    // if (father_task)
    //     father_task->_in_active_handle = this->_in_active_handle;

    // // 沿 _parent 链向上更新所有祖先 Task 的 _in_active_handle
    // auto ancestor_handle = _father.promise()._parent;
    // while (ancestor_handle) {
    //     auto* ancestor_task = ancestor_handle.promise()._owner;
    //     if (ancestor_task)
    //         ancestor_task->_in_active_handle = this->_in_active_handle;
    //     ancestor_handle = ancestor_handle.promise()._parent;
    // }
    // 设置当前子协程的 _parent
    _handle.promise()._parent_handle = _father;
    _handle.promise()._parent_promise_ptr = &_father.promise();

    auto* top_Task_ptr=_handle.promise()._owner=_father.promise()._owner;//全部统一指向最顶层句柄
    for(_base_promise *cur_ptr=_in_active_promise_ptr;
        cur_ptr!=static_cast<_base_promise*>(&_handle.promise());
        cur_ptr=cur_ptr->_parent_promise_ptr)
        cur_ptr->_owner=top_Task_ptr;
    
    //并且通过_owner赋值链一直传递到底部
    top_Task_ptr->_in_active_handle=_in_active_handle;//修改活跃协程为本协程
    top_Task_ptr->_in_active_promise_ptr=_in_active_promise_ptr;

    _father.promise()._state_code=2;
    return _in_active_handle;
}
template<typename T>
std::optional<T> Task<T>::await_resume()noexcept
{
    return _handle.promise()._value;//返回的是结束的子协程的co_return值
}
template<typename T>
Task<T>::Task(std::coroutine_handle<promise_type> handle)noexcept:_handle{handle}
{
    _in_active_handle=handle;
    _handle.promise()._owner=this;//注意，刚创建时是这样的
    _in_active_promise_ptr=&handle.promise();//指向实在的promise,用基类指针
}

/*=========================然后做一下void特化=========================*/
template<>
class Task<void>:public _base_task
{
public:
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
        if(_handle)
        {
            _handle.destroy();
        }
        _handle=other._handle;
        _in_active_handle=other._in_active_handle;
        _in_active_promise_ptr=other._in_active_promise_ptr;
        other._handle=nullptr;
        other._in_active_handle=nullptr;
        other._in_active_promise_ptr=nullptr;
        coro_repoint();
        return *this;
    }
    Task& operator&&(const Task&)=delete;
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
    void unhandled_exception()noexcept{}
};
template<typename Father_T>
std::coroutine_handle<> Task<void>::await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept {
    if(!valid())
        return std::noop_coroutine();
    _handle.promise()._parent_handle = _father;
    _handle.promise()._parent_promise_ptr = &_father.promise();

    auto* top_Task_ptr=_handle.promise()._owner=_father.promise()._owner;//全部统一指向最顶层句柄
    for(_base_promise *cur_ptr=_in_active_promise_ptr;
        cur_ptr!=static_cast<_base_promise*>(&_handle.promise());
        cur_ptr=cur_ptr->_parent_promise_ptr)
        cur_ptr->_owner=top_Task_ptr;
    
    //并且通过_owner赋值链一直传递到底部
    top_Task_ptr->_in_active_handle=_in_active_handle;//修改活跃协程为本协程
    top_Task_ptr->_in_active_promise_ptr=_in_active_promise_ptr;

    _father.promise()._state_code=2;
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
    for(_base_promise* cur_ptr=_in_active_promise_ptr;
        cur_ptr!=nullptr;cur_ptr=cur_ptr->_parent_promise_ptr)
        cur_ptr->_owner=this;
}
template<typename U>
std::optional<U> _base_task::get_value()noexcept
{
    if( _in_active_handle)
    {
        auto real_type_handle=std::coroutine_handle<_promise_type<U>>::from_address(
            _in_active_handle.address()
        );
        auto result=real_type_handle.promise()._value;
        real_type_handle.promise()._value=std::nullopt;
        return result;
    }
    return std::nullopt;
}
//补上最开始的final_awaitable实现，依赖promise_type具体成员
template<typename T>
std::coroutine_handle<> final_awaitable::await_suspend(std::coroutine_handle<_promise_type<T>> self)const noexcept {
    auto &father_handle=self.promise()._parent_handle;
    if (father_handle) {
        // auto* father = _father_handle.promise()._owner;
        // // 回退直接父 Task
        // if (father)
        //     father->_in_active_handle = _father_handle;
        // // 再向上回退所有祖先
        // auto ancestor = _father_handle.promise()._parent;
        // while (ancestor) {
        //     auto* anc_task = ancestor.promise()._owner;
        //     if (anc_task)
        //         anc_task->_in_active_handle = _father_handle;
        //     ancestor = ancestor.promise()._parent;
        // }
        auto *top_task_ptr=self.promise()._owner;//owner需要泛型化
        /*self是正在结束中的子协程，它有顶层task的指针*/
        top_task_ptr->_in_active_handle=father_handle;//转移活动协程
        top_task_ptr->_in_active_promise_ptr=self.promise()._parent_promise_ptr;

        self.promise()._parent_promise_ptr->_state_code=1;
        return father_handle;//不逗留，对称转移立刻resume父协程
    }
    self.promise()._state_code=0;
    return std::noop_coroutine();
}
//定义顶层协程包装器
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
//如果顶部Task类型不同将导致UB
//这是类型擦除的_base_task本身的限制，目前并没有更好的解决方案，
//本库的目的是简洁高效和泛用，所以不会引入RTTI和虚函数等机制，类型安全部分交给程序员保证
template<typename T>
class _switch_to
{
public:
    _switch_to(Task<T> *des,int code):_destination{des},_code{code},if_success{false}{}
    bool await_ready()noexcept{return false;}
    template<typename U>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type<U>> _origin)noexcept
    {
        Task<T> *origin_task=static_cast<Task<T>*>(_origin.promise()._owner);
        std::swap(*origin_task,*_destination);//移动赋值自动处理了所有子协程帧的_owner
        if(!origin_task->valid())
            return std::noop_coroutine();
        if_success=true;
        _origin.promise()._state_code=_code;
        return origin_task->_in_active_handle;//其实现在_origin_task已经指向整个destination桟了
        /*注意这会直接改变整个Task的指向，所以如果转移到的目标桟发生了co_exit并不会丢失-1码
          因为在Task::resume内部_handle_resume结束后，再检查_in_active_promise_ptr->_state_code正是destination协程上的-1
          对应的协程会按预期销毁*/
    }
    bool await_resume()noexcept{return if_success;}
private:
    Task<T> *_destination;
    int _code;
    bool if_success;
};
template<typename T>
_switch_to<T> swap_switch_to(Task<T> &destination,int code=1)noexcept
{
    return {&destination,code};
}

#include <iostream>
#include<deque>


Task<int> child(int q)
{
    if(q!=0)
    {
        co_await std::suspend_always{};
        co_await child(q-1);
    }
    co_return 10;
}

Task<int> function()
{
    std::cout<<"one\n";
    co_await std::suspend_always();
    std::cout<<"two\n";
    auto a=co_await child(3);
    std::cout<<"three\n";
    co_await std::suspend_always();
    std::cout<<"four\n";
    co_return a.value_or(-1);
}

int main() {
    auto task=function(); 
    while(task.resume())
        std::cout<<"main "<<task.get_value<int>().value_or(-1)<<'\n';
}