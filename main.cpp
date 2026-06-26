#include <coroutine>
#include<optional>


struct _base_promise;
template<typename T>
struct _promise_type;//这行，非常关键，不然编译报大量错

struct _base_task
{
    std::coroutine_handle<> _in_active_handle{nullptr};
    _base_promise *_in_active_promise_ptr{nullptr};//两者共同合作处理一个协程帧
    //要使用_in_active_handle激活底部协程
    //要使用_in_active_handle修改_owner(在Task被移动时)
};

struct _base_promise
{
    std::coroutine_handle<> _parent_handle{nullptr};
    _base_promise *_parent_promise_ptr{nullptr};//同理

    _base_task* _owner{nullptr};
};

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
    void coro_repoint()noexcept;
    
    template<typename U=T>
    std::optional<U> get_return()noexcept;

    void resume()noexcept
    {
        if(_in_active_handle && _in_active_promise_ptr && !_in_active_handle.done())
            _in_active_handle.resume();
    }
    ~Task()
    {
        if(_handle)
        {
            _handle.destroy();
        }
    }

    //接下来让Task自身可以被co_await
    bool await_ready()noexcept
    {
        //要是子协程直接co_return了，那应该不需要挂起
        if(_handle)
            return _handle.done();//协程帧永远不会自动销毁，所以done是安全的
        //那要是没有子协程了呢？
        return false;//先写个false看看
    }
    template<typename Father_T>
    void await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept;

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
//然后Task持有的_in_active_handle会在get_return时试图访问promise_type的_value成员
template<typename T>
struct _promise_type:public _base_promise
{
    Task<T> get_return_object()noexcept
    {
        return  std::coroutine_handle<_promise_type>::from_promise(*this);
    }
    ~_promise_type(){}  
    std::suspend_never initial_suspend()noexcept{ return {}; }


    struct final_awaitable//final_suspend返回的awaiter的作用是让子协程结束后直接跳转到父协程继续执行
    {
        // std::coroutine_handle<_promise_type> _father_handle;//这个应当是结束的协程自身还是其父协程呢？
        //回应：应当传父亲的句柄，尽管可能为空
        //但是现在不需要了
        bool await_ready()noexcept
        {
            //所以永远先挂起？
            return false;//留住这个协程帧，指不定还有用
        }
        //作为即将结束的类型T协程，它收到的self就是自身T类型的句柄
        std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type> self) noexcept {
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
                return father_handle;//不逗留，对称转移立刻resume父协程
            }
            return std::noop_coroutine();
        }
        void await_resume()noexcept{}//都final_waiter了怎么还会resume呢
    };
    //当这个被调用，程序一定co_return了，那么return了之后该：
    //1.是顶层的协程，直接返回resumer
    //2.不是顶层协程，返回父
    final_awaitable final_suspend()noexcept
    {
        return {};
    }
    std::suspend_always yield_value(T value)noexcept//yield值之后当然是回到最顶层的resumer啦
    {
        _value=value;
        return{};
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
void Task<T>::coro_repoint()noexcept
{
    if(!_handle) return;
    //先修改直属协程
    _handle.promise()._owner=this;
    // 然后从底往上依次纠正
    _base_promise *cur_ptr=_in_active_promise_ptr;
    while(cur_ptr && cur_ptr->_parent_promise_ptr && cur_ptr->_parent_handle)//这还是个子协程，有父协程
    {
        cur_ptr->_owner=this;
        cur_ptr=cur_ptr->_parent_promise_ptr;//向上迭代一层
    }//循环不会触碰到直属协程
}
template<typename T>
template<typename U>
std::optional<U> Task<T>::get_return()noexcept
{
    if(_in_active_promise_ptr && _in_active_handle)
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
//这个成员一会儿要单独模板化
template<typename T>
template<typename Father_T>
void Task<T>::await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept {
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
    //并且通过_owner赋值链一直传递到底部
    top_Task_ptr->_in_active_handle=_handle;//修改活跃协程为本协程
    top_Task_ptr->_in_active_promise_ptr=&_handle.promise();
}
template<typename T>
std::optional<T> Task<T>::await_resume()noexcept
{
    return _handle.promise()._value;//返回的是结束的子协程的co_return值
}
template<typename T>
Task<T>::Task(std::coroutine_handle<promise_type> handle)noexcept:_handle(handle)
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

    void coro_repoint()noexcept;

    template<typename U>
    std::optional<U> get_return()noexcept;

    void resume()noexcept
    {
        if(_in_active_handle && _in_active_promise_ptr && !_in_active_handle.done())
            _in_active_handle.resume();
    }
    ~Task()
    {
        if(_handle)
        {
            _handle.destroy();
        }
    }

    bool await_ready()noexcept
    {
        if(_handle)
            return _handle.done();
        return false;
    }
    template<typename Father_T>
    void await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept;

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
    std::suspend_never initial_suspend()noexcept{ return {}; }


    struct final_awaitable
    {
        bool await_ready()noexcept
        {
            return false;
        }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type> self) noexcept {
            auto &father_handle=self.promise()._parent_handle;
            if (father_handle) {
                auto *top_task_ptr=self.promise()._owner;
                top_task_ptr->_in_active_handle=father_handle;
                top_task_ptr->_in_active_promise_ptr=self.promise()._parent_promise_ptr;
                return father_handle;
            }
            return std::noop_coroutine();
        }
        void await_resume()noexcept{}//都final_waiter了怎么还会resume
    };
    final_awaitable final_suspend()noexcept
    {
        return {};
    }
    // std::suspend_always yield_value(T value)noexcept
    // {
    //     _value=value;
    //     return{};
    // }
    void return_void()noexcept{}
    void unhandled_exception()noexcept{}
};
void Task<void>::coro_repoint()noexcept
{
    if(!_handle) return;
    _handle.promise()._owner=this;

    _base_promise *cur_ptr=_in_active_promise_ptr;
    while(cur_ptr && cur_ptr->_parent_promise_ptr && cur_ptr->_parent_handle)
    {
        cur_ptr->_owner=this;
        cur_ptr=cur_ptr->_parent_promise_ptr;
    }
}
template<typename U>
std::optional<U> Task<void>::get_return()noexcept
{
    if(_in_active_promise_ptr && _in_active_handle)
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
template<typename Father_T>
void Task<void>::await_suspend(std::coroutine_handle<_promise_type<Father_T>> _father) noexcept {
    _handle.promise()._parent_handle = _father;
    _handle.promise()._parent_promise_ptr = &_father.promise();

    auto* top_Task_ptr=_handle.promise()._owner=_father.promise()._owner;
    top_Task_ptr->_in_active_handle=_handle;
    top_Task_ptr->_in_active_promise_ptr=&_handle.promise();
}
Task<void>::Task(std::coroutine_handle<promise_type> handle)noexcept:_handle(handle)
{
    _in_active_handle=handle;
    _handle.promise()._owner=this;
    _in_active_promise_ptr=&handle.promise();
}

using async=Task<void>;

#include <iostream>

async inner()
{
    std::cout<<"inner one\n";
    co_await std::suspend_always();
    std::cout<<"inner two\n";
    co_return;
}
Task<int> top()
{
    std::cout<<"top one\n";
    co_await inner();
    std::cout<<"top two\n";
    co_await std::suspend_always();
    std::cout<<"top three\n";
    co_return 10;
}
int main() {
    auto task=top();
    task.resume();
    std::cout<<"main two "<<task.get_return().value_or(-1)<<'\n';
    task.resume();
    std::cout<<"main three "<<task.get_return().value_or(-1)<<'\n';

}