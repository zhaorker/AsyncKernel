#include <coroutine>
#include <exception>
#include <utility>
#include <iostream>
#include <cassert>



class Task
{
public:
    struct _promise_type
    {
        Task get_return_object()noexcept
        {
            return  std::coroutine_handle<_promise_type>::from_promise(*this);
        }
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
            std::coroutine_handle<> await_suspend(std::coroutine_handle<_promise_type> self) noexcept {
                auto &father_handle=self.promise()._parent;
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
                    auto *top_task_ptr=self.promise()._owner;
                    /*self是正在结束中的子协程，它有顶层task的指针*/
                    top_task_ptr->_in_active_handle=father_handle;//转移活动协程
                    return father_handle;//不逗留，对称转移立刻resume父协程
                }
                return std::noop_coroutine();
            }
            void await_resume()noexcept{}//都final_waiter了怎么还会resume呢
        };
        ~_promise_type()
        {
            std::cout<<"一个协程帧被销毁了\n";
        }  
        //当这个被调用，程序一定co_return了，那么return了之后该：
        //1.是顶层的协程，直接返回resumer
        //2.不是顶层协程，返回父
        final_awaitable final_suspend()noexcept
        {
            return {};
        }


        std::suspend_always yield_value(int value)noexcept//yield值之后当然是回到最顶层的resumer啦
        {
            _value=value;
            return{};
        }
        void return_value(int value)noexcept{ _value=value; }
        void unhandled_exception()noexcept{}
        std::coroutine_handle<_promise_type> _parent{nullptr};
        Task *_owner;//谁拥有着这个协程帧？
        //还是说并不是拥有语义，而是：谁是整个“调用栈”的顶层句柄
        //就是顶层句柄，这样能把回退的复杂度将为O(1)
        std::optional<int> _value{std::nullopt};
    };


    using promise_type=_promise_type;
    Task(std::coroutine_handle<promise_type> handle):_handle(handle)
    {
        _in_active_handle=handle;
        _handle.promise()._owner=this;//注意，刚创建时是这样的
    }
    void coro_repoint()noexcept
    {
        if(!_handle) return;
        //先修改直属协程
        _handle.promise()._owner=this;
        // 然后从底往上依次纠正
        auto coro_handle=_in_active_handle;
        while(coro_handle && coro_handle.promise()._parent)//这还是个子协程，有父协程
        {
            coro_handle.promise()._owner=this;
            coro_handle=coro_handle.promise()._parent;//向上迭代一层
        }//循环不会触碰到直属协程
    }
    Task(Task&&other)noexcept:_handle(other._handle),_in_active_handle(other._in_active_handle)
    {
        other._handle=nullptr;
        other._in_active_handle=nullptr;
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
        other._handle=other._in_active_handle=nullptr;
        coro_repoint();
        return *this;
    }
    std::optional<int> get_return()noexcept
    {
        if(_in_active_handle)
        {
            auto result=_in_active_handle.promise()._value;
            _in_active_handle.promise()._value=std::nullopt;
            return result;
        }
        return std::nullopt;
    }
    void resume()noexcept
    {
        if(_in_active_handle && !_in_active_handle.done())
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
    void await_suspend(std::coroutine_handle<promise_type> _father) noexcept {
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
        _handle.promise()._parent = _father;
        auto* top_Task_ptr=_handle.promise()._owner=_father.promise()._owner;//全部统一指向最顶层句柄
        //并且通过_owner赋值链一直传递到底部
        top_Task_ptr->_in_active_handle=_handle;//修改活跃协程为本协程
    }
    std::optional<int> await_resume()noexcept
    {
        return _handle.promise()._value;//返回的是结束的子协程的co_return值
    }
private:
    std::coroutine_handle<promise_type> _handle;//你只管理你亲儿子的命
    std::coroutine_handle<promise_type> _in_active_handle;//更像个weak,不管理生命周期
};


Task inner()
{
    std::cout<<"inner one\n";
    co_await std::suspend_always();
    std::cout<<"inner two\n";
    co_yield 3;
    std::cout<<"inner three\n";
    co_return 10;
}
Task top()
{
    std::cout<<"top one\n";
    std::optional<int> value=co_await inner();
    std::cout<<"top two\n";
    co_await std::suspend_always();
    std::cout<<"top three\n";
    co_return value.value_or(-1);
}
int main() {
    auto task=top();
    std::cout<<"main one "<<task.get_return().value_or(-1)<<'\n';
    task.resume();
    std::cout<<"main two "<<task.get_return().value_or(-1)<<'\n';
    task.resume();
    std::cout<<"main three "<<task.get_return().value_or(-1)<<'\n';
    task.resume();
    std::cout<<"main four "<<task.get_return().value_or(-1)<<'\n';
}