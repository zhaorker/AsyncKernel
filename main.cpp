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
            std::coroutine_handle<_promise_type> _father_handle;//这个应当是结束的协程自身还是其父协程呢？
            //回应：应当传父亲的句柄，尽管可能为空
            bool await_ready()noexcept
            {
                //所以永远先挂起？
                return false;//留住这个协程帧，指不定还有用
            }
            //如果没有父亲，
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> self/*
                这个await发生在子协程内部的结束之时，所以拿到的是这个协程的句柄，现在，父子句柄都有了*/
            )noexcept
            {
                //如果执行这个函数要处理有没有父亲，注意，没有父亲的话不能直接销毁，等着Task先把值取出来
                //等着Task析构再destroy
                if(_father_handle)//父句柄是存在的
                {
                    //转移前先重设_in_active_handle,但这
                    return _father_handle;//对称转移到父协程并resume他
                }
                return std::noop_coroutine();//那就是挂起当前协程咯，总归需要一个人来destroy
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
            return {_parent};
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
        std::optional<int> _value{std::nullopt};
    };


    using promise_type=_promise_type;
    Task(std::coroutine_handle<promise_type> handle):_handle(handle)
    {
        _in_active_handle=handle;
        _handle.promise()._owner=this;
    }
    Task(Task&&other)noexcept:_handle(other._handle),_in_active_handle(other._in_active_handle)
    {
        other._handle=nullptr;
        other._in_active_handle=nullptr;
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
        return *this;
    }
    std::optional<int> get_return()noexcept
    {
        if(_in_active_handle)
            return _in_active_handle.promise()._value;
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
    void/*将父协程挂起，继续向上传递挂起*/ await_suspend(std::coroutine_handle<promise_type> _father)noexcept
    {
        auto father_handle=_father.promise()._owner;/*获得其持有者的指针*/
        father_handle->_in_active_handle=this->_in_active_handle;
        _handle.promise()._parent=_father;
    }
    std::optional<int> await_resume()noexcept
    {
        return _handle.promise()._value;
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
    co_return 0;
}

Task coroutine()
{
    std::cout<<"part one\n";
    co_await std::suspend_always();
    std::cout<<"part two\n";
    co_await inner();
    std::cout<<"part three\n";
    co_return 10;
}
int main() {
    auto task=coroutine();
    std::cout<<"main one\n";

    task.resume();
    std::cout<<"main two\n";
    task.resume();
    std::cout<<"main three\n";
}