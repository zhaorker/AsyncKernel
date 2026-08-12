#pragma once

#include"async.hpp"
#include<deque>
#include<mutex>
#include<thread>
#include<functional>

//暂时用有锁，以后再改无锁
template<typename T>
class Pipe
{
public:
    Pipe():_mutex{},_queue{},_co_va{}{}
    void commit(T &&value)
    {
        std::unique_lock<std::mutex> guard{_mutex};
        _queue.push_back(std::move(value));
        _co_va.notify_one();
    }
    std::optional<T> get_one_nonblock()
    {
        std::unique_lock<std::mutex> guard{_mutex};
        if(_queue.empty())
            return std::nullopt;
        T return_object=std::move(_queue.front());
        _queue.pop_front();
        return return_object;
    }
    T wait_one_block()
    {
        std::unique_lock<std::mutex> guard{_mutex};
        if(_queue.empty())
        {
            _co_va.wait(guard,[this](){return !_queue.empty();});
        }
        T return_object=std::move(_queue.front());
        _queue.pop_front();
        return return_object;
    }
private:
    std::mutex _mutex;
    std::deque<T> _queue;
    std::condition_variable _co_va;
};

template<typename Context_t>
struct Coroutine_with_context
{
    using function_t=std::function<async(Context_t&)>;
    async handle;
    Context_t context;

    Coroutine_with_context(function_t &&func,Context_t &&_context)noexcept:
        context{std::move(_context)},
        _function{std::move(func)},
        handle{}
    {}
    Coroutine_with_context(const Coroutine_with_context&)=delete;
    Coroutine_with_context& operator=(const Coroutine_with_context&)=delete;
    Coroutine_with_context(Coroutine_with_context&&)noexcept=default;
    Coroutine_with_context& operator=(Coroutine_with_context&&)noexcept=default;

    void start()noexcept{handle=_function(context);}
private:
    function_t _function;
};
template<>
struct Coroutine_with_context<void>
{
    using function_t=std::function<async()>;
    async handle;

    Coroutine_with_context(function_t &&func)noexcept:
        _function{std::move(func)},
        handle{}
    {}
    Coroutine_with_context(const Coroutine_with_context&)=delete;
    Coroutine_with_context& operator=(const Coroutine_with_context&)=delete;
    Coroutine_with_context(Coroutine_with_context&&)noexcept=default;
    Coroutine_with_context& operator=(Coroutine_with_context&&)noexcept=default;

    void start()noexcept{handle=_function();}
private:
    function_t _function;
};


template<typename T>
struct Thread_pipe
{
    template<std::invocable<std::stop_token,Pipe<T>&> F>
    Thread_pipe(F&&thread_func):
        _pipe{},
        _thread(start_lambda,std::forward<F>(thread_func),&_pipe)
    {}

    //一个事件循环不应当以任何形式发生转移
    Thread_pipe(const Thread_pipe&)=delete;
    Thread_pipe& operator=(const Thread_pipe&)=delete;
    Thread_pipe(Thread_pipe&&)=delete;
    Thread_pipe& operator=(Thread_pipe&&)=delete;
    ~Thread_pipe()
    {
        request_stop();
    }

    void commit(T &&element)
    {
        _pipe.commit(std::forward<T>(element));
    }
    void request_stop()noexcept
    {
        _thread.request_stop();
    }
    auto get_stop_source()noexcept
    {
        return _thread.get_stop_source();
    }
    auto get_stop_token()noexcept
    {
        return _thread.get_stop_token();
    }
private:
    Pipe<T> _pipe;
    static constexpr inline auto start_lambda=[]<std::invocable<std::stop_token,Pipe<T>&> F>
        (std::stop_token stop_token,F&&func,Pipe<T>*_pipe)
    {
        func(stop_token,*_pipe);
    };
    std::jthread _thread;
};

template<typename T,int Thread_Count>
class Distributor
{
public:
    template<std::invocable<std::stop_token,Pipe <Coroutine_with_context<T>> &> F>
    Distributor(F&&thread_func):
        _loops{reinterpret_cast<Thread_pipe<Coroutine_with_context<T>>*>(_construct_buff)},
        _iterator{0}
    {
        for(int index=0;index<Thread_Count;++index)
        {
            try{
                new(_loops+index)Thread_pipe<Coroutine_with_context<T>>(std::forward<F>(thread_func));
            }catch(...)
            {
                while(--_index>=0)
                {
                    (_loops+index)->~Thread_pipe<Coroutine_with_context<T>>();
                }
                return;
            }
        }
    }
    ~Distributor()
    {
        for(int index=0;index<Thread_Count;++index)
        {
            (_loops+index)->~Thread_pipe<Coroutine_with_context<T>>();
        }
    }
    //非void特有
    template<typename ...argt>
    void add_coroutine(Coroutine_with_context<T>::function_t func,argt&& ...argv)
        requires (!std::is_same_v<void,T> && std::constructible_from<T, argt...>)
    {
        T context{std::forward<argt>(argv)...};
        Coroutine_with_context<T> add_element{std::move(func),std::move(context)};
        _loops[_iterator++].commit(std::move(add_element));
        _iterator%=Thread_Count;
    }
    void add_coroutine(Coroutine_with_context<T>::function_t func,T &&context)
        requires (!std::is_same_v<void,T>)
    {
        Coroutine_with_context<T> add_element{std::move(func),std::move(context)};
        _loops[_iterator++].commit(std::move(add_element));
        _iterator%=Thread_Count;
    }
private:
    alignas(Thread_pipe<Coroutine_with_context<T>>)
    std::byte _construct_buff[Thread_Count*sizeof(Thread_pipe<Coroutine_with_context<T>>)];
    Thread_pipe<Coroutine_with_context<T>>*_loops;
    int _iterator;
};
template<int Thread_Count>
class Distributor<void,Thread_Count>
{
public:
    template<std::invocable<std::stop_token,Pipe <Coroutine_with_context<void>> &> F>
    Distributor(F&&thread_func):
        _loops{reinterpret_cast<Thread_pipe<Coroutine_with_context<void>>*>(_construct_buff)},
        _iterator{0}
    {
        for(int index=0;index<Thread_Count;++index)
        {
            try{
                new(_loops+index)Thread_pipe<Coroutine_with_context<void>>(std::forward<F>(thread_func));
            }catch(...)
            {
                whiel(--index>=0)
                {
                    (_loops+index)->~Thread_pipe<Coroutine_with_context<void>>();
                }
                return;
            }
        }
    }
    ~Distributor()
    {
        for(int index=0;index<Thread_Count;++index)
        {
            (_loops+index)->~Thread_pipe<Coroutine_with_context<void>>();
        }
    }
    void add_coroutine(Coroutine_with_context<void>::function_t func)
    {
        Coroutine_with_context<void> add_element{std::move(func)};
        _loops[_iterator++].commit(std::move(add_element));
        _iterator%=Thread_Count;
    }
private:
    alignas(Thread_pipe<Coroutine_with_context<void>>)
    std::byte _construct_buff[Thread_Count*sizeof(Thread_pipe<Coroutine_with_context<void>>)];
    Thread_pipe<Coroutine_with_context<void>>*_loops;
    int _iterator;
};