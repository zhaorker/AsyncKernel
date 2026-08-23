#pragma once 

#include<thread>
#include<mutex>
#include<shared_mutex>
#include<condition_variable>
#include<semaphore>
#include<stop_token>
#include<future>
#include<atomic>

#include<unordered_map>
#include<deque>
#include<vector>

#include<utility>
#include<optional>
#include<functional>
#include<cstdint>
#include<limits>
#include<chrono>
#include<new>

#include"third_depend/concurrentqueue.h"

class lock_free_mutex
{
public:
    void lock()noexcept
    {
        while(flag.test_and_set(std::memory_order_acquire))std::this_thread::yield();
    }
    void unlock()noexcept{flag.clear(std::memory_order_release);}
private:
    std::atomic_flag flag{};
};

class Free_thread_pool;
class Dynamic_thread_pool;

struct Accept_any
{
    template<typename ...T>
    Accept_any(T&& ...v)noexcept{}
};

template<typename Of_what>
struct Token_type{using Type = void;};
template<>
struct Token_type<Free_thread_pool>{using Type=moodycamel::ProducerToken;};
template<>
struct Token_type<Dynamic_thread_pool>{using Type=Accept_any;};

template<class Pool>
class Channel
{
    friend class Free_thread_pool;
    friend class Dynamic_thread_pool;
public:
    Channel(Pool *owner)noexcept:_owner{owner},_pro_token{owner->_task_queue}{}
    struct Context
    {
        std::uint64_t tid;
        void *any_handle;
        Channel *destination;
    };
    template<typename T>
    std::optional<std::future<T>> commit_task(std::packaged_task<T()> &&task,Context &context)noexcept
    {
        try{
            auto future=task.get_future();//thw only where maybe throw
            context.destination=this;
            std::move_only_function<void()> _start_func=std::move([task=std::move(task)]mutable{task();});
            std::unique_ptr<Task_package> element(new(std::nothrow)Task_package(
                std::move(_start_func),
                context
            ));
            if(!element)
                return std::nullopt;
            if(_owner->_commit_task(&_pro_token,std::move(element)))
                return future;
            return std::nullopt;
        }catch(...){
            return std::nullopt;
        }
    }
    std::vector<Context> get_complete()
    {
        std::vector<Context> result;
        result.reserve(256);
        Context item;
        while (_complete_queue.try_dequeue(item)) {
            result.push_back(std::move(item));
        }
        return result;
    }
private:
    struct Task_package
    {
        std::move_only_function<void()> _start_func;
        Context _context_handle;
    };
    moodycamel::ConcurrentQueue<Context> _complete_queue;
    void _receive_complete(const Context &context)noexcept
    {
        _complete_queue.enqueue(context);
    }
    Pool *_owner;

    using Pro_token=typename Token_type<Pool>::Type;
    Pro_token _pro_token;
};

class Free_thread_pool
{
    friend class Channel<Free_thread_pool>;
public:
    Free_thread_pool(unsigned threads_count,unsigned queue_waterline):
        _task_queue{1024},
        _semaphore{0},
        _queue_size{0},
        _queue_waterline{queue_waterline},
        _pool_threads{}//maybe throw
    {
        try
        {    for(unsigned i=0;i<threads_count;++i)
            {
                _pool_threads.emplace_back(thread_func,&_task_queue,&_semaphore,&_queue_size);
            }
        }catch(...)
        {
            _pool_threads.clear();
            _valid=false;
            return;
        }
        _valid=true;
    }
    bool valid()const noexcept{return _valid;}
    Channel<Free_thread_pool> get_channel()noexcept{return this;}
    using Context=Channel<Free_thread_pool>::Context;
private:
    bool _valid;

    using Task_package=Channel<Free_thread_pool>::Task_package;
    using Task_queue=moodycamel::ConcurrentQueue<std::unique_ptr<Task_package>>;

    Task_queue _task_queue;
    std::counting_semaphore<std::numeric_limits<int>::max()> _semaphore;
    std::atomic<unsigned> _queue_size;
    const unsigned _queue_waterline;
    
    std::vector<std::jthread> _pool_threads;

    bool _commit_task(moodycamel::ProducerToken *protoken,std::unique_ptr<Task_package> &&element)noexcept
    {
        unsigned current_size=_queue_size.load(std::memory_order_relaxed);
        if(current_size>_queue_waterline)
            return false;
        if(!_task_queue.enqueue(*protoken,std::move(element)))
            return false;
        _queue_size.fetch_add(1,std::memory_order_relaxed);
        _semaphore.release();
        return true;
    }
    static inline void thread_func(
        std::stop_token stop_token,
        Task_queue *task_queue,
        std::counting_semaphore<std::numeric_limits<int>::max()>*semaphore,
        std::atomic<unsigned> *queue_size
    )noexcept{
        moodycamel::ConsumerToken contoken{*task_queue};
        while(!stop_token.stop_requested() || queue_size->load(std::memory_order_relaxed)!=0)
        {
            if(!semaphore->try_acquire_for(std::chrono::milliseconds(10)))
                continue;
            std::unique_ptr<Task_package> element;
            if(!task_queue->try_dequeue(contoken,element))
            {
                continue;
            }
            queue_size->fetch_sub(1,std::memory_order_relaxed);
            element->_start_func();
            element->_context_handle.destination->_receive_complete(element->_context_handle);
        }
    }
};

class Dynamic_thread_pool
{
    friend class Channel<Dynamic_thread_pool>;
public:
    using Context=Channel<Dynamic_thread_pool>::Context;
    enum class Overflow_strategy{Discard,Blocking};
    Dynamic_thread_pool(std::size_t coro_size,std::size_t max_size,std::size_t capacity,float waterline,Overflow_strategy strategy,std::chrono::milliseconds threads_exit_timeout):
        _coro_threads{coro_size},_max_threads{max_size},
        _pool_threads{},
        _threads_exit_timeout{threads_exit_timeout},
        _thread_exited{max_size,max_size,1},
        _queue_capacity{capacity},_queue_waterline{waterline},_overflow_strategy{strategy},
        _queue_blank{capacity},
        _task_queue{}//maybe throw
    {
        try{
            for(unsigned i=0;i<_coro_threads;++i)
            {
                std::jthread will_emplace{thread_func,&_task_queue,&_queue_mutex,&_con_var,&_thread_exited,true,_threads_exit_timeout,&_queue_blank};
                _pool_threads.emplace(std::make_pair(will_emplace.get_id(),std::move(will_emplace)));
            }
        }catch(...)
        {
            _pool_threads.clear();
            _valid=false;
            return;
        }
        _valid=true;
    }
    bool valid()const noexcept{return _valid;}
    Channel<Dynamic_thread_pool> get_channel()noexcept{return this;}
    ~Dynamic_thread_pool(){
        _pool_threads.clear();
    }
private:
    using Task_package=Channel<Dynamic_thread_pool>::Task_package;
    using Task_queue=std::deque<std::unique_ptr<Task_package>>;

    const std::size_t _coro_threads;
    const std::size_t _max_threads;
    std::unordered_map<std::jthread::id,std::jthread> _pool_threads;
    const std::chrono::milliseconds _threads_exit_timeout;
    moodycamel::ConcurrentQueue<std::jthread::id> _thread_exited;

    const std::size_t _queue_capacity;
    const float _queue_waterline;//(std::size_t)(_queue_capacity*_queue_waterline)向下取整
    const Overflow_strategy _overflow_strategy;
    std::counting_semaphore<std::numeric_limits<int>::max()> _queue_blank;
    Task_queue _task_queue;
    std::mutex _queue_mutex;
    std::condition_variable _con_var;
    
    bool _valid;

    bool _commit_task([[maybe_unused]]Accept_any *unused,std::unique_ptr<Task_package> &&element)
    {
        std::unique_lock<std::mutex> guard{_queue_mutex};
        std::jthread::id will_be_remove{};
        while(_thread_exited.try_dequeue(will_be_remove))
        {
            _pool_threads.erase(will_be_remove);
        }
        if(
            _task_queue.size()>=static_cast<std::size_t>(_queue_capacity*_queue_waterline) &&
            _pool_threads.size()<_max_threads
        ){
            std::jthread will_emplace{thread_func,&_task_queue,&_queue_mutex,&_con_var,&_thread_exited,false,_threads_exit_timeout,&_queue_blank};
            _pool_threads.emplace(std::make_pair(will_emplace.get_id(),std::move(will_emplace)));
        }
        if(_task_queue.size()<_queue_capacity || _overflow_strategy==Overflow_strategy::Blocking)
        {
            guard.unlock();
            _queue_blank.acquire();
            guard.lock();
            _task_queue.push_back(std::move(element));
            guard.unlock();
            _con_var.notify_one();
            return true;
        }
        return false;
    }

    static inline void thread_func(
        std::stop_token stop_token,
        Task_queue *task_queue,
        std::mutex *queue_mutex,std::condition_variable *con_var,
        moodycamel::ConcurrentQueue<std::jthread::id> *exit_thread,
        bool am_coro_thread,std::chrono::milliseconds exit_timeout,
        std::counting_semaphore<std::numeric_limits<int>::max()> *queue_blank
    ){
        std::unique_ptr<Task_package> element{};
        moodycamel::ProducerToken protoken{*exit_thread};
        while(true)
        {
            auto start_wait=std::chrono::steady_clock::now();
            {
                std::unique_lock<std::mutex> guard{*queue_mutex};
                while(task_queue->empty())
                {
                    if(stop_token.stop_requested())
                    {
                        return;
                    }
                    if(!am_coro_thread && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-start_wait)>exit_timeout)
                    {
                        exit_thread->enqueue(protoken,std::this_thread::get_id());
                        return;
                    }
                    con_var->wait_for(guard,std::chrono::milliseconds(10));
                }
                element=std::move(task_queue->front());
                task_queue->pop_front();
                queue_blank->release();
            }
            element->_start_func();
            element->_context_handle.destination->_receive_complete(element->_context_handle);
        }
    }
};