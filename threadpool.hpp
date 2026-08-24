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
    Channel(Pool *owner)noexcept:_owner{owner},_pro_token{owner->_task_queue},_complete_queue{new (std::nothrow) moodycamel::ConcurrentQueue<Context>}{}
    struct Context
    {
        std::uint64_t tid;
        void *any_handle;
        std::weak_ptr<moodycamel::ConcurrentQueue<Context>>destination;
    };
    template<typename T>
    std::optional<std::future<T>> commit_task(std::packaged_task<T()> &&task,Context &context)noexcept
    {
        if(!_complete_queue)
            return std::nullopt;
        try{
            auto future=task.get_future();//thw only where maybe throw
            context.destination=_complete_queue;
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
        if(!_complete_queue)
            return {};
        std::vector<Context> result;
        result.reserve(256);
        Context item;
        while (_complete_queue->try_dequeue(item)) {
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
    std::shared_ptr<moodycamel::ConcurrentQueue<Context>> _complete_queue;
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
            auto destination=element->_context_handle.destination.lock();
            if(!destination)
            {
                element.reset();
                continue;
            }
            element->_start_func();
            destination->enqueue(element->_context_handle);
            element.reset();
        }
    }
};

class Dynamic_thread_pool
{
    friend class Channel<Dynamic_thread_pool>;
public:
    using Context = Channel<Dynamic_thread_pool>::Context;
    enum class Overflow_strategy { Discard, Blocking };

    Dynamic_thread_pool(
        std::size_t coro_size,
        std::size_t max_size,
        std::size_t capacity,
        float waterline,
        Overflow_strategy strategy,
        std::chrono::milliseconds threads_exit_timeout)
        :
        _coro_threads{coro_size},
        _max_threads{max_size},
        _threads_exit_timeout{threads_exit_timeout},
        _queue_capacity{capacity},
        _queue_waterline{waterline},
        _overflow_strategy{strategy},
        _queue_blank{capacity},
        _task_queue{},
        _queue_mutex{},
        _con_var{},
        _thread_exited{},
        _valid{false}
    {
        if (coro_size == 0 || max_size == 0 || capacity == 0 ||
            coro_size > max_size ||
            waterline < 0.0f || waterline > 1.0f)
        {
            _valid = false;
            return;
        }

        try {
            for (std::size_t i = 0; i < coro_size; ++i) {
                std::jthread t{
                    thread_func,
                    &_task_queue,
                    &_queue_mutex,
                    &_con_var,
                    &_thread_exited,
                    true,
                    _threads_exit_timeout,
                    &_queue_blank
                };
                _pool_threads.emplace(t.get_id(), std::move(t));
            }
        }
        catch (...) {
            _pool_threads.clear();
            _valid = false;
            return;
        }
        _valid = true;
    }

    ~Dynamic_thread_pool() {
        _pool_threads.clear();
    }

    bool valid() const noexcept { return _valid; }

    Channel<Dynamic_thread_pool> get_channel() noexcept { return this; }

private:
    using Task_package = Channel<Dynamic_thread_pool>::Task_package;
    using Task_queue = std::deque<std::unique_ptr<Task_package>>;

    const std::size_t _coro_threads;
    const std::size_t _max_threads;
    const std::chrono::milliseconds _threads_exit_timeout;
    const std::size_t _queue_capacity;
    const float _queue_waterline;
    const Overflow_strategy _overflow_strategy;

    std::counting_semaphore<std::numeric_limits<int>::max()> _queue_blank;
    Task_queue _task_queue;
    std::mutex _queue_mutex;
    std::condition_variable_any _con_var;
    moodycamel::ConcurrentQueue<std::jthread::id> _thread_exited;

    std::unordered_map<std::jthread::id, std::jthread> _pool_threads;

    bool _valid;

    bool _commit_task([[maybe_unused]] Accept_any *unused,
                      std::unique_ptr<Task_package> &&element)
    {
        std::jthread::id exited_id;
        while (_thread_exited.try_dequeue(exited_id)) {
            std::lock_guard<std::mutex> lg(_queue_mutex);
            _pool_threads.erase(exited_id);
        }
        bool need_expand = false;
        {
            std::lock_guard<std::mutex> lg(_queue_mutex);
            if (_task_queue.size() >= static_cast<std::size_t>(_queue_capacity * _queue_waterline) &&
                _pool_threads.size() < _max_threads)
            {
                need_expand = true;
            }
        }
        if (need_expand) {
            try {
                std::jthread t{
                    thread_func,
                    &_task_queue,
                    &_queue_mutex,
                    &_con_var,
                    &_thread_exited,
                    false,
                    _threads_exit_timeout,
                    &_queue_blank
                };
                std::lock_guard<std::mutex> lg(_queue_mutex);
                _pool_threads.emplace(t.get_id(), std::move(t));
            }
            catch (...) {}
        }
        if (_overflow_strategy == Overflow_strategy::Blocking) {
            _queue_blank.acquire();
            std::lock_guard<std::mutex> lg(_queue_mutex);
            try {
                _task_queue.push_back(std::move(element));
                _con_var.notify_one();
                return true;
            }
            catch (...) {
                _queue_blank.release();
                return false;
            }
        }
        else {
            if (!_queue_blank.try_acquire()) {
                return false;
            }
            std::lock_guard<std::mutex> lg(_queue_mutex);
            try {
                _task_queue.push_back(std::move(element));
                _con_var.notify_one();
                return true;
            }
            catch (...) {
                _queue_blank.release();
                return false;
            }
        }
    }

    static inline void thread_func(
        std::stop_token stop_token,
        Task_queue *task_queue,
        std::mutex *queue_mutex,
        std::condition_variable_any *con_var,
        moodycamel::ConcurrentQueue<std::jthread::id> *exit_thread,
        bool am_coro_thread,
        std::chrono::milliseconds exit_timeout,
        std::counting_semaphore<std::numeric_limits<int>::max()> *queue_blank)
    {
        moodycamel::ProducerToken protoken{*exit_thread};
        std::unique_ptr<Task_package> element;

        while (true) {
            std::unique_lock<std::mutex> guard{*queue_mutex};

            if (am_coro_thread) {
                con_var->wait(guard, stop_token, [&]{ return !task_queue->empty(); });
                if (stop_token.stop_requested() && task_queue->empty()) {
                    return;
                }
            }
            else {
                auto deadline = std::chrono::steady_clock::now() + exit_timeout;
                con_var->wait_until(guard, stop_token, deadline,
                                     [&]{ return !task_queue->empty(); });
                if (stop_token.stop_requested() && task_queue->empty()) {
                    return;
                }
                if (task_queue->empty() &&
                    std::chrono::steady_clock::now() >= deadline)
                {
                    exit_thread->enqueue(protoken, std::this_thread::get_id());
                    return;
                }
                if (task_queue->empty()) {
                    continue;
                }
            }

            element = std::move(task_queue->front());
            task_queue->pop_front();
            queue_blank->release();
            guard.unlock();

            auto destination=element->_context_handle.destination.lock();
            if(!destination)
            {
                element.reset();
                continue;
            }
            element->_start_func();
            destination->enqueue(element->_context_handle);
            element.reset();
        }
    }
};