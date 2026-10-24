/**
 * @file threadpool.hpp.hpp
 * @brief AsyncKernel 协程框架附带线程池头文件。
 * SPDX-License-Identifier: GPL-3.0-or-later
 * SPDX-FileCopyrightText: Copyright (c) 2026 zhaorker kevin081006@outlook.com
 */

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
#include<memory>

#include<utility>
#include<optional>
#include<functional>
#include<cstdint>
#include<limits>
#include<chrono>
#include<new>

#include"third_party/concurrentqueue/concurrentqueue.h"


class Free_thread_pool;
class Dynamic_thread_pool;

/**
 *@brief 通用channel类，用于向线程池提交任务并获取完成通知
 *@tparam Poll指这是哪类线程池的Channel
 *@warning Channel本身非线程安全，不要跨线程使用Channel
 */
template<class Pool>
class Channel
{
    friend class Free_thread_pool;
    friend class Dynamic_thread_pool;
    Channel(Pool *owner)noexcept:_owner{owner},_SC_queue{}{}
public:
    struct Context
    {
        std::uint64_t tid;
        void *any_handle{nullptr};
    };
    /**
     *@brief 提交一个任务
     *@param 一个包装任务的packaged_task,和一个任务上下文
     *@tparam 提交的任务的返回值T
     *@return std::optional<std::future<T>>
     *@note 若因某些原因提交任务失败，会返回nullopt
     */
    template<typename T,typename ... Argt>
    std::optional<std::future<T>> commit_task(const Context &context,
            std::packaged_task<T(Argt&...)> &&task,Argt &...argv)noexcept
    //关于为什么所有参数必须全是引用，如果用万能引用，就没有办法安全捕获进lambda——引用会decay掉，地址变了。因此不得不改成全引用参数 
    {
        try{
            auto future=task.get_future();//the where maybe throw
            std::move_only_function<void()> _start_func=std::move(
                [task=std::move(task),&argv...]mutable{
                    task(argv...);
                }
            );
            std::shared_ptr<Task_package> element(new(std::nothrow)Task_package(
                std::move(_start_func),
                context,
                false
            ));
            if(!element)
                return std::nullopt;
            if(_owner->_commit_task(element))
            {
                try{
                    _SC_queue.push_back(std::move(element));
                }catch(...){
                    return std::nullopt;
                }
                return future;
            }
            return std::nullopt;
        }catch(...){
            return std::nullopt;
        }
    }
    /**
     *@brief 获取已完成的任务的上下文
     *@return 所有已完成的任务的上下文的vector
     */
    std::vector<Context> get_complete()
    {
        std::vector<Context> result;
        result.reserve(256);
        while (!_SC_queue.empty() && _SC_queue.front()->_finish.load(std::memory_order_acquire)) {
            try{
                result.push_back(std::move(_SC_queue.front()->_context_handle));
            }catch(...)
            {
                return result;
            }
            _SC_queue.pop_front();
        }
        return result;
    }
    /**
     *@brief 取消任务
     *@param uint64_t tid
     *取消所有上下文任务号为tid的任务
     */
    void give_up_tid(std::uint64_t tid)
    {
        std::erase_if(_SC_queue,[&](std::shared_ptr<Task_package> &element)noexcept{return element->_context_handle.tid==tid;});
    }
    /**
     *@brief 取消所有任务 */
    void give_up_all()
    {
        _SC_queue.clear();
    }
private:
    struct Task_package
    {
        std::move_only_function<void()> _start_func;
        Context _context_handle;
        std::atomic<bool> _finish;
    };
    std::deque<std::shared_ptr<Task_package>> _SC_queue;
    Pool *_owner;
};

/**
 *@brief 无锁线程池
 *内部用无锁队列储存任务，池线程数不动，适合纯计算类任务 */
class Free_thread_pool
{
    friend class Channel<Free_thread_pool>;
public:
    /**
     *@brief 无锁线程池的构造函数
     *@param threads_count要开的线程数和queue_waterline任务队列的最大水位
     *@note 内部对queue_waterline使用模糊判断，即任务队列长度可能会偶尔超过水位线
     */
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
    /**
     *@brief 该函数获取该线程池对应的Channel
     *@return Channel<Free_thread_pool>
     */
    Channel<Free_thread_pool> get_channel()noexcept{return this;}
    using Context=Channel<Free_thread_pool>::Context;
private:
    bool _valid;

    using Task_package=Channel<Free_thread_pool>::Task_package;
    using Task_queue=moodycamel::ConcurrentQueue<std::weak_ptr<Task_package>>;

    Task_queue _task_queue;
    std::counting_semaphore<std::numeric_limits<int>::max()> _semaphore;
    std::atomic<unsigned> _queue_size;
    const unsigned _queue_waterline;
    
    std::vector<std::jthread> _pool_threads;

    bool _commit_task(std::weak_ptr<Task_package> &&element)noexcept
    {
        unsigned current_size=_queue_size.load(std::memory_order_relaxed);
        if(current_size>_queue_waterline)
            return false;
        if(!_task_queue.enqueue(std::move(element)))
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
            std::weak_ptr<Task_package> element;
            if(!task_queue->try_dequeue(contoken,element))
            {
                continue;
            }
            queue_size->fetch_sub(1,std::memory_order_relaxed);
            auto shared=element.lock();
            if(!shared)
            {
                element.reset();
                continue;
            }
            shared->_start_func();
            shared->_finish.store(true,std::memory_order_release);
            element.reset();
        }
    }
};

/**
 @brief 有锁动态线程池
 *用互斥锁实现并发，支持复杂的线程池扩容策略，更多用于io
 */
class Dynamic_thread_pool
{
    friend class Channel<Dynamic_thread_pool>;
public:
    using Context = Channel<Dynamic_thread_pool>::Context;
    /**
     *@brief 有锁线程池提交任务时队列满的策略
     *Discard即忽略任务，Channel::commit_task会返回nullopt
     *Blocking即阻塞直到队列留出空位，该选项让提交任务一定成功
     */
    enum class Overflow_strategy { Discard, Blocking };

    /**
     *@brief 动态线程池构造函数
     *@param 核心线程数，最大线程数，队列上限，触发开新线程的队列水位线，任务队列满时的策略，临时线程销毁的空闲超时
     */
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
    using Task_queue = std::deque<std::weak_ptr<Task_package>>;

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

    bool _commit_task(std::weak_ptr<Task_package> &&element)
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
        std::weak_ptr<Task_package> element;

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

            auto shared=element.lock();
            if(!shared)
            {
                element.reset();
                continue;
            }
            shared->_start_func();
            shared->_finish.store(true,std::memory_order_release);
            element.reset();
        }
    }
};
