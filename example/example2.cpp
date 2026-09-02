/**
 *@file example2.cpp
 *@brief When_all和When_any的结构化协程并发
 *结构化并发借助When_all_readyable和When_any_readyable两个类实现，两者提供非阻塞接口ready()和get_result()
 */
#include<cstdio>
#include"../async.hpp"
#include<chrono>

//用unique_ptr包一层，能解决this悬垂的问题
std::vector<std::unique_ptr<async>> global_all_task{};

async async_task(std::chrono::seconds spendtime)
{
    printf("task start\n");
    auto end=std::chrono::steady_clock::now()+spendtime;
    while(std::chrono::steady_clock::now()<end)
        co_await std::suspend_always{};
    printf("task finish\n");
}
Task<int> async_get_int(std::chrono::seconds spendtime)
{
    printf("task start\n");
    auto end=std::chrono::steady_clock::now()+spendtime;
    while(std::chrono::steady_clock::now()<end)
        co_await std::suspend_always{};
    printf("task finish\n");
    co_return 17;
}
async main_task()
{
    using namespace std::chrono_literals;
    async task1=async_task(1s),task2=async_task(2s);
    Task<int> task3=async_get_int(3s);
    //模拟尝试对一个非voidTask获取两次观察令牌，其中一次会失败
    When_all_readyable waiter(task1,task2,task3,task3);
    global_all_task.emplace_back(new async(std::move(task1)));
    global_all_task.emplace_back(new async(std::move(task2)));
    //用start包一层也可以有统一类型的效果，并且观察令牌和返回值不会丢失
    global_all_task.emplace_back(new async(start(std::move(task3))));
    while(!waiter.ready())
    {
        co_await std::suspend_always{};
    }
    //get_result返回的tuple用结构化绑定解构
    auto [r1,r2,r3,r4]=waiter.get_result();
    auto check=[](auto &expected_result)
    {
        if constexpr(std::is_same_v<std::decay_t<decltype(expected_result)>,std::expected<void,When_error>>)
        {
            switch(expected_result.error()){
            case When_error::VOID_TASK:printf("void task hasn't return value\n");break;
            case When_error::NOT_YET:printf("not yet\n");break;
            case When_error::INVALID_HANDLE:printf("can't get valid token\n");break;
            }
        }else{
            if(expected_result)
                printf("get_result %d\n",*expected_result);
            else
            switch(expected_result.error()){
            case When_error::NO_VALUE:printf("task don't return value\n");break;
            case When_error::NOT_YET:printf("not yet\n");break;
            case When_error::INVALID_HANDLE:printf("can't get valid token\n");break;
            }
        }
    };
    check(r1);
    check(r2);
    check(r3);
    check(r4);
}
int main()
{
    global_all_task.emplace_back(new async(main_task()));
    while(!global_all_task.empty())
    {
        //for遍历采用不会失效的下标遍历
        for(int index=0;index<global_all_task.size();++index)
        {
            global_all_task[index]->resume();
        }
        for(auto it=global_all_task.begin();it!=global_all_task.end();)
        {
            if((**it).done())
            {
                it=global_all_task.erase(it);
            }else
            {
                ++it;
            }
        }
    }
}