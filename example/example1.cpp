/**
 *@file example1.cpp
 *@brief 演示协程的基本用法
 *
 *Task分为有返回值和无返回值特化，本文件介绍所有Task的用户态接口
 */

#include"../async.hpp"
#include<cstdio>
//嵌套协程，与co_await同步获取返回值
struct LOGGER
{
    int a;
    LOGGER(int b):a{b}{printf("create %d\n",a);}
    ~LOGGER(){printf("destroy %d\n",a);}
};
Task<int> get_int(int value)
{
    co_await std::suspend_always{};
    co_return value*value;
}
async test1(int deep)
{
    auto result=co_await get_int(deep);
    printf("level %d get value %d\n",deep,*result);
    if(deep==0)
        co_return;
    co_await test1(deep-1);
}

//异常传播与强杀调用链
async floor1()
{
    throw std::runtime_error("jock");
}
async floor2()
{
    LOGGER a(2);
    co_await floor1();
}
async floor3()
{
    LOGGER a(3);
    try{
        co_await floor2();
    }catch(std::runtime_error e)
    {
        printf("catch error :%s\n",e.what());
    }
    co_await co_exit;
    printf("won't run\n");
}
async test2()
{
    LOGGER a(4);
    co_await floor3();
    printf("won't run too\n");
}

//乒乓球对称转移
//全局版
async global_handle;//默认构造空句柄
async pingpang()
{
    int count=0;
    while(count<5)
    {
        printf("loop %d\n",count++);
        co_await swap_switch_to(global_handle,Do_nothing);
    }
}
//内部局部版
async test4()
{
    async handle;
    auto func=[&]()->async
    {
        int count=0;
        while(count<5)
        {
            printf("inner loop %d\n",count++);
            co_await swap_switch_to(handle,Do_nothing);
        }
    };
    handle=func();
    int count=0;
    while(count<5)
    {
        printf("out loop %d\n",count++);
        co_await swap_switch_to(handle,Do_nothing);
    }
}

//自杀式对称转移
async origin()
{
    auto func=[]()->async
    {
        co_await std::suspend_always{};
        printf("world\n");
        co_return;
    };
    LOGGER a(1);
    printf("hello ");
    co_await swap_switch_to(func(),Delete_origin);
}
int main()
{
    printf("test1——测试嵌套协程与同步获取返回值\n");
    {
        auto handle=test1(5);
        do{printf("resume ");}while(handle.resume());
    }
    printf("test2——测试异常传播和强杀调用栈\n");
    {
        auto handle=test2();
        do{printf("resume ");}while(handle.resume());
    }
    printf("test3——乒乓球对称转移\n");
    {
        global_handle=pingpang();
        auto handle=pingpang();
        handle.resume();
    }
    printf("test4——内部乒乓\n");
    {
        auto handle=test4();
        handle.resume();
    }
    printf("test5——自杀式对称转移\n");
    {
        auto handle=origin();
        printf("resume ");
        handle.resume();
        printf("resume ");
        handle.resume();
    }
}