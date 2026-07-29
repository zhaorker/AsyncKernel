#include"async.hpp"

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