#include"threadpool.hpp"
#include<cstdio>

int main()
{
    Free_thread_pool a{4,100};
    auto channel=a.get_channel();
    int va=10;
    auto future=channel.commit_task({0},std::packaged_task<int(int&)>{
            [](int& a){
                std::this_thread::sleep_for(std::chrono::seconds(3));
                printf("%llu\n",&a);
                return a;}

        },va);
    if(!future)
        return 0;
    va=20;
   printf("%llu : %d\n",&va,future->get()); 
}
