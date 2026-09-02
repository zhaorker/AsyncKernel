/**
 *@file example3.cpp
 *@brief 将README里的echo服务器改成用线程池做耗时任务的版本 
 *线程池的接口较为简单，获取channel,用channel查询完成情况，或者用channel取消任务
 */

#include "../async.hpp"
#include"../threadpool.hpp"
#include<chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <map>

Free_thread_pool pool{5,10};//5池，队列水位线10个元素

// 异步读取，采用非阻塞io轮训，主动让出执行权
Task<std::string> async_read(int fd, size_t len) {
    std::string buf(len, '\0');
    while (true) {
        ssize_t n = recv(fd, buf.data(), len, MSG_DONTWAIT);
        if (n > 0) {
            buf.resize(n);
            co_return std::move(buf);
        } else if (n == 0) {
            co_return std::string{};  // 对端关闭
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // IO未就绪：挂起协程，等待下次 resume 重试
            co_await std::suspend_always{};
        } else {
            co_return std::string{};
        }
    }
}

// 异步写入（同样是非阻塞轮询）
Task<void> async_write(int fd, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent,
                         MSG_DONTWAIT | MSG_NOSIGNAL);
        if (n > 0) {
            sent += n;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            co_await std::suspend_always{};
        } else {
            co_return;
        }
    }
    co_return;
}

// 异步等待,但这里改成轮训线程池是否完成
Task<void> async_wait(int ms) {
    //模拟耗时任务等待数秒
    std::packaged_task<int()> task{[=](){std::this_thread::sleep_for(std::chrono::milliseconds(ms));return 17;}};
    auto channel=pool.get_channel();
    auto future=channel.commit_task(std::move(task));/*默认tid为0*/
    while(future)
    {
        co_await std::suspend_always{};
        auto complete=channel.get_complete();
        if(complete.empty())
            continue;
        printf("task complete return value %d\n",future->get());
        break;
    }
}

//一个完整客户连接的处理流程
Task<void> handle_client(int fd) {
    while (true) {
        auto data = co_await async_read(fd, 1024);
        if (!data) break;
        if (data->empty()) break;
        co_await async_wait(1000);
        co_await async_write(fd, *data);
    }
    close(fd);
    co_return;
}

int main() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) return 1;
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFL) | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8888);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, 128);

    std::cout << "Echo server running on port 8888\n";

    std::map<int, async> tasks;
    pollfd pfd{listen_fd, POLLIN, 0};

  //纯轮训的主循环
    while (true) {
        int ret = poll(&pfd, 1, 10);  // 10ms 超时，兼顾响应性与CPU占用
        if (ret < 0) break;

        if (pfd.revents & POLLIN) {
            int client_fd = accept(listen_fd, nullptr, nullptr);
            if (client_fd >= 0) {
                fcntl(client_fd, F_SETFL, fcntl(client_fd, F_GETFL) | O_NONBLOCK);
                tasks[client_fd] = handle_client(client_fd);
                std::cout << "[+] New client: " << client_fd << "\n";
            }
        }

        for (auto it = tasks.begin(); it != tasks.end(); ) {
            async& task = it->second;
            bool resumed = task.resume();
            if (!resumed || task.done()) {
                int fd = it->first;
                close(fd);
                it = tasks.erase(it);
                std::cout << "[-] Client closed: " << fd << "\n";
            } else {
                ++it;
            }
        }
    }

    for (auto& [fd, task] : tasks) close(fd);
    close(listen_fd);
    return 0;
}