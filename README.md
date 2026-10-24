# AsyncKernel library
**Write asynchronous code like synchronous code. Your scheduler, your rules.**

[![License](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue)](https://en.cppreference.com/w/cpp/23)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows%20%7C%20macOS-lightgrey)]()

这是一个基于C++20协程的**高性能异步内核原语**，旨在提供更简便且符合直觉的异步编程体验，并提升运行效率。  

## 为什么选择 AsyncKernel？

编写 C++ 异步代码时，你可能会面临以下困境：
- 使用 **原生协程**（`std::coroutine_handle`）→ 要手动管理协程帧的生命周期，极易内存泄漏或 use-after-free。
- 使用 **大型异步框架**（如 asio、cppcoro）→ 框架接管了线程模型和调度逻辑，**你的代码必须“适配框架”，而非框架服务于你的业务**。
- 使用 **回调 / future** → 回调地狱、异常传播困难、组合性差。

AsyncKernel 的目标是：**提供协程执行流控制的最小原语，把控制权完整地交还给你。**

### 核心理念

1. **控制权反转**  
   AsyncKernel 不做调度器，不规定线程模型，不要求你继承特定类型。它只提供一个核心类型 `Task<T>` —— 一个 RAII、Move-Only 的协程句柄。你可以：
   - 在单线程中串行驱动它（`while(task.resume())`）。
   - 丢到自己的线程池里并发驱动。
   - 用 `co_await` 自由嵌套，用 `co_yield` 向外传递状态。
   - 用 `swap_switch_to` 实现协程间的对称转移（执行权交换）。
   ———— **AsyncKernel不替你决策，你拥有全部控制权。**

2. **单一职责（Unix 哲学）**  
   AsyncKernel **只做好一件事**：管理协程的执行流和生命周期。它不提供：
   - 网络 I/O 封装（你自己用 asio/libcurl 等配合）。
   - 文件 I/O 封装。
   - 定时器 / 时钟。
   - 内置调度器。
   
   这些都可以**无缝接入**——你只需要把异步操作包装成 `Task<T>` 即可。

3. **接口简洁，符合直觉**  
   `Task<T>` 在概念上就是一个“可以暂停和恢复的函数”：
   - `resume()` — 恢复执行一步（或直到挂起/结束）。
   - `get_value()` — 获取 `co_return` / `co_yield` 的值（`std::expected<T>`）。
   - `get_state()` — 查询协程挂起时设置的状态码（用于进度汇报/轻量级通信）。
   - `done()` / `valid()` — 查询协程状态。

   你的异步代码看起来就像同步代码——`co_await` 子协程，`try-catch` 捕获跨协程异常。

4. **生命周期安全（RAII）**  
   `Task` 是 unique 语义的 RAII 句柄：
   - 析构时自动销毁底层协程帧（除非已被移动）。
   - 移动后原 `Task` 置空，新 `Task` 接管所有权。
   - **你不需要手动调用 `handle.destroy()`**，协程帧的生命周期由 `Task` 自动管理。

   这套模型消除了 C++20 原生协程最大的痛点——协程帧的悬挂引用和内存泄漏。

5. **零开销抽象（Zero-overhead）**  
   - 不使用虚函数（无 vtable 开销）。
   - 在 `NDEBUG` 模式下完全剥离 RTTI。
   - 协程帧内存布局紧凑，状态传递通过平凡结构体完成（无动态分配）。
   - 对称转移（`final_awaitable`）避免了嵌套协程的逐层返回开销，性能接近手写的状态机。

   你可以用 AsyncKernel 构建 **硬实时系统** 或 **高性能服务器**，而不必担心引入的额外开销。

### 与其他方案的对比

| 方案 | 生命周期管理 | 异常传播 | 调度器控制权 | 学习曲线 |
|------|-------------|----------|-------------|----------|
| 原生 `std::coroutine_handle` | 手动（易错） | 手动传播 | 完全控制 | 陡峭 |
| asio / cppcoro | 自动（但框架接管） | 支持 | 框架主导 | 陡峭 |
| **AsyncKernel** | 自动（RAII） | 自动传播 | **完全归你** | 平缓 |

AsyncKernel 是 **“最小可行协程抽象层”**——它给你足够多的工具，让你自由组装出任何你想要的异步架构，同时不强迫你接受任何预设的调度策略。

## 功能速览

- ✅ RAII 协程句柄 `Task<T>`（Move-Only，自动生命周期管理）
- ✅ `co_await`/`co_return`/`co_yield` 完整支持
- ✅ 嵌套协程与异常自动传播
- ✅ 对称转移（`swap_switch_to`）——协程间执行权交换
- ✅ 轻量状态码传递（`suspend_state_code` / `co_exit`）
- ✅ 结构化并发（`When_all_readyable` / `When_any_readyable`）
- ✅ 两种线程池（无锁固定线程池 + 有锁动态线程池）

---

## 环境要求
- **C++23 兼容编译器**：
  - GCC 11+（推荐 13+）
  - Clang 14+（推荐 17+）
  - MSVC 2022 17.5+
- **CMake 3.20+**（推荐 3.25+）
- **依赖库**：
  - [`moodycamel::ConcurrentQueue`](https://github.com/cameron314/concurrentqueue)（BSD 2-Clause 许可）

---

## 依赖与第三方声明

本项目使用了以下开源组件：
### async.hpp
***无***
### threadpool.hpp
- **[ConcurrentQueue](https://github.com/cameron314/concurrentqueue)** – 用于无锁任务队列。
  - 版权所有者：Copyright (c) 2013-2020, Cameron Desrochers.
  - 许可证：BSD 2-Clause License（详见 [LICENSE](third_party/concurrentqueue/LICENSE)）。
  - 该组件与 GPLv3 许可证兼容，使用条件已满足。

所有第三方代码均保留原始版权声明，并置于 [third_party](third_party/) 目录下。

## 快速开始

以下演示协程的基本用法，更多用法详见[example](example)
```cpp
#include "async.hpp"
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

// 异步等待
Task<void> async_wait(int ms) {
    auto start = std::chrono::steady_clock::now();
    auto dur = std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() - start < dur) {
        co_await std::suspend_always{};
    }
    co_return;
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
```
### 设计注记：为什么示例中用非阻塞io轮询而不是 epoll？

如果你注意到这个示例中有大量的空 resume，说明你对效率很敏感——这正是我们想要传达的。

**AsyncKernel 故意不做调度器。** 这个示例用非阻塞io只是为了展示协程如何与底层 I/O 原语配合。你可以轻松地：
- 把轮询换成 `epoll` / `kqueue` / `io_uring`。
- 把 `std::suspend_always{}` 换成协程注册到全局事件循环（通过suspend_state_code通知外部，然后在事件触发时 `resume()`）。

**调度策略是你的业务逻辑的一部分**，不是库的职责。用 AsyncKernel 你可以在 100 行内实现自己的 epoll 驱动调度器。

**异步的本质**，异步最底层的原理永远都只是轮询，但大多库总是想用千奇百怪的方式封装回调，隐藏调度流。协程是我们回归本源的一个机会


## 提醒和警告
协程表面上没有传统多线程的竞态，但协程安全一点都不比线程安全简单。在协程的世界里唯一，也是最为脆弱就是Task本身。把Task的控制权交还给你，既是权力自由也是责任负担。比如你喝多了写出这样一段代码
```cpp
#include"../async.hpp"
#include<vector>
std::vector<async> global_all_task{};

async async_task()
{
    //do something
}
async main_task()
{
    async task1=async_task();
    global_all_task.push_back(std::move(task1));
    co_await std::suspend_always{};
    //do something next
}
int main()
{
    global_all_task.push_back(main_task());
    for(auto &task:global_all_task)
        task.resume();
}
```
你能一秒说出它有什么问题吗？
1. 在resume里，vector被插入了一个元素，按标准规定这时候指向原vector的迭代器（for展开后获取的begin和end）已经失效了，但for循环还跟没事人一样在遍历global_all_task
1. 更糟的结果，插入元素导致vector扩容重新分配内存，**Task被移动了**，在coroutine_handle::resume返回后，也就是返回Task::resume里，**resume使用的this就是个悬垂指针**！

因此，在协程内部操作Task通常是非常危险的操作，因为你手动产生了撕裂的读写，却很难察觉  
Task本身的移动操作和析构的实现只考虑了部分协程自指操作引发的问题，但实际情况远比这复杂。使用者必须慎重对待这类代码  
如果遇到了鬼魅般的段错误，可以先尝试把所有Task用unique_ptr包一层并总是用->间接访问，或许能解决九成的问题。

---

## 集成到你的项目
本库为纯头文件库，只要把async.hpp、threadpool.hpp、third_party放在同一级目录，你就可以安心地include它们

---

## 许可证
本项目采用 GNU General Public License v3.0 授权。详见 LICENSE 文件。
>特别声明：本库使用的第三方组件 concurrentqueue 采用 BSD 2-Clause 许可证，其版权声明和免责条款已包含在 third_party/concurrentqueue/LICENSE 中，符合 GPL 兼容性要求。

## 致谢
- 感谢 Cameron Desrochers 提供的高性能无锁队列。
- 感谢 C++ 标准委员会带来的协程特性
