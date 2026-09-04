/**
 * ThreadPool.h — M4 线程池接口声明（W8D5 Day1 产出）
 *
 * 设计：任务队列 + 工作线程池，与 epoll 事件循环解耦。
 * 生产者（epoll 主线程）→ submit() 入队 → 消费者（worker）锁内 front+pop → 锁外执行。
 *
 * 编译：g++ -std=c++17 -Wall -Wextra -pedantic ThreadPool.cpp -o threadpool_test
 */

#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
public:
    // 任务类型：任何"无参数、无返回值"的可调用对象（函数指针 / lambda / bind 结果）
    using Task = std::function<void()>;

    // 构造：只保存线程数，不立刻启动（把"创建线程"推迟到 start()）
    explicit ThreadPool(size_t num_threads);

    // 析构：RAII 兜底 —— 对象离开作用域时若线程还活着，必须 stop()
    ~ThreadPool();

    // 禁止拷贝：线程池持有线程/锁，拷贝语义无意义（std::thread/mutex 本身不可拷贝）
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 启动 N 个工作线程
    void start();

    // 提交任务入队（生产者调用，epoll 主线程）
    void submit(Task task);

    // 优雅停止：停止接收新任务 → 消费残留 → join
    void stop();

private:
    // 工作线程函数：每个 worker 的执行体
    void worker_loop();

    size_t thread_count_;           // 工作线程数（构造参数）
    std::vector<std::thread> workers_;  // N 个工作线程
    std::queue<Task> tasks_;        // 共享任务队列（FIFO）
    std::mutex mutex_;              // 保护 tasks_ 和 stopping_
    std::condition_variable cv_;    // 唤醒/休眠 worker
    bool stopping_ = false;         // 优雅停止标志
};