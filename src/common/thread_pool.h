#ifndef THREAD_POOL_H
#define THREAD_POOL_H

/**
 * thread_pool.h — 线程池接口
 *
 * 任务队列 + 工作线程池，与 epoll 事件循环解耦（生产者 submit 入队 → worker 锁内取任务 → 锁外执行）。
 *
 * 提供线程池核心功能：
 * - start: 创建 N 个线程
 * - submit: 提交任务到任务队列
 * - stop: 优雅关闭线程池，释放资源
 */

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

    /**
     * 构造函数：只保存线程数，线程创建推迟到 start()
     * @param num_threads 工作线程数量
     * @param max_queue_size 任务队列上限（达上限后 submit 拒绝新任务）
     */
    explicit ThreadPool(size_t num_threads, size_t max_queue_size = 4096);

    ~ThreadPool();

    // 禁止拷贝：成员含 std::thread/mutex，拷贝语义无意义
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    /**
     * 启动 N 个工作线程（只允许调用一次）
     */
    void start();

    /**
     * 提交任务入队（生产者调用，epoll 主线程）
     * @param task 待执行对象
     * @return true=已入队；false=被拒绝（队列已满 / 线程池已停止），任务不执行
     */
    bool submit(Task task);

    /**
     * 优雅停止：停止接收新任务 → 消费残留任务 → join 全部 worker（幂等）
     */
    void stop();

private:
    /**
     * 工作线程函数：每个 worker 的执行体（等待任务 → 取队首 → 锁外执行）
     */
    void worker_loop();

    size_t thread_count_;               // 工作线程数（构造参数）
    size_t max_queue_size_;             // 任务队列上限（背压阈值，构造参数）
    std::vector<std::thread> workers_;  // N 个工作线程
    std::queue<Task> tasks_;            // 共享任务队列（FIFO）
    std::mutex mutex_;                  // 保护 tasks_ 和 stopping_
    std::condition_variable cv_;        // 唤醒/休眠 worker
    bool stopping_ = false;             // 优雅停止标志
};

#endif // THREAD_POOL_H