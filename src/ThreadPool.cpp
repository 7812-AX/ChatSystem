/**
 * ThreadPool.cpp — M4 线程池核心实现（W8D5 Day1 学生填空）
 *
 * 填空规则：
 *   🧠 常规逻辑 —— 你应该直接写（W8D1 已学的 mutex/CV 用法）
 *   💡 提示引导 —— 先看提示自己试，卡住再问
 *   🔑 关键难点 —— 想一下，仍然卡住就向 AI 要这行答案 + 解释
 *
 * 编译：g++ -std=c++17 -Wall -Wextra -pedantic ThreadPool.cpp -o threadpool_test
 * 运行：./threadpool_test
 */

#include "ThreadPool.h"

#include <iostream>

// TODO 🧠 构造函数：保存线程数到 thread_count_
//     注意：这里只保存数量，不要把 stop() 和线程创建写进来
ThreadPool::ThreadPool(size_t num_threads) {
    // 🧠 你写：thread_count_ = ?
    thread_count_ = num_threads;
}

// TODO 🧠 析构函数：RAII 兜底
//     用户忘记调 stop() 时，析构必须兜底，否则 joinable 的 std::thread 直接析构 → std::terminate()
ThreadPool::~ThreadPool() {
    // 🧠 你写：调用 stop()（幂等，多次调用安全）
    stop();
}

// TODO 🧠 启动 N 个工作线程
//     每个 worker 用 std::thread(this->worker_loop) 启动，push_back 到 workers_
//     提示：worker_loop 的执行者是这个新线程，线程函数必须是"能调用到 worker_loop"的东西
void ThreadPool::start() // TODO 🧠 你写完整签名
{
    // 🧠 你写：for 循环创建 thread_count_ 个线程
    // 💡 提示：std::thread(&ThreadPool::worker_loop, this) —— 绑定成员函数的写法
    for(size_t i=0;i<thread_count_;++i){
        workers_.emplace_back(&ThreadPool::worker_loop,this);
    }
}

// TODO 💡 提交任务入队（生产者 = epoll 主线程）
//     三步：① 加锁 ② 入队 ③ 解锁后通知一个 worker
//     💡 提示：
//       - 用 std::lock_guard 还是 std::unique_lock？这里只需要加锁/解锁，不涉及 wait，你选哪个？
//       - 入队后解锁，再 notify_one() —— 先解锁再通知可以减少竞争（W8D1 笔记约定）
//       - 🔑 关键难点：任务用 std::move 进队列里，避免拷贝 std::function 的开销
void ThreadPool::submit(Task task) {
    // 你写
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
}

// TODO 💡 优雅停止
//     ① 加锁置 stopping_ = true
//     ② 解锁后 notify_all() 唤醒所有等待 worker
//     ③ join 全部 worker
//     🔑 关键难点顺序：先置标志（停止接新活）→ 再唤醒（让 worker 重新检查谓词）→ 最后 join
//     💡 提示：join 前要拿锁吗？join 需要独占锁吗？—— 想一下再写
void ThreadPool::stop() {
    // 你写
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_all();

    for(auto& w : workers_){
        if(w.joinable()){
            w.join();
        }
    }
}

// TODO 🔑 工作线程函数（本文件最核心，W8D1 笔记的落地）
//     谓词循环 + wait 持锁原子性 + 锁外执行，全在这一个函数里
//     提示框架（对照 W8D1 笔记 §条件变量与 M4 线程池队列）：
//
//     for (;;) {
//         Task task;
//         {
//             // ① 持锁 + 谓词循环等待
//             //    std::unique_lock<std::mutex> lock(mutex_);
//             //    cv_.wait(lock, [&] { ?? });
//             //
//             // ② 优雅停止判定：stopping_ 为真且队列已空 → return（没有遗留任务可消费了）
//             //
//             // ③ 锁内取出队首：task = std::move(tasks_.front()); tasks_.pop();
//             //    🔑 为什么要在锁内取？—— 保证"一个任务只被一个 worker 取走"
//         }
//         // ④ 锁外执行任务
//         //    🔑 为什么在锁外执行？—— 耗时业务不该卡住队列，其他 worker 才能并发取任务
//         task();
//     }
void ThreadPool::worker_loop() {
    // 你写
    Task task;
    for(;;){
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock,[&]{return !tasks_.empty() || stopping_;});

            if(tasks_.empty() && stopping_) return ;
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
    }
}

// ==================== 测试 main（不要改，填空完成后编译运行） ====================
// 验收标准：
//   1. 多任务并发消费：提交 10 个任务，输出能看到不同 worker id 交叉执行
//   2. 优雅停止 join 不卡死：stop() 后程序能正常退出，不会 hang
//   3. 残留任务被消费：先提交一批任务，立刻 stop()，这些任务应被全部执行完（FIFO 顺序）

#ifdef THREADPOLL_STANDALONE_TEST
int main() {
    ThreadPool pool(3);      // 3 个工作线程
    pool.start();

    // 场景 1：并发消费 —— 10 个任务，检查是否被 3 个 worker 分摊
    std::cout << "[场景1] 提交 10 个任务，3 worker 并发消费\n";
    for (int i = 0; i < 10; ++i) {
        pool.submit([i] {
            std::cout << "worker 执行任务 " << i << "  (线程 "
                      << std::this_thread::get_id() << ")\n";
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 场景 2：优雅停止 —— stop 后主线程 join 全部 worker，程序正常退出
    std::cout << "[场景2] 调用 stop()，验证 join 不卡死\n";
    pool.stop();
    std::cout << "[场景2] stop() 完成，全部 worker 已 join，程序正常退出 ✅\n";

    // 场景 3：残留任务 —— stop 前已有排队任务，验证它们被消费完
    ThreadPool pool2(1);
    pool2.start();
    std::cout << "[场景3] 提交 5 个任务后立即 stop()，验证残留任务被消费\n";
    for (int i = 0; i < 5; ++i) {
        pool2.submit([i] {
            std::cout << "残留任务 " << i << " 被执行\n";
        });
    }
    pool2.stop();
    std::cout << "[场景3] 完成 ✅\n";

    return 0;
}
#endif