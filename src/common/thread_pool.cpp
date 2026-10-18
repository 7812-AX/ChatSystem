#include "thread_pool.h"
#include "logger.h"    // LOG_ERR：日志统一出口
#include <iostream>    // 仅供 THREADPOLL_STANDALONE_TEST 的独立测试入口使用，勿删

ThreadPool::ThreadPool(size_t num_threads,  size_t max_queue_size) {
    thread_count_ = num_threads;  // 只保存数量：线程创建推迟到 start()（两阶段）
    max_queue_size_ = max_queue_size;
}

ThreadPool::~ThreadPool() {
    stop();  // RAII 兜底：用户忘记调 stop() 时由析构补上
}

void ThreadPool::start() {
    for(size_t i=0;i<thread_count_;++i){
        workers_.emplace_back(&ThreadPool::worker_loop,this);  // 新线程执行 this->worker_loop()
    }
}

bool ThreadPool::submit(Task task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (stopping_) {  // stop() 后不再接收：拒绝任务而非静默接收
            LOG_ERR << "线程池已停止，拒绝当前任务";
            return false;
        }

        if (tasks_.size() >= max_queue_size_) {  // 任务队列达上限，拒绝新任务
            LOG_ERR << "线程池队列已满 [" << tasks_.size() << "/" << max_queue_size_ << "]，拒绝当前任务";
            return false;
        }

        tasks_.push(std::move(task));  // std::move：避免拷贝 std::function
    }
    cv_.notify_one();  // 先解锁再通知：worker 醒来后可直接拿锁
    return true;
}

void ThreadPool::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_all();

    for(auto& w : workers_){
        if(w.joinable()){  // joinable 检查：使 stop 幂等
            w.join();
        }
    }
}

void ThreadPool::worker_loop() {

    for(;;){
        Task task;  // 循环内构建 task，保证每个任务完成后立刻析构释放捕获的 shared_ptr
        {
            std::unique_lock<std::mutex> lock(mutex_);  // unique_lock：wait 需睡时解锁/醒时重拿，lock_guard 无此能力
            cv_.wait(lock,[&]{return !tasks_.empty() || stopping_;});  // 谓词：有任务或被要求停止才醒

            if(tasks_.empty() && stopping_) return ;
            task = std::move(tasks_.front());  // 锁内取队首：一个任务只被一个 worker 取走
            tasks_.pop();
        }
        task();  // 锁外执行：耗时业务不卡队列，其余 worker 可并发取任务
    }
}

// ==================== 独立测试入口（定义 THREADPOLL_STANDALONE_TEST 宏启用） ====================

#ifdef THREADPOLL_STANDALONE_TEST
int main() {
    ThreadPool pool(3);  // 3 个工作线程
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
