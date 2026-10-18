#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

/**
 * event_loop.h — epoll 事件循环
 *
 * 提供 epoll 事件循环核心功能：
 * - run: 主事件循环（epoll_wait + 事件分发）
 * - handle_accept: 接受新连接
 * - handle_readable: 处理可读事件（EPOLLIN 就绪 → recv 到 EAGAIN → 原始字节交上层）
 */

#include <functional>
#include <memory>

#include "connection.h"
#include "tcp_connection.h"
#include "../common/thread_pool.h"
#include "../session/session_store.h"

class EventLoop {
public:
    /**
     * 构造函数
     * @param port 监听端口
     * @param thread_pool 线程池引用
     * @param tcp_conn TCP 连接管理器引用
     * @param sessions 会话表引用（不拥有，main 注入，两表读写由其内部锁保护）
     * @param client_data_handler 消息处理回调（连接身份, raw_message）
     * @note 回调拿 shared_ptr<Connection> 而不是 int fd：任务把它按值存进 lambda，
     *       投递出去之后事件循环即便断开这条连接，对象也不会析构、fd 号不会归还内核，
     *       下一次 accept 就复用它 —— 机制见 connection.h
     */
    EventLoop(int port,
              ThreadPool& thread_pool,
              TcpConnection& tcp_conn,
              SessionStore& sessions,
              std::function<void(std::shared_ptr<Connection>, const std::string&)> client_data_handler);

    ~EventLoop();

    /**
     * 启动事件循环
     * @return true：正常退出（含信号停机）；false：启动失败 / 运行时致命异常
     */
    bool run();

    /**
     * 停止事件循环
     * @note 仅置 running_ = false，需等当前 epoll_wait 返回后才退出 ⇒ **异步请求，不是同步停止**。
     * @note 当前**无调用者**：实际退出路径是信号处理器置 g_running（run() 的双退出条件 running_ && g_running）；
     *       保留本接口供将来从其它线程/管理接口做优雅停机。
     */
    void stop();

#if defined(CHAT_TEST_HOOKS)
    /// 测试构建观测接口：监听 fd（供进程内 harness 在循环暂停期间执行一次真实 accept）
    int listen_fd_for_test() const { return listen_fd_; }

    /// 测试构建观测接口：epoll 实例 fd（供 harness 用真实 add_epoll_in 完成同号再登记）
    int epoll_fd_for_test() const { return epoll_fd_; }
#endif

private:
    /**
     * 接受新连接
     * @param listen_fd 监听套接字
     * @param epoll_fd epoll 实例
     */
    void handle_accept(int listen_fd, int epoll_fd);

    /**
     * 处理客户端消息
     * @param conn 客户端连接（调用方已 find_conn 取到；保证非空且 fd 有效）
     * @note 参数是 shared_ptr 而不是 fd：本轮每个投递出去的任务都要按值捕获它，
     *       整轮只查一次注册表（别放进 recv 循环里逐块查）
     */
    void handle_readable(std::shared_ptr<Connection> conn);

    int port_;  // 监听端口（构造参数，只读）
    int listen_fd_;  // 监听 socket（run() 创建，析构 close）
    int epoll_fd_;  // epoll 实例（run() 创建，析构 close）
    int idle_fd_;  // 兜底空闲 fd，进程 accept 新连接触发 EMFILE 时用于兜底断连

    bool running_;  // 运行标志（stop() 置 false；与信号路径的 g_running 共同构成 run() 的双退出条件）

    ThreadPool& thread_pool_;  // 线程池引用（不拥有，main 注入）
    TcpConnection& tcp_conn_;  // TCP 连接管理器引用（不拥有，main 注入）
    SessionStore& sessions_;  // 会话表引用（不拥有，main 注入，两表读写由其内部锁保护）
    std::function<void(std::shared_ptr<Connection>, const std::string&)> client_data_handler_;  // 消息回调（main 注入）
};

#endif // EVENT_LOOP_H
