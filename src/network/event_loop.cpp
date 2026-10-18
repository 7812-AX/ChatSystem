#include "event_loop.h"
#include "../common/logger.h"    // LOG_ERR/LOG_INFO：日志统一出口
#include "../common/test_hooks.h"  // 测试构建专用同步钩子（正常构建为空宏）
#include "socket_utils.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <csignal>
#include <atomic>
#include <cstdint>

constexpr int MAX_EVENTS = 1024;
constexpr int BUFFER_SIZE = 4096;

// 全局变量：信号处理用（原子类型）
static std::atomic<bool> g_running{true};

// 信号处理：只置标志、不做清理——异步信号安全（handler 内不能调非可重入函数）
static void handle_signal(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        g_running = false;
    }
}

// ==================== 生命周期 ====================

EventLoop::EventLoop(int port,
                     ThreadPool& thread_pool,
                     TcpConnection& tcp_conn,
                     SessionStore& sessions,
                     std::function<void(std::shared_ptr<Connection>, const std::string&)> client_data_handler)
    : port_(port),
      listen_fd_(-1),
      epoll_fd_(-1),
      idle_fd_(-1),
      running_(true),
      thread_pool_(thread_pool),
      tcp_conn_(tcp_conn),
      sessions_(sessions),
      client_data_handler_(client_data_handler) {

    // 注册信号处理
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
}

EventLoop::~EventLoop() {
    if (epoll_fd_ != -1) close(epoll_fd_);
    if (listen_fd_ != -1) close(listen_fd_);
    if (idle_fd_ != -1) close(idle_fd_);
}

// ==================== 事件循环 ====================

bool EventLoop::run() {
    // 创建监听套接字
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        std::perror("socket 创建失败");
        return false;
    }

    // SO_REUSEADDR：重启时不被 TIME_WAIT 的旧连接占用 bind
    int opt = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::perror("setsockopt(SO_REUSEADDR) 失败");
        return false;
    }

    // 绑定地址
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(listen_fd_, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::perror("bind 失败");
        return false;
    }

    // 监听
    if (listen(listen_fd_, SOMAXCONN) < 0) {
        std::perror("listen 失败");
        return false;
    }

    // 预留兜底 fd：EMFILE 时处理积压连接
    idle_fd_ = open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (idle_fd_ < 0) {
        std::perror("预留 fd 打开失败");  // 等待后续裁决是否在兜底 fd 失效时结束服务器
    }

    LOG_INFO << "服务器启动成功，监听端口：" << port_;

    // 监听 fd 非阻塞：ET 下循环 accept 靠 EAGAIN 退出
    if (!set_nonblocking(listen_fd_)) {
        return false;
    }

    // 创建 epoll 实例
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        std::perror("epoll_create1 失败");
        return false;
    }

    // 注入 epoll_fd：Service 层断开旧连接走 TcpConnection::disconnect_client，拿不到这个局部量
    tcp_conn_.set_epoll_fd(epoll_fd_);

    // 注册监听套接字到 epoll
    // 用 0 （ConnectionId::invalid）作为专属身份号，和客户端从 1 开始进行区分
    if (!add_epoll_in(epoll_fd_, listen_fd_, ConnectionId::invalid)) {
        return false;
    }

    epoll_event ready_events[MAX_EVENTS];

    bool is_running_error = false;  // 服务器是否中途运行时异常

    // 主事件循环双退出条件：running_（stop() 置位）+ g_running（信号处理器置位）
    while (running_ && g_running) {
        const int ready_count = epoll_wait(epoll_fd_, ready_events, MAX_EVENTS, -1);
        if (ready_count < 0) {
            if (errno == EINTR) continue;  // 信号打断
            std::perror("epoll_wait 失败");
            break;
        }

#if defined(CHAT_TEST_HOOKS)
        // 测试钩子：批次刚从内核返回、尚未分发。记录本批事件携带的身份集合；
        // 命中 CHAT_HOOK_PAUSE_ON_IDS 时在此暂停，用于构造"旧事件在批次里、换号发生在分发前"的交错
        test_hooks::observe_event_loop_batch(ready_events, ready_count);
#endif

        // 循环取出所有 ready_event
        for (int count = 0; count < ready_count; ++count) {
            const std::uint64_t raw_id = ready_events[count].data.u64;  // 获取 epoll 携带的身份 id
            const uint32_t ready_flags = ready_events[count].events;

            // listen_fd 监听 --> 添加新客户端连接
            if (raw_id == 0) {
                if (ready_flags & EPOLLIN) {
                    handle_accept(listen_fd_, epoll_fd_);
                    continue;
                }

                // 监听 fd 的非可读事件（EPOLLERR/EPOLLHUP）：已无法继续接受连接，显式退出，不静默瘫痪
                LOG_ERR << "监听 fd 异常事件 flags=" << ready_flags << "，服务器退出";
                is_running_error = true;  // 服务器运行中异常
                running_ = false;   // 退出标识
                break;
            }

            const ConnectionId ready_id = static_cast<ConnectionId>(raw_id);
            std::shared_ptr<Connection> conn = tcp_conn_.find_conn(ready_id);
            if (!conn) {
                TH_MARK("event_loop.identity_gone", "id=" + std::to_string(raw_id));
                continue;  // 身份已注销（断连/顶号），丢弃该连接
            }

            // 客户端 fd 连接异常
            if (ready_flags & (EPOLLHUP | EPOLLERR)) {
                LOG_INFO << "连接异常，断开  client_conn_id=" << static_cast<std::uint64_t>(ready_id)
                         << " client_fd=" << conn->fd();
                tcp_conn_.disconnect_client(ready_id);
                continue;
            }

            // 可读事件：处理客户端消息
            if (ready_flags & EPOLLIN) { handle_readable(conn); }

            // 可写事件
            if (ready_flags & EPOLLOUT) { conn->send_all_queue_msg(); }
        }
    }

    LOG_INFO << "epoll_[stop]";
    close(epoll_fd_); epoll_fd_ = -1;  // 置 -1 避免析构二次 close
    close(listen_fd_); listen_fd_ = -1;
    LOG_INFO << (is_running_error ? "服务器中途异常退出" : "服务器正常退出");

    return !is_running_error;
}

void EventLoop::stop() {
    // 仅置标志：由 run() 的双退出条件在**下一次 epoll_wait 返回后**生效（本函数不阻塞、不 join）
    running_ = false;
}

// ==================== 连接与消息处理 ====================

void EventLoop::handle_accept(int listen_fd, int epoll_fd) {
    bool emfile_logged = false;  // 本轮是否已告警：积压启动兜底 fd 打印日志只触发一次

    // ET 模式循环 accept
    for (;;) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (sockaddr*)&client_addr, &client_len);

        if (client_fd < 0) {
            if (errno == EINTR) continue;  // 信号打断

            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 缓冲暂空

            if (errno == EMFILE) {  // 进程 fd 耗尽
                if (idle_fd_ < 0) {
                    LOG_ERR << "兜底 idle_fd 失效，无法接管积压连接";
                    break;
                }

                // 兜底 fd 原子四步：腾位 - 吃 - 关 - 补位
                close(idle_fd_);
                int tmp = accept(listen_fd, nullptr, nullptr);
                if (tmp >= 0) close(tmp);
                idle_fd_ = open("/dev/null", O_RDONLY | O_CLOEXEC);  // O_RDONLY 只读打开 | O_CLOEXEC 新进程开启时关闭，避免子进程泄露 fd
                if (idle_fd_ < 0) {
                    std::perror("兜底 idle_fd 重开失败");
                    break;  // 兜底失效
                }

                if (!emfile_logged) {
                    LOG_ERR << "当前进程 fd 耗尽，已拒绝积压连接";
                    emfile_logged = true;
                }

                if (tmp < 0) break;  // 积压连接处理完毕

                continue;  // 处理所有积压连接
            }
            std::perror("accept 失败");  // 真实失败
            break;
        }

        std::shared_ptr<Connection> conn = tcp_conn_.create(client_fd);  // 登记连接身份，接管 fd 号所有权
        sessions_.add_connection(conn->id());  // 录入哈希表，等待登录

        LOG_INFO << "accept 成功  客户端IP:" << inet_ntoa(client_addr.sin_addr)
                 << "  端口：" << ntohs(client_addr.sin_port);

        // 单一 client_fd 状态设置失败不应该打断后续 fd，用 continue
        // 设置非阻塞
        if (!set_nonblocking(client_fd)) {
            tcp_conn_.disconnect_client(conn->id());  // 统一出口：摘身份 + 摘会话表 + 摘 epoll，fd 由 ~Connection 归还
            continue;
        }

        // 注册 epoll
        if (!add_epoll_in(epoll_fd, client_fd, conn->id())) {
            tcp_conn_.disconnect_client(conn->id());
            continue;
        }

        LOG_INFO << "客户端已连接，等待用户登录  client_conn_id=" << static_cast<std::uint64_t>(conn->id())
                 << " client_fd=" << client_fd;
    }
}

void EventLoop::handle_readable(std::shared_ptr<Connection> conn) {
    const int fd = conn->fd();
    char buffer[BUFFER_SIZE];
    // 仅限本次 handle_readable：超长/提交失败提示共用一次入队尝试额度，下次调用重新计数。
    // 标志记录尝试，不保证入队成功或对端收到；多个超长行可能合并成一条提示。
    bool rejected = false;
    std::vector<std::string> lines;  // 本轮拆分出的完整请求行

    // ET 模式：一次可读事件必须 recv 到 EAGAIN，否则边缘不再触发
    for (;;) {
        ssize_t n = recv(fd, buffer, sizeof(buffer), 0);

        if (n < 0) {
            if (errno == EINTR) continue;  // 信号打断

            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 读缓冲暂空

            std::perror("recv 失败");  // 真实失败
            tcp_conn_.disconnect_client(conn->id());
            return;
        }

        if (n == 0) {
            LOG_INFO << "客户端主动断开连接  client_conn_id=" << static_cast<std::uint64_t>(conn->id())
                     << " client_fd=" << fd;
            tcp_conn_.disconnect_client(conn->id());
            return;
        }

        if (!conn->split_data_to_lines(buffer, n, lines)){
            // 同一轮 recv 到 EAGAIN 期间，多次超长检测最多尝试入队一条提示。
            if (!rejected) {
                tcp_conn_.add_to_send_queue(conn->id(), "[系统] 当前消息长度过长\n");
                rejected = true;
            }
        }
    }

    // 投递线程池：消息处理含 DB IO，不能阻塞事件循环
    if (!conn->enqueue_lines(lines)) return;  // 已有 worker 消费 / 无新行需要投递

    if (!thread_pool_.submit([this,conn]() {  // 值捕获避免局部变量销毁后的悬垂引用
        std::string line;
        while (conn->pop_line(line)) {  // 循环投递到等候队列空
            client_data_handler_(conn,line);
        }
    })){
        // 提交失败清理资源；本轮若已尝试超长提示，则不再尝试繁忙提示。
        conn->abandon_pending();
        if (!rejected) {
                tcp_conn_.add_to_send_queue(conn->id(), "[系统] 当前服务器繁忙，请稍后重试\n");
                rejected = true;
            }
    }
}
