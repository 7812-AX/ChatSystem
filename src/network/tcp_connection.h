#ifndef TCP_CONNECTION_H
#define TCP_CONNECTION_H

/**
 * tcp_connection.h — TCP 连接管理
 *
 * 所在层：网络层。由 main 创建后同时注入给 EventLoop 与 Service 层，是两者的共享件。
 * 职责范围：
 * - 连接注册表：连接身份 → Connection 的登记 / 查找 / 注销（见下「注册表」）
 * - set_epoll_fd：注入 epoll 实例 fd
 * - disconnect_client: 断开客户端连接
 * - add_to_send_queue: 把消息投进该连接的出站队列（真正的发送在事件循环线程）
 *
 * 注册表：
 *   表里存 shared_ptr<Connection>，同时是 fd 号的持有者。事件循环 accept 时登记，
 *   断开时注销 —— 注销只摘身份、不 close(fd)；真正的 close 在最后一个 shared_ptr
 *   析构时由 ~Connection 执行。于是在途任务攥着 shared_ptr 期间，fd 号不会被还给内核，
 *   下一次 accept 也就拿不到这个号 —— 旧 fd 号落不到新连接上。机制见 connection.h。
 *
 * 为什么表在这里而不是 EventLoop 里：
 *   断开有两个调用方 —— 事件循环线程（对端关闭 / EPOLLERR）和 worker 线程
 *   （顶号断开，由 auth_service 发起）。本类已经是这两侧共用的注入件，放这里不用新增接线；
 *   表由本类自己的 mutex 守护，与 SessionStore 的形态一致。
 */

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <atomic>
#include <cstdint>

#if defined(CHAT_TEST_HOOKS)
#include <cstddef>
#include <utility>
#include <vector>
#endif

#include "connection.h"
#include "../session/session_store.h"
#include "../common/connection_id.h"

class TcpConnection {
public:
    /**
     * 构造函数
     * @param sessions 会话表引用（不拥有，main 注入）
     */
    explicit TcpConnection(SessionStore& sessions);

    /**
     * 注入 epoll 实例 fd
     * @param epoll_fd EventLoop 创建 epoll 后传入；未注入前 disconnect_client 不可用
     */
    void set_epoll_fd(int epoll_fd);

    /**
     * 登记一条新 accept 的连接，并接管其 fd 号。
     * @param fd 已 accept 的客户端描述符
     * @return 新建的 Connection
     * @note 只在事件循环线程的 accept 路径调用（event_loop.cpp）
     * @note 必须用 std::make_shared，不要写 shared_ptr<Connection>(new Connection(...))：
     *       后者要做两次分配（对象 + 控制块），且若控制块分配抛异常，new 出来的对象没人接管会泄漏
     */
    std::shared_ptr<Connection> create(int fd);

    /**
     * 按连接身份取连接。
     * @param id 连接身份
     * @return 该身份对应的 Connection；未登记（已断开 / 从未 accept）返回 nullptr
     * @note 身份校验与取 shared_ptr 在同一临界区内完成：返回非空即可安全使用
     */
    std::shared_ptr<Connection> find_conn(ConnectionId id) const;

    /**
     * 按连接身份断开。
     * @param id 连接身份
     * @note 身份查不到即视为已断开，空转返回（幂等）：迟到的断开不会落到复用同一 fd 号的新连接上
     */
    void disconnect_client(ConnectionId id);

    /**
     * 按连接身份把一条消息投进出站队列。
     * @param id 连接身份
     * @param msg 消息内容（完整一行，含'\n'）
     * @return true = 已入队；false = 该身份未登记/已断开，或队列超限被断
     * @note 身份查找与取 shared_ptr 在同一临界区内完成，返回后对象不会在投递前析构
     */
    bool add_to_send_queue(ConnectionId id, const std::string& msg);

#if defined(CHAT_TEST_HOOKS)
    /// 测试构建观测接口：注册表当前条目数（资源不变量检查用，正常构建不编译）
    std::size_t registry_size() const;

    /// 测试构建观测接口：当前 (身份, fd) 列表（构造"同号复用"交错用，正常构建不编译）
    std::vector<std::pair<std::uint64_t, int>> snapshot_ids_fds() const;
#endif

private:
    SessionStore& sessions_;  // 会话表引用（不拥有，main 注入；两表读写由其内部锁守护）
    int epoll_fd_ = -1;  // epoll 实例 fd（不拥有，EventLoop 在 epoll_create1 后注入；-1=未注入）

    mutable std::mutex conns_mutex_;  // 守护 conns_by_id_：accept / 断开在事件循环线程，顶号断开在 worker 线程
    std::atomic<std::uint64_t> next_conn_id_{1};  // 身份源：只增不复用；0 保留给 invalid
    std::unordered_map<ConnectionId, std::shared_ptr<Connection>> conns_by_id_;  // 连接身份 → 连接（权威索引），持有 fd 所有权
};

#endif // TCP_CONNECTION_H
