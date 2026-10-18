#include "tcp_connection.h"
#include "../common/test_hooks.h"  // 测试构建专用观测钩子（正常构建为空宏）

TcpConnection::TcpConnection(SessionStore& sessions) : sessions_(sessions) {}

void TcpConnection::set_epoll_fd(int epoll_fd) {
    epoll_fd_ = epoll_fd;
}

std::shared_ptr<Connection> TcpConnection::create(int fd) {
    const ConnectionId id = static_cast<ConnectionId>(next_conn_id_.fetch_add(1));
    std::shared_ptr<Connection> conn = std::make_shared<Connection>(fd, epoll_fd_, id);
    {
        std::lock_guard<std::mutex> lock(conns_mutex_);  // 锁内表操作
        conns_by_id_[id] = conn;  // 以连接身份为键登记（权威索引；非 fd）
    }
#if defined(CHAT_TEST_HOOKS)
    test_hooks::observe_create(static_cast<std::uint64_t>(id), registry_size());
#endif
    return conn;
}

std::shared_ptr<Connection> TcpConnection::find_conn(ConnectionId id) const {
    std::lock_guard<std::mutex> lock(conns_mutex_);
    auto it = conns_by_id_.find(id);
    return (it != conns_by_id_.end()) ? it->second : nullptr;
}

// 断开顺序：先摘注册表（锁内一次做完「查找 + 摘除」），出锁后再做清理。
// 与修复前最大的不同是最后那行 close(fd) 不在了 —— close 挪进了 ~Connection，由最后一个
// shared_ptr 析构时执行，而那个时刻必然晚于本函数返回（本函数全程攥着一个局部 shared_ptr）。
// 于是「所有按 fd 号寻址的清理都跑在 fd 号还给内核之前」这条不变量，
// 修复前靠手工「把 close 写在最后一行」保证，现在由生命周期保证 —— 写错顺序也破坏不了它。
void TcpConnection::disconnect_client(ConnectionId id) {
    std::shared_ptr<Connection> conn;
    {
        // 锁内表操作
        std::lock_guard<std::mutex> lock(conns_mutex_);
        auto it = conns_by_id_.find(id);
        if (it == conns_by_id_.end()) {
            // 迟到的断开：身份已注销，空转（幂等）。测试构建记录之，供"清理是否真的生效"判读
            TH_MARK("registry.disconnect_miss", "id=" + std::to_string(static_cast<std::uint64_t>(id)));
            return;
        }
        conn = it->second;
        conns_by_id_.erase(it);
    }
    conn->close_logical();  // 标记关闭（幂等）：真正的 close(fd) 在最后一个 shared_ptr 析构时执行
    sessions_.remove_connection(id);  // 摘掉会话表路由；该身份已注销，不会有新任务再认领它
#if defined(CHAT_TEST_HOOKS)
    // 注销完成后的资源观测：注册表大小 + 该身份遗留的孤儿路由数（应为 0）
    test_hooks::observe_disconnect(static_cast<std::uint64_t>(id), registry_size(),
                                   sessions_.routes_pointing_to(id).size());
#endif
}

#if defined(CHAT_TEST_HOOKS)
std::size_t TcpConnection::registry_size() const {
    std::lock_guard<std::mutex> lock(conns_mutex_);
    return conns_by_id_.size();
}

std::vector<std::pair<std::uint64_t, int>> TcpConnection::snapshot_ids_fds() const {
    std::lock_guard<std::mutex> lock(conns_mutex_);
    std::vector<std::pair<std::uint64_t, int>> out;
    out.reserve(conns_by_id_.size());
    for (const auto& entry : conns_by_id_) {
        out.emplace_back(static_cast<std::uint64_t>(entry.first), entry.second->fd());
    }
    return out;
}
#endif

// 入队失败且连接已关（超限断连）：close_logical 摘不了注册表和会话表，在这一层补摘。
// 本函数攥着 shared_ptr 期间，fd 号不会被回收，所以 disconnect_client 断不错人。
bool TcpConnection::add_to_send_queue(ConnectionId id, const std::string& msg) {
    std::shared_ptr<Connection> conn = find_conn(id);
    if (!conn) return false;

    if (conn->add_to_queue_out(msg)) return true;

    if (conn->closed()) disconnect_client(id);  // 只在"已关"时摘；EPOLLOUT 登记失败是活连接，不能断

    return false;
}
