#include "session_store.h"

// ========================== 写接口 ==========================

void SessionStore::add_connection(ConnectionId id) {
    std::lock_guard<std::mutex> lock(mutex_);
    id_to_user_[id] = "";  // 空串表示已连接未登录
}

SessionStore::BindResult SessionStore::bind_login(ConnectionId id, const std::string& user) {
    std::lock_guard<std::mutex> lock(mutex_);

    // 守卫 1：id 未登记：登录任务在途时客户端已 close
    //        拒绝写入，避免留下无法回收的幽灵路由
    auto id_it = id_to_user_.find(id);
    if (id_it == id_to_user_.end()) {
        return {false, std::nullopt};
    }

    // 守卫 2：同一连接下不同 user 登录：清理旧用户名路由，id 路由下方双写时覆盖
    //        首次登录，同账号重复登录，旧账号被别处登录时不清理
    const std::string old_user = id_it->second;
    if (!old_user.empty() && old_user != user) {
        auto old_user_it = user_to_id_.find(old_user);
        if (old_user_it != user_to_id_.end() && old_user_it->second == id) {
            user_to_id_.erase(old_user_it);
        }
    }

    // 守卫 3：当前登录 user 已登录+存在路由时返回旧连接 id（顶号 / 同 id 重复登录）
    //        旧连接 id 交由调用方进行判断是否断开旧连接
    std::optional<ConnectionId> old_id = std::nullopt;
    auto user_it = user_to_id_.find(user);
    if (user_it != user_to_id_.end()) {
        old_id = user_it->second;
    }

    // 镜像双写
    id_to_user_[id] = user;  // 具有覆盖作用
    user_to_id_[user] = id;

    return {true, old_id};
}

std::optional<std::string> SessionStore::remove_connection(ConnectionId id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = id_to_user_.find(id);
    if (it == id_to_user_.end()) {
        return std::nullopt;
    }

    const std::string username = it->second;  // 先拷贝，后用 it 删除
    id_to_user_.erase(it);

    if (username.empty()) {
        return std::nullopt;  // 已连接未登录的连接在表中没有数据
    }

    auto user_it = user_to_id_.find(username);
    if (user_it != user_to_id_.end() && user_it->second == id) {  // 被顶号后保留新路由
        user_to_id_.erase(user_it);
    }

    return username;
}

// ========================== 读接口 ==========================

std::optional<ConnectionId> SessionStore::find_connection_id(const std::string& user) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = user_to_id_.find(user);
    if (it != user_to_id_.end()) return it->second;
    return std::nullopt;
}

std::string SessionStore::find_user(ConnectionId id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = id_to_user_.find(id);
    return (it != id_to_user_.end()) ? it->second : "";
}

std::vector<ConnectionId> SessionStore::snapshot_logged_in_ids_except(ConnectionId id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<ConnectionId> result;
    for (const auto& entry : user_to_id_) {  // 遍历 user_to_id_ 只含已登录连接
        if (entry.second != id) {
            result.push_back(entry.second);
        }
    }

    return result;  // 锁内拷贝，锁外发送
}

#if defined(CHAT_TEST_HOOKS)
std::vector<std::string> SessionStore::routes_pointing_to(ConnectionId id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> out;
    for (const auto& entry : user_to_id_) {
        if (entry.second == id) out.push_back(entry.first);
    }
    return out;
}

std::pair<std::size_t, std::size_t> SessionStore::table_sizes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {id_to_user_.size(), user_to_id_.size()};
}
#endif