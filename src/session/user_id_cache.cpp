#include "user_id_cache.h"

std::optional<int> UserIdCache::get(const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(username);
    return (it != cache_.end()) ? std::optional<int>{it->second} : std::nullopt;
}

void UserIdCache::put(const std::string& username, int user_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_[username] = user_id;  // 覆盖或插入
}

void UserIdCache::remove(const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.erase(username);  // 幂等
}

void UserIdCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
}