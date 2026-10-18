#ifndef USER_ID_CACHE_H
#define USER_ID_CACHE_H

/**
 * user_id_cache.h - 会话层：主键缓存（username -> user_id）
 *
 * 所在层：session。实例由 main 创建，注入 service 层使用。
 * 职责范围：只维护缓存的存取，不查 DB，不持有 DAO，未命中时的回源由调用方决定。
 *
 * 接口：get、put、remove、clear
 *
 * 真相源：MySQL。本类型是缓存，进程重启后全部丢失，依赖调用方的重建。
 */

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

class UserIdCache {
public:
    // mutex_ 是值成员，拷贝构造与移动构造被编译器删除，缓存只保留一份实例。

    /**
     * 获取缓存中的 user_id。
     * @param username 用户名
     * @return 命中返回 user_id，未命中返回 nullopt
     */
    std::optional<int> get(const std::string& username) const;

    /**
     * 写入缓存哈希表，键存在时覆盖
     * @param username 用户名
     * @param user_id 用户ID - 主键
     */
    void put(const std::string& username, int user_id);

    /**
     * 使某个用户名缓存失效
     * @param username 用户名
     * @note 删除不存在的缓存是幂等操作
     */
    void remove(const std::string& username);

    /**
     * 清空缓存
     */
    void clear();

private:
    mutable std::mutex mutex_;  // 保护 cache_，读写都走临界区
    std::unordered_map<std::string, int> cache_;  // username -> user_id（缓存，真相源 MySQL，由 mutex_ 保护）
};

#endif  // USER_ID_CACHE_H