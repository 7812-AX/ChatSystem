#ifndef SESSION_STORE_H
#define SESSION_STORE_H

/**
 * session_store.h - 会话层：进程内存中的权威会话状态（连接 id <-> username 双向表）
 *
 * 所在层：session。实例由 main 创建，注入 network 与 service 层使用。
 * 职责范围：只维护两张镜像表的读写，不碰 socket、不做 I/O、不查 DB。
 *
 * 写接口：add_connection、bind_login、remove_connection
 * 读接口：find_connection_id、find_user、snapshot_logged_in_ids_except
 *
 * 不变量：id_to_user_ 与 user_to_id_ 互为镜像，两张表的写入都落在同一个临界区内。
 */

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#if defined(CHAT_TEST_HOOKS)
#include <cstddef>
#include <utility>
#endif

#include "../common/connection_id.h"

class SessionStore {
public:
    // mutex_ 是值成员，拷贝构造与移动构造被编译器删除，会话状态只保留一份实例。

    /**
     * 登记一条新连接，id_to_user_ 的值置为空串，表示已连接未登录。
     * @param id 已 accept 的连接身份号
     * @note id 已存在时直接覆盖，重复 accept 不在本类型的处理范围内
     */
    void add_connection(ConnectionId id);

    /**
     * @brief bind_login 的返回结果。
     *
     * 表示登录绑定是否成功，以及是否顶掉了旧连接。
     *
     * @note 当 ok == false 时，表示 id 未登记（连接已断开），用户表和 id 表均不改动。
     */
    struct BindResult {
        bool ok;                       // false = 拒绝写入
        std::optional<ConnectionId> old_id;  // 被本次登录替换掉的旧连接 id，无则 nullopt
    };

    /**
     * 登录成功后写入两张镜像表，id_to_user_ 与 user_to_id_ 在同一临界区内更新。
     * @param id 登录成功的连接身份号
     * @param user 登录用户名
     * @return BindResult：ok=false 表示 id 未登记、拒绝写入；ok=true 时 old_id 为被本次登录替换掉的旧连接 id（无则 nullopt）
     * @note 返回值为调用方提供清理入口，关闭旧 socket 与删除旧 id 表项由调用方执行
     */
    SessionStore::BindResult bind_login(ConnectionId id, const std::string& user);

    /**
     * 断开连接时删除两张镜像表，先删 id_to_user_ 的行，再删 user_to_id_ 的行。
     * @param id 已 close 的连接身份号
     * @return 被删除的用户名，未知 id 与未登录连接返回 nullopt
     * @note user_to_id_ 中的该 username 仍指向 id 时才删除该行，顶号后的旧 id 断开不删新路由
     */
    std::optional<std::string> remove_connection(ConnectionId id);

    /**
     * 通过用户名查询在线路由。
     * @param user 用户名
     * @return 该用户名对应的 id，未找到返回 nullopt
     */
    std::optional<ConnectionId> find_connection_id(const std::string& user) const;

    /**
     * 通过 id 查询已绑定的用户名。
     * @param id 连接身份号
     * @return 该 id 对应的用户名，未知 id 与未登录连接返回空串
     */
    std::string find_user(ConnectionId id) const;

    /**
     * 在锁内拷贝当前已登录的 id 列表，排除指定 id，不负责发送业务。
     * @param id 需要排除的连接身份号，通常是发送者自己
     * @return 已登录 id 的列表，内容来自 user_to_id_，没有其他在线者时为空 vector
     * @note 调用方拿到列表后在锁外发送，返回拷贝而非引用，锁释放后引用会随 rehash 与 erase 失效
     */
    std::vector<ConnectionId> snapshot_logged_in_ids_except(ConnectionId id) const;

#if defined(CHAT_TEST_HOOKS)
    /// 测试构建观测接口：路由表中指向该身份的用户名（孤儿路由检查用，正常构建不编译）
    std::vector<std::string> routes_pointing_to(ConnectionId id) const;

    /// 测试构建观测接口：两张镜像表当前条目数 (id_to_user_, user_to_id_)
    std::pair<std::size_t, std::size_t> table_sizes() const;
#endif

private:
    mutable std::mutex mutex_;                        // 保护下面两张镜像表，两张表的写入落在同一临界区
    std::unordered_map<ConnectionId, std::string> id_to_user_;  // id -> username，空串表示已连接未登录；权威状态，随进程消亡；由 mutex_ 保护
    std::unordered_map<std::string, ConnectionId> user_to_id_;  // username -> id，在线路由；权威状态，随进程消亡；由 mutex_ 保护
};

#endif  // SESSION_STORE_H
