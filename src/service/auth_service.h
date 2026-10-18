#ifndef AUTH_SERVICE_H
#define AUTH_SERVICE_H

/**
 * auth_service.h — 认证服务
 *
 * 提供用户认证相关的业务逻辑：
 * - handle_login: 登录验证与会话管理
 */

#include <functional>
#include "../database/user_dao.h"
#include "../session/session_store.h"
#include "../session/user_id_cache.h"
#include "../common/connection_id.h"

class AuthService {
public:
    /**
     * verify_login 返回码语义（与 AuthService 的判定共用）
     * - LoginUserFail    : 业务失败 —— 用户名不存在或密码错误（故意不区分，不泄露账号是否存在）
     * - LoginSysBusy : 系统侧失败 —— 连接池借出失败 / SQL 执行失败（与业务失败区分，避免误导用户）
     * 成功返回 user_id (>= 0)
     */
    static constexpr int LoginUserFail = -1;
    static constexpr int LoginSysBusy = -2;

    /**
     * 构造函数
     * @param user_dao 用户表 DAO
     * @param sessions 会话表引用（不拥有，main 注入，两表读写由其内部锁保护）
     * @param user_ids 缓存表映射引用（不拥有，main 注入，映射表只读由其内部锁保护）
     * @param send_to_client 发送消息的回调函数（连接身份, msg）
     * @param disconnect_old_client 断开被本次登录替换的旧连接的回调函数（连接身份）
     */
    AuthService(UserDao& user_dao,
                SessionStore& sessions,
                UserIdCache& user_ids,
                std::function<bool(ConnectionId, const std::string&)> send_to_client,
                std::function<void(ConnectionId)> disconnect_old_client);

    /**
     * 处理登录请求
     * @param id 连接身份
     * @param username 用户名
     * @param password 密码
     */
    void handle_login(ConnectionId id, const std::string& username, const std::string& password);

private:
    UserDao& user_dao_;  // 用户表 DAO（不拥有，main 注入；内部走连接池 borrow/return）
    SessionStore& sessions_;  // 会话表引用（不拥有，main 注入，两表读写由其内部锁保护）
    UserIdCache& user_ids_;  // 缓存表映射引用（不拥有，main 注入，映射表只读由其内部锁保护）
    std::function<bool(ConnectionId, const std::string&)> send_to_client_;  // 发送回调（main 注入，内部走 TcpConnection::add_to_send_queue）
    std::function<void(ConnectionId)> disconnect_old_client_;  // 断开旧连接回调（main 注入，内部走 TcpConnection::disconnect_client）
};

#endif // AUTH_SERVICE_H
