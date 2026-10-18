#ifndef MESSAGE_SERVICE_H
#define MESSAGE_SERVICE_H

/**
 * message_service.h — 消息服务
 *
 * 提供消息分发相关的业务逻辑：
 * - broadcast: 广播消息给所有在线用户
 * - send_private: 私聊消息
 * - build_history_response: 构建历史消息 JSON 响应
 */

#include <functional>
#include "../database/message_dao.h"
#include "../session/session_store.h"
#include "../session/user_id_cache.h"
#include "../common/connection_id.h"

class MessageService {
public:
    /**
     * 构造函数
     * @param message_dao 消息表 DAO
     * @param sessions 会话表引用（不拥有，main 注入，两表读写由其内部锁保护）
     * @param user_ids 缓存表映射引用（不拥有，main 注入，映射表只读由其内部锁保护）
     * @param send_to_client 发送消息回调函数（连接身份, msg）
     */
    MessageService(MessageDao& message_dao,
                   SessionStore& sessions,
                   UserIdCache& user_ids,
                   std::function<bool(ConnectionId, const std::string&)> send_to_client);

    /**
     * 广播消息给所有在线用户（除发送者外）
     * @param content 消息内容
     * @param sender_id 发送者连接身份
     */
    void broadcast(const std::string& content, ConnectionId sender_id);

    /**
     * 发送私聊消息
     * @param target_name 目标用户名
     * @param content 消息内容
     * @param sender_id 发送者连接身份
     */
    void send_private(const std::string& target_name,
                     const std::string& content,
                     ConnectionId sender_id);

    /**
     * 构建历史消息 JSON 响应
     * @param peer 对方用户名
     * @param messages 消息记录列表
     * @return JSON 字符串（含换行符）
     */
    std::string build_history_response(const std::string& peer,
                                      const std::vector<MessageRecord>& messages);

private:
    MessageDao& message_dao_;  // 消息表 DAO（不拥有，main 注入；内部走连接池 borrow/return）
    SessionStore& sessions_;  // 会话表引用（不拥有，main 注入，两表读写由其内部锁保护）
    UserIdCache& user_ids_;  // 缓存表映射引用（不拥有，main 注入，映射表只读由其内部锁保护）
    std::function<bool(ConnectionId, const std::string&)> send_to_client_;  // 发送回调（main 注入，内部走 TcpConnection::add_to_send_queue）
};

#endif // MESSAGE_SERVICE_H
