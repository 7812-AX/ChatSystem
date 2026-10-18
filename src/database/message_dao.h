#ifndef MESSAGE_DAO_H
#define MESSAGE_DAO_H

/**
 * message_dao.h — 消息表数据访问对象
 *
 * 提供消息相关的数据库操作：
 * - query_message_history: 查询两个用户之间的历史消息
 * - save_message: 保存私聊消息
 */

#include "mysql_connect_pool.h"
#include <string>
#include <vector>

struct MessageRecord {
    unsigned int id;
    unsigned int sender_id;
    unsigned int receiver_id;
    std::string content;
};

class MessageDao {
public:
    /**
     * 构造函数
     * @param pool 所属连接池
     */
    explicit MessageDao(MysqlConnectionPool& pool);

    /**
     * 查询两个用户之间最近 20 条消息，结果保持 id 降序
     * @param user_id 用户ID
     * @param peer_id 对方ID
     * @param messages 输出参数，消息列表（会先清空）
     * @return true=查询成功（允许结果为空），false=数据库操作失败
     */
    bool query_message_history(int user_id, int peer_id, std::vector<MessageRecord>& messages);

    /**
     * 保存私聊消息
     * @param sender_id 发送者ID
     * @param receiver_id 接收者ID
     * @param content 消息内容
     * @return true=成功，false=失败
     */
    bool save_message(int sender_id, int receiver_id, const std::string& content);

private:
    MysqlConnectionPool& pool_;  // 所属连接池
};

#endif // MESSAGE_DAO_H
