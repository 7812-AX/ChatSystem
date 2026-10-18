#include "message_dao.h"
#include "../common/logger.h"    // LOG_ERR/LOG_INFO：日志统一出口
#include <cstring>

// 【缓冲覆盖常规消息】单条内容结果缓冲：列宽 VARCHAR(2048) 字符（utf8mb4 最大 8192 字节）
// 4096 未覆盖列上限；超长会使 fetch 返回 MYSQL_DATA_TRUNCATED → 整个历史查询失败
constexpr size_t MESSAGE_CONTENT_BUF_SIZE = 4096;

MessageDao::MessageDao(MysqlConnectionPool& pool) : pool_(pool){}

bool MessageDao::query_message_history(int user_id, int peer_id, std::vector<MessageRecord>& messages) {
    messages.clear();

    // MySQL 连接获取惯例：borrow 借出，guard 析构自动归还（异常/早退也归还）
    PooledConnection guard = pool_.borrow();
    MYSQL* conn = guard.get();
    if (conn == nullptr) {  // 借用失败（超时/池已停止）
        return false;
    }

    bool select_ok = false;
    std::string select_error;

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        select_error = mysql_error(conn);
    } else {
        do {
            const char sql[] = "SELECT id,sender_id,receiver_id,content FROM messages WHERE (sender_id=? AND receiver_id=?) OR (sender_id=? AND receiver_id=?) ORDER BY id DESC LIMIT 20";
            if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
                select_error = mysql_stmt_error(stmt);
                break;
            }

            MYSQL_BIND bind[4];
            memset(bind, 0, sizeof(bind));
            bind[0].buffer_type = MYSQL_TYPE_LONG;
            bind[0].buffer = &user_id;
            bind[1].buffer_type = MYSQL_TYPE_LONG;
            bind[1].buffer = &peer_id;
            bind[2].buffer_type = MYSQL_TYPE_LONG;
            bind[2].buffer = &peer_id;
            bind[3].buffer_type = MYSQL_TYPE_LONG;
            bind[3].buffer = &user_id;

            if (mysql_stmt_bind_param(stmt, bind) != 0) {
                select_error = mysql_stmt_error(stmt);
                break;
            }

            unsigned int id;
            unsigned int sender_id;
            unsigned int receiver_id;
            char content[MESSAGE_CONTENT_BUF_SIZE]{};
            unsigned long content_len = 0;

            MYSQL_BIND bind_res[4];
            memset(bind_res, 0, sizeof(bind_res));

            bind_res[0].buffer_type = MYSQL_TYPE_LONG;
            bind_res[0].buffer = &id;
            bind_res[1].buffer_type = MYSQL_TYPE_LONG;
            bind_res[1].buffer = &sender_id;
            bind_res[2].buffer_type = MYSQL_TYPE_LONG;
            bind_res[2].buffer = &receiver_id;
            bind_res[3].buffer_type = MYSQL_TYPE_STRING;
            bind_res[3].buffer = content;
            bind_res[3].buffer_length = sizeof(content);
            bind_res[3].length = &content_len;

            if (mysql_stmt_bind_result(stmt, bind_res) != 0) {
                select_error = mysql_stmt_error(stmt);
                break;
            }

            if (mysql_stmt_execute(stmt) != 0) {
                select_error = mysql_stmt_error(stmt);
                break;
            }

            for (;;) {
                int fetch_res = mysql_stmt_fetch(stmt);
                if (fetch_res == 0) {
                    messages.push_back(MessageRecord{id, sender_id, receiver_id, std::string(content, content_len)});
                    continue;
                }

                if (fetch_res == MYSQL_NO_DATA) {
                    select_ok = true;
                    break;
                }

                select_error = mysql_stmt_error(stmt);
                break;
            }
        } while (false);  // do-while(false) + break = 无 goto 的逐级错误退出；错误先存 select_error，末尾统一打印一次
        mysql_stmt_close(stmt);
    }

    if (!select_ok) {
        LOG_ERR << "查询历史消息失败：" << select_error;
    }
    return select_ok;
}

bool MessageDao::save_message(int sender_id, int receiver_id, const std::string& content) {
    // MySQL 连接获取惯例：borrow 借出，guard 析构自动归还（异常/早退也归还）
    PooledConnection guard = pool_.borrow();
    MYSQL* conn = guard.get();
    if (conn == nullptr) {  // 借用失败（超时/池已停止）
        return false;
    }

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERR << "mysql_stmt_init 失败：" << mysql_error(conn);
        return false;
    }
    const char sql[] = "INSERT INTO messages (sender_id,receiver_id,content) VALUES (?,?,?)";

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        LOG_ERR << "mysql_stmt_prepare 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    unsigned long content_len = content.size();
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &sender_id;
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &receiver_id;
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = const_cast<char*>(content.data());
    bind[2].buffer_length = content.size();
    bind[2].length = &content_len;
    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        LOG_ERR << "mysql_stmt_bind_param 失败: " << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    bool ok = (mysql_stmt_execute(stmt) == 0);
    if (!ok) LOG_ERR << "INSERT 失败: " << mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return ok;
}
