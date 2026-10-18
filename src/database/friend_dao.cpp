#include "friend_dao.h"
#include "../common/logger.h"    // LOG_ERR/LOG_INFO：日志统一出口
#include <cstring>

FriendDao::FriendDao(MysqlConnectionPool& pool) : pool_(pool){}

bool FriendDao::query_friend_ids(int user_id, std::vector<int>& friend_ids) {
    friend_ids.clear();

    // MySQL 连接获取惯例：borrow 借出，guard 析构自动归还（异常/早退也归还）
    PooledConnection guard = pool_.borrow();
    MYSQL* conn = guard.get();
    if (conn == nullptr) {  // 借用失败（超时/池已停止）
        return false;
    }

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERR << "mysql_stmt_init 失败: " << mysql_error(conn);
        return false;
    }

    const char sql[] = "SELECT friend_id FROM friendships WHERE user_id = ? ORDER BY created_at DESC";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        LOG_ERR << "mysql_stmt_prepare 失败: " << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &user_id;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        LOG_ERR << "mysql_stmt_bind_param 失败: " << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    int friend_id_out = -1;

    MYSQL_BIND bind_res[1];
    memset(bind_res, 0, sizeof(bind_res));
    bind_res[0].buffer_type = MYSQL_TYPE_LONG;
    bind_res[0].buffer = &friend_id_out;

    if (mysql_stmt_bind_result(stmt, bind_res) != 0) {
        LOG_ERR << "mysql_stmt_bind_result 失败: " << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERR << "mysql_stmt_execute 失败: " << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    // fetch 三态：0=取到一行 / MYSQL_NO_DATA=取完（包含空结果）/ 其他=出错
    for (;;) {
        int fetch_res = mysql_stmt_fetch(stmt);

        if (fetch_res == 0) {
            friend_ids.push_back(friend_id_out);
            continue;
        }

        if (fetch_res == MYSQL_NO_DATA) {
            break;
        }

        LOG_ERR << "mysql_stmt_fetch 失败: " << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    mysql_stmt_close(stmt);
    return true;
}
