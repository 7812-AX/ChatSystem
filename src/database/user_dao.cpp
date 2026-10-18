#include "user_dao.h"
#include "../common/logger.h"    // LOG_ERR/LOG_INFO：日志统一出口
#include <cstring>

UserDao::UserDao(MysqlConnectionPool& pool) : pool_(pool){}

int UserDao::query_id_by_name(const std::string& username) {
    // MySQL 连接获取惯例：borrow 借出，guard 析构自动归还（异常/早退也归还）
    PooledConnection guard = pool_.borrow();
    MYSQL* conn = guard.get();
    if (conn == nullptr) {  // 借用失败（超时/池已停止）
        return LoginSysBusy;
    }

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERR << "mysql_stmt_init 失败：" << mysql_error(conn);
        return LoginSysBusy;
    }

    const char sql[] = "SELECT id FROM users WHERE username = ?";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        LOG_ERR << "mysql_stmt_prepare 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    unsigned long username_len = username.size();

    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(username.data());
    bind[0].buffer_length = username.size();
    bind[0].length = &username_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        LOG_ERR << "mysql_stmt_bind_param 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    int id_out;

    MYSQL_BIND bind_res[1];
    memset(bind_res, 0, sizeof(bind_res));
    bind_res[0].buffer_type = MYSQL_TYPE_LONG;
    bind_res[0].buffer = &id_out;

    if (mysql_stmt_bind_result(stmt, bind_res) != 0) {
        LOG_ERR << "mysql_stmt_bind_result 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERR << "mysql_stmt_execute 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    int fetch_res = mysql_stmt_fetch(stmt);
    if (fetch_res != 0) {
        if (fetch_res == MYSQL_NO_DATA) {
            mysql_stmt_close(stmt);
            return LoginUserFail;
        }
        LOG_ERR << "mysql_stmt_fetch 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    } else {
        mysql_stmt_close(stmt);
        return id_out;
    }
}

int UserDao::verify_login(const std::string& username, const std::string& password) {
    // MySQL 连接获取惯例：borrow 借出，guard 析构自动归还（异常/早退也归还）
    PooledConnection guard = pool_.borrow();
    MYSQL* conn = guard.get();
     if (conn == nullptr) {  // 系统侧失败：与"密码错"区分
        return LoginSysBusy;
    }

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERR << "mysql_stmt_init 失败：" << mysql_error(conn);
        return LoginSysBusy;
    }

    const char sql[] = "SELECT id,password FROM users WHERE username = ?";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        LOG_ERR << "mysql_stmt_prepare 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    unsigned long username_len = username.size();

    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(username.data());
    bind[0].buffer_length = username.size();
    bind[0].length = &username_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        LOG_ERR << "mysql_stmt_bind_param 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    int id_out;
    char password_out[256] = {};  // 覆盖 varchar(255) 全宽 + '\0'；= {} 让 strcmp 有终止符可停
    unsigned long password_len;

    MYSQL_BIND bind_res[2];
    memset(bind_res, 0, sizeof(bind_res));
    bind_res[0].buffer_type = MYSQL_TYPE_LONG;
    bind_res[0].buffer = &id_out;
    bind_res[1].buffer_type = MYSQL_TYPE_STRING;
    bind_res[1].buffer = password_out;
    bind_res[1].buffer_length = sizeof(password_out);
    bind_res[1].length = &password_len;

    if (mysql_stmt_bind_result(stmt, bind_res) != 0) {
        LOG_ERR << "mysql_stmt_bind_result 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERR << "mysql_stmt_execute 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    }

    int fetch_res = mysql_stmt_fetch(stmt);
    if (fetch_res != 0) {
        if (fetch_res == MYSQL_NO_DATA) {
            mysql_stmt_close(stmt);
            return LoginUserFail;
        }
        LOG_ERR << "mysql_stmt_fetch 失败：" << mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return LoginSysBusy;
    } else {
        if (strcmp(password.c_str(), password_out) == 0) {
            mysql_stmt_close(stmt);
            return id_out;
        } else {
            LOG_ERR << "用户ID查找失败，密码错误";
            mysql_stmt_close(stmt);
            return LoginUserFail;
        }
    }
}
