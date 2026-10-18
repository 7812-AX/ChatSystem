/**
 * main.cpp — 聊天服务器启动入口
 *
 * 组装所有层：
 * 1. 基础资源（互斥锁、连接表、线程池、MySQL 连接池）
 * 2. DAO 层（UserDao、FriendDao、MessageDao）
 * 3. Service 层（AuthService、MessageService）
 * 4. 网络层（TcpConnection、EventLoop）
 * 5. 启动事件循环
 *
 * 编译：参见 CMakeLists.txt
 * 运行：./build/src/chat_server（环境变量见 README）
 */

#include "common/thread_pool.h"
#include "common/logger.h"    // 日志原子化输出
#include "common/connection_id.h"  // 连接身份信息
#include "database/mysql_connect_pool.h"
#include "database/user_dao.h"
#include "database/friend_dao.h"
#include "database/message_dao.h"
#include "service/auth_service.h"
#include "service/message_service.h"
#include "network/tcp_connection.h"
#include "network/event_loop.h"
#include "session/session_store.h"
#include "session/user_id_cache.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <string>


namespace {

// 读取非空字符串配置；未设置或为空时使用默认值
std::string env_string(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? std::string(value) : fallback;
}

// 读取整数配置；格式错误或越界时回退默认值，避免把非法值带进系统调用
int env_int(const char* name, int fallback, int min_value, int max_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;

    try {
        const std::string text(value);
        std::size_t consumed = 0;
        const int parsed = std::stoi(text, &consumed);
        if (consumed == text.size() && parsed >= min_value && parsed <= max_value) {
            return parsed;
        }
    } catch (const std::exception&) {
        // 落到下面统一的回退路径
    }

    LOG_ERR << "环境变量 " << name << " 非法，使用默认值 " << fallback;
    return fallback;
}

}  // namespace


int main() {
    // ==================== 第 1 层：基础资源 ====================

    // 会话表（连接身份 id -> username / username -> 连接身份 id）
    SessionStore sessions;

    // 缓存映射表（username -> userid / MySQL源）
    UserIdCache user_ids;

    // 运行参数全部来自环境变量；默认值对应 scripts/init_db.sql 的本地演示配置
    const int port = env_int("CHAT_PORT", 8080, 1, 65535);
    const int worker_count = env_int("CHAT_WORKERS", 6, 1, 256);
    const int db_pool_size = env_int("CHAT_DB_POOL", 6, 1, 256);
    const int db_port = env_int("CHAT_DB_PORT", 3306, 1, 65535);
    const std::string db_host = env_string("CHAT_DB_HOST", "127.0.0.1");
    const std::string db_user = env_string("CHAT_DB_USER", "chat_user");
    const std::string db_password = env_string("CHAT_DB_PASSWORD", "chat_password");
    const std::string db_name = env_string("CHAT_DB_NAME", "chat_db");

    LOG_INFO << "配置  port=" << port
             << " workers=" << worker_count
             << " db_pool=" << db_pool_size
             << " db=" << db_user << "@" << db_host << ":" << db_port << "/" << db_name;

    // 线程池
    ThreadPool thread_pool(static_cast<std::size_t>(worker_count));
    thread_pool.start();  // 两段式：start() 才创建 worker 线程

    // MySQL 连接池
    MysqlConnectionPool db_pool(db_host,
                                db_user,
                                db_password,
                                db_name,
                                static_cast<std::uint16_t>(db_port),
                                static_cast<std::size_t>(db_pool_size));
    db_pool.start();  // 两段式：start() 才创建 N 条 MySQL 连接

    // ==================== 第 2 层：DAO 层 ====================

    UserDao user_dao(db_pool);
    FriendDao friend_dao(db_pool);
    MessageDao message_dao(db_pool);

    // ==================== 第 3 层：Service 层 ====================

    // TCP 连接管理器（需要在这里创建，因为 Service 层和 EventLoop 都要用）
    TcpConnection tcp_conn(sessions);

    // 发送消息回调（封装 tcp_conn.add_to_send_queue）
    auto send_to_client = [&tcp_conn](ConnectionId id, const std::string& msg) {
        return tcp_conn.add_to_send_queue(id, msg);
    };

    // 断开旧连接回调（顶号时用；单参：epoll_fd 已由 TcpConnection 持有）
    auto disconnect_old_client = [&tcp_conn](ConnectionId id) {
        tcp_conn.disconnect_client(id);
    };

    // 认证服务
    AuthService auth_service(user_dao,
                             sessions,
                             user_ids,
                             send_to_client,
                             disconnect_old_client);

    // 消息服务
    MessageService message_service(message_dao,
                                   sessions,
                                   user_ids,
                                   send_to_client);

    // ==================== 第 4 层：消息处理器 ====================

    // 消息处理回调（解析协议 + 分发到 Service）
    auto client_data_handler = [&](std::shared_ptr<Connection> conn, const std::string& raw) {
        // 空消息直接返回
        if (raw.empty()) return;

        // 任务闭包持有 shared_ptr conn：任务途中连接对象不析构，fd 号不归还内核
        // 函数间传递连接身份信息（ConnectionId），保证跨 DB I/O 等操作时不会对被复用的 fd 进行错误操作
        const ConnectionId id = conn->id();

        std::string line = raw;

        // 清除尾部垃圾符号（\n, \r）
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
            line.pop_back();
        }

        // 登录
        if (line.rfind("login:", 0) == 0) {
            std::string rest = line.substr(6);
            size_t pos = rest.find(" ");
            if (pos == std::string::npos || pos == 0) {
                send_to_client(id, "[系统] 非法登录格式  正确格式:login:用户名 密码\n");
                return;
            }
            std::string username = rest.substr(0, pos);
            std::string password = rest.substr(pos + 1);
            auth_service.handle_login(id, username, password);
            return;
        }

        // 私聊
        if (line.rfind("to ", 0) == 0) {
            std::string rest = line.substr(3);
            size_t pos = rest.find(" ");
            if (pos == std::string::npos || pos == 0) {
                send_to_client(id, "[系统] 非法私聊格式  正确格式:to 用户名 消息内容\n");
                return;
            }
            std::string target = rest.substr(0, pos);
            std::string content = rest.substr(pos + 1);
            message_service.send_private(target, content + "\n", id);
            return;
        }

        // 广播
        message_service.broadcast(line + "\n", id);
    };

    // ==================== 第 5 层：网络层 ====================

    EventLoop event_loop(port,
                         thread_pool,
                         tcp_conn,
                         sessions,
                         client_data_handler);

    // ==================== 启动服务器 ====================

    LOG_INFO << "聊天服务器启动中...";
    const bool ok = event_loop.run();

    // ==================== 清理资源 ====================

    thread_pool.stop();  // 顺序：先停线程池（等 worker 把在途 DB 任务做完）
    db_pool.stop();      // 再关连接池（此时已无借用者，可安全释放）
    LOG_INFO << "服务器已关闭";

    return ok ? 0 : 1;  // POSIX 硬规定：0 = 成功
}
