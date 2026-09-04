/**
 * chat_server.cpp — M4 消息分发服务器（W8D6 学生填空）
 *
 * 聊天服务器：epoll 事件循环 + 连接表 + 广播/私聊路由 + 线程池集成
 * 基于：M3 epoll 事件循环（v3）+ M4 线程池本体（D5 产出）
 *
 * 填空规则：
 *   🧠 常规逻辑 —— 你应该直接写（M3 已学的 epoll/socket 用法）
 *   💡 提示引导 —— 先看提示自己试，卡住再问
 *   🔑 关键难点 —— 想一下仍卡住就向 AI 要这行答案 + 解释
 *
 * 编译：cd /home/ghl/Cpp_Job_2026/projects/chat_system/m4_threadpool && g++ -std=c++17 -Wall -Wextra -pedantic chat_server.cpp ThreadPool.cpp -o chat_server -I/usr/include/mysql -lmysqlclient

 * 运行：./chat_server
 * 测试：多开终端 ./chat_client
 */

#include "ThreadPool.h"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <mysql/mysql.h>
#include <nlohmann/json.hpp>

constexpr int PORT = 8080;
constexpr int BACKLOG = 8;
constexpr int MAX_EVENTS = 16;
constexpr int BUFFER_SIZE = 4096;
constexpr int MAX_LINE = 1024;          // 单条消息最大长度（超出截断）
volatile sig_atomic_t running = 1;

void handle_signal(int) { running = 0; }

// ==================== 工具函数（M3 已学，填空） ====================

// 非阻塞设置：fcntl(F_GETFL) 读旧标志 → 或 O_NONBLOCK → F_SETFL 写回
bool set_nonblocking(int fd);           // 🧠 M3 已实现过，直接写

// 注册 EPOLLIN 到 epoll 实例（用 LT 还是 ET？提示：M3 验证过 ET 要排空）
bool add_epollin(int epoll_fd, int fd); // 🧠 M3 已实现过，直接写

// ==================== 服务器类 ====================

class ChatServer {
public:
    explicit ChatServer(size_t num_workers,MYSQL* db): pool_(num_workers),db_(db) {}

    // 启动：注册信号 → 建 listen socket → 建 epoll → 启动线程池 → 事件循环
    void run();

private:
    //  连接表（W8D6 第1步设计） 
    std::unordered_map<int, std::string> connections_;  // fd -> username（""=未登录）
    std::unordered_map<std::string, int> users_;        // username -> fd（私聊用）
    std::mutex table_mutex_;                            // 🔒 保护两张表

    // 线程池 
    ThreadPool pool_;

    // MySQL
    MYSQL* db_;     //MYSQL连接
    std::unordered_map<std::string,int> user_ids_;  //username -> user_id(用户名映射数据库主键)
    std::mutex mysql_mutex_;

    // 连接生命周期 
    void on_accept(int listen_fd, int epoll_fd);
    // 从 epoll 移除并 close；⭐连接表清理顺序：先查 fd 拿 username → 删 users_ → 删 connections_
    void disconnect_client(int fd, int epoll_fd);

    // 消息处理（worker 线程执行，池化任务） 
    // Echo 排空 send：🔑 关键难点 —— 短写循环补发 + EAGAIN 时 return（不丢数据，等 EPOLLOUT）
    //     执行语义：返回值 true=发完；false=EAGAIN 或错误
    bool send_all(int fd, const std::string& msg);

    // 查询用户ID--无登录验证
    int query_id_by_name(const std::string& username);

    // 查询用户ID--登录验证
    int query_user_id(const std::string& username,const std::string& password);

    // 查询 user_id 对应的全部 friend_id。
    // true：查询成功，允许结果为空；false：数据库操作失败。
    bool query_friend_ids(int user_id, std::vector<int>& friend_ids);

    struct MessageRecord {
        // TODO(W9D4-2): 保存 history 响应需要的消息列。
        unsigned int id;
        unsigned int sender_id;
        unsigned int receiver_id;
        std::string content;
    };

    // 查询两个用户之间最近 20 条消息，结果保持 id 降序。
    // true：查询成功，允许结果为空；false：数据库操作失败。
    bool query_message_history(int user_id, int peer_id, std::vector<MessageRecord>& messages);

    // 将查询结果序列化成单行 JSON；不得在 MySQL/table 锁内调用。
    std::string build_history_response(const std::string& peer,const std::vector<MessageRecord>& messages);

    // 私聊信息落库
    bool save_message(int sender_id,int receiver_id,const std::string& content);

    // 登录：connections_[fd] = username; users_[username] = fd
    //     💡 加锁：改表需要 table_mutex_，锁内做修改，锁外 send 回复
    void handle_login(int fd, const std::string& username,const std::string& password);

    // 广播：遍历 users_ 群发；方案A —— 跳过发送者自己（乐观更新）
    //     💡 锁粒度：锁内拷贝所有目标 fd 到 vector，解锁后逐个 send_all
    void broadcast(const std::string& content, int sender_fd);

    // 私聊：users_.find(target) O(1) 查找；查不到 → 给发送者回提示
    //     💡 同样：锁内 find + 拷贝 target_fd，锁外 send_all（避免持锁 IO）
    void send_private(const std::string& target, const std::string& content, int sender_fd);

    // 协议解析与分发：login: 前缀 → handle_login；"to " 前缀 → send_private；其他 → broadcast
    void handle_message(int fd, const std::string& line);
};

// ==================== 成员函数骨架 ====================

bool set_nonblocking(int fd) {

    //读取旧标志
    int old_flags = fcntl(fd,F_GETFL,0);
    if(old_flags < 0){
        std::perror("fcntl(F_GETFL) 读取旧标志失败");
        return false;
    }

    //设置非阻塞
    if(fcntl(fd,F_SETFL,old_flags | O_NONBLOCK) < 0){
        std::perror("fcntl(F_SETFL) 设置非阻塞失败");
        return false;
    }

    return true;
}

bool add_epollin(int epoll_fd, int fd) {

    //fd注册epoll实例
    epoll_event event_{};
    event_.events = EPOLLIN | EPOLLET;
    event_.data.fd = fd;

    if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,fd,&event_) < 0){
        std::perror("epoll_ctl(EPOLL_CTL_ADD) 注册epoll失败");
        return false;
    }

    return true;
}

void ChatServer::on_accept(int listen_fd, int epoll_fd) {
    // 🧠 M3 accept_new_client 复用：循环 accept 到 EAGAIN（ET）
    // 💡 关键新点：accept 成功后立即 connections_[fd] = ""（登记匿名连接）
    //      这一步必须在主线程做（改表 → 加 table_mutex_）
    int client_fd = -1;

    //accept端口--ET
    for(;;){
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        client_fd = accept(listen_fd,(sockaddr*)& client_addr,&client_len);
        
        if(client_fd < 0){
            if(errno == EINTR) continue;
            if(errno == EAGAIN || errno == EWOULDBLOCK) break;
            std::perror("accept 失败");
            break;
        }else{
            {
                std::lock_guard<std::mutex> lock(table_mutex_);
                connections_[client_fd] = "";   //录入哈希表，等待登录
            }
            
            std::cout<<"accept 成功  客户端IP:"<<inet_ntoa(client_addr.sin_addr)
                        <<"  端口："<<ntohs(client_addr.sin_port)<<std::endl;
        }

        //单一client_fd状态设置失败不应该打断后续fd，用continue
        //设置非阻塞
        if(!set_nonblocking(client_fd)){
            close(client_fd);
            continue;
        }
        //注册epoll
        if(!add_epollin(epoll_fd,client_fd)){
            close(client_fd);
            continue;
        }

        std::cout<<"客户端已连接，等待用户登录  client_fd="<<client_fd<<std::endl;
    }

    

}

void ChatServer::disconnect_client(int fd, int epoll_fd) {
    // 🧠 轮廓（M3 清理 + W8D6 表清理）：
    //   ① epoll_ctl DEL + close(fd)
    //   ② 🔑 双向表删除顺序（面试答法）：
    //      先查 connections_[fd] 拿到 username（唯一的获取途径）
    //      → users_.erase(username)
    //      → connections_.erase(fd)
    //      💡 加锁：改表要 table_mutex_
    //   ③ 但要小心：未登录连接（username=""）users_ 里没有它，erase("") 无害但没必要，可加 if

    epoll_ctl(epoll_fd,EPOLL_CTL_DEL,fd,nullptr);
    close(fd);

    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        auto it = connections_.find(fd);
        if(it != connections_.end()){
            if(!it->second.empty()){
                users_.erase(it->second);
            }
            connections_.erase(it);
        }
    }
}

bool ChatServer::send_all(int fd, const std::string& msg) {
    // 🔑 关键难点 —— 今天核心绑定点（EAGAIN 排空循环）
    //   轮廓：
    //   size_t sent = 0;
    //   while (sent < msg.size()) {
    //       ssize_t n = send(fd, msg.data() + sent, msg.size() - sent, 0);
    //       if (n < 0) {
    //           if (errno == EAGAIN || errno == EWOULDBLOCK) {
    //               💡 缓冲区满：不能死等！剩余数据由上层发送队列接管（本版本直接放弃本轮）
    //               🔑 记忆：recv 的 EAGAIN=读空 break；send 的 EAGAIN=满 return
    //               → 返回 false
    //           }
    //           perror("send 失败");
    //           → 返回 false
    //       }
    //       sent += n;   // 短写：指针前移，长度减少
    //   }
    //   return true;
    
    //发送数据
    size_t sent = 0;    //已发送字节数
    
    while(sent < msg.size()){
        ssize_t n = send(fd,msg.data()+sent,msg.size()-sent,0);
        if(n < 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                std::cout<<"缓冲区已满"<<std::endl;
                return false;
            }
            std::perror("send 失败");
            return false;
        }
        sent += n;
    }
    return true;
}

int ChatServer::query_id_by_name(const std::string& username){
     std::lock_guard<std::mutex> lock(mysql_mutex_);

    MYSQL_STMT* stmt = mysql_stmt_init(db_);
    if(!stmt){
        std::cerr<<"mysql_stmt_init 失败："<<mysql_error(db_)<<'\n';
        return -1;
    }

    const char sql[] = "SELECT id FROM users WHERE username = ?";
    if(mysql_stmt_prepare(stmt,sql,strlen(sql)) != 0){
        std::cerr<<"mysql_stmt_prepare 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }

    unsigned long username_len = username.size();

    MYSQL_BIND bind[1];
    memset(bind,0,sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(username.data());
    bind[0].buffer_length = username.size();
    bind[0].length = &username_len;

    if(mysql_stmt_bind_param(stmt,bind) != 0){
        std::cerr<<"mysql_stmt_bind_param 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }

    int id_out;

    MYSQL_BIND bind_res[1]; 
    memset(bind_res,0,sizeof(bind_res));
    bind_res[0].buffer_type = MYSQL_TYPE_LONG;
    bind_res[0].buffer = &id_out;

    if(mysql_stmt_bind_result(stmt,bind_res) != 0){
        std::cerr<<"mysql_stmt_bind_result 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }

    if(mysql_stmt_execute(stmt) != 0){
        std::cerr<<"mysql_stmt_bind_result 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }

    int fetch_res = mysql_stmt_fetch(stmt);
    if(fetch_res != 0){
        if(fetch_res == MYSQL_NO_DATA){
            std::cout<<"用户不存在"<<std::endl;
            mysql_stmt_close(stmt);
            return -1;
        }
        std::cerr<<"mysql_stmt_fetch 失败："<<mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return -1;
    }else{
        std::cout<<"用户ID查找成功"<<std::endl;
        mysql_stmt_close(stmt);
        return id_out;
    }
}

int ChatServer::query_user_id(const std::string& username,const std::string& password){
    std::lock_guard<std::mutex> lock(mysql_mutex_);

    MYSQL_STMT* stmt = mysql_stmt_init(db_);    //初始化数据库连接
    if(!stmt){
        std::cerr<<"mysql_stmt_init 失败："<<mysql_error(db_)<<'\n';
        return -1;
    }

    //查询用户名ID--防范MySQL语句注入
    const char sql[] = "SELECT id,password FROM users WHERE username = ?";
    if(mysql_stmt_prepare(stmt,sql,strlen(sql)) != 0){
        std::cerr<<"mysql_stmt_prepare 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }

    //绑定查询和接收数组
    unsigned long username_len = username.size();

    MYSQL_BIND bind[1];
    memset(bind,0,sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(username.data());
    bind[0].buffer_length = username.size();
    bind[0].length = &username_len;

    if(mysql_stmt_bind_param(stmt,bind) != 0){
        std::cerr<<"mysql_stmt_bind_param 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }

    int id_out;
    char password_out[64];
    unsigned long password_len;

    MYSQL_BIND bind_res[2]; 
    memset(bind_res,0,sizeof(bind_res));
    bind_res[0].buffer_type = MYSQL_TYPE_LONG;
    bind_res[0].buffer = &id_out;
    bind_res[1].buffer_type = MYSQL_TYPE_STRING;
    bind_res[1].buffer = password_out;
    bind_res[1].buffer_length = sizeof(password_out);
    bind_res[1].length = &password_len;

    if(mysql_stmt_bind_result(stmt,bind_res) != 0){
        std::cerr<<"mysql_stmt_bind_result 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }
    
    //启动stmt
    if(mysql_stmt_execute(stmt) != 0){
        std::cerr<<"mysql_stmt_execute 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return -1;
    }

    //查询username并接收ID，password
    int fetch_res = mysql_stmt_fetch(stmt);
    if(fetch_res != 0){
        if(fetch_res == MYSQL_NO_DATA){
            std::cout<<"用户不存在"<<std::endl;
            mysql_stmt_close(stmt);
            return -1;
        }
        std::cerr<<"mysql_stmt_fetch 失败："<<mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return -1;
    }else{
        if(strcmp(password.c_str(),password_out) == 0){
            std::cout<<"用户ID查找成功"<<std::endl;
            mysql_stmt_close(stmt);
            return id_out;
        }else{
            std::cerr<<"用户ID查找失败，密码错误"<<'\n';
            mysql_stmt_close(stmt);
            return -1;
        }
    }
}

bool ChatServer::query_friend_ids(int user_id, std::vector<int>& friend_ids){

    friend_ids.clear(); //清空容器

    std::lock_guard<std::mutex> lock(mysql_mutex_); //互斥锁，保护MySQL操作

    MYSQL_STMT* stmt = mysql_stmt_init(db_);
    if(!stmt){
        std::cerr << "mysql_stmt_init 失败: "<< mysql_error(db_) << '\n';
        return false;
    }

    const char sql[] = "SELECT friend_id FROM friendships WHERE user_id = ? ORDER BY created_at DESC";
    if(mysql_stmt_prepare(stmt,sql,strlen(sql)) != 0){
        std::cerr << "mysql_stmt_prepare 失败: "<< mysql_stmt_error(stmt) << '\n';
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[1];
    memset(bind,0,sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &user_id;

    if(mysql_stmt_bind_param(stmt,bind) != 0){
        std::cerr << "mysql_stmt_bind_param 失败: "<< mysql_stmt_error(stmt) << '\n';
        mysql_stmt_close(stmt);
        return false;
    }

    int friend_id_out = -1;

    MYSQL_BIND bind_res[1];
    memset(bind_res,0,sizeof(bind_res));
    bind_res[0].buffer_type = MYSQL_TYPE_LONG;
    bind_res[0].buffer = &friend_id_out;

    if(mysql_stmt_bind_result(stmt,bind_res) != 0){
        std::cerr << "mysql_stmt_bind_result 失败: "<< mysql_stmt_error(stmt) << '\n';
        mysql_stmt_close(stmt);
        return false;
    }

    if(mysql_stmt_execute(stmt) !=0){
        std::cerr << "mysql_stmt_execute 失败: "<< mysql_stmt_error(stmt) << '\n';
        mysql_stmt_close(stmt);
        return false;
    }

    for(;;){
        int fetch_res = mysql_stmt_fetch(stmt);

        //查找成功--continue查找所有
        if(fetch_res == 0){
            friend_ids.push_back(friend_id_out);
            continue;
        }

        //没有好友关系
        if(fetch_res == MYSQL_NO_DATA){
            break;
        }

        //查找失败
        std::cerr << "mysql_stmt_fetch 失败: "<< mysql_stmt_error(stmt) << '\n';
        mysql_stmt_close(stmt);
        return false;
    }

    mysql_stmt_close(stmt);
    return true;
}

// W9D4-2 TODO: 在此实现 query_message_history 与 build_history_response。
// 1. SQL 使用双向 OR，四个参数依次绑定 (user, peer, peer, user)。
// 2. ORDER BY id DESC LIMIT 20；fetch 复用 0 / MYSQL_NO_DATA / error 三态。
// 3. mysql_mutex_ 覆盖 init/prepare/bind/execute/fetch/close 完整生命周期。
// 4. JSON 数组保持查询顺序；空结果返回 messages: []，不算查询失败。
// 查询两个用户之间最近 20 条消息，结果保持 id 降序。
// true：查询成功，允许结果为空；false：数据库操作失败。
bool ChatServer::query_message_history(int user_id, int peer_id, std::vector<MessageRecord>& messages){
    messages.clear();

    std::lock_guard<std::mutex> lock(mysql_mutex_);

    bool select_ok = false;
    std::string select_error;

    MYSQL_STMT* stmt = mysql_stmt_init(db_);
    if(!stmt){
        select_error = mysql_error(db_);
    }else{
        do{
            const char sql[] = "SELECT id,sender_id,receiver_id,content FROM messages WHERE (sender_id=? AND receiver_id=?) OR (sender_id=? AND receiver_id=?) ORDER BY id DESC LIMIT 20";
            if(mysql_stmt_prepare(stmt,sql,strlen(sql)) != 0){
                select_error = mysql_stmt_error(stmt);
                break;
            }

            MYSQL_BIND bind[4];
            memset(bind,0,sizeof(bind));

            bind[0].buffer_type = MYSQL_TYPE_LONG;
            bind[0].buffer = &user_id;
            bind[1].buffer_type = MYSQL_TYPE_LONG;
            bind[1].buffer = &peer_id;
            bind[2].buffer_type = MYSQL_TYPE_LONG;
            bind[2].buffer = &peer_id;
            bind[3].buffer_type = MYSQL_TYPE_LONG;
            bind[3].buffer = &user_id;

            if(mysql_stmt_bind_param(stmt,bind) != 0){
                select_error = mysql_stmt_error(stmt);
                break;
            }

            //接收结果
            unsigned int id;
            unsigned int sender_id;
            unsigned int receiver_id;
            char content[4096]{};
            unsigned long content_len = 0;

            MYSQL_BIND bind_res[4];
            memset(bind_res,0,sizeof(bind_res));

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

            if(mysql_stmt_bind_result(stmt,bind_res) != 0){
                select_error = mysql_stmt_error(stmt);
                break;
            }

            if(mysql_stmt_execute(stmt) != 0){
                select_error = mysql_stmt_error(stmt);
                break;
            }

            for(;;){
                int fetch_res = mysql_stmt_fetch(stmt);
                if(fetch_res == 0){
                    messages.push_back(MessageRecord{id,sender_id,receiver_id,std::string(content,content_len)});
                    continue;
                }
                
                if(fetch_res == MYSQL_NO_DATA){
                    select_ok = true;
                    break;
                }

                select_error = mysql_stmt_error(stmt);
                break;
            }
            
        }while(false);
        mysql_stmt_close(stmt);
    }
    
    if(!select_ok){
        std::cerr<<"查询历史消息失败："<<select_error<<std::endl;
    }
    return select_ok;
}

// 将查询结果序列化成单行 JSON；不得在 MySQL/table 锁内调用。
std::string ChatServer::build_history_response(const std::string& peer,const std::vector<MessageRecord>& messages){
    nlohmann::json response;

    response["type"] = "history";
    response["peer"] = peer;
    response["messages"] = nlohmann::json::array(); //初始化为空->否则消息可能接收为null

    //循环写入messages
    for(const auto& m : messages){
        nlohmann::json item;

        item["id"] = m.id;
        item["sender_id"] = m.sender_id;
        item["receiver_id"] = m.receiver_id;
        item["content"] = m.content;

        response["messages"].push_back(item);
    }

    //.dump()生成单行紧凑JSON，\n作为消息边界
    return response.dump()+"\n";
}

bool ChatServer::save_message(int sender_id,int receiver_id,const std::string& content){
    std::lock_guard<std::mutex> lock(mysql_mutex_);
    
    MYSQL_STMT* stmt = mysql_stmt_init(db_);
    if(!stmt){
        std::cerr<<"mysql_stmt_init 失败："<<mysql_error(db_)<<'\n';
        return false;
    }
    const char sql[] = "INSERT INTO messages (sender_id,receiver_id,content) VALUES (?,?,?)";

    if(mysql_stmt_prepare(stmt,sql,strlen(sql)) != 0){
        std::cerr<<"mysql_stmt_prepare 失败："<<mysql_stmt_error(stmt)<<'\n';
        mysql_stmt_close(stmt);
        return false;
    }

    unsigned long content_len = content.size();
    MYSQL_BIND bind[3];
    memset(bind,0,sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &sender_id;
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &receiver_id;
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = const_cast<char*>(content.data());
    bind[2].buffer_length = content.size();
    bind[2].length = &content_len;
    if(mysql_stmt_bind_param(stmt,bind) != 0){
        std::cerr << "mysql_stmt_bind_param 失败: " << mysql_stmt_error(stmt) << '\n';
        mysql_stmt_close(stmt);
        return false;
    }

    bool ok = (mysql_stmt_execute(stmt) == 0);
    if (!ok) std::cerr << "INSERT 失败: " << mysql_stmt_error(stmt) << '\n';
    mysql_stmt_close(stmt);
    return ok;

}

void ChatServer::handle_login(int fd, const std::string& username,const std::string& password){
    
    //获取user_id
    int user_id = query_user_id(username,password);
    if(user_id == -1){
        send_all(fd,"[系统] 登录失败：用户名或密码错误\n");
        return;
    }

    //登录成功
    {
        //锁内更新
        std::lock_guard<std::mutex> lock(table_mutex_);
        connections_[fd]=username;
        users_[username]=fd;
    }
    {
        //锁内更新
        std::lock_guard<std::mutex> lock(mysql_mutex_);
        user_ids_[username] = user_id;
    }

    //暂时忽略返回错误处理，交由disconnect统一管理生命周期

    //广播所有人
    send_all(fd,"[ChatSystem] 欢迎 "+username+"\n");  // 系统消息加换行
}

void ChatServer::broadcast(const std::string& content, int sender_fd) {
    // 🧠 W8D6 第2步设计落地：
    //   ① 锁内：遍历 users_，把 fd != sender_fd 的目标 fd 拷贝到本地 vector
    //   ② 🔓 解锁
    //   ③ 锁外：对每个目标 fd 调 send_all(fd, content)
    //   💡 锁内为什么只拷贝不发送？—— 慢客户端 EAGAIN 会持锁阻塞 → 线程池退化单线程
    std::vector<int> fd_send;
    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        for(auto [username,fd] : users_){
            if(fd == sender_fd) continue;
            fd_send.push_back(fd);
        }
    }
    for(int f : fd_send){
        send_all(f,content);//暂时忽略返回错误处理，交由disconnect统一管理生命周期
    }
}

void ChatServer::send_private(const std::string& target, const std::string& content, int sender_fd) {
    //   ① 🔒 锁内：auto it = users_.find(target);
    //   ② 查不到（it == users_.end()）→ 锁外给 sender_fd 回 "[系统] 用户 xx 不在线或不存在"
    //   ③ 查到了 → 拷贝 target_fd = it->second → 🔓 解锁 → 锁外 send_all(target_fd, content)
    //   ⚠️ 注意：不要在锁内直接 send（同 broadcast 理由）
    int target_fd;
    {
        //锁内取表元素
        std::lock_guard<std::mutex> lock(table_mutex_);
        auto it = users_.find(target);
        if(it == users_.end()){
            target_fd = -1;
        }else{
            target_fd = it->second;
        }
    }

    //私聊失败
    if(target_fd == -1){
        send_all(sender_fd, "[系统] 用户 " + target + " 离线或不存在\n");  // 系统消息加换行
        return;
    }

    //私聊成功 -> 落库
    std::string my_name;
    {
        //锁内查username
        std::lock_guard<std::mutex> lock(table_mutex_);
        auto it = connections_.find(sender_fd);
        if(it != connections_.end()) my_name = it->second;
    }
    int sender_id = -1,receiver_id = -1;
    {
        //锁内查id
        std::lock_guard<std::mutex> lock(mysql_mutex_);
        auto s = user_ids_.find(my_name);
        if(s != user_ids_.end()) sender_id = s->second;
        auto r = user_ids_.find(target);
        if(r != user_ids_.end()) receiver_id = r->second;
    }
    if(receiver_id == -1) receiver_id = query_id_by_name(target);   //目标离线时通过数据库核验id进行离线兜底
    if(sender_id != -1 && receiver_id != -1){
        if(!save_message(sender_id,receiver_id,content)){
            send_all(sender_fd,"[系统] 消息发送失败，消息未保存\n");
            return;
        }
    }else{
        std::cerr<<"[落库失败]  sender_id="<<sender_id<<"  receiver_id="<<receiver_id<<'\n';
        send_all(sender_fd,"[系统] 消息发送失败，无法确认用户信息\n");
        return;
    }

    //发送消息
    send_all(target_fd,content);  // 用户消息不加（本身带\n）
}

void ChatServer::handle_message(int fd, const std::string& raw) {

    //空消息直接返回
    if(raw.empty()) return;

    std::string line = raw;

    //清除尾部垃圾符号（\n,\r,' '）
    while(!line.empty() && (line.back()=='\n' || line.back()=='\r')){
        line.pop_back();
    }

    //登录
    if(line.rfind("login:",0) == 0){
        std::string rest = line.substr(6);

        size_t pos = rest.find(" ");    //定位用户名和内容的分割符：" "
        if(pos == std::string::npos || pos == 0){
            send_all(fd,"[系统] 非法登录格式  正确格式:login:用户名 密码\n");// 系统消息加换行
            return;
        }
        std::string username = rest.substr(0,pos);  //0-pos之间为接收用户名
        std::string password = rest.substr(pos+1);  //pos后一字符为内容起点

        handle_login(fd,username,password);
        return;
    }

    //私聊
    if(line.rfind("to ",0) == 0){
        std::string rest = line.substr(3);

        size_t pos = rest.find(" ");    
        if(pos == std::string::npos || pos == 0){
            send_all(fd,"[系统] 非法私聊格式 正确格式:to 接收者名 消息\n");  
            return;
        }
        std::string target = rest.substr(0,pos);    
        std::string content = rest.substr(pos+1);   

        send_private(target,content,fd);
        return;
    }

    //添加好友
    if(line.rfind("addfriend:",0) == 0){
        std::string rest = line.substr(10);

        if(rest.empty()){
            std::cout<<"[系统] 非法添加好友  正确格式:addfriend:用户名\n";
            return;
        }

        //查找发送者 username 
        bool sender_logged_in = false;
        std::string my_name;
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            auto it = connections_.find(fd);
            if(it != connections_.end() && !it->second.empty()){
                my_name = it->second;
                sender_logged_in = true;
            }
        }

        if(!sender_logged_in){
            send_all(fd, "[系统] 请先登录\n");
            return;
        }

        //查找发送者 ID
        int my_id = -1;
        {
            std::lock_guard<std::mutex> lock(mysql_mutex_);
            auto it = user_ids_.find(my_name);
            if(it != user_ids_.end()){
                my_id = it->second;
            }
        }

        if(my_id == -1){
            send_all(fd,"[系统] 登录状态异常，请重新登录");
            return;
        }

        //查找目标ID -> 添加好友关系(双向) -> 落库
        int friend_id = query_id_by_name(rest);
        if(friend_id == my_id){
            send_all(fd,"[系统] 不能添加自己为好友\n");
            return;
        }
        if(friend_id == -1){
            send_all(fd,"[系统] 用户 <"+rest+"> 不存在\n");
            return;
        }

        //stmt -> MySQL查询语句
        bool insert_ok = false;
        unsigned int insert_errno = 0;
        std::string insert_error;
        
        {
            //锁内MySQL操作
            std::lock_guard<std::mutex> lock(mysql_mutex_);

            MYSQL_STMT* stmt = mysql_stmt_init(db_);
            if (!stmt) {
                insert_error = mysql_error(db_);
            }else{
                do{
                    const char sql[] = "INSERT INTO friendships (user_id,friend_id) VALUES (?,?),(?,?)";
                    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
                        insert_error = mysql_stmt_error(stmt);
                        break;
                    }

                    //参数绑定--friendships 好友关系表--双向连接
                    MYSQL_BIND bind[4];
                    memset(bind,0,sizeof(bind));
                    bind[0].buffer_type = MYSQL_TYPE_LONG;
                    bind[0].buffer = &my_id;
                    bind[1].buffer_type = MYSQL_TYPE_LONG;
                    bind[1].buffer = &friend_id;
                    bind[2].buffer_type = MYSQL_TYPE_LONG;
                    bind[2].buffer = &friend_id;
                    bind[3].buffer_type = MYSQL_TYPE_LONG;
                    bind[3].buffer = &my_id;
                    

                    if(mysql_stmt_bind_param(stmt,bind) != 0){
                        insert_error = mysql_stmt_error(stmt);
                        break;
                    }

                    if(mysql_stmt_execute(stmt) != 0){
                        insert_errno = mysql_stmt_errno(stmt);  //记录真实错误码
                        insert_error = mysql_stmt_error(stmt);  //记录真实错误   
                    }else{
                        insert_ok = true;
                    }
                }while(false);

                mysql_stmt_close(stmt);     //统一关闭句柄
            }
        }

        //锁外业务响应
        if(insert_ok){
            send_all(fd,"[系统] 添加好友成功\n");
            return;
        }

        if(insert_errno == 1062){
            send_all(fd,"[系统] 你们已经是好友了\n");
            return;
        }

        std::cerr<<"INSERT 失败："<<insert_error;
        send_all(fd,"[系统] 好友添加失败\n");
        return;
    }

    
    // W9D4-2 TODO: 在默认广播前新增 `history <username>` 分支。
    // - table_mutex_ 内只复制当前登录用户名，目标用户允许离线。
    // - 锁外查询双方 ID，调用 query_message_history，再构造 JSON 并 send_all。
    // - 错误路径：格式非法 / 未登录 / 用户不存在 / 数据库失败；空历史不是错误。
    // nc 验收：双向发送后按 id DESC 返回最多 20 条；服务重启并重新登录后结果仍在。
    if(line.rfind("history ",0) == 0){
        std::string rest = line.substr(8);
        if(rest.empty()){
            send_all(fd,"[系统] 错误查询格式  正确格式:history 用户名\n");
            return;
        }

        //查找my_name -> my_id -> rest查找peer_id
        std::string my_name;
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            auto it = connections_.find(fd);
            if(it != connections_.end() && !it->second.empty()){
                my_name = it->second;
            }
        }
        if(my_name.empty()){
            send_all(fd,"[系统] 请先登录\n");
            return;
        }

        int my_id = query_id_by_name(my_name);
        if(my_id == -1){
            send_all(fd,"[系统] 登录用户不存在\n");
            return;
        }

        int peer_id = query_id_by_name(rest);
        if(peer_id == -1){
            send_all(fd,"[系统] 查找对象不存在\n");
            return;
        }

        //接收history
        std::vector<MessageRecord> messages;
        if(!query_message_history(my_id,peer_id,messages)){
            send_all(fd,"[系统] 查找历史消息失败\n");
            return;
        }

        //拼接JSON -> send_all
        std::string response = build_history_response(rest,messages);

        send_all(fd,response);
        return;
    }

    //查询好友列表
    if(line == "listfriends"){
        bool logged_in = false;
        std::string my_name;

        {
            //锁内MySQL查询name事务
            std::lock_guard<std::mutex> lock(table_mutex_);

            auto it = connections_.find(fd);
            if(it != connections_.end() && !it->second.empty()){
                my_name = it->second;
                logged_in = true;
            }
        }
        //锁外send事务
        if(!logged_in){
            send_all(fd,"[系统] 请先登录\n");
            return;
        }

        //查询user_id
        int user_id = query_id_by_name(my_name);
        if(user_id == -1){
            send_all(fd,"[系统] 登录用户不存在\n");
            return;
        }

        //查询好友关系表
        std::vector<int> friend_ids;

        if(!query_friend_ids(user_id,friend_ids)){
            send_all(fd,"[系统] 查询好友列表失败\n");
            return;
        }
        if(friend_ids.empty()){
            send_all(fd,"[系统] 好友列表为空\n");
            return;
        }

        //查找成功--拼接结果
        std::string response = "[系统] 好友ID列表：";

        for(size_t i = 0;i<friend_ids.size();++i){
            if(i > 0) response += ",";
            response += std::to_string(friend_ids[i]);
        }
        response += "\n";
        
        //返回结果
        send_all(fd,response);
        return;
    }

    //默认广播
    broadcast(line,fd);
}

void ChatServer::run() {
    // 🧠 M3 main() 复用 + 新集成点：
    //   ① 注册 SIGINT（sigaction）
    //   ② create listen socket（M3 的 3 步），非阻塞
    //   ③ epoll_create1 + 注册 listen fd
    //   ④ pool_.start();               ← ⭐ 启动线程池
    //   ⑤ 主循环：
    //      epoll_wait → EINTR continue
    //      → listen 可读 → on_accept
    //      → HUP/ERR → disconnect_client
    //      → EPOLLIN → recv 排空到 EAGAIN（M3 三分支）
    //           recv > 0 → pool_.submit([this, fd, line] { handle_message(fd, line); })
    //                       ⭐ 线程池集成点：主线程只收数据，业务路由丢给 worker
    //           recv == 0 → disconnect_client（主线程直接清理，别排队）
    //           recv < 0 EAGAIN → break（读空）
    //   ⑥ 清理：pool_.stop() → close(epoll_fd) → close(listen_fd)

    //注册信号
    struct sigaction signal_act{};
    signal_act.sa_handler = handle_signal;
    signal_act.sa_flags = 0;                    //不设自动重启等
    sigemptyset(&signal_act.sa_mask);           //清空合集
    if(sigaction(SIGINT,&signal_act,nullptr) < 0){
        std::perror("sigaction(SIGINT) 注册信号失败");
        return;
    }

    //创建socket-->绑定IP,端口-->bind-->listen监听端口-->设置非阻塞
    const int listen_fd = socket(AF_INET,SOCK_STREAM,0);
    if(listen_fd < 0){
        std::perror("socket 创建失败");
        return;
    }
    sockaddr_in listen_addr{};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(PORT);
    listen_addr.sin_addr.s_addr = INADDR_ANY;

    if(bind(listen_fd,(sockaddr*)& listen_addr,sizeof(listen_addr)) < 0){
        std::perror("bind 绑定失败");
        close(listen_fd);
        return;
    }
    
    if(listen(listen_fd,BACKLOG) < 0){
        std::perror("listen 监听失败");
        close(listen_fd);
        return;
    }
    
    if(!set_nonblocking(listen_fd)){
        close(listen_fd);
        return;
    }

    //创建epoll实例-->注册epoll
    const int epoll_fd = epoll_create1(0);
    if(epoll_fd < 0){
        std::perror("epoll_create1 失败");
        close(listen_fd);
        return;
    }
    if(!add_epollin(epoll_fd,listen_fd)){
        close(epoll_fd);
        close(listen_fd);
        return;
    }

    std::cout<<"[EPOLL][非阻塞监听启动]  epoll_fd="<<epoll_fd<<"  listen_fd="<<listen_fd<<std::endl;

    //主循环
    //线程池启动
    pool_.start();
    std::cout<<"poll_[start]"<<std::endl;

    epoll_event ready_events[MAX_EVENTS]{};     //就绪fd集合

    while(running){
        const int ready_count = epoll_wait(epoll_fd,ready_events,MAX_EVENTS,-1);
        if(ready_count < 0){
            if(errno == EINTR) continue;     //信号打断
            std::perror("epoll_wait 失败");
            break;
        }

        //循环取出所有ready_event
        for(int count = 0;count < ready_count;++count){
            const int ready_fd = ready_events[count].data.fd;       //就绪事件fd
            const uint32_t ready_flags = ready_events[count].events;//就绪事件状态

            //listen_fd监听-->添加新客户端连接
            if(ready_fd == listen_fd && (ready_flags & EPOLLIN)){
                on_accept(listen_fd,epoll_fd);
                continue;
            }

            //客户端事件处理
            
            if(ready_flags & (EPOLLHUP | EPOLLERR)) //连接异常
            {
                std::cout<<"[EPOLLHUP | EPOLLERR] 当前客户端异常断开  client_fd="<<ready_fd<<std::endl;
                disconnect_client(ready_fd,epoll_fd);
                continue;   //跳过连接异常客户端，进行下一个客户端事件处理
            }
            else if(ready_flags & EPOLLIN)  //连接正常
            {
                std::cout<<"[EPOLLIN] 客户端连接正常  client_fd="<<ready_fd<<std::endl;

                char buffer[BUFFER_SIZE];
                for(;;){
                    ssize_t b_read = recv(ready_fd,buffer,BUFFER_SIZE-1,0);

                    //异常/挂起
                    if(b_read < 0){
                        if(errno == EINTR){
                            std::cout<<"[EPOLL][EINTR] 信号打断"<<std::endl;
                            continue;
                        }                         
                        if(errno == EAGAIN || errno == EWOULDBLOCK){
                            std::cout<<"[EPOLL][EAGAIN | EWOULDBLOCK] 队列读空"<<std::endl;
                            break;
                        } 
                        std::perror("[EPOLL]recv 失败");//接收错误                           
                        disconnect_client(ready_fd,epoll_fd);
                        break;
                    }

                    //对端关闭
                    if(b_read == 0){
                        std::cout<<"[EPOLL] 客户端关闭连接"<<std::endl;
                        disconnect_client(ready_fd,epoll_fd);
                        break;
                    }

                    //业务处理
                    buffer[b_read] = '\0';  //C边界识别
                    std::string line(buffer);
                    pool_.submit([this,ready_fd,line]{ handle_message(ready_fd,line);});
                }
            }
        }
    }

    //线程池停止
    pool_.stop();
    std::cout<<"poll_[stop]"<<std::endl;
    close(epoll_fd);
    close(listen_fd);
    std::cout<<"服务器正常退出"<<std::endl;
    return;
}

//连接MySQL
MYSQL* db_connect(){
    MYSQL* conn = mysql_init(NULL);
    if(conn == NULL) return NULL;

    if(mysql_real_connect(conn,"localhost","root","123","chat_db",0,NULL,0) == NULL){
        std::cerr<<"数据库chat_db连接失败";
        return NULL;
    }
    std::cout<<"数据库chat_db连接成功"<<std::endl;
    return conn;
}
// ==================== main ====================

int main() {
    MYSQL* db = db_connect();   //连接数据库
    if(db == NULL){
        std::cerr<<"MySQL连接失败，服务器退出"<<'\n';
        return 1;
    }

    ChatServer server(6,db);    //初始化服务器
    server.run();               //运行服务器
    mysql_close(db);
    return 0;

}
