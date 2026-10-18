#include "mysql_connect_pool.h"
#include "../common/logger.h"    // LOG_ERR/LOG_INFO：日志统一出口
#include <vector>

// ==================== MySQL 连接 ====================

// 构造：保存配置（连接创建留 2 阶段 start()）
MysqlConnectionPool::MysqlConnectionPool(std::string host,
                                         std::string username,
                                         std::string password,
                                         std::string db,
                                         uint16_t port,
                                         size_t conn_capacity)
    : host_(host),
      username_(username),
      password_(password),
      db_(db),
      port_(port),
      conn_capacity_(conn_capacity) {}

MysqlConnectionPool::~MysqlConnectionPool(){
    stop();  // RAII 兜底
}

// 创建新连接
MYSQL* MysqlConnectionPool::create_conn() {
    MYSQL* conn = mysql_init(nullptr);
    if (conn == nullptr) {
        LOG_ERR << "mysql_init 失败";
        return nullptr;
    }

    // 超时设置：连接 / 读写
    unsigned int t_connect = connect_timeout_s_;  // 建连（握手+认证）最大时间
    unsigned int t_opt = opt_timeout_s_;  // 读写慢操作 / 等锁最大时间
    mysql_options(conn,MYSQL_OPT_CONNECT_TIMEOUT,&t_connect);
    mysql_options(conn,MYSQL_OPT_READ_TIMEOUT,&t_opt);
    mysql_options(conn,MYSQL_OPT_WRITE_TIMEOUT,&t_opt);

    if (mysql_real_connect(conn, host_.c_str(), username_.c_str(), password_.c_str(), db_.c_str(), port_, nullptr, 0) == nullptr) {
        LOG_ERR << "连接创建失败：" << mysql_error(conn);
        mysql_close(conn);
        return nullptr;
    }

    return conn;  // 成功返回连接句柄
}

// 启动：创建 N 条连接入空闲队列（只允许调用一次）
void MysqlConnectionPool::start() {
    size_t success_count = 0;

    for(size_t i = 0; i < conn_capacity_; ++i){
        MYSQL* conn = create_conn();
        int recreate_count = 0;
        while(conn == nullptr && recreate_count < 3) {
            conn = create_conn();
            ++recreate_count;
        }
        if(conn == nullptr) {
            LOG_ERR << "第 [" << (i + 1) << "] 条连接创建失败"
                      << "（重试 [" << (recreate_count) << "/3] 次），"
                      << "当前容量 [" << success_count << "/" << conn_capacity_ << "]";
            continue;  // 3 次重试依旧失败：跳过当前连接，连接池容量 -1
        }
        idle_conn_.push(conn);  // 成功连接入队
        ++success_count;
    }

    if (success_count < conn_capacity_) {
        LOG_ERR << "连接池容量永久下降 | "
                  << "当前容量 [" << success_count << "/" << conn_capacity_ << "]";
    }
}

// 借出 MySQL conn ：守卫原地构造（C++17 复制省略），借用权随对象流转
PooledConnection MysqlConnectionPool::borrow() {
    std::unique_lock<std::mutex> lock(mutex_);

    // borrow_timeout_ms_ 超时判定 + 谓词：队列非空 / 停止
    cv_.wait_for(lock, borrow_timeout_ms_,[&]{ return !idle_conn_.empty() || stopping_; });

    // stopping_ = true / 借出 conn 超时未归还 -> 检查连接池容量
    if (idle_conn_.empty()) {
        LOG_ERR << "连接池借出失败（" << (stopping_ ? "已停止" : "等待归还超时") << "）";
        return PooledConnection(*this, nullptr);
    }

    MYSQL* conn = idle_conn_.front();
    idle_conn_.pop();
    return PooledConnection(*this, conn);  // 守卫构造，析构自动归还
}

// 归还：连接归队 + 唤醒等待者
void MysqlConnectionPool::return_conn(MYSQL* conn) {
    if(conn == nullptr) return;
    if(mysql_ping(conn) != 0){
        mysql_close(conn);
        MYSQL* new_conn = create_conn();
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if(new_conn != nullptr){
                if (stopping_) {  // 池已停止，停止补全连接池
                    mysql_close(new_conn);
                    return;
                }
                idle_conn_.push(new_conn);  // 错误连接舍弃，新连接补全连接池
            }else if(!stopping_) {  // 池已停止：容量不再有意义，不告警
                LOG_ERR << "新连接创建失败，连接池容量永久 -1";
            }
        }
    }else{
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {  // 池已停止，跳过归队直接关闭
            mysql_close(conn);
            return;
        }
        idle_conn_.push(conn);
    }
    cv_.notify_one();  // 解锁 + 唤醒线程
}

// 停止：关闭空闲队列中的全部连接（幂等）
void MysqlConnectionPool::stop() {
    std::vector<MYSQL*> conns;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;  // 连接池停止标志

        while(!idle_conn_.empty()){
            conns.push_back(idle_conn_.front());
            idle_conn_.pop();
        }
    }

    cv_.notify_all();  // 唤醒所有等待者
    for(MYSQL* c : conns){
        mysql_close(c);
    }
}

// ==================== 守卫 ====================

// 构造：连接池 + 句柄
PooledConnection::PooledConnection(MysqlConnectionPool& pool, MYSQL* conn)
    : pool_(pool), conn_(conn) {}

PooledConnection::~PooledConnection() {
    pool_.return_conn(conn_);  // 析构 + 归还连接
}

// 移动构造：转移借用权 + 原守卫置空（避免同时析构时双重归还）
PooledConnection::PooledConnection(PooledConnection&& other) noexcept
    : pool_(other.pool_), conn_(other.conn_) {
        other.conn_ = nullptr;
}

// 获取连接句柄：通过封装实现分层
MYSQL* PooledConnection::get() const {
    return conn_;
}