#ifndef MYSQL_CONNECT_POOL_H
#define MYSQL_CONNECT_POOL_H

/**
 * mysql_connect_pool.h — MySQL 连接池
 *
 * 创建 N 条 MySQL 连接并复用，消除全局单连接串行化
 *
 * 提供 MySQL 连接池核心功能：
 * - start: 创建 N 条连接，形成空闲队列
 * - borrow: 借出一条空闲连接
 * - return_conn: 归还连接
 * - stop: 停止连接池维护，释放所有连接资源
 */

#include <mysql/mysql.h>
#include <condition_variable>
#include <chrono>
#include <mutex>
#include <queue>
#include <string>
#include <cstdint>

class PooledConnection;  // 守卫前置声明：borrow() 的返回类型

class MysqlConnectionPool {
public:
    /**
     * 构造函数
     * @param host 主机
     * @param username 用户名
     * @param password 数据库密码
     * @param db 数据库名
     * @param port 端口
     * @param conn_capacity 连接池大小（和线程池容量一致）
     */
    MysqlConnectionPool(std::string host,
                        std::string username,
                        std::string password,
                        std::string db,
                        uint16_t port,
                        size_t conn_capacity);

    ~MysqlConnectionPool();

    // 禁止拷贝：持有句柄 + 互斥锁，拷贝无意义
    MysqlConnectionPool(const MysqlConnectionPool&) = delete;
    MysqlConnectionPool& operator=(const MysqlConnectionPool&) = delete;

    /**
     * 启动连接池（只允许启动一次）
     * MySQL 连接失败重试 3 次；仍然失败跳过当前连接，连接池容量 -1 并告警
     */
    void start();

    /**
     * 借出一条空闲 MySQL 连接
     * 池空：cv.wait_for 阻塞等待，上限 borrow_timeout_ms ；超时 / 池停止 -> 返回空守卫
     * @return 池连接守卫（析构时自动归还）；成功携带真实信息 / 失败返回空守卫 nullptr
     */
    PooledConnection borrow();

    /**
     * 归还数据库连接到空闲队列
     * 连接检测：归还前探活，坏连接丢弃后创建新连接入连接池
     * @param conn 等待归还的连接句柄
     */
    void return_conn(MYSQL* conn);

    /**
     * 停止连接池 + 资源释放（幂等）
     * 置 stopping_ 并唤醒全部等待者：此后 borrow 立即返回空守卫
     */
    void stop();

private:
    /**
     * 创建一条新连接（start() 内部调用）
     * @return MYSQL* 数据库句柄
     */
    MYSQL* create_conn();

    std::string host_;              // 主机
    std::string username_;          // 用户名
    std::string password_;          // 数据库密码
    std::string db_;                // 数据库名
    uint16_t port_;                 // 端口
    size_t conn_capacity_;          // 总连接数量
    std::queue<MYSQL*> idle_conn_;  // 空闲句柄队列（可用连接池）
    std::mutex mutex_;              // 保护 idle_conn_ / stopping_
    std::condition_variable cv_;    // 池空时等待连接归还

    std::chrono::milliseconds borrow_timeout_ms_{100};  // 借出等待上限
    unsigned int connect_timeout_s_ = 2;                // MySQL 连接超时
    unsigned int opt_timeout_s_ = 1;                    // MySQL 读写超时
    bool stopping_ = false;                             // 池停止标志（stop() 置位，borrow 立即空守卫）
};

/**
 * 池连接守卫：持有借用权的外壳（RAII）
 */
class PooledConnection {
public:
    /**
     * 构造函数
     * @param pool 归属的连接池（析构时自动归还）
     * @param conn 借到的连接句柄（nullptr = 借用失败）
     */
    PooledConnection(MysqlConnectionPool& pool, MYSQL* conn);

    ~PooledConnection();  // 析构：归还连接句柄

    // 禁止拷贝：借用权不可复制
    PooledConnection(const PooledConnection&) = delete;
    PooledConnection& operator=(const PooledConnection&) = delete;

    // 移动构造：接管 other 的全部（池引用 + 连接）；移动赋值已删除
    PooledConnection(PooledConnection&& other) noexcept;

    // （引用成员 pool_ 无法重绑定，跨池赋值会把连接还给错的池）

    /**
     * 取连接句柄
     * @return 借到的 MYSQL* （失败时返回 nullptr）
     */
    MYSQL* get() const;

private:
    MysqlConnectionPool& pool_;  // 所属连接池
    MYSQL* conn_;                // 借到的连接句柄
};

#endif // MYSQL_CONNECT_POOL_H