#ifndef FRIEND_DAO_H
#define FRIEND_DAO_H

/**
 * friend_dao.h — 好友表数据访问对象
 *
 * 提供好友关系相关的数据库操作：
 * - query_friend_ids: 查询用户的所有好友ID
 */

#include <vector>
#include "mysql_connect_pool.h"

class FriendDao {
public:
    /**
     * 构造函数
     * @param pool 所属连接池
     */
    explicit FriendDao(MysqlConnectionPool& pool);

    /**
     * 查询用户的所有好友ID
     * @param user_id 用户ID
     * @param friend_ids 输出参数，好友ID列表（会先清空）
     * @return true=查询成功（允许结果为空），false=数据库操作失败
     */
    bool query_friend_ids(int user_id, std::vector<int>& friend_ids);

private:
    MysqlConnectionPool& pool_;  // 所属连接池
};

#endif // FRIEND_DAO_H
