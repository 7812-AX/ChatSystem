#ifndef USER_DAO_H
#define USER_DAO_H

/**
 * user_dao.h — 用户表数据访问对象
 *
 * 提供用户相关的数据库操作：
 * - query_id_by_name: 通过用户名查询用户ID
 * - verify_login: 登录验证（用户名+密码）
 */

#include <string>
#include "mysql_connect_pool.h"

class UserDao {
public:
     /**
     * verify_login 返回码语义（与 AuthService 的判定共用）
     * - LoginUserFail    : 业务失败 —— 用户名不存在或密码错误（故意不区分，不泄露账号是否存在）
     * - LoginSysBusy : 系统侧失败 —— 连接池借出失败 / SQL 执行失败（与业务失败区分，避免误导用户）
     * 成功返回 user_id (>= 0)
     */
    static constexpr int LoginUserFail = -1;
    static constexpr int LoginSysBusy = -2;

    /**
     * 构造函数
     * @param pool 所属连接池
     */
    explicit UserDao(MysqlConnectionPool& pool);

    /**
     * 通过用户名查询用户ID（无密码验证）
     * @param username 用户名
     * @return 用户ID；用户名不存在或密码错误返回 LoginUserFail；系统侧失败返回 LoginSysBusy
     */
    int query_id_by_name(const std::string& username);

    /**
     * 登录验证：通过用户名和密码查询用户ID
     * @param username 用户名
     * @param password 密码
     * @return 用户ID (>= 0)；用户名不存在或密码错误返回 LoginUserFail；系统侧失败返回 LoginSysBusy
     */
    int verify_login(const std::string& username, const std::string& password);

private:
    MysqlConnectionPool& pool_;  // 所属连接池
};

#endif // USER_DAO_H
