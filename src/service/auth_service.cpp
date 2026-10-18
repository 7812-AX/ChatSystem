#include "auth_service.h"
#include "../common/logger.h"    // LOG_ERR/LOG_INFO：日志统一出口
#include "../common/test_hooks.h"  // 测试构建专用同步钩子（正常构建为空宏）

AuthService::AuthService(UserDao& user_dao,
                         SessionStore& sessions,
                         UserIdCache& user_ids,
                         std::function<bool(ConnectionId, const std::string&)> send_to_client,
                         std::function<void(ConnectionId)> disconnect_old_client)
    : user_dao_(user_dao),
      sessions_(sessions),
      user_ids_(user_ids),
      send_to_client_(send_to_client),
      disconnect_old_client_(disconnect_old_client) {}

void AuthService::handle_login(ConnectionId id, const std::string& username, const std::string& password) {
    // 获取 user_id
    int user_id = user_dao_.verify_login(username, password);


    if (user_id == UserDao::LoginSysBusy) {  // 系统侧失败
        send_to_client_(id, "[系统] 服务繁忙，请稍后重试\n");
        return;
    }
    if (user_id == UserDao::LoginUserFail) {  // 用户侧失败
        send_to_client_(id, "[系统] 登录失败：用户名或密码错误\n");
        return;
    }

    // 登录成功
    user_ids_.put(username, user_id);  // 先更新缓存映射表：路由可见时缓存必已就绪（读者先查路由、再查缓存）

    auto bind = sessions_.bind_login(id, username);  // 更新会话表
    if (!bind.ok) {
        LOG_ERR << "会话表信息登记失败；id=" << static_cast<std::uint64_t>(id) << " / " << "username=" << username << "未入表";
        return;
    }

    // 顶号：断开被本次登录替换掉的旧连接（old_id == id 表示同一连接重复登录，不能断自己）
    if (bind.old_id.has_value() && *bind.old_id != id) {
        // 测试钩子：旧身份已取到、清理尚未执行 —— 窗口就在这里，可确认地暂停
        TH_PAUSE("auth.login.old_resolved",
                 "id=" + std::to_string(static_cast<std::uint64_t>(id)) +
                     " old=" + std::to_string(static_cast<std::uint64_t>(*bind.old_id)));
        disconnect_old_client_(*bind.old_id);
        TH_MARK("auth.login.old_cleaned",
                "old=" + std::to_string(static_cast<std::uint64_t>(*bind.old_id)));
    }

    // 发送欢迎消息
    send_to_client_(id, "[ChatSystem] 欢迎 " + username + "\n");  // 入队即受理：返回 false 只说明该连接已经不在了
    TH_MARK("auth.login.welcome_sent", "id=" + std::to_string(static_cast<std::uint64_t>(id)));
}
