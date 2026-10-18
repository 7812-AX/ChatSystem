#include "message_service.h"
#include "../common/logger.h"    // LOG_ERR/LOG_INFO：日志统一出口
#include "../common/test_hooks.h"  // 测试构建专用同步钩子（正常构建为空宏）
#include <nlohmann/json.hpp>

MessageService::MessageService(MessageDao& message_dao,
                               SessionStore& sessions,
                               UserIdCache& user_ids,
                               std::function<bool(ConnectionId, const std::string&)> send_to_client)
    : message_dao_(message_dao),
      sessions_(sessions),
      user_ids_(user_ids),
      send_to_client_(send_to_client) {}

void MessageService::broadcast(const std::string& content, ConnectionId sender_id) {
    // 认证是广播的前置条件：未登录连接（含从未登记/已断开）不得触达其他用户
    if (sessions_.find_user(sender_id).empty()) {
        LOG_ERR << "未登录连接尝试广播，已拒绝  id=" << static_cast<std::uint64_t>(sender_id);
        send_to_client_(sender_id, "[系统] 请先登录\n");
        return;
    }

    // 会话表内部锁内拷贝已登录用户 id 数组
    std::vector<ConnectionId> send_ids = sessions_.snapshot_logged_in_ids_except(sender_id);
    TH_PAUSE("message.broadcast.snapshot",
             "count=" + std::to_string(send_ids.size()) + " ids=" + test_hooks::format_ids(send_ids));

    // 锁外发送：慢客户端 EAGAIN 会持锁阻塞 → 线程池退化单线程
    for (ConnectionId id : send_ids) {
        // 入队即受理：返回 false = 未入队（目标不在/已关）或已入队但未挂上 EPOLLOUT
        [[maybe_unused]] const bool delivered = send_to_client_(id, "[广播]" + content);
        TH_MARK("message.broadcast.delivered",
                "tid=" + std::to_string(static_cast<std::uint64_t>(id)) +
                    " ok=" + (delivered ? "1" : "0"));
    }
    TH_MARK("message.broadcast.done", "count=" + std::to_string(send_ids.size()));
}

void MessageService::send_private(const std::string& target_name,
                                 const std::string& content,
                                 ConnectionId sender_id) {
    // 查找目标用户的连接身份 id
    std::optional<ConnectionId> target_id = sessions_.find_connection_id(target_name);

    // 私聊失败（用户离线或不存在）
    if (!target_id) {
        send_to_client_(sender_id, "[系统] 用户 " + target_name + " 离线或不存在\n");
        return;
    }

    // 私聊成功 → 落库
    // 测试钩子：目标身份已固定、尚未进入 DB —— 窗口就在这里，可确认地暂停
    TH_PAUSE("message.private.target_resolved",
             "target=" + target_name + " tid=" + std::to_string(static_cast<std::uint64_t>(*target_id)));

    std::string my_name = sessions_.find_user(sender_id);

    // 查询发送者和接收者的 user_id
    std::optional<int> sender_user_id = user_ids_.get(my_name);
    std::optional<int> receiver_user_id = user_ids_.get(target_name);

    // 路由命中 -> 该用户已登录过 -> 缓存必命中（登录写序先缓存后路由 + 缓存只增不删）-> 不设回源

    // 两个 user_id 齐备才落库；任一缺失即拒绝 —— 缺 id 还往下走会解引用空 optional
    // 先落库，后投递
    if (!sender_user_id.has_value() || !receiver_user_id.has_value()) {
        LOG_ERR << "[落库条件缺失] sender_user_id=" << sender_user_id.value_or(-1)
                  << " receiver_user_id=" << receiver_user_id.value_or(-1);
        send_to_client_(sender_id, "[系统] 消息发送失败，无法确认用户信息\n");
        return;
    }

    const bool saved = message_dao_.save_message(*sender_user_id, *receiver_user_id, content);
    TH_MARK("message.private.db_result", std::string("ok=") + (saved ? "1" : "0"));
    if (!saved) {
        LOG_ERR << "[落库失败] sender_user_id=" << sender_user_id.value_or(-1)
                  << " receiver_user_id=" << receiver_user_id.value_or(-1);
        send_to_client_(sender_id, "[系统] 消息发送失败，消息未保存\n");
        return;
    }

    // 按照连接身份投递，落库期间目标断连 + 注销则丢弃投递的消息
    // 不将旧消息投递复用同一个 fd 的新连接
    // 用户消息不加换行（本身带 \n）；返回值用于向发送方回执（投递层状态，非端到端 ACK）
    const bool delivered = send_to_client_(*target_id, "[私聊|" + my_name + "]" + content);
    TH_MARK("message.private.delivered",
            "tid=" + std::to_string(static_cast<std::uint64_t>(*target_id)) +
                " ok=" + (delivered ? "1" : "0"));
    TH_MARK("message.private.done", "");
    if (delivered) {
        send_to_client_(sender_id, "[系统] 消息已受理\n");
    } else {
        send_to_client_(sender_id, "[系统] 消息已保存，本次投递发生异常，送达状态未确认\n");
    }
}

std::string MessageService::build_history_response(const std::string& peer,
                                                  const std::vector<MessageRecord>& messages) {
    nlohmann::json response;

    response["type"] = "history";
    response["peer"] = peer;
    response["messages"] = nlohmann::json::array();  // 初始化为空 → 否则消息可能接收为 null

    // 循环写入 messages
    for (const auto& m : messages) {
        nlohmann::json item;

        item["id"] = m.id;
        item["sender_id"] = m.sender_id;
        item["receiver_id"] = m.receiver_id;
        item["content"] = m.content;

        response["messages"].push_back(item);
    }

    // .dump() 生成单行紧凑 JSON，\n 作为消息边界
    return response.dump() + "\n";
}
