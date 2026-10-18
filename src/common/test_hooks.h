#ifndef TEST_HOOKS_H
#define TEST_HOOKS_H

/**
 * test_hooks.h — 测试构建专用同步钩子与观测接口
 *
 * 仅在 -DCHAT_TEST_HOOKS 构建下存在；正常构建里两个宏展开为空操作，
 * 不产生暂停、输出或任何业务语义变化。
 *
 * 运行期开关（环境变量）：
 *   CHAT_HOOK_LOG           记录文件路径（JSONL）。未设置 = 钩子全部空转，
 *                           回归/压测可复用同一个二进制。
 *   CHAT_HOOK_PAUSE         "0" 时只记录不暂停（默认暂停）
 *   CHAT_HOOK_PAUSE_ON_IDS  事件循环批次暂停过滤：批次身份集合命中任一值才暂停（逗号分隔）
 *   CHAT_HOOK_DEADLINE_MS   暂停截止，默认 30000；超时落 timeout 记录后继续
 *
 * pause 语义：先落一条 phase=ready 记录（带 seq），再轮询 <log>.resume 里是否出现该 seq。
 * 于是"已到达暂停点"是可确认事件；调用方不得用 sleep 代替该确认。
 */

#if defined(CHAT_TEST_HOOKS)

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "connection_id.h"

struct epoll_event;

namespace test_hooks {

/// 记录一条观测事件（不停顿）
void mark(const char* tag, const std::string& detail);

/// 到达暂停点：落 ready 记录后阻塞，直到被恢复（<log>.resume 出现该 seq）或截止
void pause(const char* tag, const std::string& detail);

/// 事件循环批次观测：记录本批事件的身份集合；命中 CHAT_HOOK_PAUSE_ON_IDS 时暂停
void observe_event_loop_batch(const struct epoll_event* events, int count);

/// 注册表观测：登记 / 注销（注销记录附带该身份遗留的孤儿路由数）
void observe_create(std::uint64_t id, std::size_t registry_size);
void observe_disconnect(std::uint64_t id, std::size_t registry_size, std::size_t orphan_routes);

/// 逗号分隔的身份列表（用作 mark 的 detail）
std::string format_ids(const std::vector<ConnectionId>& ids);

}  // namespace test_hooks

#define TH_MARK(tag, detail) ::test_hooks::mark(tag, detail)
#define TH_PAUSE(tag, detail) ::test_hooks::pause(tag, detail)

#else

#define TH_MARK(tag, detail) ((void)0)
#define TH_PAUSE(tag, detail) ((void)0)

#endif  // CHAT_TEST_HOOKS

#endif  // TEST_HOOKS_H
