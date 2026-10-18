#ifndef CONNECTION_ID_H
#define CONNECTION_ID_H

/**
 * connection_id.h —— 连接身份
 *
 * 进程内单调递增的连接身份号：1，2，3，...
 * - 只增不复用：采用 uint64_t 有效防止超限（按每秒 1 万连接算，2⁶⁴ 可分配约 5800 万年）
 * - 0 保留（invalid），作为 epoll 中监听 fd 的标识
 *
 * 作为 connection 跨线程、跨 I/O 传递时的唯一标识，不受 fd 号归还等操作的影响
 */

#include <cstdint>

enum class ConnectionId : std::uint64_t { invalid = 0 };

#endif // CONNECTION_ID_H
