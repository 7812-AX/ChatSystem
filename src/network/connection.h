#ifndef CONNECTION_H
#define CONNECTION_H

/**
 * connection.h — 单条客户端连接的身份与生命周期
 *
 * 所在层：网络层。被 TcpConnection（注册表）与 EventLoop（任务捕获）持有，不依赖业务层。
 * 职责范围：管住「这个 fd 号什么时候还给内核」——逻辑关闭（摘 epoll + 通知对端）与关闭状态查询。
 *           不持有会话表：fd → username 的增删由上层负责。
 *
 * 为什么需要它：
 *   线程池任务原本只捕获裸 int fd（event_loop.cpp 投递处）。事件循环在任务排队期间
 *   close(fd) 把号还给内核，下一次 accept 拿到同一个号 ⇒ 迟到的任务把本该发给旧连接的
 *   字节，写进了新连接的 socket。send 成功返回、日志无异常。
 *
 *   任务改为持有 shared_ptr<Connection> 之后：只要任务还活着，本对象就不析构，
 *   ~Connection 里的 close(fd) 就不发生 ⇒ fd 号不可能被复用。
 *   注意这不是「把竞态窗口缩小」，是把时序从等式里删掉：能不能复用不再取决于谁跑得快。
 *
 *   行分帧：in_buf_ 按连接攒字节，凑齐一行才交给上层。
 *   分帧必须在**事件循环线程**里做（recv 只在那里发生），所以 in_buf_ / discarding_ 天然单线程。
 *
 *   同连接任务串行化：pending_lines_ FIFO + worker_active_ 标志。
 *   与 in_buf_ 相反，这两个成员由事件循环（push）和 worker（pop）双方访问 ⇒ 必须加锁。
 *
 *   出站队列（已挂到这里）：out_buf_ + EPOLLOUT 补发。worker 只入队，
 *   真正的 send 由事件循环独家执行，见 send_all_queue_msg 的说明。
 */

#include <atomic>
#include <cstddef>
#include <string>
#include <vector>
#include <deque>
#include <mutex>
#include "../common/connection_id.h"

class Connection {
public:
    /**
     * @param fd 已 accept 的客户端描述符，本对象接管其所有权
     * @param epoll_fd 所属 epoll 实例，不拥有
     * @param id 连接身份（TcpConnection 分配；进程内唯一、永不复用）
     * @note 必须用 std::make_shared 创建，理由见 tcp_connection.h 的 create()
     * @note epoll_fd 在构造时定下：EventLoop::run() 里 set_epoll_fd 先于 accept 执行
     */
    Connection(int fd, int epoll_fd, ConnectionId id);

    /**
     * 析构：兜底逻辑关闭，然后 close(fd) 把 fd 号物理归还内核。
     * @note 全项目只有这里做 close(fd)，accept 失败路径除外
     * @note 可能跑在 worker 线程上（最后一个任务释放 shared_ptr 时），
     *       所以 close_logical 与 close 都必须能跨线程调用
     */
    ~Connection();

    // 禁拷贝：fd 的所有权是唯一的。若允许复制，两个副本析构时会各 close 一次同一个号，
    // 第二次 close 关的可能是已经被 accept 复用的别人的连接
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    /// fd 号。对象活着期间它保证有效且未被复用
    int fd() const { return fd_; }

    /// 连接身份：进程内唯一、永不复用。判是不是同一条连接的唯一标识
    ConnectionId id() const { return id_; }

    /**
     * 逻辑关闭：从 epoll 摘除 + 通知对端，但不释放 fd 号。
     * @return true 本次调用完成了关闭；false 之前已经关过（幂等，重复调用无副作用）
     * @note 顶号断开是从 worker 线程调进来的（auth_service 发起），所以本函数必须可并发调用：
     *       写成 if (closed_) return; closed_ = true; 是两个独立操作，两个线程可能同时通过判断
     * @note 这里用 shutdown(SHUT_RDWR) 而不是 close：close 会把 fd 号还给内核，
     *       而「号被还回去」正是本轮改造要杜绝的事。shutdown 只通知对端，号仍攥在本对象手里
     * @note 对端已关时 shutdown 返回 ENOTCONN，属正常，不当错误处理
     */
    bool close_logical();

    /// 关闭状态。任务在慢调用返回后可据此决定还要不要发送
    bool closed() const { return closed_.load(); }

    // ==================== 行分帧 ====================

    /// 行缓冲上限（字节，**不含**行尾换行）。取值与 event_loop.cpp 的 BUFFER_SIZE 一致：
    /// 一块 recv 装得下的行就一定能过，不会因为分块方式不同而时通时不通。
    static constexpr std::size_t MAX_LINE = 4096;

    /**
     * 将一段数据拆分成完整单行。
     * @param data 本次 recv 到的字节起点
     * @param len  本次 recv 到的字节数（可能含 0 个、1 个或多个 '\n'）
     * @param out_lines  出口。切出的完整行。不含行尾 '\n'、行尾单个 '\r' 已剥掉、空行不进 out
     * @return false = 本次发现完整超长行或首次发现残段超长；不表示所有输出行都无效。
     *         true = 本次没有新检测到超长；可能仍在丢弃已检测超长行的续片。
     * @note 本函数不发送提示。调用方在每次 handle_readable 内合并超长检测，
     *       最多尝试入队一条「消息过长」提示，不保证每条超长行各有回执或提示送达。
     * @note **只允许事件循环线程调用**。recv 只发生在 handle_readable 里，
     *       in_buf_ / discarding_ 因而天然单线程 —— **不要加锁**，加了就是白付代价，
     *       而且会把自己拖进「同一线程重复 lock 非递归互斥量」那类自死锁。
     * @note 已带 '\n' 的完整超长行连同换行直接丢弃，然后继续解析后续行。
     *       未带 '\n' 的残段超长时清空缓冲并进入丢弃态，后续续片丢到第一个 '\n' 为止；
     *       丢弃续片不重复报告该行超长，换行后的字节恢复正常分帧，避免把残段当作新命令。
     * @note 半包（一段里没收到 '\n'）时残段留在 in_buf_ 里等下一次 feed，本次不产出任何行。
     */
    bool split_data_to_lines(const char* data, std::size_t len, std::vector<std::string>& out_lines);

    // ==================== 单连接任务串行化 ====================
    /**
     * 把本次切出的行并入本连接的处理队列。
     * @return true  = 本连接此前空闲，调用方需提交一个 drain 任务来消费
     *         false = 已有 worker 在跑（它会把本次刚塞进去的行一并带走），或 lines 为空
     * @note 只在事件循环线程调用。这里**为什么反而要加锁**：pending_ / worker_active_
     *       由事件循环（push）和 worker（pop）双方访问 —— 这是 in_buf_ 之外唯一跨线程的状态。
     *       in_buf_ 不加锁、这里加锁，不是双标，是访问者不同。
     */
    bool enqueue_lines(const std::vector<std::string>& lines);

    /**
     * 取一条待处理行。
     * @return false = 队列已空，且 worker_active_ 已在**本次临界区内**置回 false
     */
    bool pop_line(std::string& line);

    /// 提交 drain 任务失败时回滚：队列还在这儿，但已经没人会来取了
    void abandon_pending();

    // ==================== 出站队列 ====================

    /// 出站队列上限（字节）。超限即判定该客户端「只发不收」，记日志并断开该连接。
    /// 取值约 1.8 倍于「内核发送缓冲灌满后、队列里实际会堆住」的量（实测约 2.2MB）。
    static constexpr std::size_t MAX_OUT_BYTES = 4u * 1024 * 1024;

    /**
     * 把一条消息追加到出站队列，必要时向 epoll 申请 EPOLLOUT。
     * @param msg 完整一行（包含行尾 '\n'）
     * @return true  = 已入队，发送时机由事件循环保证
     *         false = 连接已关闭，或队列超限（后者本函数已发起断连）
     * @note 可从任意线程调用（worker / 事件循环都走这里）。
     * @note **返回 false 不再等于「这条消息丢了」**。入队即受理，其后的 send / EAGAIN /
     *       补发全由事件循环负责到底。所以调用方沿用「忽略返回值」的写法，从这一刻起是
     *       **正确的**，不再是缺陷 —— message_service.cpp 里那句「暂时忽略返回错误处理」
     *       如今该读作设计，不是待办。不要再去"修"它。
     * @note 本函数自己解决唤醒：入队后若 EPOLLOUT 尚未挂上就 epoll_ctl(MOD) 挂上。
     *       内核在 MOD 时发现该 fd 当前可写，会立刻把它放进就绪链表并唤醒 epoll_wait
     *       （已实测）⇒ **不需要 eventfd**，socket 自己的可写性就是跨线程唤醒通道。
     */
    bool add_to_queue_out(std::string msg);

    /**
     * 把出站队列的数据发出去，直到 EAGAIN 或队列排空。
     * @note 只允许事件循环线程调用。全项目只有这里一处调用 send（单写者）
     *       发送消息只调 add_to_queue_out，不要绕过去碰 send。
     * @note 与 split_data_to_lines 同为「循环到 EAGAIN」的 ET 风格：进函数时队列非空，
     *       撞 EAGAIN 就把余量塞回**队首**并让 EPOLLOUT 继续挂着，内核腾出空位时会再来一次边缘。
     */
    void send_all_queue_msg();

private:
    const int fd_;        // 客户端 fd，本对象拥有，~Connection 负责 close
    const int epoll_fd_;  // epoll 实例，不拥有；EventLoop 在 run() 里注入，-1 = 未注入
    const ConnectionId id_;  // 连接身份（进程内单调递增，永不复用）
    std::atomic<bool> closed_{false};  // 逻辑关闭标志，由 close_logical 认领

    std::string in_buf_;       // 入队行缓冲：**只被事件循环线程读写**
    bool discarding_ = false;  // 越界丢弃态

    std::deque<std::string> pending_lines_;  // 待处理的行 FIFO（多线程访问）
    bool worker_active_  = false;  // 是否有 worker 在消费当前连接
    std::mutex queue_mutex_;  // 保护 pending_lines_ + worker_active_

    std::string out_buf_;  // 出站字节流（多线程访问）
    bool epollout_armed_ = false;  // EPOLLOUT 是否已挂在 epoll 上（避免多次 epoll_ctl）
    std::mutex out_mutex_;  // 保护 out_buf_ + epollout_armed_
};

#endif // CONNECTION_H
