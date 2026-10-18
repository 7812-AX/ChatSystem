#include "connection.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

#include "../common/logger.h"

Connection::Connection(int fd, int epoll_fd, ConnectionId id) : fd_(fd), epoll_fd_(epoll_fd), id_(id) {}

Connection::~Connection() {
    close_logical();  // 先停止连接 + 留 fd
    close(fd_);  // 最后进行物理归还
}

bool Connection::close_logical() {
    if (!closed_.exchange(true)) {  // 原子判定（从 false 翻成 true）：确保关闭逻辑只执行一次
        epoll_ctl(epoll_fd_,EPOLL_CTL_DEL,fd_,nullptr);
        shutdown(fd_,SHUT_RDWR);  // 读写同时关闭
        return true;
    }

    return false;
}

bool Connection::split_data_to_lines (const char* data, std::size_t len, std::vector<std::string>& out_lines){
    if (discarding_) {  // 丢弃态
        const void* p = memchr(data, '\n', len);
        if (p == nullptr) {
            return true;  // 仍是已检测超长行的续片，继续丢弃，不重复报告超长
        }
        const char* rest = static_cast<const char*>(p) + 1;
        len -= static_cast<std::size_t>(rest - data);
        data = rest;
        discarding_ = false;  // 已丢弃到换行，从其后字节恢复正常分帧
    }

    in_buf_ += std::string(data,len);

    bool overflowed = false;  // 本次是否切出了完整超长行；残段超长由下方分支单独报告
    for (;;) {
        size_t pos = in_buf_.find('\n');
        if (pos == std::string::npos) break;

        std::string line = in_buf_.substr(0, pos);
        if(line.size() > MAX_LINE) {
            in_buf_.erase(0, pos + 1);  // 连同行尾换行丢弃，边界已知，无需进入丢弃态
            overflowed = true;
            continue;
        } else {
            while(!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!line.empty()) {
                out_lines.push_back(line);
            }
            in_buf_.erase(0, pos + 1);
        }
    }

    if (in_buf_.find('\n') == std::string::npos && in_buf_.size() > MAX_LINE) {
        in_buf_.clear();
        discarding_ = true;
        return false;  // 未收齐换行的残段超长；此时 overflowed 仍可能为 false
    }

    return !overflowed;  // 报告本次是否发现完整超长行，调用方决定是否尝试入队提醒
}

bool Connection::enqueue_lines(const std::vector<std::string>& lines) {
    if (lines.empty()) return false;

    std::lock_guard<std::mutex> lock(queue_mutex_);
    pending_lines_.insert(pending_lines_.end(),lines.begin(),lines.end());

    if (worker_active_) return false;  // 已有 worker

    worker_active_ = true;
    return true;  // 无 worker 消费交由调用方提交消费
}

bool Connection::pop_line(std::string& line) {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    if (pending_lines_.empty()) {
        worker_active_ = false;
        return false;
    }

    line = std::move(pending_lines_.front());  // move 避免一次拷贝
    pending_lines_.pop_front();
    return true;
}

void Connection::abandon_pending() {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    pending_lines_.clear();
    worker_active_ = false;  // 清理资源 + 通知无 worker 消费
}

bool Connection::add_to_queue_out(std::string msg) {
    if (closed_.load()) return false;  // 已摘 epoll：再挂 EPOLLOUT 只会拿到 ENOENT

    std::lock_guard<std::mutex> lock(out_mutex_);  // 锁内：判定 + 标志更改 + 进化关注 fd events

    if (out_buf_.size() + msg.size() > MAX_OUT_BYTES) {  // 超限判定包含当前 msg
        LOG_ERR << "出站队列超限，断开客户端  fd=" << fd_
                << "  积压=" << out_buf_.size() << "B"
                << "  本条=" << msg.size() << "B"
                << "  上限=" << MAX_OUT_BYTES << "B";

        close_logical();
        epollout_armed_ = false;
        out_buf_.clear();
        return false;  // 超限断连
    }

    out_buf_ += msg;

    if (epollout_armed_) return true;  // 已挂起 EPOLLOUT

    epollout_armed_ = true;
    epoll_event ev{};  // 初始化 data.u64 = 0
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;  // 进化到关注可读 + 可写 + ET
    ev.data.u64 = static_cast<std::uint64_t>(id_);
    if(epoll_ctl(epoll_fd_,EPOLL_CTL_MOD,fd_,&ev) < 0) {
        const int err = errno;
        LOG_ERR << "登记 EPOLLOUT 失败  fd=" << fd_ << "  errno=" << err;
        epollout_armed_ = false;
        return false;
    }

    return true;
}

void Connection::send_all_queue_msg() {
    if(closed_.load()) return;

    for (;;) {
        std::string need_send_buf;
        {
            std::lock_guard<std::mutex> lock(out_mutex_);  // 锁内判定 + 标志更改 + 退化关注 fd events

            if (out_buf_.empty()) {
                if (epollout_armed_) {
                    epollout_armed_ = false;
                    epoll_event ev{};
                    ev.events = EPOLLIN | EPOLLET;  // 退化到只关注可读 + ET
                    ev.data.u64 = static_cast<std::uint64_t>(id_);
                    epoll_ctl(epoll_fd_,EPOLL_CTL_MOD,fd_,&ev);
                }
                return;  // 队空即收工
            }

            need_send_buf.swap(out_buf_);  // 锁内 swap 搬运，锁外 send
        }

        std::size_t sent = 0;
        bool temp_full = false;

        while (sent < need_send_buf.size()) {
            // MSG_NOSIGNAL：对端已关闭时send 返回 -1 + errno=EPIPE，不发 SIGPIPE（默认动作=终止整个进程）
            ssize_t n = send(fd_, need_send_buf.data() + sent, need_send_buf.size() - sent, MSG_NOSIGNAL);

            if (n > 0) {
                sent += static_cast<std::size_t>(n);
                continue;  // ET 循环发完
            }

            if (n == 0) {
                LOG_INFO << "发送返回 0，对端已关闭  fd=" << fd_;
                {
                    std::lock_guard<std::mutex> lock(out_mutex_);
                    out_buf_.clear();
                    epollout_armed_ = false;  // close_logical 摘 epoll，标志同步落回
                }
                close_logical();
                return;
            }

            if (n < 0) {
                if (errno == EINTR) continue;  // 信号打断

                if(errno == EAGAIN || errno == EWOULDBLOCK) {  // 写缓冲暂满
                    temp_full = true;
                    break;
                }
            }

            const int err = errno;  // 真实失败 + 清场
            LOG_INFO << "发送失败，断开客户端  fd=" << fd_ << "  errno=" << err;
            {
                std::lock_guard<std::mutex> lock(out_mutex_);
                out_buf_.clear();
                epollout_armed_ = false;  // 标志同步落回
            }
            close_logical();
            return;
        }

        if (temp_full) {  // 残留消息：置出站字节流头部等待后续补发（）
            std::lock_guard<std::mutex> lock(out_mutex_);
            out_buf_.insert(0, need_send_buf, sent, std::string::npos);
            return;
        }
    }
}
