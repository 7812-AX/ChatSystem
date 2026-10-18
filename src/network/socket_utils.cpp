#include "socket_utils.h"
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstdio>

bool set_nonblocking(int fd) {
    // 读取旧标志
    int old_flags = fcntl(fd, F_GETFL, 0);
    if (old_flags < 0) {
        std::perror("fcntl(F_GETFL) 读取旧标志失败");
        return false;
    }

    // 设置非阻塞
    if (fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) < 0) {
        std::perror("fcntl(F_SETFL) 设置非阻塞失败");
        return false;
    }

    return true;
}

bool add_epoll_in(int epoll_fd, int fd, ConnectionId id) {
    // fd注册epoll实例
    epoll_event event_{};
    event_.events = EPOLLIN | EPOLLET;  // ET 模式
    event_.data.u64 = static_cast<std::uint64_t>(id);

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event_) < 0) {
        std::perror("epoll_ctl(EPOLL_CTL_ADD) 注册epoll失败");
        return false;
    }

    return true;
}
