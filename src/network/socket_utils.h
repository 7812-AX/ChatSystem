#ifndef SOCKET_UTILS_H
#define SOCKET_UTILS_H

#include "../common/connection_id.h"

/**
 * socket_utils.h — 网络工具函数
 *
 * 提供 socket 相关的工具函数：
 * - set_nonblocking: 设置文件描述符为非阻塞模式
 * - add_epoll_in: 将文件描述符注册到 epoll 实例
 */

/**
 * 设置文件描述符为非阻塞模式
 * @param fd 文件描述符
 * @return true=成功，false=失败
 */
bool set_nonblocking(int fd);

/**
 * 将文件描述符 + 连接身份号注册到 epoll 实例，监听 EPOLLIN 事件（ET 模式）
 * @param epoll_fd epoll 实例文件描述符
 * @param fd 要注册的文件描述符
 * @param id 连接身份号
 * @return true=成功，false=失败
 */
bool add_epoll_in(int epoll_fd, int fd, ConnectionId id);

#endif // SOCKET_UTILS_H
