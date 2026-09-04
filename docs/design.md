
ChatSystem 架构设计
总体架构
采用 Reactor 模式 + 线程池：

Main Thread：epoll 监听网络事件（EPOLLIN/EPOLLOUT/EPOLLHUP/EPOLLERR）
Worker Threads：线程池处理业务逻辑 + 数据库访问
网络层 (M1-M3)
epoll 边缘触发 (ET)
单次 EPOLLIN 必须循环读至 EAGAIN（否则漏数据）
单次 EPOLLOUT 必须循环写至 EAGAIN（否则粘包）
EPOLLHUP/EPOLLERR 触发连接清理
连接管理

std::unordered_map<int, ClientState> clients;  // fd -> state
存储每个连接的读缓冲、写缓冲、登录用户名
异常断开时双向清理：clients.erase(fd) + epoll_ctl(DEL)
并发层 (M4)
ThreadPool 设计
任务队列：std::queue<std::function<void()>>
同步机制：mutex 保护队列，condition_variable 等待任务
生产者：Main Thread 收到完整请求后 push
消费者：Worker 线程 pop + 执行业务 + 回写响应
执行流程

Client -> [EPOLLIN] -> Main Thread 读数据 -> 判断完整性
   -> push 任务队列 -> Worker pop -> 业务逻辑 -> 写响应到缓冲
   -> [EPOLLOUT] -> Main Thread 发送 -> Client
业务层 (M4-M5)
支持的功能
用户注册 / 登录
添加好友（双向关系）
广播消息（向所有在线用户）
私聊消息（点对点 + 持久化）
历史消息查询（最近 20 条）
协议格式

请求：command:ARG1 ARG2\n
响应：[ChatSystem] message\n
持久化层 (M5)
表结构
users：id, username, password (明文，仅测试)
friendships：user_id, friend_id (双向存储)
messages：id, sender_id, receiver_id, content, created_at
索引设计
friendships: (user_id, friend_id) 联合唯一索引
messages: (receiver_id, id) 联合索引 (支持 ORDER BY id DESC LIMIT 20)
性能优化方向
已完成
epoll ET 减少系统调用
线程池复用线程
Prepared Statement 预编译
待优化 (M11-M12)
HTTP 接口支持 (M11)
MySQL 连接池 (M12)
更多细节见各模块源码注释。
