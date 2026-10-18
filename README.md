# ChatSystem

基于 C++17 的轻量即时通信服务端：单 Reactor + epoll 边缘触发 + 固定线程池 + MySQL 连接池。项目重点在 Linux 网络编程、连接生命周期管理和并发路径上的资源安全，附带一个自研 TCP 负载驱动器用于复现性能数据。

## 核心特性

- **单 Reactor + epoll ET**：主线程负责 accept、收包、分帧和发包；读写都循环到 `EAGAIN`。
- **固定线程池**：默认 6 个 worker 执行业务与数据库操作，网络 I/O 与业务逻辑分离。
- **连接身份与 fd 生命周期**：用进程内单调递增的 `ConnectionId` 标识连接；在途任务持有 `shared_ptr<Connection>`，fd 号在任务结束前不会归还内核，避免 fd 复用导致的跨连接误写。
- **同连接请求串行化**：每条连接维护待处理行 FIFO，同一时刻最多一个 drain 任务，保证单连接内的处理顺序。
- **出站队列单写者**：worker 只把完整报文追加到每连接的出站队列；真正的 `send` 只由事件循环线程执行，配合 `EPOLLOUT` 补发。
- **有界队列与背压**：任务队列上限 4096（超限拒绝），单连接出站队列上限 4 MiB（超限断开），入站单行上限 4096 字节（超长丢弃）。
- **MySQL 连接池**：默认 6 条连接，RAII 守卫自动归还，借用超时 100 ms，归还前探活；全部 SQL 使用 Prepared Statement。
- **优雅停机**：`SIGINT` / `SIGTERM` 停止事件循环后，先等待线程池把在途任务做完，再关闭连接池。

## 架构

```text
                    TCP
                     │
              ┌──────▼─────────────────────────┐
              │ EventLoop（单线程）             │
              │ epoll ET / accept / recv       │
              │ 行分帧 / 出站队列 / send       │
              └──────┬─────────────────────────┘
                     │ 每连接 FIFO，最多一个 drain 任务在途
              ┌──────▼─────────────────────────┐
              │ ThreadPool（默认 6 workers）    │
              │ AuthService / MessageService   │
              └──────┬─────────────────────────┘
                     │ borrow / return
              ┌──────▼─────────────────────────┐
              │ MySQL 连接池（默认 6 条）       │
              │ users / friendships / messages │
              └────────────────────────────────┘
```

发送路径与接收路径分开：worker 不直接调用 `send`，只把报文放进对应连接的出站队列，并确保 `EPOLLOUT` 已挂上；内核可写时唤醒事件循环，由事件循环把队列发到 `EAGAIN`。这样单条连接的字节流只有一个写者，不会出现多线程交叉写导致的报文撕裂。

## 快速开始

### 依赖

- Linux（epoll）
- CMake ≥ 3.16
- 支持 C++17 的编译器（GCC 11+ / Clang 14+）
- MySQL 8.x 客户端库
- nlohmann/json

Debian / Ubuntu：

```bash
sudo apt update
sudo apt install -y build-essential cmake libmysqlclient-dev nlohmann-json3-dev mysql-server
```

### 编译

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

需要确定性并发测试钩子时追加 `-DCHAT_TEST_HOOKS=ON`；正常构建下钩子为空操作，不改变业务行为。

产物：

- `build/src/chat_server`
- `build/src/chat_loadgen`

### 初始化数据库

```bash
sudo mysql < scripts/init_db.sql
```

脚本会创建 `chat_db`、本地演示账号 `chat_user / chat_password`、三张表，以及 14 个演示用户（`alice`、`bob`、`u1..u12`）。脚本会重建表，重复执行会清空已有数据。

### 启动

默认配置与初始化脚本一致，直接运行即可：

```bash
./build/src/chat_server
```

需要改端口或数据库连接时，用环境变量覆盖：

```bash
export CHAT_PORT=8080
export CHAT_WORKERS=6
export CHAT_DB_POOL=6
export CHAT_DB_HOST=127.0.0.1
export CHAT_DB_PORT=3306
export CHAT_DB_USER=chat_user
export CHAT_DB_PASSWORD=chat_password
export CHAT_DB_NAME=chat_db

./build/src/chat_server
```

也可以复制 `.env.example` 为 `.env`，用 `set -a; source .env; set +a` 导入；服务端本身不解析 `.env` 文件。

### 手工验证

开两个客户端终端（`nc` 或 `telnet`），分别登录 `alice` 和 `bob`：

```text
# 终端 A
$ nc 127.0.0.1 8080
login:alice pass123
[ChatSystem] 欢迎 alice

# 终端 B
$ nc 127.0.0.1 8080
login:bob pass456
[ChatSystem] 欢迎 bob
```

在 alice 的终端发送广播和私聊：

```text
hello
to bob hello bob
[系统] 消息已受理
```

bob 的终端会收到：

```text
[广播]hello
[私聊|alice]hello bob
```

广播只发给其他在线用户，发送者自己不会收到回显；私聊消息在投递前写入 `messages` 表，广播不落库。

## 文本协议

每条命令占一行，以 `\n` 结束，单行上限 4096 字节。

| 客户端输入 | 含义 | 说明 |
|---|---|---|
| `login:<username> <password>` | 登录 | 成功返回欢迎语；同一账号重复登录会顶掉旧连接 |
| `to <username> <message>` | 私聊 | 目标必须在线；消息先落库再投递 |
| 其他任意一行 | 广播 | 发送给除自己以外的所有已登录用户；不落库 |

当前协议没有注册命令，用户由 `scripts/init_db.sql` 预置。好友列表与历史消息的 DAO 已在数据层实现，但尚未接入文本协议路由。

## 负载测试

`chat_loadgen` 是协议感知的登录压测工具。每条并发连接独占一个 worker 线程和一个账号，并发数不能超过账号数。

```bash
./build/src/chat_loadgen 127.0.0.1 8080 8 2000 1000 \
  alice:pass123 bob:pass456 u1:pass123 u2:pass123 \
  u3:pass123 u4:pass123 u5:pass123 u6:pass123
```

参数依次为：

```text
chat_loadgen HOST PORT CONCURRENCY REQUESTS TIMEOUT_MS USER:PASS [USER:PASS ...]
```

输出包含 `attempts`、`successes`、`failures`、`timeouts`、`connect_failures`、`protocol_failures`、`error_rate`、`elapsed_s`、`throughput_success_per_s` 以及成功请求的 `latency_p50_ms` / `p95` / `p99`。延迟分位数只统计成功请求；吞吐计时包含建连和线程启动。

## 性能数据

测试环境：i5-13500H（16 逻辑核）、5.8 GiB 内存、WSL2 Linux 6.6、g++ 15.2.0；服务端 6 worker、MySQL 连接池 6 条。

低并发、高请求数模式（每连接 2000 次登录，单请求超时 1000 ms）：

| 并发 | 吞吐 (req/s) | P99 (ms) | 错误率 |
|---:|---:|---:|---:|
| 4 | 12,968 | 0.453 | 0 |
| 8 | 19,183 | 0.595 | 0 |
| 11 | 22,562 | 0.669 | 0 |
| 16 | 22,822 | 0.908 | 0 |
| 32 | 22,855 | 1.666 | 0 |

高并发、低请求数模式（每连接 50 次登录，单请求超时 5000 ms）：

| 并发 | 吞吐 (req/s) | P99 (ms) | 错误率 |
|---:|---:|---:|---:|
| 400 | 21,965 | 20.87 | 0 |
| 800 | 21,403 | 41.50 | 0 |
| 1600 | 20,878 | 88.88 | 0 |
| 3200 | 20,868 | 176.65 | 0 |

在 `R=2000` 的短轮测试中，约 11 个并发后进入约 2.2 万 req/s 的平台；`R=50` 的高并发批次累计约 134 万请求，观测到 0 失败、0 超时、0 建连失败。任务队列上限为 4096，约 4.1k 并发附近观察到繁忙拒绝，但该边界受连接状态、预热和宿主调度影响，不是固定阈值。

这些数字只描述当前机器、当前构建和登录负载，不是生产容量承诺，也不能外推到私聊、广播或其他硬件。详细口径与限制见 [docs/performance.md](docs/performance.md)。

## 项目结构

```text
ChatSystem/
├── CMakeLists.txt
├── src/
│   ├── common/       日志、线程池、ConnectionId、测试钩子
│   ├── database/     MySQL 连接池与 users / friendships / messages DAO
│   ├── network/      epoll 事件循环、连接生命周期、出站队列、行分帧
│   ├── service/      登录与消息业务
│   ├── session/      会话路由与 user_id 缓存
│   ├── tools/        chat_loadgen
│   └── main.cpp      依赖组装与启动
├── scripts/
│   └── init_db.sql   建库、建表、演示账号
├── docs/
│   ├── design.md     架构与并发设计
│   ├── performance.md 压测口径、数据与限制
│   └── table_design.md 表结构与索引
├── .env.example
└── LICENSE
```

## 已知限制

- 演示项目：口令明文存储、无注册/改密、无 TLS、无权限模型，不适合直接用于生产。
- 单进程、单事件循环；没有集群、分片、复制或跨机会话共享。
- 好友与历史消息的 DAO 已实现，但当前文本协议未暴露对应命令。
- 广播是尽力而为的在线转发，不落库、不保证送达。
- 单条消息正文受 `messages.content VARCHAR(2048)` 限制；超过列宽时落库失败并向发送方返回错误。

## 文档

- [架构与并发设计](docs/design.md)
- [性能测试口径与数据](docs/performance.md)
- [数据库表设计](docs/table_design.md)

## License

本项目采用 [PolyForm Noncommercial License 1.0.0](LICENSE)：允许个人学习、研究、实验等非商业用途；未经授权不得用于商业目的。完整条款以 `LICENSE` 英文原文为准。
