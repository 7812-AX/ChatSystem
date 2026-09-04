# ChatSystem - 高性能即时通信服务器

基于 C++17 的并发即时通信服务，采用 epoll ET + 线程池 + MySQL，支持登录、好友管理、广播、私聊及历史消息功能。

## ✨ 特性

- ⚡ **高并发网络 I/O**：epoll 边缘触发 + Reactor 模型
- 🔀 **线程池架构**：网络 I/O 与业务逻辑分离，生产者-消费者模式
- 💾 **MySQL 持久化**：Prepared Statement + 双向好友关系 + 消息存储
- 📊 **完整压测闭环**：基线测试 + 加压复测 + 性能分析 + 优化决策

## 📈 性能数据

**测试环境**：i5-13500H / 5.8GiB / WSL2 Linux 6.6 / g++ 15.2.0

| 并发数 | 吞吐量 (次/秒) | P99 延迟 | 错误率 |
|:---:|:---:|:---:|:---:|
| 2 | 3441.3 | 1.13-1.19ms | 0% |
| 4 | 6028 | ~2ms | 0% |
| 8 | 6314 | ~3ms | 0% |
| 16 | 6348 | ~4ms | 0% |

- **拐点定位**：并发 4 处进入平台期（+75% 后增长趋缓）
- **性能分析**：使用 perf + pidstat + strace 定位 futex 等待与 MySQL 串行化
- 详见 [性能测试报告](docs/performance.md)

## 🚀 快速开始

### 前置依赖
```bash
# Ubuntu/Debian
sudo apt-get install mysql-server libmysqlclient-dev nlohmann-json3-dev

# 检查版本
g++ --version  # 需要支持 C++17
mysql --version
编译运行

# 1. 初始化数据库
mysql -u root -p < scripts/init_db.sql

# 2. 编译服务端
cd src/
g++ -std=c++17 -Wall -Wextra -pedantic \
    chat_server.cpp ThreadPool.cpp \
    -o chat_server \
    -I/usr/include/mysql -lmysqlclient

# 3. 启动服务
./chat_server

# 4. 编译压测工具（另一个终端）
g++ -std=c++17 -Wall -Wextra -pedantic \
    chat_loadgen.cpp -o chat_loadgen

# 5. 运行压测
./chat_loadgen 127.0.0.1 8080 2 1000 1000 alice:pass123 bob:pass456
🏗️ 架构设计

┌─────────────┐
│   Client    │
└──────┬──────┘
       │ TCP
┌──────▼──────────────────────────────┐
│  Main Thread (epoll ET)              │
│  - accept()                          │
│  - EPOLLIN/EPOLLOUT 事件分发         │
└──────┬──────────────────────────────┘
       │ 任务队列
┌──────▼──────────────────────────────┐
│  ThreadPool (Worker Threads)         │
│  - 处理业务逻辑                       │
│  - MySQL 查询                        │
│  - 消息分发                          │
└──────┬──────────────────────────────┘
       │
┌──────▼──────────────────────────────┐
│  MySQL Database                      │
│  - users / friendships / messages    │
└─────────────────────────────────────┘
详见 设计文档

📂 项目结构

ChatSystem/
├── src/                    # 源码
│   ├── echo_server_v3_epoll.cpp    # M1: epoll 基础服务
│   ├── chat_server.cpp             # M4: 完整聊天服务
│   ├── ThreadPool.h/cpp            # 线程池实现
│   └── chat_loadgen.cpp            # 压测工具
├── docs/                   # 文档
│   ├── design.md          # 架构设计
│   ├── performance.md     # 性能测试报告
│   └── table_design.md    # 数据库设计
├── scripts/               # 脚本
│   └── init_db.sql       # 数据库初始化
└── README.md
🛠️ 技术栈
语言：C++17
网络：TCP Socket, epoll ET, 非阻塞 I/O
并发：线程池, mutex, condition_variable
数据库：MySQL C API, Prepared Statement
序列化：nlohmann/json
📝 开发日志
2026-07: M1-M3 完成阻塞 → 非阻塞 → epoll 演进
2026-08: M4 线程池 + 消息分发；M5 MySQL 持久化
2026-08: M10-A1~A4 压测基线 + 性能分析 + 不改裁决
2026-09: M10-A5 并发加压复测 + 拐点定位
📧 联系方式
如有问题或建议，欢迎提 Issue 或 PR。

⭐ 如果这个项目对你有帮助，欢迎 Star！

## 📊 性能测试证据

本仓库 `benchmark/` 目录包含：
- **experiment_card.md**：测试环境配置（CPU/内存/编译器/协议）
- **证据对照表.md**：基线 + 加压全部数据汇总
- **results/**：原始测试输出（基线 3 轮 + 加压并发 4/8/16）

所有数字可通过原始输出复现验证。
