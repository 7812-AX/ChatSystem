<!-- @只读 | @原模式:见下一行原文件头 -->
<!-- @原始证据 | @M10-A2实验卡 | @冻结后只追加结果，不改固定变量 -->

# M10-A2 固定环境三轮基线实验卡

```yaml
schema_version: 1
task_id: W10-D3-1-M10-A2-baseline
date: 2026-08-24
status: FROZEN
freeze_revision: 1
run_id: W10-D3-1-M10-A2-baseline/2026-08-24/1
result_status: STABLE_PASS
```

## 1. 实验目标

在固定环境和固定登录场景下执行三轮，保存每轮原始计数、吞吐、成功请求延迟 P50/P95/P99 和错误率；波动或错误率超阈值时回到 A1/A2，不进入 A3。

## 2. 环境快照

| 项目 | 冻结值 | 证据/备注 |
|---|---|---|
| 主机/内核 | `TBOOK-14` / `Linux 6.6.114.1-microsoft-standard-WSL2` | `uname -a` |
| CPU | 13th Gen Intel(R) Core(TM) i5-13500H；16 logical CPUs；8 cores × 2 threads | `lscpu` |
| 内存 | total `5.8GiB`；snapshot available `3.0GiB` | `free -h` |
| 编译器 | `g++ 15.2.0` | `g++ --version` |
| 驱动编译 | `g++ -std=c++17 -Wall -Wextra -pedantic chat_loadgen.cpp -o chat_loadgen_a2` | 本轮仪表化驱动器 |
| 服务编译 | `g++ -std=c++17 -Wall -Wextra -pedantic chat_server.cpp ThreadPool.cpp -o chat_server -I/usr/include/mysql -lmysqlclient` | M4 ChatServer |
| 服务端口 | `127.0.0.1:8080` | `chat_server.cpp::PORT=8080` |
| 服务模型 | 非阻塞 socket + epoll ET；`BACKLOG=8`；`MAX_EVENTS=16` | `chat_server.cpp` |
| 数据库 | `chat_db`，服务启动需连接成功 | 服务启动日志 |

## 3. 固定场景

| 参数 | 冻结值 |
|---|---|
| workload | Login |
| host/port | `127.0.0.1:8080` |
| concurrency | `2`（每个 worker 一条 TCP 连接） |
| requests per connection | `1000` |
| total attempts per round | `2000` |
| timeout | `1000 ms` |
| credentials | `alice:pass123`、`bob:pass456` |
| payload/protocol | `login:USER PASS\n`；预期响应为 `[ChatSystem] 欢迎 USER\n` |
| warm-up | 同一服务进程先执行 `2 × 100`，不纳入三轮结果 |
| recorded rounds | `round=1,2,3`，同一已预热服务进程 |

## 4. 指标与判定口径

- `attempts = successes + failures`；失败细分为 `timeouts`、`connect_failures`、`protocol_failures`。
- `error_rate = failures / attempts × 100%`；本实验错误率目标为 `0%`，`>1%` 视为异常并回补 A1/A2。
- `throughput_success_per_s = successes / elapsed_s`。
- `latency_p50_ms/p95_ms/p99_ms` 只对成功请求的端到端响应延迟样本计算，采用 nearest-rank（`ceil(p × n)`）规则。
- 三轮吞吐以均值为基准，单轮偏差目标在 `±5%` 内；超出则不进入 A3，先诊断样本量、预热、驱动器或服务端有效性。

## 5. 原始结果索引（执行后追加）

| 轮次 | 原始文件 | attempts | successes | failures | throughput/s | P50/P95/P99 ms | error rate | 判定 |
|---|---|---:|---:|---:|---:|---|---:|---|
| warm-up | 不纳入基线 | 200 | 200 | 0 | 2303.258 | 0.676/1.260/1.537 | 0% | 仅预热 |
| round=1 | [08-24_M10-A2_round1.txt](08-24_M10-A2_round1.txt) | 2000 | 2000 | 0 | 3311.948 | 0.549/0.963/1.185 | 0% | 通过 |
| round=2 | [08-24_M10-A2_round2.txt](08-24_M10-A2_round2.txt) | 2000 | 2000 | 0 | 3486.243 | 0.532/0.926/1.140 | 0% | 通过 |
| round=3 | [08-24_M10-A2_round3.txt](08-24_M10-A2_round3.txt) | 2000 | 2000 | 0 | 3525.754 | 0.512/0.937/1.132 | 0% | 通过 |

## 6. 回补路径

- 任一轮 `error_rate > 1%`：回查 A1 协议/计数/超时路径。
- 三轮吞吐偏差超过 `±5%`：先增加样本并保持同一已预热服务；仍不稳定则回查 A1/A2，不进入 A3。

## 7. 三轮分析与放行

- 吞吐均值：`3441.315 success/s`。
- 相对均值偏差：round 1 `-3.76%`，round 2 `+1.31%`，round 3 `+2.46%`；均在 `±5%` 内。
- 错误率：三轮均为 `0.000%`；`attempts=successes+failures` 三轮均为 `2000=2000+0`。
- 成功请求延迟：P50 `0.512~0.549 ms`，P95 `0.926~0.963 ms`，P99 `1.132~1.185 ms`，未见异常尾延迟漂移。
- 结论：固定环境/场景三轮基线稳定且错误率合理，满足 M10-A2，可进入 A3 采样；A3 仍必须保留本卡和三份原始结果作为前置证据。

<!-- BASELINE_RESULTS_APPEND_HERE -->
