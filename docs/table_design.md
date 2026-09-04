# M5 表设计文档 — friendships + messages

> 日期：2026-08-11 | 修订：2026-08-14（双向好友模型，W9D3 裁决）| 对应：projects/project_strategy.md §Milestone 5

## 1. 表结构

### friendships

```sql
CREATE TABLE friendships (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id     INT UNSIGNED NOT NULL COMMENT '发起好友的一方（users.id）',
    friend_id   INT UNSIGNED NOT NULL COMMENT '被添加的一方（users.id）',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_friend (user_id, friend_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友关系表';
```

### messages

```sql
CREATE TABLE messages (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sender_id   INT UNSIGNED NOT NULL COMMENT '发送方（users.id）',
    receiver_id INT UNSIGNED NOT NULL COMMENT '接收方（users.id）',
    content     VARCHAR(2048) NOT NULL COMMENT '消息内容',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_sender_receiver_id (sender_id, receiver_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='聊天消息表';
```

## 2. 设计决策

| 项 | friendships | messages |
|----|-------------|----------|
| 主键 | `INT UNSIGNED AUTO_INCREMENT`（对齐 users.id） | `BIGINT UNSIGNED AUTO_INCREMENT`（消息量大预留） |
| 防重复 | `UNIQUE(user_id, friend_id)` + 双向对称边（两条边表达一段好友关系，08-14 修订） | — |
| 内容长度 | — | `VARCHAR(2048)`（对齐 M4 MAX_LINE=1024，留冗余） |
| 时间 | `created_at DATETIME DEFAULT CURRENT_TIMESTAMP` | 同左 |
| 排序键 | `created_at DESC`（好友列表） | `id DESC`（id 自增即时间序） |
| 查询索引 | `uk_user_friend` 覆盖 user_id 前缀 | `(sender_id, receiver_id, id)` 等值+有序，免 filesort |

## 3. 查询路径

```sql
-- 我的好友列表（listfriends 已落地，W9D3；双向模型下每个用户查自己的出向边）
SELECT friend_id FROM friendships WHERE user_id = ? ORDER BY created_at DESC;

-- 最近 N 条历史（A↔B 双向）
SELECT * FROM messages
WHERE (sender_id=? AND receiver_id=?) OR (sender_id=? AND receiver_id=?)
ORDER BY id DESC LIMIT 20;
```

### 3.5 双向模型修订（2026-08-14 裁决）

- `addfriend` 改为单条 `INSERT INTO friendships VALUES (?,?),(?,?)` 原子写 (A,B)+(B,A) 两条对称边；`listfriends` SQL 不变。
- 自加好友应用层拒绝；任一方向重复由 UNIQUE 触发 1062 整条拦截。
- 旧单向数据需手动事务迁移（提交前核对），不可自动迁移。

## 4. 迁移方案

- 文件：`migrations/001_create_friendships_messages.sql`
- 幂等：`DROP TABLE IF EXISTS` 打头 + `CREATE TABLE`，重复执行结果一致
- 删除顺序：先 messages 后 friendships（子表先于父表）
- 学习版边界：生产环境应使用 `CREATE TABLE IF NOT EXISTS` + 版本表，禁止 DROP 防误删（M12 评估）

## 5. 接入点（M4/M2 复用）

| 接入点 | 位置 | 动作 |
|--------|------|------|
| 私聊成功落库 | `chat_server.cpp` send_private 内 send_all(target_fd, content) 处 |  INSERT messages |
| 添加好友 | `chat_server.cpp` handle_message 的 `addfriend:` 分支 | 单语句多行 INSERT 两条对称边 + 自加拒绝 + 1062 拦截（W9D2-3 已落地） |
| 好友列表 | `chat_server.cpp` handle_message 的 `listfriends` 分支 | SELECT friend_id WHERE user_id=? ORDER BY created_at DESC，多行 fetch（W9D3 已落地） |
| 消息历史 | `chat_server.cpp` handle_message 的 `history <username>` 分支 | 双向 OR + ORDER BY id DESC LIMIT 20，多行 fetch + 单行 JSON，锁外 send（W9D4 顺延 8/15 已落地） |
| 认证链路复用 | `tcp_server.cpp` handle_register/handle_login（Prepared Statement） | 不重写 |

## 6. 范围冻结（M5 明确不做）

群聊历史 / 未读系统 / 复杂分页 / 好友申请状态机 / 连接池（留 M12）——均不纳入本设计。
