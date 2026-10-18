# 数据库表设计

数据库：`chat_db`，字符集 `utf8mb4`，存储引擎 InnoDB。

## 1. 表结构

### users

```sql
CREATE TABLE users (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(64)  NOT NULL,
    password    VARCHAR(255) NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- `username` 唯一，登录查询走唯一索引。
- `password` 当前为明文，仅用于本地演示；生产环境应改为加盐哈希（如 Argon2/bcrypt）并增加账号状态、失败次数等字段。
- `id` 使用 `INT UNSIGNED`，与 `friendships`、`messages` 中的用户 id 对齐。

### friendships

```sql
CREATE TABLE friendships (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id     INT UNSIGNED NOT NULL,
    friend_id   INT UNSIGNED NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_friend (user_id, friend_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- 采用单向边：A→B 与 B→A 各存一行，一段好友关系对应两条记录。
- `UNIQUE(user_id, friend_id)` 防止同一方向重复添加；重复插入由应用层捕获 1062 并拒绝。
- `uk_user_friend` 的最左前缀 `user_id` 覆盖“我的好友列表”查询，不需要额外索引。

### messages

```sql
CREATE TABLE messages (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sender_id   INT UNSIGNED    NOT NULL,
    receiver_id INT UNSIGNED    NOT NULL,
    content     VARCHAR(2048)   NOT NULL,
    created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_sender_receiver_id (sender_id, receiver_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- 主键用 `BIGINT UNSIGNED`，为消息量预留空间；`id` 自增即时间序。
- `(sender_id, receiver_id, id)` 联合索引覆盖“A 发给 B 的最近 N 条”：前两列等值过滤，第三列有序，避免 filesort。
- `content` 列宽 2048 字节，是单条私聊正文的实际上限；超过时 `INSERT` 失败，服务端向发送方返回“消息未保存”。

## 2. 查询路径

登录：

```sql
SELECT id, password
FROM users
WHERE username = ?;
```

好友列表：

```sql
SELECT friend_id
FROM friendships
WHERE user_id = ?
ORDER BY created_at DESC;
```

两个用户之间的最近 20 条消息（双向）：

```sql
SELECT id, sender_id, receiver_id, content
FROM messages
WHERE (sender_id = ? AND receiver_id = ?)
   OR (sender_id = ? AND receiver_id = ?)
ORDER BY id DESC
LIMIT 20;
```

> 当前文本协议只接入了登录与私聊落库；好友列表和历史消息的 DAO 已实现，但协议路由尚未开放。上面的查询用于说明表结构与索引的设计意图。

## 3. 设计决策

| 项 | 选择 | 理由 |
|---|---|---|
| 好友方向 | 单向边，双向写两行 | “谁加了谁”可保留；查询“我的好友”只需一个方向；删除一侧关系时语义明确 |
| 防重复 | `UNIQUE(user_id, friend_id)` | 由数据库兜底，应用层不需要先查后插 |
| 消息主键 | `BIGINT UNSIGNED` | 消息增长快于用户和好友关系，提前留出空间 |
| 消息排序 | `id DESC` | 自增主键即插入顺序，避免按 `created_at` 排序时的同秒并列问题 |
| 历史查询索引 | `(sender_id, receiver_id, id)` | 等值 + 有序，覆盖双向 OR 的两个分支 |
| 外键 | 未声明 | 演示项目由应用层保证引用完整性；避免初始化顺序和批量导入时的约束耦合 |
| 时间 | `DATETIME DEFAULT CURRENT_TIMESTAMP` | 由数据库统一生成，减少客户端时钟差异 |

## 4. 初始化与可重复执行

`scripts/init_db.sql` 会：

1. 创建数据库与本地账号 `chat_user`；
2. 按 `messages → friendships → users` 顺序删除旧表；
3. 重建三张表；
4. 插入 14 个演示账号。

脚本面向本地开发与压测，重复执行会清空数据。生产环境不应直接使用带 `DROP TABLE` 的初始化脚本，应改为版本化迁移（如 Flyway、Liquibase 或自建 schema_version 表）。

## 5. 后续演进

- 口令哈希与账号状态；
- 好友申请状态机（pending / accepted / blocked）；
- 消息已读位点与未读数；
- 历史消息分页（按 `id < last_id` 的游标分页，避免深分页）；
- 消息内容与大字段分离，或按时间分区；
- 引入外键或由应用层统一事务边界，明确删除用户时的级联策略。
