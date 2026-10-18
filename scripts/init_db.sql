-- ChatSystem 数据库初始化脚本
--
-- 用法（需要 MySQL 管理员权限；脚本会重建三张表）：
--   sudo mysql < scripts/init_db.sql
--
-- 脚本创建：
--   - 数据库 chat_db
--   - 本地账号 chat_user / chat_password（仅供开发演示）
--   - users / friendships / messages 三张表
--   - 14 个演示账号（alice、bob、u1..u12）
--
-- 注意：演示账号口令为明文，仅用于本地开发与压测，不可用于生产。

CREATE DATABASE IF NOT EXISTS chat_db
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

-- 服务端通过 TCP 连接 127.0.0.1，两个 host 形式都建，避免 localhost 解析差异
CREATE USER IF NOT EXISTS 'chat_user'@'localhost' IDENTIFIED BY 'chat_password';
CREATE USER IF NOT EXISTS 'chat_user'@'127.0.0.1' IDENTIFIED BY 'chat_password';
-- 账号已存在时也把口令对齐到演示默认值，保证重复执行后能直接启动
ALTER USER 'chat_user'@'localhost' IDENTIFIED BY 'chat_password';
ALTER USER 'chat_user'@'127.0.0.1' IDENTIFIED BY 'chat_password';
GRANT SELECT, INSERT, UPDATE, DELETE ON chat_db.* TO 'chat_user'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON chat_db.* TO 'chat_user'@'127.0.0.1';
FLUSH PRIVILEGES;

USE chat_db;

-- 先删有引用关系的表，再删被引用表；本脚本用于可重复的本地初始化
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS friendships;
DROP TABLE IF EXISTS users;

-- ==================== users ====================

CREATE TABLE users (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(64)  NOT NULL,
    password    VARCHAR(255) NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

-- ==================== friendships ====================

-- 单向边：A→B 与 B→A 各一行。UNIQUE 防止同一方向重复添加；
-- (user_id, friend_id) 索引同时覆盖「我的好友列表」查询。
CREATE TABLE friendships (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id     INT UNSIGNED NOT NULL,
    friend_id   INT UNSIGNED NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_friend (user_id, friend_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友关系表';

-- ==================== messages ====================

-- id 自增即时间序；联合索引覆盖「A↔B 最近 N 条」的等值 + 排序查询。
CREATE TABLE messages (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sender_id   INT UNSIGNED    NOT NULL,
    receiver_id INT UNSIGNED    NOT NULL,
    content     VARCHAR(2048)   NOT NULL,
    created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_sender_receiver_id (sender_id, receiver_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='聊天消息表';

-- ==================== 演示账号 ====================

INSERT INTO users (username, password) VALUES
    ('alice', 'pass123'),
    ('bob',   'pass456'),
    ('u1',    'pass123'),
    ('u2',    'pass123'),
    ('u3',    'pass123'),
    ('u4',    'pass123'),
    ('u5',    'pass123'),
    ('u6',    'pass123'),
    ('u7',    'pass123'),
    ('u8',    'pass123'),
    ('u9',    'pass123'),
    ('u10',   'pass123'),
    ('u11',   'pass123'),
    ('u12',   'pass123');
