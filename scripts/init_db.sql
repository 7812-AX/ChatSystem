-- M5 数据持久化迁移文件 v001
-- 创建 friendships + messages 两表（可重放/幂等）
-- 执行：mysql -u root -p chat_db < 001_xxx.sql
-- 🧠 填空规则：🧠=常规（应会） 💡=提示引导 🔑=关键难点（卡住问 AI）

-- 🧠 可重放第一步：先删旧表（幂等关键）
-- 💡 思考：为什么后删 friendships？（外键视角：谁依赖谁）
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS friendships;

-- ==================== friendships 好友关系表 ====================
-- 决策：单向发起（无状态机）；UNIQUE(user_id,friend_id) 防 A→B 重复；该索引覆盖"我的好友列表"
CREATE TABLE friendships (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,      -- 🧠 主键类型（对齐 M2 users.id）
    user_id     INT UNSIGNED NOT NULL,                        -- 💡 发起方（users.id）
    friend_id   INT UNSIGNED NOT NULL,                        -- 💡 被添加方（users.id）
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- 🧠 默认当前时间
    UNIQUE KEY uk_user_friend (user_id, friend_id)            -- 🔑 防重复 A→B 的约束类型
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友关系表';

-- ==================== messages 聊天消息表 ====================
-- 决策：BIGINT 主键预留；content 对齐 M4 MAX_LINE=1024；最近N条 ORDER BY id DESC
--       联合索引 (sender_id,receiver_id,id) 覆盖「A↔B 最近N条」查询
CREATE TABLE messages (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,   -- 💡 消息量大，选 BIGINT 预留
    sender_id   INT UNSIGNED NOT NULL,                        -- 🧠 发送方（users.id）
    receiver_id INT UNSIGNED NOT NULL,                        -- 🧠 接收方（users.id）
    content     VARCHAR(2048) NOT NULL,                       -- 🧠 对齐 M4 MAX_LINE 的最大长度类型
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_sender_receiver_id (sender_id, receiver_id, id)   -- 🔑 联合索引三列（纯转发查询）
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='聊天消息表';
