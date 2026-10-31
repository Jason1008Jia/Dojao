-- ============================================================
-- Dojao MetaStore 初始化脚本 (MySQL)
-- 执行顺序：建库 → 建表
-- 字符集：utf8mb4
-- ============================================================

-- ==================== catalog 注册表 ====================

CREATE TABLE IF NOT EXISTS catalog (
    name         VARCHAR(255)  NOT NULL PRIMARY KEY  COMMENT 'Catalog 名称（唯一标识）',
    type         VARCHAR(100)  NOT NULL              COMMENT '连接器类型：mysql / postgres / sqlserver / hive 等',
    properties   VARCHAR(1000)                       COMMENT 'Catalog 属性（JSON 格式，键为 catalog.<类型>.<属性名>，如 catalog.mysql.jdbcUrl）',
    created_at   VARCHAR(23)   NOT NULL              COMMENT '创建时间 YYYY-MM-dd HH:mm:ss.SSS',
    updated_at   VARCHAR(23)   NOT NULL              COMMENT '更新时间 YYYY-MM-dd HH:mm:ss.SSS'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Catalog 注册表';
