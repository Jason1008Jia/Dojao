package com.dojao.spi.bean;

/**
 * Dojao 内部标准数据类型 —— 所有 Connector 将源类型映射到此枚举，
 * 保证进入算子流水线的数据列类型一致。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/19
 */
public enum DojaoType {

    /** 布尔值 */
    BOOLEAN,

    // -- 整数 --
    /** 8 位有符号整数 (TINYINT) */

    TINYINT,

    /** 16 位有符号整数 (SMALLINT) */
    SMALLINT,

    /** 32 位有符号整数 (INT / INTEGER) */
    INT,

    /** 64 位有符号整数 (BIGINT) */
    BIGINT,

    // -- 浮点 --
    /** 32 位 IEEE 754 浮点数 (FLOAT) */
    FLOAT,

    /** 64 位 IEEE 754 浮点数 (DOUBLE) */
    DOUBLE,

    // -- 定点 --
    /** 十进制定点数 DECIMAL(p, s)，precision 和 scale 作为元数据携带 */
    DECIMAL,

    // -- 字符串 --
    /** UTF-8 可变长字符串 (VARCHAR / TEXT) */
    STRING,

    // -- 日期/时间 --
    /** 日期，无时区 (DATE，如 2026-07-19) */
    DATE,
    /** 日历日期时间，无时区 (DATETIME，如 2026-07-19 14:30:00.123) */
    DATETIME,
    /** Unix 时间戳，自 epoch 起的秒/毫秒数 (如 1784456553 或 1784456552052) */
    TIMESTAMP,
    /** 带时区的 Unix 时间戳 (TIMESTAMP WITH TIME ZONE) */
    TIMESTAMP_TZ,

    // -- 复合类型 --
    /** 定长数组 ARRAY elementType，定长作为元数据携带（存储 = FixedSizeList，物理是 (rowCount×N) 矩阵）。
     *  向量列（pgvector vector / MySQL vector）读入后即此语义 —— 连接器层保留 {@link #VECTOR} 标记识别，引擎层完全按定长数组处理。 */
    ARRAY,
    /** 变长列表 LIST elementType，元素类型作为元数据携带（存储 = Arrow List，有 offsets buffer，每行长度可变）。
     *  用于 Hive 数组、ORC LIST、PG 原生数组（_int4 等）等每行元素个数不定的场景。 */
    LIST,
    /** 映射 MAP&lt;keyType, valueType&gt;，键/值类型作为元数据携带 */
    MAP,
    /** 结构体 STRUCT&lt;字段名:字段类型, ...&gt;，字段清单作为元数据携带（存储 = Arrow Struct）。
     *  用于 Hive struct&lt;...&gt;、ORC STRUCT、Parquet group 等有名子字段的场景。 */
    STRUCT,
    // -- 半结构化 --
    /** JSON 类型 */
    JSON,
    // -- 向量 --
    /** 向量 VECTOR dim —— 仅作连接器层的类型标记（识别"这是向量列"，pgvector vector / halfvec）。
     *  数据读入后按 {@link #ARRAY}（定长数组，FixedSizeList&lt;Float32&gt;）语义进入算子流水线。 */
    VECTOR
}
