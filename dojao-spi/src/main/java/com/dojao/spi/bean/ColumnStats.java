package com.dojao.spi.bean;

import java.util.Optional;

/**
 * 列统计信息 —— 用于代价估算和数据分布分析。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public record ColumnStats(
    String columnName,
    long nullCount,
    long distinctCount,         // 近似去重数，-1 表示未知
    Optional<String> minValue,  // 最小值（字符串表示），空表示未知
    Optional<String> maxValue,  // 最大值（字符串表示），空表示未知
    Optional<Long> avgLength    // 平均字节长度，空表示未知
) {}
