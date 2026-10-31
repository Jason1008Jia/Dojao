package com.dojao.spi.bean;

import java.util.List;
import java.util.Map;

/**
 * Dojao Table 描述类（对应 CREATE TABLE 语法）
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:13
 */
public record Table(
    String catalogName,
    String schemaName,
    String name,
    List<Column> columns,
    Map<String, String> properties,
    String comment
) {}
