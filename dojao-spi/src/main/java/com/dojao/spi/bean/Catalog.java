package com.dojao.spi.bean;

import java.util.Map;

/**
 * Dojao Catalog 描述类（对应 CREATE CATALOG 语法）
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:09
 */
public record Catalog(
    String name,
    String type,
    Map<String, String> properties,
    String createAt,
    String updateAt
) {}
