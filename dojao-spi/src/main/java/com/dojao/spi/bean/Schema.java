package com.dojao.spi.bean;

import java.util.Map;

/**
 * Dojao Schema 描述类（对应 CREATE SCHEMA 语法）
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:12
 */
public record Schema(
    String catalogName,
    String name,
    Map<String, String> properties
) {}
