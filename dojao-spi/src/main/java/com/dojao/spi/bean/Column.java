package com.dojao.spi.bean;

/**
 * Dojao Column 描述类（对应 CREATE TABLE 中的 columnDefinition 语法）
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:14
 */
public record Column(
    String name,
    String dataType,
    String comment,
    String defaultValue,
    boolean nullable
) {

    /**
     * 默认 nullable = true（列默认可为空，只有指定 NOT NULL 时才为 false）
     */
    public Column(String name, String dataType, String comment, String defaultValue) {
        this(name, dataType, comment, defaultValue, true);
    }
}
