package com.dojao.connector.common.util;

/**
 * 嵌套 Arrow 向量的子字段命名（读 / 写侧的<b>唯一</b>口径）。
 *
 * <p>Arrow 只强制 Map 的结构（{@code entries/key/value}）；List 的元素名与 Struct 的字段名
 * 是自由的。但 Dojao 的文件转换器（ORC / Parquet）、{@code HiveTypeMapper} 以及
 * {@code HiveConnector.alignToTableSchema} 的零拷贝判据都按「Field 逐字节相等」比对，
 * 任何一处命名不一致都会退化成搬数据甚至抛错，因此集中在这里定义。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class NestedFieldNames {

    /** Arrow List 的元素字段名。 */
    public static final String LIST_ELEMENT = "item";
    /** Arrow Map 的数据字段名（Arrow 规范固定值）。 */
    public static final String MAP_ENTRIES = "entries";
    public static final String MAP_KEY = "key";
    public static final String MAP_VALUE = "value";

    private NestedFieldNames() {
    }
}
