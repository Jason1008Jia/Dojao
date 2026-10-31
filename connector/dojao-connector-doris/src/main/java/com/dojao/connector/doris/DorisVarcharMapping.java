package com.dojao.connector.doris;

import com.dojao.spi.util.VarcharMapping;

/**
 * Doris 字符串列 DDL 类型映射（{@link VarcharMapping} 的 Doris 实现）。
 *
 * <p>Doris 的字符串类型是变长的 {@code STRING}（最大约 2GB，无需声明长度），也没有
 * MySQL 的 65535 行大小上限。因此不管字符集是什么，Arrow {@code Utf8} 一律落
 * {@code STRING}；{@link #MAX_ROW_BYTES} 只作为「行大小预算」的保守参考值保留，
 * 建表逻辑不会因为它把字符串降级成别的类型。</p>
 *
 * @author jiaxin
 * @date 2026/10/25
 */
public final class DorisVarcharMapping implements VarcharMapping {

    /** Doris VARCHAR 的最大声明长度（字节）；STRING 本身无此上限，仅作预算参考。 */
    static final int MAX_ROW_BYTES = 65533;

    /** 指向变长字符串体的行内指针占用字节数。 */
    static final int TEXT_ROW_BYTES = 16;

    /** 单例：映射本身无状态，连接器通过 {@link com.dojao.spi.service.DojaoConnector#varcharMapping()} 暴露。 */
    public static final DorisVarcharMapping INSTANCE = new DorisVarcharMapping();

    private DorisVarcharMapping() {}

    @Override
    public String resolveVarcharType(String charset) {
        return "STRING";
    }

    @Override
    public int varcharMaxBytes(String charset) {
        return MAX_ROW_BYTES;
    }

    @Override
    public int textRowBytes() {
        return TEXT_ROW_BYTES;
    }
}
