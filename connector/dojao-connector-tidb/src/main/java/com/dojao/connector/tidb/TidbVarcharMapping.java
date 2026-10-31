package com.dojao.connector.tidb;

import com.dojao.spi.util.VarcharMapping;

import java.util.Map;

/**
 * TiDB 字符串列 DDL 类型映射（{@link VarcharMapping} 的 TiDB 实现）。
 *
 * <p>TiDB 与 MySQL 的字符集 / 行大小规则一致（默认 utf8mb4，非大字段行声明字节上限
 * 65535），因此按目标库默认字符集推算 VARCHAR 上限：
 * {@link #MAX_ROW_BYTES} / 每字符最大字节数（如 utf8mb4 → 16383）。字符集在枚举
 * 之外时兜底 {@link #TEXT}（长度未知，不能保证不超行上限）。</p>
 *
 * <p>{@link #MAX_ROW_BYTES} 供建表行大小预算：多列并存时若整行超限，
 * 由建表方把字符串列整体降级 TEXT（TEXT 行内仅占指针，见 {@link #TEXT_ROW_BYTES}）。</p>
 *
 * @author jiaxin
 * @date 2026/10/25
 */
public final class TidbVarcharMapping implements VarcharMapping {

    /** InnoDB 单行（非大字段）声明字节上限。 */
    static final int MAX_ROW_BYTES = 65535;

    /** TEXT 列在行内的指针占用字节数（不占行字节上限的大头）。 */
    static final int TEXT_ROW_BYTES = 16;

    private static final Map<String, Integer> CHARSET_BYTES_PER_CHAR = Map.ofEntries(
            Map.entry("utf8mb4", 4), Map.entry("utf16", 4), Map.entry("utf16le", 4),
            Map.entry("utf32", 4),
            Map.entry("utf8", 3), Map.entry("utf8mb3", 3), Map.entry("ujis", 3),
            Map.entry("eucjpms", 3),
            Map.entry("gbk", 2), Map.entry("gb2312", 2), Map.entry("big5", 2),
            Map.entry("cp932", 2), Map.entry("euckr", 2), Map.entry("sjis", 2),
            Map.entry("latin1", 1), Map.entry("latin2", 1), Map.entry("latin5", 1),
            Map.entry("latin7", 1), Map.entry("ascii", 1), Map.entry("binary", 1),
            Map.entry("tis620", 1), Map.entry("cp1250", 1), Map.entry("cp1251", 1),
            Map.entry("cp1256", 1), Map.entry("cp1257", 1), Map.entry("greek", 1),
            Map.entry("hebrew", 1), Map.entry("koi8r", 1), Map.entry("macroman", 1),
            Map.entry("swe7", 1), Map.entry("dec8", 1), Map.entry("hp8", 1),
            Map.entry("armscii8", 1), Map.entry("geostd8", 1));

    /** 单例：映射本身无状态，连接器通过 {@link com.dojao.spi.service.DojaoConnector#varcharMapping()} 暴露。 */
    public static final TidbVarcharMapping INSTANCE = new TidbVarcharMapping();

    private TidbVarcharMapping() {}

    @Override
    public String resolveVarcharType(String charset) {
        Integer bpc = bytesPerChar(charset);
        if (bpc == null) return TEXT;
        return "VARCHAR(" + (MAX_ROW_BYTES / bpc) + ")";
    }

    @Override
    public int varcharMaxBytes(String charset) {
        Integer bpc = bytesPerChar(charset);
        if (bpc == null) return textRowBytes();
        return (MAX_ROW_BYTES / bpc) * bpc;
    }

    @Override
    public int textRowBytes() {
        return TEXT_ROW_BYTES;
    }

    /** 已知字符集的每字符最大字节数；未知返回 null。 */
    private static Integer bytesPerChar(String charset) {
        if (charset == null) return null;
        return CHARSET_BYTES_PER_CHAR.get(charset.toLowerCase());
    }
}
