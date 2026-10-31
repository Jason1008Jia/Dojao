package com.dojao.spi.util;

/**
 * 字符串列 DDL 类型映射（SPI 扩展点）—— 管理「连接器 + 目标库/表字符集 → VARCHAR 类型」的适配。
 *
 * <p>CTAS 建表时，Arrow {@code Utf8} 列需要落成目标库的字符串类型。内置连接器给一个可用基线；
 * 自定义 Connector 的用户可覆写 {@link com.dojao.spi.service.DojaoConnector#varcharMapping()}，
 * 自定义默认写入的数据类型。</p>
 *
 * <p>实现要点：已知字符集可按每字符最大字节数反推 VARCHAR 上限（如 MySQL utf8mb4 → 16383）；
 * 字符集未知时回退 {@link #TEXT}。此外 {@link #varcharMaxBytes} 供建表行大小预算使用，
 * 避免多列并存时整行声明字节超过数据库行上限。</p>
 *
 * @author jiaxin
 * @date 2026/8/10
 */
public interface VarcharMapping {

    /** 未知字符集 / 无法适配时的兜底字符串类型。 */
    String TEXT = "TEXT";

    /**
     * 解析字符串列的 DDL 类型。
     *
     * @param charset 目标库/表默认字符集（如 {@code "utf8mb4"}）；null/未知 → 兜底 TEXT
     * @return 字符串列 DDL 类型（如 {@code "VARCHAR(16383)"} 或 {@code "TEXT"}）
     */
    String resolveVarcharType(String charset);

    /**
     * 估算字符串列在行内声明的最大字节数，供建表行大小预算
     * （多列并存时整行声明字节不能超过数据库行上限）。未知字符集返回 {@link #textRowBytes()}。
     */
    int varcharMaxBytes(String charset);

    /** TEXT 列在行内的指针占用字节数（不占行字节上限的大头）。 */
    int textRowBytes();

    /**
     * 兜底实现：任何字符集一律 TEXT（未知连接器 / 用户未自定义时的安全默认）。
     */
    static VarcharMapping fallback() {
        return new VarcharMapping() {
            @Override public String resolveVarcharType(String charset) { return TEXT; }
            @Override public int varcharMaxBytes(String charset) { return textRowBytes(); }
            @Override public int textRowBytes() { return 16; }
        };
    }
}
