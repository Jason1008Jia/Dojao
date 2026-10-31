package com.dojao.spi.bean;

/**
 * 一个并行扫描切片 —— {@link DojaoConnector#planSplits} 的产出单元，由「读什么」（{@link #request()}）
 * 与「连接器私有定位信息」（{@link #split()}）两部分组成。
 *
 * <p><b>为什么是两段而不是一段。</b>{@link ReadRequest} 是跨 JVM 的公共语言：catalog / schema /
 * table / 列投影 / 谓词 / 分页，引擎全程看得懂，也能通过 {@code ReadRequestMsg} 原样搬运。但
 * 「读哪几个文件」这种信息只有连接器自己认得，塞不进公共字段（也不该塞 —— 塞了就等于把 Hive 的
 * 私有布局写进 SPI）。于是切成两半：公共那半照旧走 {@link ReadRequest}，私有那半走
 * {@link Split} 的不透明字节，引擎只搬运不解释（见 {@link Split}）。</p>
 *
 * <p>两个极端都自然成立：</p>
 * <ul>
 *   <li><b>谓词型</b>（MySQL / PostgreSQL）：切片完全由谓词描述，{@code split} 恒为
 *       {@link Split#WHOLE}，{@code request} 各片不同 —— 与老的 {@code planParallelScan}
 *       返回 {@code List<ReadRequest>} 完全等价，默认实现即由此桥接而来。</li>
 *   <li><b>载荷型</b>（Hive）：{@code request} 是整表请求（可带分区值过滤），{@code split}
 *       携带该片的文件清单 —— 一个请求 + N 个不同载荷。</li>
 * </ul>
 *
 * @param request 本片的数据读取请求（叶子扫描语义：已列裁剪 + 谓词下推 + 片内范围）
 * @param split   本片的连接器私有定位信息；{@link Split#WHOLE} 表示整表/无需私有信息
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/20
 */
public record ScanSlice(ReadRequest request, Split split) {

    /** 不带私有定位信息的切片（谓词型连接器的全部切片都是这种）。 */
    public static ScanSlice whole(ReadRequest request) {
        return new ScanSlice(request, Split.WHOLE);
    }

    /** 是否携带连接器私有定位信息。 */
    public boolean hasSplit() {
        return split != null && split.hasPayload();
    }

    /** 把本片的读请求替换为 {@code other}，保留私有定位信息（片内分页/限流改写用）。 */
    public ScanSlice withRequest(ReadRequest other) {
        return new ScanSlice(other, split);
    }
}
