package com.dojao.connector.hive;

/**
 * 一个<b>读取单元</b> —— 分片规划的最小粒度，两种形态：
 * <ul>
 *   <li><b>整文件</b>（{@code length <= 0}，见 {@link #wholeFile}）—— 常规情形，一个文件一片；</li>
 *   <li><b>文件内的一个字节区间</b>（{@code length > 0}）—— 单个文件远大于目标单元大小时，
 *       在文件内部按格式自带的索引边界（ORC = stripe、Parquet = row group）切成若干区间，
 *       让「一个 10GB 的大文件」也能被多路并行读。</li>
 * </ul>
 *
 * <p>区间的 {@code [start, start + length)} 语义由各格式的读取器解释：</p>
 * <ul>
 *   <li>ORC —— 直接交给 {@code Reader.Options.range(start, length)}，由 ORC 定位覆盖到的 stripe；</li>
 *   <li>Parquet —— 用作 row group 的选取条件：{@code startingPos ∈ [start, start + length)}
 *       （Master 造区间时把 {@code start} 取成第一段的起始偏移、{@code start + length} 取成
 *       「下一段起始偏移」（末段取文件长度），因此取值是精确的，不依赖压缩后字节数的估算）。</li>
 * </ul>
 *
 * <p>文本文件无索引、压缩文本更不可切，一律整文件（见 {@code HiveConnector.splitBigFile}）。</p>
 *
 * <p><b>这些单元的粒度必须在 Master 的 {@code planSplits} 就定下来</b>：一个切片只交给一个
 * Worker 上的一条 reader 串行读，留到读取侧再切，只会让同一条 reader 顺序读更多片段，换不来并行。</p>
 *
 * @param path   HDFS 路径
 * @param start  区间起始字节偏移；整文件为 {@code 0}
 * @param length 区间字节长度；整文件为 {@code -1}
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/20
 */
public record HiveSplitUnit(String path, long start, long length) {

    /** 整文件单元（不做文件内切割）。 */
    public static HiveSplitUnit wholeFile(String path) {
        return new HiveSplitUnit(path, 0L, -1L);
    }

    /** 是否是文件内的一个区间（否则为整文件）。 */
    public boolean isRange() {
        return length > 0L;
    }

    /** 人类可读的简短描述（日志 / 冒烟测试用）。 */
    public String describe() {
        return isRange() ? path + "[" + start + "+" + length + "]" : path;
    }
}
