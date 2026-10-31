package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * COUNT(DISTINCT x) —— State = {@link DistinctSet}，元素已 {@link #normalize 规范化}、NULL 已剔除。
 *
 * <h3>为什么能跨分区正确</h3>
 * 本引擎的聚合 shuffle <b>按分组键共分区</b>（同一个 key 只落唯一 Worker / 唯一分区），
 * 因此每个 state 就是该组的完整去重集合，{@code merge} 只在「同组跨溢写文件」时发生
 * （{@link #merge} = 集合并集）。若将来出现「部分聚合结果需跨 Worker 再合并」的路径，
 * 必须传 state 而非 {@code evaluate} 标量 —— 标量相加会重复计不同 Worker 的同值。
 *
 * <h3>规范化</h3>
 * 同一列的值在不同路径下装箱类型可能不同（Arrow {@code IntVector} → Integer、{@code BigIntVector} →
 * Long、溢写回读 → 由 {@link #decode} 逆出），而 {@code Integer(5).equals(Long(5))} 为 false ——
 * 不规范化会把同一个值数两次。故一律收敛为 Long / Double / String。
 *
 * <h3>性能（2026-10-19 优化）</h3>
 * 状态从 {@code LinkedHashSet<Object>} 换成 {@link DistinctSet}（原始类型开放寻址 + 字符串字节 arena）：
 * 高基数列（千万行、单组）不再为每个值分配装箱对象与哈希节点，内存约为旧实现的 1/3，GC 压力大幅下降；
 * {@link AggregationHashTable} 还会对 Arrow 向量走 {@link #accumulateLong}/{@link #accumulateDouble}/
 * {@link #accumulateUtf8} 快路径，跳过 {@code getObject} 装箱。语义不变（仍精确去重，非近似）。
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/9/24
 */
public class CountDistinctAggregateFunction implements AggregateFunction<Set<Object>> {

    public static final CountDistinctAggregateFunction INSTANCE = new CountDistinctAggregateFunction();

    @Override
    public Set<Object> createState() {
        return new DistinctSet();
    }

    @Override
    public void accumulate(Set<Object> state, Object[] inputs) {
        // SQL 语义：COUNT(DISTINCT x) 不计 NULL
        if (inputs.length == 0 || inputs[0] == null) return;
        state.add(normalize(inputs[0]));
    }

    // ==================== 向量快路径（不装箱）====================

    /** 整型列快路径（NULL 由调用方先判掉）。 */
    public void accumulateLong(Set<Object> state, long v) {
        if (state instanceof DistinctSet ds) ds.addLong(v);
        else state.add(v);
    }

    /** 浮点列快路径（NULL 由调用方先判掉）。 */
    public void accumulateDouble(Set<Object> state, double v) {
        if (state instanceof DistinctSet ds) ds.addDouble(v);
        else state.add(v);
    }

    /** UTF-8 文本列快路径（NULL 由调用方先判掉）：直接写入字节 arena，不构造 String。 */
    public void accumulateUtf8(Set<Object> state, byte[] b, int off, int len) {
        if (state instanceof DistinctSet ds) ds.addUtf8(b, off, len);
        else state.add(new String(b, off, len, StandardCharsets.UTF_8));
    }

    @Override
    public void merge(Set<Object> target, Set<Object> source) {
        target.addAll(source);
    }

    @Override
    public Object evaluate(Set<Object> state) {
        return (long) state.size();
    }

    @Override
    public void reset(Set<Object> state) {
        state.clear();
    }

    @Override
    public String functionName() {
        return "count_distinct";
    }

    @Override
    public String display() {
        return "去重计数：COUNT(DISTINCT x)";
    }

    /** 规范化输入值：byte[] → String、整型 Number → Long、其余 Number → Double、其它 → String。 */
    static Object normalize(Object v) {
        if (v == null) return null;
        if (v instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return ((Number) v).longValue();
        }
        if (v instanceof Number n) return n.doubleValue();
        if (v instanceof CharSequence cs) return cs.toString();
        return v.toString();
    }

    // ==================== 溢写编解码 ====================

    /**
     * 状态 → 单列 VarChar 的二进制 payload（见 {@link DistinctSet#encode()}）。
     * 旧实现编码成文本串：每个值都要 {@code toString()} + {@code substring}，内存峰值 ≈ 3×；
     * 现在定长/变长裸字节直出。
     */
    public static byte[] encode(Set<Object> state) {
        if (state instanceof DistinctSet ds) return ds.encode();
        // 兼容非 DistinctSet 状态（历史状态 / 测试）：按通用集合编码
        DistinctSet copy = new DistinctSet();
        copy.addAll(state);
        return copy.encode();
    }

    /** {@link #encode} 的逆操作（null / 空数组 → 空状态）。 */
    public static Set<Object> decode(byte[] bytes) {
        return DistinctSet.decode(bytes);
    }
}
