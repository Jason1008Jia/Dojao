package com.dojao.core.operator;

/**
 * 算子输出 / 拉取缓冲的<b>字节</b>上界统一口径（默认 32MB）。
 *
 * <p><b>为什么所有「攒批」都必须有字节上界</b>：只按行数（10000 / 50000 行）切批在窄表上没问题，
 * 但宽行（ES 全量 JSON、{@code collect_list} 大组）下「5 万行一批」可以是几百 MB ——
 * Arrow 向量按 2 的幂扩容，128MB 之后直接申请 256MB，瞬间打穿算子分配器
 * （线上 2026-11-23 {@code SortMergeJoin ... Unable to allocate buffer of size 268435456}）。</p>
 *
 * <p>口径与 TEXT / JDBC 转换器的单批字节上界一致（32MB）；可用系统属性
 * {@code -Ddojao.operator.batchBytes=...} 覆盖（探针据此压小上界验证判别力）。</p>
 */
final class BatchLimits {

    /** 单批逻辑字节上界（32MB）。 */
    static final long DEFAULT_BATCH_BYTES = 32L * 1024 * 1024;

    /** 覆盖属性（字节）。 */
    static final String BATCH_BYTES_PROPERTY = "dojao.operator.batchBytes";

    private BatchLimits() {
    }

    /** 当前生效的单批字节上界（每次读取，便于测试在类加载前设置属性）。 */
    static long batchBytes() {
        return Math.max(1L, Long.getLong(BATCH_BYTES_PROPERTY, DEFAULT_BATCH_BYTES));
    }
}
