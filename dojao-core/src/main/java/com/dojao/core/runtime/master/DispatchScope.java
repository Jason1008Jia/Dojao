package com.dojao.core.runtime.master;

import java.util.function.Consumer;

/**
 * 查询执行期的「片段派发作用域」—— 把 Master 派发出去的每个 jobId 归属到<b>当前执行的查询</b>。
 *
 * <p><b>为什么需要它</b>：Master 的「僵尸片段回收」按 jobId 的查询归属判定要不要取消 Worker 上的
 * 残留片段。归属集合原来只从主物理计划树收集（{@code QueryManager.collect(root)}），但执行期存在
 * <b>不在主树内</b>的分布式子计划：物化 CTE / 物化派生体（A 方案）的查询体由 {@code Store.materialize()}
 * 惰性规划并执行，用自己的 {@code query-<hex>} jobId 派发片段。归属收不到 → 被误判成「Master 已不认」
 * → 正在运行的查询体片段被就地取消（线上表现：{@code 回收残留片段} + 中间片段 {@code InterruptedException}）。</p>
 *
 * <p><b>机制</b>：Master 上所有 Worker 片段都从两个入口派发 ——
 * {@link com.dojao.core.operator.AbstractDistributedOperator}（fragment）与
 * {@link com.dojao.core.operator.DistributedScanOperator}（切片）。它们派发时调用
 * {@link #register(String)}；执行入口（QueryServiceImpl）在算子链 {@code open()} 前
 * {@link #begin(Consumer)}（回调 = {@code QueryManager::registerOperatorQueryId}）、{@code finally} 里
 * {@link #end()}。于是<b>任何</b>派发分支（含未来新增的辅助子计划）都自动获得归属，不再依赖
 * 「规划期记得把子计划挂进主树」。</p>
 *
 * <p>未 {@code begin} 的执行路径（探针 / 伪分布式 / 手工执行计划）中 {@code register} 是无操作。</p>
 *
 * @author jiaxin
 * @date 2026/11/11
 */
public final class DispatchScope {

    private static final ThreadLocal<Consumer<String>> REGISTRAR = new ThreadLocal<>();

    private DispatchScope() { }

    /** 进入执行期派发作用域（执行线程上调用；必须与 {@link #end()} 成对，放在 finally 里）。 */
    public static void begin(Consumer<String> registrar) {
        REGISTRAR.set(registrar);
    }

    /** 退出作用域：必须移除，避免线程池复用线程时把登记回调泄漏给下一条查询。 */
    public static void end() {
        REGISTRAR.remove();
    }

    /** 登记一个刚派发出去的 jobId（无作用域时空操作）。 */
    public static void register(String jobId) {
        if (jobId == null) {
            return;
        }
        Consumer<String> registrar = REGISTRAR.get();
        if (registrar != null) {
            registrar.accept(jobId);
        }
    }
}
