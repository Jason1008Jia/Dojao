package com.dojao.core.operator;

/**
 * 算子链的<b>归并进度</b>出口 —— 单调递增的 tick 计数，供 Worker 心跳判断「阻塞中的算子是否仍在推进」。
 *
 * <p><b>解决什么</b>：溢写算子的 Phase 2（分区归并）在<b>产出任何页之前</b>可能持续数十秒甚至更久
 * （把已溢写文件 K 路归并、逐组输出）。这段时间里 {@code runAndStream}/{@code runAndSink} 的心跳进度源
 * （上游 shuffle 收页数）完全静止 —— 输入早收齐了，卡的是归并本身。心跳线程据此判定「无进展」而不发
 * {@code FragmentProgress} ⇒ Master 结果看门狗（默认 60s）把长归并误判为「doExchange result 链路故障」
 * 并 fail-fast。<b>实锤矛盾</b>：同一次归并，算子内部容的是 {@code f.get(120, TimeUnit.SECONDS)}
 * （120s），却先被 60s 看门狗杀掉 —— 长归并必然误杀。</p>
 *
 * <p><b>怎么用</b>：溢写算子在归并推进处调 {@code bumpMergeProgress()}（每写完一个输出批 / 每吐一页 /
 * 每处理完一个 key 批），链上的包装算子（{@code LimitOperator} / {@code SinglePartitionOperator} /
 * {@code WindowFunctionOperator}）把自己的 tick 委托给下游真实干活的溢写算子。Worker 侧只需对算子链
 * 顶端调一次 {@link #mergeProgressTicks()}，即得整条链的累计归并进度，并入心跳进度源。</p>
 *
 * <p><b>保真度</b>：tick 只在归并<b>真正前进</b>时自增，因此「真卡死」（归并线程阻塞在 I/O 或死锁）
 * 时 tick 不动 ⇒ 心跳不发 ⇒ Master 看门狗照常 fail-fast，不会把死锁也一并续命。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/30
 */
public interface MergeProgress {

    /**
     * 本算子（含其上游链）自启动以来累计的归并进度 tick。单调不减；没有溢写归并的算子返回 0。
     */
    long mergeProgressTicks();
}
