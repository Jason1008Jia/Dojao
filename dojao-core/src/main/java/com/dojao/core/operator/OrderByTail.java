package com.dojao.core.operator;

import java.util.List;
import java.util.Optional;

/**
 * 顶层 ORDER BY/LIMIT 下推的 Worker 侧截断尾 —— 把某 Worker 结果产出缝的最终算子再接一段
 * 「单分区 → 本地全局排序 → 截断到 offset+limit」链，让每个切片的排序结果只回传 {@code top-(offset+limit)}
 * 行候选，而非全量。
 *
 * <p><b>正确性（下推定理）</b>：行被切成 P 个不相交切片；全局第 N 名必然来自某切片内的前 N 名
 * （{@code rank_global(r) ≥ rank_local(r)}），故全局 top-(offset+limit) 每一行都落在其所在切片的
 * 前 (offset+limit) 行内 —— 每个切片本地排序并截断到 offset+limit，Master 对 P×(offset+limit) 候选集
 * 的既有全局排序 + OFFSET 仍是精确答案。切片行数 ≤ offset+limit 时整片透传（无害）。</p>
 *
 * <p><b>形态 = Master {@code applyOrderLimit} 的复刻</b>：切片内排序键与 Master 全局一致，故切片 top-k
 * 行就是这些行在全局排序下的前 k 个 —— Master 只需归并语义上等价。链式：{@link SinglePartitionOperator}
 * 归一 pid（{@link WindowSortOperator} 空 groupKey = 全局单分组）→ {@link WindowSortOperator} 本地
 * 排序（可溢写）→ {@code truncates()} 时再包 {@link LimitOperator} 截断到 {@link
 * FragmentSpec.SortLimitStep#keepRows()}。</p>
 *
 * <p><b>内存/溢写</b>：{@link WindowSortOperator} 复用 Master 全局排序的溢写能力（超限落盘归并），
 * Worker 侧内存可测。候选集 = P×keepRows 仍是界内，Master 全局排序不因下推退化。</p>
 *
 * <p><b>仅终端缝可挂</b>：{@link FragmentSpec} 中只有回传 Master 的 terminal Consumer / 纯扫描
 * 分发携带 {@code tail}；中间层（Producer / MiddleConsumer）不落尾 —— 它们不回 Master，截断会破坏
 * 后续 shuffle 的正确性。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public final class OrderByTail {

    private OrderByTail() {
    }

    /**
     * 给某缝的最终算子包上截断尾链。{@code tail} 非 null 才调用（调用方先判空）。
     *
     * @param upstream 该缝回传 Master 前的最终算子（terminal Consumer 的本地结果算子 / 纯扫描的读循环）
     * @param tail     下推目标：排序键 + 方向 + offset/limit + Worker 内存上限
     * @return 包好尾链的算子；调用方须把本返回值的 next/close 接续到原回传路径
     */
    public static Operator wrap(Operator upstream, FragmentSpec.SortLimitStep tail) {
        Operator unify = new SinglePartitionOperator(upstream);
        WindowSortOperator sort = new WindowSortOperator(unify, List.of(), tail.orderByKeys(),
                tail.ascending(), tail.memoryLimit());
        return tail.truncates()
                ? new LimitOperator(sort, 0L, Optional.of(tail.keepRows()))
                : sort;
    }
}
