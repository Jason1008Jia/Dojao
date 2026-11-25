package com.dojao.core.operator;

import com.dojao.common.data.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code UNION ALL} 之上的分布式写编排 —— 每条腿各自把 sink 下推到归属 Worker，
 * <b>数据页不回 Master</b>（UNION ALL 只是拼接，按位置写入目标表即可）。
 *
 * <p>为什么需要独立算子：{@code UNION ALL} 原本是 Master 本地 {@link UnionAllOperator}
 * （逐页拼接后再交给 TableSink），一旦顶层出现它就整表回 Master。这里把「拼接」这一步省掉：
 * 各腿本来就已经产出完整行，直接各写各的；Master 只负责生命周期（建 staging / 提交 / 回滚）
 * 和等待各腿的 FINISHED。</p>
 *
 * <p>腿（{@link Leg}）可以是：</p>
 * <ul>
 *   <li>{@link AbstractDistributedOperator}（join / 聚合 / 窗口链；可带投影描述）；</li>
 *   <li>{@link DistributedScanOperator}（scan-sink：纯扫描 ± 投影）。</li>
 * </ul>
 *
 * <p>行数 = 各腿 {@code totalOutputRows()} / {@code writtenRows()} 之和（Master 不再收数据页）。</p>
 *
 * @author jiaxin
 * @date 2026/11/24
 */
public final class DistributedUnionAllOperator extends AbstractMeasurableOperator {

    /**
     * 一条腿：{@code operator} 是真正执行写的算子，{@code projection} 是它自己的 sink 投影
     * （null = 裸算子按输出列写）。
     */
    public record Leg(Operator operator, FragmentSpec.ScanProjection projection) {
    }

    private final List<Leg> legs;
    private volatile Runnable prepare;
    private volatile Runnable successFinalizer;
    private volatile Runnable abortFinalizer;
    private final AtomicBoolean finalized = new AtomicBoolean();
    private volatile boolean skipDispatch;

    public DistributedUnionAllOperator(List<Leg> legs) {
        super("DistributedUnionAll", "UnionAll", Math.max(1, legs.size()));
        this.legs = List.copyOf(legs == null ? List.of() : legs);
    }

    /**
     * 给每条腿挂上同一个写目标（INSERT INTO 立即调用；OVERWRITE / CTAS 在 prepare 里用 staging /
     * 新建表名调用，保证 EXPLAIN 零副作用）。
     */
    public void setSink(String catalog, String schema, String table, List<String> targetColumns) {
        for (Leg leg : legs) {
            FragmentSpec.SinkStep step = new FragmentSpec.SinkStep(
                    catalog, schema, table, targetColumns, leg.projection());
            if (leg.operator() instanceof AbstractDistributedOperator d) {
                d.setSink(step);
            } else if (leg.operator() instanceof DistributedScanOperator s) {
                s.setSink(step);
            }
        }
    }

    /** 与 {@link AbstractDistributedOperator#setOverwriteLifecycle} 同语义（prepare / success / abort）。 */
    public void setOverwriteLifecycle(Runnable prepare, Runnable success, Runnable abort) {
        this.prepare = prepare;
        this.successFinalizer = success;
        this.abortFinalizer = abort;
    }

    /** 目标表已存在且 IF NOT EXISTS → 不派发、rows=0（CTAS 跳过语义）。 */
    public void skipDispatch() {
        this.skipDispatch = true;
    }

    /** 各腿实际写入行数之和（Master 只等 FINISHED，不接收数据页）。 */
    public long totalRows() {
        long rows = 0L;
        for (Leg leg : legs) {
            if (leg.operator() instanceof AbstractDistributedOperator d) {
                rows += d.totalOutputRows();
            } else if (leg.operator() instanceof DistributedScanOperator s) {
                rows += s.writtenRows();
            }
        }
        return rows;
    }

    @Override
    protected void doOpen() throws Exception {
        if (prepare != null) prepare.run();
        if (skipDispatch) return;
        for (Leg leg : legs) {
            leg.operator().open();
        }
    }

    @Override
    protected Page doNext() throws Exception {
        if (skipDispatch) {
            finalizeSuccess();
            return Page.eof();
        }
        for (Leg leg : legs) {
            while (true) {
                Page page = leg.operator().next();
                if (page.isEOF()) break;
                // 有 sink 的腿不该回传数据页；出现也只丢弃（绝不重复写）。
                page.close();
            }
        }
        finalizeSuccess();
        return Page.eof();
    }

    @Override
    protected void doClose() throws Exception {
        Exception first = null;
        for (Leg leg : legs) {
            try {
                leg.operator().close();
            } catch (Exception e) {
                if (first == null) first = e;
            }
        }
        // 未走到 EOF（失败 / 取消）→ 回滚 staging / 删刚建的表
        if (finalized.compareAndSet(false, true) && abortFinalizer != null) {
            abortFinalizer.run();
        }
        if (first != null) throw first;
    }

    private void finalizeSuccess() {
        if (finalized.compareAndSet(false, true) && successFinalizer != null) {
            successFinalizer.run();
        }
    }

    /** 供 EXPLAIN / 诊断：每条腿的算子名。 */
    public List<String> legNames() {
        List<String> out = new ArrayList<>(legs.size());
        for (Leg leg : legs) out.add(leg.operator().getClass().getSimpleName());
        return out;
    }
}
