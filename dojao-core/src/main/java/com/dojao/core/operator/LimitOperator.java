package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * LIMIT / OFFSET 算子 —— 流式截断顶层结果。
 *
 * <p>维护 {@code skipped}/{@code emitted} 两个长计数器跨页推进：先跳过 {@code offset} 行（整页
 * 整页丢），再逐行拷贝直至 {@code limit} 行返回 EOF。分页边界处理：某页跨过跳过/截断边界时，按
 * 需要的子区间 {@code [from, from+count)} 逐行拷贝出新页（拷贝范式同 {@link FilterOperator#doNext}：
 * 逐列 {@code field.createVector → allocateNew → makeTransferPair → copyValueSafe → setValueCount}）。
 * 整页完全命中且无后续截断 → 零拷贝直接透传上游页。</p>
 *
 * <p>{@code limit} 缺省 = 只跳 {@code offset} 后透传到底；{@code offset=0 且 limit 缺省} 时上限为
 * {@code Long.MAX_VALUE}，等价恒等透传（每次整页命中走零拷贝，开销仅一层调用）。{@code LIMIT 0}
 * / 超界 {@code OFFSET} → 首拉即 EOF，返回 0 行不崩。</p>
 *
 * <p><b>页所有权</b>：透传分支页所有权移交下游（本算子不 close）；拷贝分支先拷出再 close 源页。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public class LimitOperator extends AbstractMeasurableOperator implements MergeProgress {

    private final Operator upstream;
    private final long offset;
    private final Optional<Long> limit;
    private long skipped;
    private long emitted;
    private boolean done;

    public LimitOperator(Operator upstream, long offset, Optional<Long> limit) {
        super("Limit", "Limit", 1);
        this.upstream = upstream;
        this.offset = Math.max(0, offset);
        this.limit = limit == null ? Optional.empty() : limit;
        this.skipped = 0;
        this.emitted = 0;
        this.done = false;
    }

    /** 归并进度透传上游：顶层 ORDER BY/LIMIT 下推尾链里真正在归并的是下游的排序算子（可溢写）。 */
    @Override
    public long mergeProgressTicks() {
        return upstream instanceof MergeProgress mp ? mp.mergeProgressTicks() : 0L;
    }

    @Override
    protected void doOpen() throws Exception {
        upstream.open();
    }

    @Override
    protected Page doNext() throws Exception {
        long lim = limit.orElse(Long.MAX_VALUE);
        while (true) {
            if (emitted >= lim) {
                // 已达 limit 上限：停止拉取返回 EOF。残余上游由 doClose → upstream.close() 收尾
                // （分布式子算子在 doClose 反取消任务），不在此排空（排空可能阻塞在慢生产端）。
                done = true;
                return Page.eof();
            }
            Page input = upstream.next();
            recordInput(input);
            if (input.isEOF()) {
                done = true;
                return input;
            }
            int rc = input.getRowCount();
            if (rc <= 0) {
                input.close();
                continue;
            }

            int from = 0;   // 本页首个要输出的行
            if (skipped < offset) {
                long needSkip = offset - skipped;
                if (needSkip >= rc) {
                    skipped += rc;               // 整页都在 offset 内 → 整页跳过
                    input.close();
                    continue;
                }
                from = (int) needSkip;
                skipped += needSkip;
            }

            int count = (int) Math.min((long) rc - from, lim - emitted);
            if (count <= 0) {
                input.close();
                done = true;
                return Page.eof();
            }

            // 整页完全命中（from==0 且 count==rc）且未达截断边界 → 零拷贝透传
            if (from == 0 && count == rc) {
                emitted += count;
                return input;   // 页所有权移交下游，本算子不 close
            }

            VectorSchemaRoot inRoot = input.getRoot();
            List<Field> fields = inRoot.getSchema().getFields();
            List<FieldVector> out = new ArrayList<>(fields.size());
            for (int col = 0; col < fields.size(); col++) {
                Field field = fields.get(col);
                FieldVector src = inRoot.getVector(col);
                FieldVector dst = field.createVector(src.getAllocator());
                dst.setInitialCapacity(count);
                dst.allocateNew();
                TransferPair tp = src.makeTransferPair(dst);
                for (int j = 0; j < count; j++) {
                    tp.copyValueSafe(from + j, j);
                }
                dst.setValueCount(count);
                out.add(dst);
            }
            input.close();
            emitted += count;
            return Page.fromRoot(new VectorSchemaRoot(fields, out, count));
        }
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
    }
}
