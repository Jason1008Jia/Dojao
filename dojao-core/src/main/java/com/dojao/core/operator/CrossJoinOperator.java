package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Cross Join 算子 —— 笛卡尔积。
 *
 * <p>无 join key，不涉及 hash 分区（与"分区即并行"的 hash join 栈无关），直接缓存右表全部数据，
 * 逐左页与右表做笛卡尔积，按 {@link #OUTPUT_BATCH_SIZE} 分批输出，避免整张结果一次性驻留内存。
 *
 * <p>输出 Schema：左表所有列 + 右表所有列。
 *
 * @author jiaxin
 * @date 2026/8/5
 */
public class CrossJoinOperator extends AbstractMeasurableOperator {

    /** 输出批大小：每凑满 5W 行组装一个 VectorSchemaRoot。 */
    private static final int OUTPUT_BATCH_SIZE = 50000;

    private final Operator leftOperator;
    private final Operator rightOperator;
    private final BufferAllocator allocator;

    // 右表缓存（Phase 1 一次性缓冲）
    private boolean buildPhaseDone;
    private List<Page> crossJoinRightPages;
    private Schema rightSchema;

    // 输出 schema（左列 + 右列）
    private Schema leftSchema;
    private Schema outputSchema;
    private int[] rightColumnOutputMapping;

    // 当前左页产生的分批输出（未消费完则续拉）
    private List<VectorSchemaRoot> outputBatches;
    private Iterator<VectorSchemaRoot> outputIterator;

    public CrossJoinOperator(Operator leftOperator, Operator rightOperator) {
        super("CrossJoin", "CrossJoin", 1);
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
        // 分配器是进程级共享根（NodeMemory）的子 —— 全进程堆外用量统一记账，受共享根硬上限约束。
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator("crossjoin", ResourceConfig.queryDirectLimit());
        this.buildPhaseDone = false;
    }

    // ==================== 生命周期 ====================

    @Override
    protected void doOpen() throws Exception {
        leftOperator.open();
        rightOperator.open();
    }

    @Override
    protected Page doNext() throws Exception {
        // Phase 1: 缓存右表所有数据
        if (!buildPhaseDone) {
            System.err.println("[CrossJoin] buffering right source...");
            crossJoinRightPages = new ArrayList<>();
            Page rightPage;
            while (!(rightPage = rightOperator.next()).isEOF()) {
                recordInput(rightPage);
                if (rightSchema == null) rightSchema = rightPage.getSchema();
                crossJoinRightPages.add(rightPage);
            }
            buildPhaseDone = true;
            System.err.printf("[CrossJoin] %d right pages buffered%n", crossJoinRightPages.size());
        }

        // 空右表 → EOF
        if (crossJoinRightPages.isEmpty()) {
            return Page.eof();
        }

        // 先检查是否有未消费完的输出批次
        if (outputIterator != null && outputIterator.hasNext()) {
            return Page.fromRoot(outputIterator.next());
        }

        // 读取下一个左页，产生笛卡尔积输出批次
        Page leftPage = leftOperator.next();
        if (leftPage.isEOF()) {
            return Page.eof();
        }
        recordInput(leftPage);
        if (leftSchema == null) {
            leftSchema = leftPage.getSchema();
            ensureOutputSchema();
        }

        List<VectorSchemaRoot> batches = buildCrossJoinBatches(leftPage);
        leftPage.close();
        releaseInput(leftPage);

        if (batches.isEmpty()) {
            return Page.eof();
        }
        outputBatches = batches;
        outputIterator = batches.iterator();
        return Page.fromRoot(outputIterator.next());
    }

    @Override
    protected void doClose() throws Exception {
        // 1. 关闭未消费完的输出批次
        if (outputBatches != null) {
            for (VectorSchemaRoot batch : outputBatches) {
                try { batch.close(); } catch (Exception ignored) {}
            }
            outputBatches.clear();
        }
        // 2. 关闭缓存的右表数据
        if (crossJoinRightPages != null) {
            for (Page p : crossJoinRightPages) {
                try { p.close(); } catch (Exception ignored) {}
            }
            crossJoinRightPages.clear();
        }
        // 3. 关闭上游
        try { leftOperator.close(); } catch (Exception ignored) {}
        try { rightOperator.close(); } catch (Exception ignored) {}
        // 4. 关闭自己的 allocator
        allocator.close();
    }

    // ==================== 笛卡尔积组装 ====================

    /** 计算当前左页与全部右表行的配对，按 {@link #OUTPUT_BATCH_SIZE} 分批组装。 */
    private List<VectorSchemaRoot> buildCrossJoinBatches(Page leftPage) {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        VectorSchemaRoot leftRoot = leftPage.getRoot();
        int leftRowCount = leftRoot.getRowCount();
        int totalRightRows = 0;
        for (Page rp : crossJoinRightPages) {
            totalRightRows += rp.getRowCount();
        }

        // 计算每个左行对应的所有右行索引（右行索引是跨 Page 的全局索引）
        List<int[]> allPairs = new ArrayList<>();
        for (int lr = 0; lr < leftRowCount; lr++) {
            int globalRightIdx = 0;
            for (Page rightPage : crossJoinRightPages) {
                int rrCount = rightPage.getRowCount();
                for (int rr = 0; rr < rrCount; rr++) {
                    allPairs.add(new int[]{lr, globalRightIdx});
                    globalRightIdx++;
                }
            }
        }

        // 分批组装
        for (int i = 0; i < allPairs.size(); i += OUTPUT_BATCH_SIZE) {
            int end = Math.min(i + OUTPUT_BATCH_SIZE, allPairs.size());
            List<int[]> batch = allPairs.subList(i, end);
            batches.add(assembleCrossJoinBatch(batch, leftRoot));
        }

        return batches;
    }

    private VectorSchemaRoot assembleCrossJoinBatch(List<int[]> pairs, VectorSchemaRoot leftRoot) {
        int outRows = pairs.size();
        VectorSchemaRoot outRoot = VectorSchemaRoot.create(outputSchema, allocator);
        outRoot.allocateNew();

        int leftOutCols = leftSchema.getFields().size();
        List<FieldVector> outVectors = outRoot.getFieldVectors();

        // 为左列创建 TransferPair
        List<TransferPair> leftTransfers = new ArrayList<>();
        for (int c = 0; c < leftOutCols; c++) {
            FieldVector src = leftRoot.getVector(c);
            leftTransfers.add(src.makeTransferPair(outVectors.get(c)));
        }

        // 为右列批量拷贝：需要将全局右行索引映射回具体的 Page + 页内行索引
        int[] rightPageStarts = new int[crossJoinRightPages.size() + 1];
        for (int i = 0; i < crossJoinRightPages.size(); i++) {
            rightPageStarts[i + 1] = rightPageStarts[i] + crossJoinRightPages.get(i).getRowCount();
        }

        for (int outR = 0; outR < outRows; outR++) {
            int[] pair = pairs.get(outR);

            // 拷贝左列
            for (int c = 0; c < leftOutCols; c++) {
                leftTransfers.get(c).copyValueSafe(pair[0], outR);
            }

            // 拷贝右列：定位右行所在的 Page 和页内索引
            int globalRightIdx = pair[1];
            int rpIdx = 0;
            while (rpIdx < rightPageStarts.length - 1
                    && globalRightIdx >= rightPageStarts[rpIdx + 1]) {
                rpIdx++;
            }
            int localRightIdx = globalRightIdx - rightPageStarts[rpIdx];
            Page rightPage = crossJoinRightPages.get(rpIdx);
            VectorSchemaRoot rightRoot = rightPage.getRoot();

            for (int c = 0; c < rightColumnOutputMapping.length; c++) {
                int outCol = rightColumnOutputMapping[c];
                if (outCol >= 0) {
                    FieldVector src = rightRoot.getVector(c);
                    outVectors.get(outCol).copyFromSafe(localRightIdx, outR, src);
                }
            }
        }

        outRoot.setRowCount(outRows);
        return outRoot;
    }

    // ==================== 输出 schema ====================

    /** 首次看到左表数据后构建输出 schema：左表所有列 + 右表所有列。 */
    private void ensureOutputSchema() {
        if (outputSchema != null) return;

        List<Field> outFields = new ArrayList<>();
        for (Field f : leftSchema.getFields()) {
            outFields.add(f);
        }
        if (rightSchema != null) {
            rightColumnOutputMapping = new int[rightSchema.getFields().size()];
            for (int i = 0; i < rightSchema.getFields().size(); i++) {
                rightColumnOutputMapping[i] = outFields.size();
                outFields.add(rightSchema.getFields().get(i));
            }
        } else {
            rightColumnOutputMapping = new int[0];
        }

        outputSchema = new Schema(outFields);
        System.err.printf("[CrossJoin] Output schema: %d cols (left=%d + right=%d)%n",
                outFields.size(), leftSchema.getFields().size(),
                rightSchema != null ? rightSchema.getFields().size() : 0);
    }
}
