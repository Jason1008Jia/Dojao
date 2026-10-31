package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Broadcast Hash Join 算子。
 *
 * <h3>执行流程</h3>
 * <pre>
 *   Phase 1 (Build): 小表全量加载 → Arrow 列式向量 + HashMap key → List&lt;行号&gt; 索引
 *   Phase 2 (Probe): 大表逐页流过 → 查 HashMap 索引 → cross join → 输出
 *   Phase 3 (Orphan): RIGHT/FULL 时输出 build 侧未匹配的行 (BitSet 追踪)
 * </pre>
 *
 * <h3>适用条件</h3>
 * <ul>
 *   <li>右表 (build 侧) 数据量小于可用内存</li>
 *   <li>典型场景: 小维度表 join 大事实表</li>
 *   <li>比 SortMergeJoin 快: 不排序、不溢写、不归并</li>
 * </ul>
 *
 * <h3>支持 Join 类型</h3>
 * <ul>
 *   <li>INNER: 只输出匹配 key</li>
 *   <li>LEFT: 左表全部保留, 右表无匹配填 NULL</li>
 *   <li>RIGHT: 右表全部保留, 左表无匹配填 NULL</li>
 *   <li>FULL: 两侧全部保留, 无匹配侧填 NULL</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/7/30
 */
public class BroadcastHashJoinOperator extends AbstractMeasurableOperator {

    private static final int OUTPUT_BATCH_SIZE = 10000;

    private final Operator leftUpstream;   // probe 侧 (大表)
    private final Operator rightUpstream;  // build 侧 (小表)
    private final List<String> leftKeyColumns;
    private final List<String> rightKeyColumns;
    private final JoinType joinType;
    private final BufferAllocator allocator;

    // ---- build 侧 (右表, 全量加载到内存) ----
    private List<FieldVector> buildVectors;
    private Map<Object, List<Integer>> buildIndex;  // key → 行号列表
    private int buildRowCount;
    private Schema buildSchema;
    private BitSet buildMatched;  // RIGHT/FULL 追踪未匹配行

    // ---- probe 侧 + 输出 ----
    private Schema probeSchema;
    private List<Field> outputFields;
    private boolean buildDone;
    private boolean probeDone;
    private final Queue<Page> outputQueue;

    // ==================== 构造函数 ====================

    public BroadcastHashJoinOperator(Operator leftUpstream,
                                      Operator rightUpstream,
                                      List<String> leftKeyColumns,
                                      List<String> rightKeyColumns,
                                      JoinType joinType,
                                      long memoryLimit) {
        super("BroadcastHashJoin", "BroadcastHashJoin", 2);
        this.leftUpstream = leftUpstream;
        this.rightUpstream = rightUpstream;
        this.leftKeyColumns = leftKeyColumns;
        this.rightKeyColumns = rightKeyColumns;
        this.joinType = joinType;
        // 分配器是进程级共享根（NodeMemory）的子 —— 全进程堆外用量统一记账（背压水位看得见本算子的占用），
        // 并受共享根硬上限约束（超限抛 OOM 使查询失败，而非被内核 OOM-killer 杀进程）。
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator("hashjoin-broadcast", ResourceConfig.queryDirectLimit());
        this.outputQueue = new ConcurrentLinkedQueue<>();
        this.buildDone = false;
        this.probeDone = false;
    }

    public BroadcastHashJoinOperator(Operator leftUpstream,
                                      Operator rightUpstream,
                                      String leftKeyColumn,
                                      String rightKeyColumn,
                                      JoinType joinType,
                                      long memoryLimit) {
        this(leftUpstream, rightUpstream,
                List.of(leftKeyColumn), List.of(rightKeyColumn),
                joinType, memoryLimit);
    }

    // ==================== 生命周期 ====================

    @Override
    protected void doOpen() throws Exception {
        leftUpstream.open();
        rightUpstream.open();
    }

    @Override
    protected Page doNext() throws Exception {
        // ---- Phase 1: 构建 (消费右表, 建立 hash 索引) ----
        if (!buildDone) {
            buildRightTable();
            buildDone = true;
        }

        // ---- Phase 2: 探测 (流式消费左表, 查索引输出) ----
        if (!probeDone) {
            probeLeftTable();
            probeDone = true;
        }

        // ---- Phase 3: 孤儿 (RIGHT/FULL 时输出未匹配的 build 行) ----
        if (probeDone) {
            if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
                outputUnmatchedBuildRows();
            }
            if (!outputQueue.isEmpty()) return outputQueue.poll();
            return Page.eof();
        }

        if (!outputQueue.isEmpty()) return outputQueue.poll();
        return Page.eof();
    }

    @Override
    protected void doClose() throws Exception {
        Page p;
        while ((p = outputQueue.poll()) != null) {
            try { p.close(); } catch (Exception ignored) {}
        }
        if (buildVectors != null) {
            for (FieldVector v : buildVectors) {
                try { v.close(); } catch (Exception ignored) {}
            }
        }
        leftUpstream.close();
        rightUpstream.close();
        allocator.close();
    }

    // ==================== Phase 1: Build ====================

    private void buildRightTable() {
        buildVectors = new ArrayList<>();
        buildIndex = new HashMap<>();
        buildRowCount = 0;

        int keyColIdx = -1;
        boolean vectorsReady = false;

        Page page;
        while (!(page = rightUpstream.next()).isEOF()) {
            recordInput(page);
            VectorSchemaRoot root = page.getRoot();
            if (root == null || root.getRowCount() == 0) {
                page.close();
                continue;
            }

            if (keyColIdx < 0) {
                buildSchema = root.getSchema();
                keyColIdx = resolveKeyIndices(buildSchema, rightKeyColumns)[0];
            }

            int pageRows = root.getRowCount();
            int needed = buildRowCount + pageRows;

            // 按需创建/扩容 buildVectors（不用 reAlloc → Netty setBytes 自动扩容）
            if (!vectorsReady) {
                for (int c = 0; c < root.getFieldVectors().size(); c++) {
                    FieldVector src = root.getVector(c);
                    FieldVector dst = src.getField().createVector(allocator);
                    dst.setInitialCapacity(needed);
                    dst.allocateNew();
                    buildVectors.add(dst);
                }
                vectorsReady = true;
            }

            // 列批量拷贝到 buildVectors
            for (int c = 0; c < buildVectors.size(); c++) {
                RowAccumulator.copyColumnBatch(
                        root.getVector(c), buildVectors.get(c), 0, buildRowCount, pageRows);
            }

            // 逐行建 hash 索引
            FieldVector keyVec = root.getVector(keyColIdx);
            for (int r = 0; r < pageRows; r++) {
                Object key = RowAccumulator.readKeyValue(keyVec, r);
                buildIndex.computeIfAbsent(key, k -> new ArrayList<>(4)).add(buildRowCount + r);
            }

            buildRowCount += pageRows;
            page.close();
            releaseInput(page);
        }
        page.close();
        releaseInput(page);  // EOF page

        for (FieldVector v : buildVectors) v.setValueCount(buildRowCount);

        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
            buildMatched = new BitSet(buildRowCount);
        }
    }

    // ==================== Phase 2: Probe ====================

    private void probeLeftTable() {
        int keyColIdx = -1;

        Page page;
        while (!(page = leftUpstream.next()).isEOF()) {
            recordInput(page);
            VectorSchemaRoot root = page.getRoot();
            if (root == null || root.getRowCount() == 0) {
                page.close();
                continue;
            }

            if (keyColIdx < 0) {
                probeSchema = root.getSchema();
                int[] keyIndices = resolveKeyIndices(probeSchema, leftKeyColumns);
                keyColIdx = keyIndices[0];

                // 构建输出 schema
                outputFields = new ArrayList<>(probeSchema.getFields());
                outputFields.addAll(buildSchema.getFields());
            }

            int lCols = root.getFieldVectors().size();
            int rCols = buildVectors.size();
            List<FieldVector> outVectors = null;
            int[] lWidths = null, rWidths = null;
            int filled = 0;

            FieldVector keyVec = root.getVector(keyColIdx);
            for (int r = 0; r < root.getRowCount(); r++) {
                Object key = RowAccumulator.readKeyValue(keyVec, r);
                List<Integer> buildRows = buildIndex.get(key);

                if (buildRows != null && !buildRows.isEmpty()) {
                    // 匹配 → cross join
                    for (int bRow : buildRows) {
                        if (filled >= OUTPUT_BATCH_SIZE || outVectors == null) {
                            if (outVectors != null && filled > 0) {
                                flushOutput(outVectors, filled);
                            }
                            outVectors = new ArrayList<>();
                            lWidths = new int[lCols];
                            rWidths = new int[rCols];
                            filled = 0;
                            // 创建左列
                            for (int c = 0; c < lCols; c++) {
                                FieldVector src = root.getVector(c);
                                FieldVector v = src.getField().createVector(allocator);
                                v.setInitialCapacity(OUTPUT_BATCH_SIZE);
                                v.allocateNew();
                                outVectors.add(v);
                                lWidths[c] = RowAccumulator.typeWidthOf(src.getField().getType());
                            }
                            // 创建右列
                            for (int c = 0; c < rCols; c++) {
                                FieldVector src = buildVectors.get(c);
                                FieldVector v = src.getField().createVector(allocator);
                                v.setInitialCapacity(OUTPUT_BATCH_SIZE);
                                v.allocateNew();
                                outVectors.add(v);
                                rWidths[c] = RowAccumulator.typeWidthOf(src.getField().getType());
                            }
                        }

                        // 拷贝左行
                        for (int c = 0; c < lCols; c++) {
                            copyCell(root.getVector(c), outVectors.get(c), lWidths[c], r, filled);
                        }
                        // 拷贝右行
                        for (int c = 0; c < rCols; c++) {
                            copyCell(buildVectors.get(c), outVectors.get(lCols + c), rWidths[c], bRow, filled);
                        }
                        filled++;

                        // RIGHT/FULL: 标记 build 行已匹配
                        if (buildMatched != null) {
                            buildMatched.set(bRow);
                        }
                    }
                } else {
                    // 不匹配 → LEFT/FULL 输出左孤儿
                    if (joinType == JoinType.LEFT || joinType == JoinType.FULL) {
                        if (filled >= OUTPUT_BATCH_SIZE || outVectors == null) {
                            if (outVectors != null && filled > 0) {
                                flushOutput(outVectors, filled);
                            }
                            outVectors = new ArrayList<>();
                            lWidths = new int[lCols];
                            rWidths = new int[rCols];
                            filled = 0;
                            for (int c = 0; c < lCols; c++) {
                                FieldVector src = root.getVector(c);
                                FieldVector v = src.getField().createVector(allocator);
                                v.setInitialCapacity(OUTPUT_BATCH_SIZE);
                                v.allocateNew();
                                outVectors.add(v);
                                lWidths[c] = RowAccumulator.typeWidthOf(src.getField().getType());
                            }
                            for (int c = 0; c < rCols; c++) {
                                FieldVector src = buildVectors.get(c);
                                FieldVector v = src.getField().createVector(allocator);
                                v.setInitialCapacity(OUTPUT_BATCH_SIZE);
                                v.allocateNew();
                                outVectors.add(v);
                                rWidths[c] = RowAccumulator.typeWidthOf(src.getField().getType());
                            }
                        }
                        for (int c = 0; c < lCols; c++) {
                            copyCell(root.getVector(c), outVectors.get(c), lWidths[c], r, filled);
                        }
                        // 右侧列显式设为 NULL（LEFT 孤儿）
                        for (int c = 0; c < rCols; c++) outVectors.get(lCols + c).setNull(filled);
                        filled++;
                    }
                }
            }

            if (outVectors != null && filled > 0) {
                flushOutput(outVectors, filled);
            }
            page.close();
            releaseInput(page);
        }
    }

    // ==================== Phase 3: Orphan (RIGHT/FULL) ====================

    private void outputUnmatchedBuildRows() {
        int lCols = probeSchema.getFields().size();
        int rCols = buildVectors.size();
        List<FieldVector> outVectors = null;
        int[] lWidths = null, rWidths = null;
        int filled = 0;

        for (int bRow = 0; bRow < buildRowCount; bRow++) {
            if (buildMatched.get(bRow)) continue;  // 已匹配, 跳过

            if (filled >= OUTPUT_BATCH_SIZE || outVectors == null) {
                if (outVectors != null && filled > 0) {
                    flushOutput(outVectors, filled);
                }
                outVectors = new ArrayList<>();
                lWidths = new int[lCols];
                rWidths = new int[rCols];
                filled = 0;
                for (int c = 0; c < lCols; c++) {
                    Field f = probeSchema.getFields().get(c);
                    FieldVector v = f.createVector(allocator);
                    v.setInitialCapacity(OUTPUT_BATCH_SIZE);
                    v.allocateNew();
                    outVectors.add(v);
                    lWidths[c] = RowAccumulator.typeWidthOf(f.getType());
                }
                for (int c = 0; c < rCols; c++) {
                    FieldVector src = buildVectors.get(c);
                    FieldVector v = src.getField().createVector(allocator);
                    v.setInitialCapacity(OUTPUT_BATCH_SIZE);
                    v.allocateNew();
                    outVectors.add(v);
                    rWidths[c] = RowAccumulator.typeWidthOf(src.getField().getType());
                }
            }

            for (int c = 0; c < rCols; c++) {
                copyCell(buildVectors.get(c), outVectors.get(lCols + c), rWidths[c], bRow, filled);
            }
            // 左侧列显式设为 NULL（RIGHT 孤儿）
            for (int c = 0; c < lCols; c++) outVectors.get(c).setNull(filled);
            filled++;
        }

        if (outVectors != null && filled > 0) {
            flushOutput(outVectors, filled);
        }
    }

    // ==================== 输出 ====================

    private void flushOutput(List<FieldVector> outVectors, int filled) {
        for (FieldVector v : outVectors) v.setValueCount(filled);
        List<Field> fields = new ArrayList<>(outVectors.size());
        for (FieldVector v : outVectors) fields.add(v.getField());
        outputQueue.add(Page.partitioned(
                new VectorSchemaRoot(fields, new ArrayList<>(outVectors), filled), 0));
    }

    // ==================== 工具 ====================

    private static void copyCell(FieldVector src, FieldVector dst, int tw, int srcIdx, int dstIdx) {
        if (tw > 0) {
            dst.getDataBuffer().setBytes((long) dstIdx * tw,
                    src.getDataBuffer(), (long) srcIdx * tw, tw);
            // Arrow allocateNew 的 validity 缓冲区可能不是全1，必须显式设置
            if (src.isNull(srcIdx)) {
                dst.setNull(dstIdx);
            } else {
                org.apache.arrow.memory.ArrowBuf vb = dst.getValidityBuffer();
                int byteIdx = dstIdx / 8, bitMask = 1 << (dstIdx % 8);
                vb.setByte(byteIdx, (byte) (vb.getByte(byteIdx) | bitMask));
            }
        } else {
            src.makeTransferPair(dst).copyValueSafe(srcIdx, dstIdx);
        }
    }

    private static int[] resolveKeyIndices(Schema schema, List<String> keyColumns) {
        List<Field> fields = schema.getFields();
        int[] indices = new int[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            String name = keyColumns.get(i);
            indices[i] = -1;
            for (int j = 0; j < fields.size(); j++) {
                if (fields.get(j).getName().equals(name)) {
                    indices[i] = j;
                    break;
                }
            }
            if (indices[i] < 0) {
                throw new IllegalArgumentException(
                        "Key column '" + name + "' not found in schema: " + fields);
            }
        }
        return indices;
    }
}
