package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 本地预聚合算子 —— 在每个 Worker 发送数据前，先对本地数据做一次预聚合，
 * 减少 Shuffle 的网络传输量。
 *
 * <p>该算子放在 {@link HashPartitionOperator} 之前：
 * <pre>{@code
 *   HashAggregation → HashPartition → LocalAggregation → Filter → Scan
 * }</pre>
 *
 * <p>LocalAggregationOperator 使用 {@link AggregationHashTable} 进行预聚合，
 * 然后为每个 group 计算目标分区 ID，输出带有 partitionId 标记的 Page。
 *
 * <p>输出 schema = key columns + accumulator columns（与 {@link HashAggregationOperator} 输出一致）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/15
 */
public class LocalAggregationOperator extends AbstractMeasurableOperator {

    private final Operator upstream;
    private final List<String> keyColumns;
    private final List<AggregationSpec> aggregations;
    private final int numPartitions;
    private final BufferAllocator allocator;

    private AggregationHashTable hashTable;
    private boolean consumed;
    private Iterator<VectorSchemaRoot> outputIterator;

    /**
     * @param upstream      上游算子（如 FilterOperator / ParallelTableScanOperator）
     * @param keyColumns    分组键列名
     * @param aggregations  聚合规格列表（与 HashAggregationOperator 一致）
     * @param numPartitions 目标分区数（用于计算 hash partition ID）
     */
    public LocalAggregationOperator(Operator upstream,
                                    List<String> keyColumns,
                                    List<AggregationSpec> aggregations,
                                    int numPartitions) {
        super("LocalAggregation", "LocalAggregation", 1);
        this.upstream = upstream;
        this.keyColumns = keyColumns;
        this.aggregations = aggregations;
        this.numPartitions = Math.max(1, numPartitions);
        // 分配器是进程级共享根（NodeMemory）的子 —— 聚合中间态堆外占用进统一账本，受共享根硬上限约束。
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator("local-agg", ResourceConfig.queryDirectLimit());
        this.consumed = false;
    }

    @Override
    protected void doOpen() throws Exception {
        upstream.open();
    }

    @Override
    protected Page doNext() throws Exception {
        // Phase 1: 消费上游，构建本地哈希表
        if (!consumed) {
            List<Field> keyFields = null;
            Page input;
            while (!(input = upstream.next()).isEOF()) {
                recordInput(input);

                if (hashTable == null) {
                    keyFields = new ArrayList<>();
                    for (String col : keyColumns) {
                        keyFields.add(input.getRoot().getSchema().findField(col));
                    }
                    hashTable = new AggregationHashTable(allocator, keyColumns, keyFields, aggregations);
                }

                int rowCount = input.getRowCount();
                for (int row = 0; row < rowCount; row++) {
                    hashTable.insertOrAppend(input, row);
                }
                input.close();
            }
            consumed = true;

            // 构建输出批次（带分区 ID）
            if (hashTable != null && !hashTable.isEmpty()) {
                List<VectorSchemaRoot> batches = buildPartitionedBatches();
                outputIterator = batches.iterator();
            }
        }

        // Phase 2: 输出带 partition ID 的 Page
        if (outputIterator != null && outputIterator.hasNext()) {
            VectorSchemaRoot root = outputIterator.next();
            return Page.fromRoot(root);
        }

        return Page.eof();
    }

    @Override
    protected void doClose() throws Exception {
        if (hashTable != null) {
            hashTable.close();
        }
        allocator.close();
        upstream.close();
    }

    /**
     * 构建带有 partition ID 的输出批次。
     * 每行的 partition ID 根据 key 列的哈希值计算。
     */
    private List<VectorSchemaRoot> buildPartitionedBatches() {
        VectorSchemaRoot fullRoot = hashTable.buildOutput();
        int totalRows = fullRoot.getRowCount();

        // 为每行计算 partition ID
        int[] partitionIds = new int[totalRows];
        for (int row = 0; row < totalRows; row++) {
            partitionIds[row] = computePartition(fullRoot, row);
        }

        // 按分区对行索引分组
        List<List<Integer>> partitionRowIndices = new ArrayList<>(numPartitions);
        for (int p = 0; p < numPartitions; p++) {
            partitionRowIndices.add(new ArrayList<>());
        }
        for (int row = 0; row < totalRows; row++) {
            partitionRowIndices.get(partitionIds[row]).add(row);
        }

        // 为每个分区创建一个输出 batch
        List<VectorSchemaRoot> batches = new ArrayList<>();
        for (int pid = 0; pid < numPartitions; pid++) {
            List<Integer> indices = partitionRowIndices.get(pid);
            if (indices.isEmpty()) continue;

            VectorSchemaRoot partRoot = copyRows(fullRoot, indices);
            batches.add(partRoot);
        }

        return batches;
    }

    private int computePartition(VectorSchemaRoot root, int row) {
        int hash = 0;
        for (String col : keyColumns) {
            FieldVector vec = root.getVector(col);
            if (vec == null) continue;
            if (vec.isNull(row)) {
                hash = 31 * hash;
            } else {
                hash = 31 * hash + vec.getObject(row).hashCode();
            }
        }
        return (hash & Integer.MAX_VALUE) % numPartitions;
    }

    /**
     * 从 VectorSchemaRoot 中拷贝指定行到一个新的 VectorSchemaRoot。
     */
    private VectorSchemaRoot copyRows(VectorSchemaRoot root, List<Integer> rowIndices) {
        int outCount = rowIndices.size();
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vectors = new ArrayList<>();

        for (int col = 0; col < root.getFieldVectors().size(); col++) {
            FieldVector src = root.getVector(col);
            Field field = root.getSchema().getFields().get(col);
            FieldVector dst = field.createVector(src.getAllocator());
            dst.setInitialCapacity(outCount);
            dst.allocateNew();
            var transfer = src.makeTransferPair(dst);
            for (int j = 0; j < outCount; j++) {
                transfer.copyValueSafe(rowIndices.get(j), j);
            }
            dst.setValueCount(outCount);
            fields.add(field);
            vectors.add(dst);
        }

        return new VectorSchemaRoot(fields, vectors, outCount);
    }
}
