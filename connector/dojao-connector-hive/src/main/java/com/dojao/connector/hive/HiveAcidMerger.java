package com.dojao.connector.hive;

import com.dojao.connector.common.util.ORCFileArrowConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <b>Hive ACID 行级合并</b> —— 把一个 (分区, 桶) 分组内的全部文件（base + delta + delete_delta +
 * 非事务遗留文件）按 {@code RecordIdentifier} 归并成「活行」。
 *
 * <p>ACID ORC 文件的顶层结构是
 * {@code struct<operation:int, originalTransaction:bigint, bucket:int, rowId:bigint,
 * currentTransaction:bigint, row:struct<_col0:...,_col1:...>>} —— 用户列嵌在 {@code row} 里
 * （按<b>位置</b>对应，名子 {@code _col0..} 不保留原名）。行身份 = Hive {@code RecordIdentifier}
 * {@code (originalTransaction, bucketId, rowId)}。</p>
 *
 * <p><b>合并规则</b>（与 Hive {@code OrcRawRecordMerger} 同口径）：同一行的多条记录里
 * {@code currentTransaction} 最大者为最新版本；若最新版本是删除（{@code operation=2}）→ 该行被删；
 * 否则以最新值保留。实现上给每个行身份分配一个 slot，读到更新版本就<b>原地覆盖</b>该 slot ——
 * 删除只落标记不写数据，因此常驻内存 ≈ 去重后的活行数。</p>
 *
 * <p>非 ACID 的遗留文件（顶层无 {@code operation} 列）原样读入，每行独占一个 slot（它们没有
 * RowId，不可能被删除/更新）。</p>
 *
 * <p>与 HDFS / HMS 解耦（只依赖 {@link Configuration} + {@link FileSystem}），因此可离线探针驱动：
 * 本地造 ACID 形状的 ORC 文件，直接调 {@link #mergeGroup}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/7
 */
final class HiveAcidMerger {

    /** Hive {@code OrcRecordUpdater} 的操作码。 */
    static final byte OP_INSERT = 0;
    static final byte OP_UPDATE = 1;
    static final byte OP_DELETE = 2;

    /** 行身份 = Hive {@code RecordIdentifier}(originalTransaction, bucketId, rowId)。 */
    record RowKey(long origTxn, int bucket, long rowId) {
    }

    /** 合并结果：一个分组的活行（数据列，顺序同 {@link HiveTableMeta#dataFields()}）。 */
    static final class Result implements AutoCloseable {
        private final String root;
        private final List<Field> fields;
        private final List<FieldVector> columns;
        private final int rowCount;

        Result(String root, List<Field> fields, List<FieldVector> columns, int rowCount) {
            this.root = root;
            this.fields = fields;
            this.columns = columns;
            this.rowCount = rowCount;
        }

        String root() { return root; }
        List<Field> fields() { return fields; }
        List<FieldVector> columns() { return columns; }
        int rowCount() { return rowCount; }

        @Override
        public void close() {
            for (FieldVector v : columns) v.close();
        }
    }

    private HiveAcidMerger() {
    }

    /**
     * 归并一个分组：读 {@code paths} 里的全部文件，按行身份合并，产出活行的数据列。
     *
     * @param root     分组所属的分区目录（仅回填进 {@link Result}，供读侧补分区列）
     * @param paths    该分组的文件路径（调用方已按 原始 → base → delta 排序；顺序只影响可复现性，
     *                 正确性由「取最大 currentTransaction」保证）
     */
    static Result mergeGroup(String root, List<String> paths, HiveTableMeta meta,
                             BufferAllocator allocator, Configuration conf, int batchSize) {
        return mergeGroup(root, paths, meta, allocator, conf, batchSize, true);
    }

    /**
     * @param nestedNative 复合类型按原生嵌套结构合并（默认）还是按文本列合并
     */
    static Result mergeGroup(String root, List<String> paths, HiveTableMeta meta,
                             BufferAllocator allocator, Configuration conf, int batchSize,
                             boolean nestedNative) {
        Stager stager = new Stager(meta.dataFields(nestedNative), allocator, batchSize, nestedNative);
        try {
            for (String p : paths) {
                stager.readFile(p, meta, conf, batchSize);
            }
            return stager.finish(root);
        } finally {
            stager.closeTemps();
        }
    }

    // ==================== 归并状态机 ====================

    private static final class Stager {
        private final List<FieldVector> staged;
        private final List<FieldVector> tmp;
        private final Map<RowKey, Integer> slotOf = new HashMap<>();
        private long[] slotTxn = new long[1024];
        private byte[] slotOp = new byte[1024];
        private final boolean nestedNative;
        private int count;

        Stager(List<Field> dataFields, BufferAllocator allocator, int batchSize, boolean nestedNative) {
            this.nestedNative = nestedNative;
            staged = new ArrayList<>(dataFields.size());
            tmp = new ArrayList<>(dataFields.size());
            for (Field f : dataFields) {
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(Math.min(batchSize, 64));
                v.allocateNew();
                staged.add(v);
                FieldVector t = f.createVector(allocator);
                t.setInitialCapacity(batchSize);
                t.allocateNew();
                tmp.add(t);
            }
        }

        void readFile(String path, HiveTableMeta meta, Configuration conf, int batchSize) {
            Path p = new Path(path);
            FileSystem fs;
            try {
                fs = p.getFileSystem(conf);
            } catch (IOException e) {
                throw new RuntimeException("取 Hive ACID 文件 FileSystem 失败: " + path, e);
            }
            try (Reader reader = OrcFile.createReader(p, OrcFile.readerOptions(conf).filesystem(fs))) {
                TypeDescription schema = reader.getSchema();
                List<String> names = schema.getFieldNames();
                int idxOp = indexOfIgnoreCase(names, "operation");
                int idxOrig = indexOfIgnoreCase(names, "originalTransaction");
                int idxBucket = indexOfIgnoreCase(names, "bucket");
                int idxRowId = indexOfIgnoreCase(names, "rowId");
                int idxCur = indexOfIgnoreCase(names, "currentTransaction");
                int idxRow = indexOfIgnoreCase(names, "row");
                boolean acid = idxOp >= 0 && idxOrig >= 0 && idxBucket >= 0
                        && idxRowId >= 0 && idxCur >= 0 && idxRow >= 0;
                try (RecordReader rows = reader.rows(reader.options().schema(schema))) {
                    VectorizedRowBatch batch = schema.createRowBatch(batchSize);
                    while (rows.nextBatch(batch)) {
                        if (acid) {
                            mergeAcidBatch(batch, idxOp, idxOrig, idxBucket, idxRowId, idxCur, idxRow,
                                    schema.getChildren().get(idxRow));
                        } else {
                            mergePlainBatch(batch, names, meta, schema.getChildren());
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("读取 Hive ACID 文件失败: " + path, e);
            }
        }

        private void mergeAcidBatch(VectorizedRowBatch batch, int idxOp, int idxOrig, int idxBucket,
                                    int idxRowId, int idxCur, int idxRow, TypeDescription rowType) {
            int size = batch.size;
            LongColumnVector op = longCol(batch.cols[idxOp], size);
            LongColumnVector orig = longCol(batch.cols[idxOrig], size);
            LongColumnVector buk = longCol(batch.cols[idxBucket], size);
            LongColumnVector rid = longCol(batch.cols[idxRowId], size);
            LongColumnVector cur = longCol(batch.cols[idxCur], size);
            StructColumnVector row = (StructColumnVector) flatten(batch.cols[idxRow], size);

            fillRowColumns(row, size, rowType);
            int cols = staged.size();
            for (int i = 0; i < size; i++) {
                int operation = (int) op.vector[i];
                RowKey key = new RowKey(orig.vector[i], (int) buk.vector[i], rid.vector[i]);
                Integer prev = slotOf.get(key);
                int slot = prev == null ? allocSlot() : prev;
                slotOf.put(key, slot);
                long cTxn = cur.vector[i];
                if (cTxn < slotTxn[slot]) continue;    // 更旧的版本 → 丢弃
                slotTxn[slot] = cTxn;
                slotOp[slot] = (byte) operation;
                if (operation == OP_DELETE) continue;  // 删除只落标记，不写数据
                if (!row.noNulls && row.isNull[i]) {   // 结构为空 → 整行 NULL
                    for (int k = 0; k < cols; k++) tmp.get(k).setNull(i);
                }
                for (int k = 0; k < cols; k++) staged.get(k).copyFromSafe(i, slot, tmp.get(k));
            }
        }

        private void mergePlainBatch(VectorizedRowBatch batch, List<String> names, HiveTableMeta meta,
                                     List<TypeDescription> childTypes) {
            int size = batch.size;
            int cols = staged.size();
            List<Field> dataFields = meta.dataFields(nestedNative);
            for (int k = 0; k < cols; k++) {
                int idx = indexOfIgnoreCase(names, dataFields.get(k).getName());
                FieldVector t = tmp.get(k);
                if (idx < 0) {
                    for (int i = 0; i < size; i++) t.setNull(i);
                } else {
                    TypeDescription type = idx < childTypes.size() ? childTypes.get(idx) : null;
                    ORCFileArrowConverter.fillColumn(t, batch.cols[idx], size, type);
                }
                t.setValueCount(size);
            }
            for (int i = 0; i < size; i++) {
                int slot = allocSlot();
                slotTxn[slot] = Long.MIN_VALUE;   // 遗留数据最旧，任何同 key 记录都能覆盖它
                slotOp[slot] = OP_INSERT;
                for (int k = 0; k < cols; k++) staged.get(k).copyFromSafe(i, slot, tmp.get(k));
            }
        }

        /** 把 ACID {@code row} 结构的子列按位置填进临时向量（用户第 k 列 = row 的第 k 个子列）。 */
        private void fillRowColumns(StructColumnVector row, int size, TypeDescription rowType) {
            int cols = staged.size();
            List<TypeDescription> childTypes = rowType == null ? List.of() : rowType.getChildren();
            for (int k = 0; k < cols; k++) {
                FieldVector t = tmp.get(k);
                if (k < row.fields.length) {
                    TypeDescription type = k < childTypes.size() ? childTypes.get(k) : null;
                    ORCFileArrowConverter.fillColumn(t, row.fields[k], size, type);
                } else {
                    for (int i = 0; i < size; i++) t.setNull(i);
                }
                t.setValueCount(size);
            }
        }

        private int allocSlot() {
            if (count == slotTxn.length) {
                int cap = count == 0 ? 1024 : count * 2;
                slotTxn = java.util.Arrays.copyOf(slotTxn, cap);
                slotOp = java.util.Arrays.copyOf(slotOp, cap);
            }
            return count++;
        }

        /** 扫完 → 把「非删除」的 slot 摘出来做成结果列。 */
        Result finish(String root) {
            for (FieldVector v : staged) v.setValueCount(count);
            List<Integer> alive = new ArrayList<>(count);
            for (int s = 0; s < count; s++) {
                if (slotOp[s] != OP_DELETE) alive.add(s);
            }
            int n = alive.size();
            List<Field> fields = new ArrayList<>(staged.size());
            List<FieldVector> out = new ArrayList<>(staged.size());
            for (int k = 0; k < staged.size(); k++) {
                FieldVector src = staged.get(k);
                FieldVector dst = src.getField().createVector(src.getAllocator());
                dst.setInitialCapacity(Math.max(n, 1));
                dst.allocateNew();
                for (int j = 0; j < n; j++) dst.copyFromSafe(alive.get(j), j, src);
                dst.setValueCount(n);
                out.add(dst);
                fields.add(dst.getField());
            }
            return new Result(root, fields, out, n);
        }

        /** 关掉中间态向量（{@link #staged} 与 {@link #tmp}）—— finish 已把活行拷进新列，可安全释放。 */
        void closeTemps() {
            for (FieldVector v : tmp) v.close();
            for (FieldVector v : staged) v.close();
        }

        private static LongColumnVector longCol(ColumnVector c, int size) {
            if (c.isRepeating) c.flatten(false, null, size);
            return (LongColumnVector) c;
        }

        private static ColumnVector flatten(ColumnVector c, int size) {
            if (c.isRepeating) c.flatten(false, null, size);
            return c;
        }

        private static int indexOfIgnoreCase(List<String> names, String wanted) {
            for (int i = 0; i < names.size(); i++) {
                if (names.get(i).equalsIgnoreCase(wanted)) return i;
            }
            return -1;
        }
    }
}
