package com.dojao.connector.hive;

import com.dojao.connector.common.util.ArrowFileWriter;
import com.dojao.connector.common.util.ORCArrowWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.List;

/**
 * <b>Hive full ACID 数据文件写入器</b> —— 把 Arrow 页写进 ACID 包装结构的 ORC 文件。
 *
 * <p>顶层结构与会话 Hive 的 {@code OrcRecordUpdater} 完全一致（顺序固定）：</p>
 * <pre>
 *   struct&lt;operation:int, originalTransaction:bigint, bucket:int, rowId:bigint,
 *          currentTransaction:bigint, row:struct&lt;用户列…&gt;&gt;
 * </pre>
 *
 * <p>本写入器只做 INSERT（纯追加）：{@code operation=0}，
 * {@code originalTransaction = currentTransaction = acidId}（Hive3 是 writeId，Hive2 是事务 id），
 * {@code bucket} 由调用方给出，{@code rowId} 在<b>同一 (分区, 桶)</b>内从 0 单调递增（这条正是
 * {@code RecordIdentifier=(originalTransaction, bucket, rowId)} 的唯一性来源，写侧必须保证）。</p>
 *
 * <p>用户列沿用 {@link ORCArrowWriter} 的 Arrow→ORC 搬运（含 array/map/struct 原生嵌套），
 * 只把结果填进 {@code row} 这个结构体的子列。数据先写进临时目录，收尾时整体 mv 到
 * {@code delta_<id>_<id>_0/} 再提交事务（见 {@link HiveWriteSession}）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/16
 */
final class HiveAcidArrowWriter implements ArrowFileWriter {

    /** Hive {@code OrcRecordUpdater.INSERT_OPERATION}。 */
    static final int OP_INSERT = 0;

    /** 包装结构里 {@code row} 字段的下标。 */
    private static final int ROW_COLUMN = 5;

    private final Writer writer;
    private final TypeDescription schema;
    private final long acidId;
    private final int bucket;
    private final int dataColumnCount;
    private long nextRowId;
    private boolean closed;

    HiveAcidArrowWriter(Configuration conf, Path file, FileSystem fs, List<Field> dataFields,
                        long acidId, int bucket, long firstRowId) throws IOException {
        this.acidId = acidId;
        this.bucket = bucket;
        this.nextRowId = firstRowId;
        this.dataColumnCount = dataFields.size();
        this.schema = wrapperSchema(dataFields);
        OrcFile.WriterOptions opts = OrcFile.writerOptions(conf)
                .fileSystem(fs)
                .setSchema(schema)
                .overwrite(true);
        this.writer = OrcFile.createWriter(file, opts);
    }

    /**
     * ACID 包装结构：前 5 个固定列 + {@code row}（用户列的原生 ORC 结构，顺序 = 表数据列序）。
     * 字段名与 Hive 一致 —— 读侧（含 Hive 自己）按此识别 ACID 文件。
     */
    static TypeDescription wrapperSchema(List<Field> dataFields) {
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField("operation", TypeDescription.createInt());
        schema.addField("originalTransaction", TypeDescription.createLong());
        schema.addField("bucket", TypeDescription.createInt());
        schema.addField("rowId", TypeDescription.createLong());
        schema.addField("currentTransaction", TypeDescription.createLong());
        schema.addField("row", ORCArrowWriter.orcSchemaOf(dataFields));
        return schema;
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        int rows = root.getRowCount();
        if (rows <= 0) return;
        VectorizedRowBatch batch = schema.createRowBatch(rows);
        batch.size = rows;
        fillConstant(batch.cols[0], OP_INSERT, rows);
        fillConstant(batch.cols[1], acidId, rows);
        fillConstant(batch.cols[2], bucket, rows);
        LongColumnVector rowIds = (LongColumnVector) batch.cols[3];
        rowIds.noNulls = true;
        for (int i = 0; i < rows; i++) rowIds.vector[i] = nextRowId + i;
        nextRowId += rows;
        fillConstant(batch.cols[4], acidId, rows);

        StructColumnVector row = (StructColumnVector) batch.cols[ROW_COLUMN];
        for (int k = 0; k < dataColumnCount && k < row.fields.length; k++) {
            ORCArrowWriter.fillColumn(row.fields[k], root.getVector(k), rows);
        }
        writer.addRowBatch(batch);
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        writer.close();
    }

    private static void fillConstant(ColumnVector col, long value, int rows) {
        LongColumnVector lcv = (LongColumnVector) col;
        lcv.noNulls = true;
        for (int i = 0; i < rows; i++) lcv.vector[i] = value;
    }
}
