package com.dojao.server.function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 桥的数据面编解码：Object[] 单行 ↔ {@link VectorSchemaRoot}（1 行 record batch）。
 *
 * <p><b>Flight 数据面</b>：doExchange 直接传 {@code VectorSchemaRoot}（Flight 内部序列化），
 * 不再像 gRPC 版那样包一层 Arrow IPC 字节做双重序列化。</p>
 *
 * <p><b>骨架实现</b>：SPI 是逐行契约（{@code ScalarEvaluator} 逐行调 {@code evaluate}），
 * 桥按行转成 1 行 batch 送侧车，功能正确但 RPC 次数与行数同阶。真正的"攒页批量"需要引擎
 * 给出 batch 边界信号（如 ScalarFunction 增加 beginBatch/endBatch 钩子），留作下一增量。</p>
 */
final class ArrowRowCodec {

    private ArrowRowCodec() {
    }

    /** Object[] 一行 → 1 行 batch 的 root（列名 c0..cn-1，类型按值推断）。调用方负责 close。 */
    static VectorSchemaRoot encodeRowRoot(BufferAllocator allocator, Object[] row) {
        List<Field> fields = new ArrayList<>(row.length);
        for (int i = 0; i < row.length; i++) {
            fields.add(new Field("c" + i, FieldType.nullable(typeOf(row[i])), null));
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
        root.allocateNew();
        root.setRowCount(1); // 先设值数，规避 var 列 fillHoles 清零（Arrow 陷阱）
        for (int i = 0; i < row.length; i++) {
            FieldVector v = root.getVector(i);
            Object val = row[i];
            if (val == null) {
                v.setNull(0);
            } else if (v instanceof Float8Vector f) {
                f.setSafe(0, ((Number) val).doubleValue());
            } else if (v instanceof BigIntVector b) {
                b.setSafe(0, ((Number) val).longValue());
            } else if (v instanceof BitVector bt) {
                bt.setSafe(0, ((Boolean) val) ? 1 : 0);
            } else if (v instanceof VarCharVector vc) {
                vc.setSafe(0, val.toString().getBytes(StandardCharsets.UTF_8));
            } else {
                throw new UnsupportedOperationException("不支持的类型: " + val.getClass().getSimpleName());
            }
        }
        return root;
    }

    /** 读结果 batch 第一列第一行 → Object（byte[] 转 String）。在响应流被复用前调用。 */
    static Object decodeResult(VectorSchemaRoot root) {
        if (root == null || root.getRowCount() == 0 || root.getFieldVectors().isEmpty()) {
            return null;
        }
        FieldVector v = root.getVector(0);
        if (v.isNull(0)) {
            return null;
        }
        Object o = v.getObject(0);
        return o instanceof byte[] ? new String((byte[]) o, StandardCharsets.UTF_8) : o;
    }

    /** 空 root（无列）：无输入/无输出的 op（CREATE_STATE / MERGE / EVALUATE 请求、ack 响应）用。 */
    static VectorSchemaRoot emptyRoot(BufferAllocator allocator) {
        return VectorSchemaRoot.create(new Schema(List.of()), allocator);
    }

    private static ArrowType typeOf(Object val) {
        if (val instanceof Double || val instanceof Float) {
            return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        }
        if (val instanceof Long || val instanceof Integer || val instanceof Short || val instanceof Byte) {
            return new ArrowType.Int(64, true);
        }
        if (val instanceof Boolean) {
            return new ArrowType.Bool();
        }
        return new ArrowType.Utf8();
    }
}
