package com.dojao.core.function.table;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.AbstractMeasurableOperator;
import com.dojao.spi.function.TableFunction;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 表函数算子 —— {@code FROM TABLE(fn(常量参数)) [AS t(a,b)]} 的叶子算子（无 upstream）。
 *
 * <p>表生成器语义：open 时调 {@code fn.evaluate(args)} 一次，把返回的 N 行 {@code Object[]}
 * 全部物化成一个 Page（1 次调用 → 1 页数据 → EOF）。schema 由各行非空值推断：
 * Integer/Long/Short/Byte → BigInt(64)，Double/Float → Float8，Boolean → Bool，其余 → VarChar。</p>
 *
 * <p>输出列名：构造传入的 {@code outputColumns}（即 {@code AS t(a,b)} 的列别名）；为空时
 * 按 {@code c0..cN-1} 命名（规划期列名未知，执行期推断）。</p>
 */
public final class TableFunctionOperator extends AbstractMeasurableOperator {

    private final TableFunction function;
    private final Object[] args;
    /** AS t(a,b) 列别名；空 → 执行期推断 c0..cN-1。 */
    private final List<String> outputColumns;

    // 分配器是进程级共享根（NodeMemory）的子 —— 表函数物化页的堆外占用进统一账本，受共享根硬上限约束。
    private final BufferAllocator allocator = NodeMemory.shared()
            .newOperatorAllocator("table-function", ResourceConfig.queryDirectLimit());
    private Page materialized;
    private boolean pageReturned;

    public TableFunctionOperator(TableFunction function, Object[] args, List<String> outputColumns) {
        super("TableFunction", "TableFunction", 1);
        this.function = function;
        this.args = args;
        this.outputColumns = outputColumns;
    }

    @Override
    protected void doOpen() {
        Iterator<Object[]> rows = function.evaluate(args);
        List<Object[]> collected = new ArrayList<>();
        rows.forEachRemaining(collected::add);
        if (!collected.isEmpty()) {
            materialized = Page.fromRoot(toRoot(collected));
        }
    }

    @Override
    protected Page doNext() {
        if (pageReturned) return Page.eof();
        pageReturned = true;
        return materialized != null ? materialized : Page.eof();
    }

    @Override
    protected void doClose() {
        if (materialized != null) {
            materialized.close();
        }
        allocator.close();
    }

    // ==================== 物化 ====================

    /** 行集合 → VectorSchemaRoot：每列类型按该列首个非空值推断，列名取别名或 c0..cN-1。 */
    private VectorSchemaRoot toRoot(List<Object[]> rows) {
        int colCount = 0;
        for (Object[] row : rows) {
            colCount = Math.max(colCount, row.length);
        }
        String[] names = new String[colCount];
        for (int c = 0; c < colCount; c++) {
            names[c] = c < outputColumns.size() ? outputColumns.get(c) : "c" + c;
        }
        Class<?>[] types = new Class<?>[colCount];
        for (Object[] row : rows) {
            for (int c = 0; c < row.length && c < colCount; c++) {
                if (row[c] != null && types[c] == null) {
                    types[c] = row[c].getClass();
                }
            }
        }

        List<Field> fields = new ArrayList<>(colCount);
        List<FieldVector> vectors = new ArrayList<>(colCount);
        for (int c = 0; c < colCount; c++) {
            FieldVector vec = createVector(types[c], allocator, names[c]);
            vec.setInitialCapacity(rows.size());
            vec.allocateNew();
            vectors.add(vec);
            fields.add(vec.getField());
        }
        for (int r = 0; r < rows.size(); r++) {
            Object[] row = rows.get(r);
            for (int c = 0; c < colCount; c++) {
                Object val = c < row.length ? row[c] : null;
                writeValue(vectors.get(c), r, val);
            }
        }
        for (FieldVector v : vectors) {
            v.setValueCount(rows.size());
        }
        return new VectorSchemaRoot(fields, vectors, rows.size());
    }

    /** 按值类型推断 Arrow 类型（与 ScalarEvaluator.createVector 同款映射）。 */
    private static FieldVector createVector(Class<?> type, BufferAllocator allocator, String name) {
        Class<?> c = type != null ? type : String.class;
        ArrowType arrowType;
        if (c == Double.class || c == Float.class) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (c == Long.class || c == Integer.class || c == Short.class || c == Byte.class) {
            arrowType = new ArrowType.Int(64, true);
        } else if (c == Boolean.class) {
            arrowType = new ArrowType.Bool();
        } else {
            arrowType = new ArrowType.Utf8();
        }
        return new Field(name, FieldType.nullable(arrowType), null).createVector(allocator);
    }

    /** 任意值写入向量指定行（BigInt/Int/Float8/Float4/Bool/VarChar）。 */
    @SuppressWarnings("rawtypes")
    private static void writeValue(FieldVector vec, int row, Object val) {
        if (val == null) {
            vec.setNull(row);
            return;
        }
        switch (vec) {
            case BigIntVector bv -> {
                long v = (val instanceof Number n) ? n.longValue() : Long.parseLong(val.toString());
                bv.setSafe(row, v);
            }
            case IntVector iv -> {
                int v = (val instanceof Number n) ? n.intValue() : Integer.parseInt(val.toString());
                iv.setSafe(row, v);
            }
            case Float8Vector fv -> {
                double v = (val instanceof Number n) ? n.doubleValue() : Double.parseDouble(val.toString());
                fv.setSafe(row, v);
            }
            case Float4Vector fv -> {
                float v = (val instanceof Number n) ? n.floatValue() : Float.parseFloat(val.toString());
                fv.setSafe(row, v);
            }
            case BitVector bt -> bt.setSafe(row, (val instanceof Boolean b) && b ? 1 : 0);
            case VarCharVector vc -> {
                byte[] bytes = (val instanceof byte[] b) ? b : val.toString().getBytes(StandardCharsets.UTF_8);
                vc.setSafe(row, bytes);
            }
            default -> vec.setNull(row);
        }
    }
}
