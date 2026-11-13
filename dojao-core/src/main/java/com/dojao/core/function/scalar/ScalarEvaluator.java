package com.dojao.core.function.scalar;

import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.planner.SqlTextUtil;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
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
import java.util.List;

/**
 * 标量表达式批求值器 —— 对单个 {@link ScalarExpr} 在一个 batch 上逐行求值，产出结果列。
 *
 * <p>逐行调用 SPI {@link com.dojao.spi.function.ScalarFunction#evaluate(Object[])}，
 * 输出类型由首个非空结果推断（相似度函数恒为 DOUBLE）。</p>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
public final class ScalarEvaluator {

    private final ScalarExpr expr;
    private final com.dojao.spi.function.ScalarFunction function;
    private final int[] colIndices;        // 列参数 → batch 列下标；-1 = 字面量
    private final Object[] literalValues;  // 仅字面量参数有效
    private final String[] cmpOps;         // 比较参数：操作符（非比较参数为 null）
    private final String[] cmpLiterals;    // 比较参数：右侧字面量（已去引号）

    public ScalarEvaluator(ScalarExpr expr) {
        this.expr = expr;
        this.function = ScalarFunctionRegistry.resolve(expr.function());
        int n = expr.args().size();
        this.colIndices = new int[n];
        this.literalValues = new Object[n];
        this.cmpOps = new String[n];
        this.cmpLiterals = new String[n];
        // colIndices 统一预置 -1：列引用留待 evaluate() 解析实际列下标；
        // 字面量保持 -1 走 literalValues。若不置位，int[] 默认 0 会把字面量误读成"第 0 列"。
        for (int i = 0; i < n; i++) {
            colIndices[i] = -1;
            ScalarExpr.ScalarArg a = expr.args().get(i);
            if (a instanceof ScalarExpr.ScalarArg.LiteralArg la) {
                literalValues[i] = la.value();
            } else if (a instanceof ScalarExpr.ScalarArg.ComparisonArg ca) {
                cmpOps[i] = ca.op();
                cmpLiterals[i] = ScalarExpr.unquote(ca.right());
            }
        }
    }

    /** 表达式引用的列（供列裁剪：列引用 + 比较参数的条件列）。 */
    public List<String> columnArgs() {
        List<String> cols = new ArrayList<>();
        for (ScalarExpr.ScalarArg a : expr.args()) {
            if (a instanceof ScalarExpr.ScalarArg.ColumnArg c) {
                if (!cols.contains(c.column())) cols.add(c.column());
            } else if (a instanceof ScalarExpr.ScalarArg.ComparisonArg ca) {
                String col = SqlTextUtil.stripQualifier(ca.left());
                if (!cols.contains(col)) cols.add(col);
            }
        }
        return cols;
    }

    /** 对一个 batch 求值，返回输出列（行数与 batch 一致），列名为 outputName。 */
    public FieldVector evaluate(VectorSchemaRoot root, BufferAllocator allocator, String outputName) {
        int rowCount = root.getRowCount();
        List<Field> schemaFields = root.getSchema().getFields();

        // 解析列参数 → 列下标（列不存在直接报错，fail-fast）
        for (int i = 0; i < expr.args().size(); i++) {
            ScalarExpr.ScalarArg a = expr.args().get(i);
            if (a instanceof ScalarExpr.ScalarArg.ColumnArg c) {
                int idx = indexOf(schemaFields, c.column());
                if (idx < 0) {
                    throw new IllegalStateException(
                            "标量函数 " + expr.function() + " 参数列不存在: " + c.column());
                }
                colIndices[i] = idx;
            } else if (a instanceof ScalarExpr.ScalarArg.ComparisonArg ca) {
                String col = SqlTextUtil.stripQualifier(ca.left());
                int idx = indexOf(schemaFields, col);
                if (idx < 0) {
                    throw new IllegalStateException(
                            "标量函数 " + expr.function() + " 比较参数列不存在: " + col);
                }
                colIndices[i] = idx;
            }
        }

        // 批量捷径：函数实现 BatchScalarFunction（如 Python 桥）且无比较参数（比较参数需逐行
        // 布尔化，不进 batch）→ 整批求值一次，避免逐行调 SPI（Python 每行一次进程往返）。
        if (function instanceof BatchScalarFunction bf && !hasComparisonArg()) {
            List<FieldVector> owned = new ArrayList<>();   // buildArgRoot 新建的常量列（本方法负责 close）
            VectorSchemaRoot argRoot = buildArgRoot(root, allocator, owned);
            try {
                return bf.evaluateBatch(argRoot, allocator, outputName);
            } finally {
                for (FieldVector v : owned) v.close();     // 只关常量列；上游向量由调用方管理
            }
        }

        // 第一遍：逐行求值收集结果 + 推断输出类型
        Object[] results = new Object[rowCount];
        Object[] input = new Object[expr.args().size()];
        Class<?> resultClass = null;
        for (int r = 0; r < rowCount; r++) {
            for (int i = 0; i < input.length; i++) {
                if (colIndices[i] >= 0) {
                    FieldVector v = root.getVector(colIndices[i]);
                    // ★统一取值入口：DATE 列 getObject 是 Integer(epoch days)，须归一为 LocalDate，
                    //   否则 to_date(d)/year(d) 会把天数当 epoch 毫秒 → 1970-01-01。
                    Object val = ExprEvaluator.cellValue(v, r);
                    // 比较参数：列值与字面量比较 → Boolean（null 列视为不满足条件 → 假）
                    if (cmpOps[i] != null) {
                        input[i] = val == null
                                ? Boolean.FALSE
                                : ScalarExpr.compare(val.toString(), cmpOps[i], cmpLiterals[i]);
                    } else {
                        input[i] = val;
                    }
                } else {
                    input[i] = literalValues[i];
                }
            }
            Object v = function.evaluate(input);
            results[r] = v;
            if (v != null && resultClass == null) resultClass = v.getClass();
        }

        // 建输出向量并回写
        FieldVector out = createVector(resultClass, allocator, outputName);
        out.setInitialCapacity(rowCount);
        out.allocateNew();
        for (int r = 0; r < rowCount; r++) {
            Object v = results[r];
            if (v == null) out.setNull(r);
            else setValue(out, r, v);
        }
        out.setValueCount(rowCount);
        return out;
    }

    /** 是否有比较参数（批量路径不支持，需逐行布尔化）。 */
    private boolean hasComparisonArg() {
        for (int i = 0; i < expr.args().size(); i++) {
            if (cmpOps[i] != null) return true;
        }
        return false;
    }

    /**
     * 构造批量求值的输入 root：参数按序，列参数引用上游向量（不拷贝，调用方管理生命周期）、
     * 字面量参数扩展成常量列（<b>新建、归 owned 管理</b>，调用方须在求值后 close 这些列）。
     * 返回 root 是视图（不拥有上游向量），不得 close。
     *
     * <p><b>列名统一为 c0..cn-1</b>（对齐外部语言契约：dojao_udf base.py 参数列 c0..cn-1，
     * 与逐行路径 {@code ArrowRowCodec.encodeRowRoot} 同款命名）——真实列名不传给外部语言，
     * 外部函数按位置（column("c0")/column("c1")）取参数，不依赖引擎列名。</p>
     */
    private VectorSchemaRoot buildArgRoot(VectorSchemaRoot src, BufferAllocator allocator, List<FieldVector> owned) {
        int n = src.getRowCount();
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vectors = new ArrayList<>();
        for (int i = 0; i < expr.args().size(); i++) {
            if (colIndices[i] >= 0) {
                FieldVector v = src.getVector(colIndices[i]);
                fields.add(new Field("c" + i, v.getField().getFieldType(), v.getField().getChildren()));
                vectors.add(v);
            } else {
                FieldVector v = createConstVector(literalValues[i], allocator, "c" + i, n);
                owned.add(v);
                fields.add(v.getField());
                vectors.add(v);
            }
        }
        return new VectorSchemaRoot(fields, vectors, n);
    }

    /** 字面量 → N 行常量列（与逐行路径 createVector/setValue 同款类型推断）。 */
    private static FieldVector createConstVector(Object lit, BufferAllocator allocator, String name, int n) {
        FieldVector v = createVector(lit == null ? String.class : lit.getClass(), allocator, name);
        v.setInitialCapacity(n);
        v.allocateNew();
        for (int r = 0; r < n; r++) {
            if (lit == null) v.setNull(r);
            else setValue(v, r, lit);
        }
        v.setValueCount(n);
        return v;
    }

    private static int indexOf(List<Field> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(name)) return i;
        }
        return -1;
    }

    private static FieldVector createVector(Class<?> resultClass, BufferAllocator allocator, String name) {
        Class<?> c = resultClass != null ? resultClass : Double.class;
        ArrowType type;
        if (c == Double.class || c == Float.class) {
            type = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (c == Long.class || c == Integer.class || c == Short.class || c == Byte.class) {
            type = new ArrowType.Int(64, true);
        } else if (c == Boolean.class) {
            type = new ArrowType.Bool();
        } else {
            type = new ArrowType.Utf8();
        }
        return new Field(name, FieldType.nullable(type), null).createVector(allocator);
    }

    private static void setValue(FieldVector out, int row, Object v) {
        if (out instanceof Float8Vector f) f.setSafe(row, ((Number) v).doubleValue());
        else if (out instanceof BigIntVector b) b.setSafe(row, ((Number) v).longValue());
        else if (out instanceof IntVector iv) iv.setSafe(row, ((Number) v).intValue());
        else if (out instanceof BitVector bt) bt.setSafe(row, ((Boolean) v) ? 1 : 0);
        else if (out instanceof VarCharVector vc) vc.setSafe(row, v.toString().getBytes(StandardCharsets.UTF_8));
        else throw new UnsupportedOperationException("标量函数输出类型不支持: " + v.getClass().getSimpleName());
    }
}
