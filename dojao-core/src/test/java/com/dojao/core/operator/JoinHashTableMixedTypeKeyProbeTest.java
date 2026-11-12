package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * 探针：<b>跨类型 join 键（BIGINT = INT 等）按各自向量宽度读取</b>。
 *
 * <p><b>为什么</b>：线上 SQL（多表 LEFT JOIN）报
 * {@code IndexOutOfBoundsException: index: 2051248, length: 8 (expected: range(0, 2033600))}
 * @ {@code RowHashTable.keysEqualAt} —— 旧实现用 <b>probe 侧</b> {@code iv} 的类型宽度
 * 去读 <b>build 侧</b>存储键 {@code kv}：probe 是 BIGINT(8B) 而 build 是 INT(4B) 时按 8 字节
 * 步长读 4 字节缓冲，build 组数越过缓冲可容纳的值数即越界（2033600 = 4 × 508400 正是
 * INT 向量容量；2051248 = 256406 × 8）。</p>
 *
 * <p>同时守住数值归一化：同一数值在 INT/BIGINT/FLOAT/DOUBLE 上的键值必须同型
 * （整型 {@code Long}、整值浮点也归一成 {@code Long}），否则
 * {@code Integer(5).equals(Long(5))} 恒 false ⇒ 跨类型 join 静默不匹配 /
 * {@code Integer.compareTo(Long)} 抛 ClassCastException。</p>
 *
 * <p>判别性：build 300000 个唯一键（跨过 INT 缓冲 254200 值的边界），旧实现在
 * 探测第 254200 个键时必抛 {@code IndexOutOfBoundsException}；负数键在旧实现下 hash
 * 口径不一致会静默 miss。</p>
 *
 * @author jiaxin
 * @date 2026-11-12
 */
public final class JoinHashTableMixedTypeKeyProbeTest {

    private static int pass = 0;
    private static int fail = 0;

    private static final Schema BUILD_SCHEMA = new Schema(List.of(
            new Field("k", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("p", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    public static void main(String[] args) throws Exception {
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            crossTypeProbeHighCardinality(alloc);
            nullKeyDoesNotMatch(alloc);
            keyValueNormalization(alloc);
            crossTypeKeyEquality();
        }
        System.out.printf("%n========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) throw new AssertionError("跨类型 join 键探针失败 " + fail + " 项");
    }

    // ------------------------------------------------- S1 高基数 BIGINT 探测 INT build

    private static void crossTypeProbeHighCardinality(BufferAllocator alloc) {
        System.out.println("---------- S1 build INT 30 万唯一键，probe 用 BIGINT（含负数） ----------");
        final int rows = 300_000;
        final int offset = 150_000;   // key = row - offset，覆盖负数
        JoinHashTable t = new JoinHashTable(alloc, List.of("k"),
                List.of(BUILD_SCHEMA.getFields().get(0)), BUILD_SCHEMA);
        try {
            int[] keys = new int[rows];
            for (int r = 0; r < rows; r++) keys[r] = r - offset;
            try (VectorSchemaRoot buildRoot = buildIntRoot(alloc, keys)) {
                Page buildPage = Page.fromRoot(buildRoot);
                for (int r = 0; r < rows; r++) t.insertOrAppend(buildPage, r);

                expect(t.buildRowCount() == rows, "buildRowCount = " + rows + "（实际 " + t.buildRowCount() + "）");
                expect(t.size() == rows, "唯一键组数 = " + rows + "（实际 " + t.size() + "）");

                // 关键行号：0（负数键）、254199（INT 缓冲边界内最后一个可 8 字节读的组）、
                // 254200（旧实现从这里开始越界）、256406（线上报错组号）、末行
                int[] probeRows = {0, 123_456, 254_199, 254_200, 256_406, rows - 1};
                for (int r : probeRows) {
                    try (VectorSchemaRoot probeRoot = buildBigIntRoot(alloc, new long[]{r - offset})) {
                        int group = t.probe(Page.fromRoot(probeRoot), new int[]{0}, 0);
                        boolean ok = group == r && t.firstMatchingRow(group) == r;
                        expect(ok, "probe(key=" + (r - offset) + ") → 组/行 " + r
                                + "（实际组 " + group + "，首行 "
                                + (group >= 0 ? t.firstMatchingRow(group) : -1) + "）");
                    }
                }

                try (VectorSchemaRoot missRoot = buildBigIntRoot(alloc, new long[]{10_000_000L})) {
                    expect(t.probe(Page.fromRoot(missRoot), new int[]{0}, 0) == -1,
                            "不存在的 BIGINT 键 probe 返回 -1");
                }

                // 同类型（INT）探测仍走原有快路径，结果必须一致
                try (VectorSchemaRoot sameRoot = buildIntRoot(alloc, new int[]{rows - 1 - offset})) {
                    expect(t.probe(Page.fromRoot(sameRoot), new int[]{0}, 0) == rows - 1,
                            "同类型 INT 探测末键仍命中组 " + (rows - 1));
                }
            }
        } catch (Throwable e) {
            System.out.println("  [!!] 运行异常: " + e);
            e.printStackTrace(System.out);
            fail++;
        } finally {
            t.close();
        }
    }

    // ------------------------------------------------- S2 NULL 键

    private static void nullKeyDoesNotMatch(BufferAllocator alloc) {
        System.out.println("---------- S2 NULL 键不匹配任何行（含另一个 NULL） ----------");
        JoinHashTable t = new JoinHashTable(alloc, List.of("k"),
                List.of(BUILD_SCHEMA.getFields().get(0)), BUILD_SCHEMA);
        try (VectorSchemaRoot buildRoot = buildIntRoot(alloc, new int[]{1, 2})) {
            Page buildPage = Page.fromRoot(buildRoot);
            t.insertOrAppend(buildPage, 0);
            t.insertOrAppend(buildPage, 1);
            try (BigIntVector k = new BigIntVector("k", alloc)) {
                k.allocateNew(1);
                k.setNull(0);
                k.setValueCount(1);
                VectorSchemaRoot probeRoot = new VectorSchemaRoot(List.of(k));
                probeRoot.setRowCount(1);
                expect(t.probe(Page.fromRoot(probeRoot), new int[]{0}, 0) == -1,
                        "NULL 键探测返回 -1（旧实现 null==null 会拼出幻影行）");
            }
        } catch (Throwable e) {
            System.out.println("  [!!] 运行异常: " + e);
            e.printStackTrace(System.out);
            fail++;
        } finally {
            t.close();
        }
    }

    // ------------------------------------------------- S3 键值归一化

    private static void keyValueNormalization(BufferAllocator alloc) {
        System.out.println("---------- S3 readKeyValue：整型同型 Long、整值浮点归一 Long ----------");
        try (IntVector i32 = new IntVector("v", alloc);
             BigIntVector i64 = new BigIntVector("v", alloc);
             Float4Vector f4 = new Float4Vector("v", alloc);
             Float8Vector f8 = new Float8Vector("v", alloc)) {
            i32.allocateNew(2);
            i32.setSafe(0, 5);
            i32.setSafe(1, -1);
            i32.setValueCount(2);

            i64.allocateNew(2);
            i64.setSafe(0, 5L);
            i64.setSafe(1, -1L);
            i64.setValueCount(2);

            f4.allocateNew(2);
            f4.setSafe(0, 5.0f);
            f4.setSafe(1, 1.5f);
            f4.setValueCount(2);

            f8.allocateNew(2);
            f8.setSafe(0, -1.0d);
            f8.setSafe(1, 2.25d);
            f8.setValueCount(2);

            expect(Long.valueOf(5).equals(RowAccumulator.readKeyValue(i32, 0))
                            && Long.valueOf(5).equals(RowAccumulator.readKeyValue(i64, 0)),
                    "INT/BIGINT 的 5 都归一成 Long(5)");
            expect(Long.valueOf(-1).equals(RowAccumulator.readKeyValue(i32, 1))
                            && Long.valueOf(-1).equals(RowAccumulator.readKeyValue(i64, 1)),
                    "INT/BIGINT 的 -1 都归一成 Long(-1)（负数跨宽度 hash 一致）");
            expect(Long.valueOf(5).equals(RowAccumulator.readKeyValue(f4, 0)),
                    "FLOAT 的 5.0 归一成 Long(5)（与整型键同型）");
            expect(Double.valueOf(1.5).equals(RowAccumulator.readKeyValue(f4, 1)),
                    "FLOAT 的 1.5 保持 Double(1.5)");
            expect(Long.valueOf(-1).equals(RowAccumulator.readKeyValue(f8, 0)),
                    "DOUBLE 的 -1.0 归一成 Long(-1)");
            expect(Double.valueOf(2.25).equals(RowAccumulator.readKeyValue(f8, 1)),
                    "DOUBLE 的 2.25 保持 Double(2.25)");
        } catch (Throwable e) {
            System.out.println("  [!!] 运行异常: " + e);
            e.printStackTrace(System.out);
            fail++;
        }
    }

    // ------------------------------------------------- S4 跨类型键相等

    private static void crossTypeKeyEquality() {
        System.out.println("---------- S4 SortMergeJoin.keysEqual：数值跨类型不抛 CCE ----------");
        try {
            expect(SortMergeJoinOperator.keysEqual(5, 5L), "keysEqual(Integer 5, Long 5) = true");
            expect(SortMergeJoinOperator.keysEqual(5L, 5.0d), "keysEqual(Long 5, Double 5.0) = true");
            expect(!SortMergeJoinOperator.keysEqual(5, 6L), "keysEqual(Integer 5, Long 6) = false");
            expect(SortMergeJoinOperator.keysEqual(5L, 5L), "keysEqual(Long 5, Long 5) = true");
        } catch (Throwable e) {
            System.out.println("  [!!] 运行异常: " + e);
            e.printStackTrace(System.out);
            fail++;
        }
    }

    // ------------------------------------------------- 工具

    private static VectorSchemaRoot buildIntRoot(BufferAllocator alloc, int[] keys) {
        IntVector k = new IntVector("k", alloc);
        IntVector p = new IntVector("p", alloc);
        k.allocateNew(keys.length);
        p.allocateNew(keys.length);
        for (int i = 0; i < keys.length; i++) {
            k.setSafe(i, keys[i]);
            p.setSafe(i, i);
        }
        k.setValueCount(keys.length);
        p.setValueCount(keys.length);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(k, p));
        root.setRowCount(keys.length);
        return root;
    }

    private static VectorSchemaRoot buildBigIntRoot(BufferAllocator alloc, long[] keys) {
        BigIntVector k = new BigIntVector("k", alloc);
        k.allocateNew(keys.length);
        for (int i = 0; i < keys.length; i++) k.setSafe(i, keys[i]);
        k.setValueCount(keys.length);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(k));
        root.setRowCount(keys.length);
        return root;
    }

    private static void expect(boolean ok, String what) {
        if (ok) { pass++; System.out.println("  [PASS] " + what); }
        else { fail++; System.out.println("  [FAIL] " + what); }
    }
}
