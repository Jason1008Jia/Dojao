package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * 探针：<b>{@link JoinHashTable} 的行号索引已数组化（{@code int[] groupHead/groupTail/nextRow}）</b>。
 *
 * <p><b>为什么</b>：线上 2026-11-11 三表 join 回退 Master 时 {@code java.lang.OutOfMemoryError: Java heap space}
 * —— 旧实现每组一个 {@code ArrayList} + 每行一个装箱 {@code Integer}，约 2M 唯一键 ≈ 200MB Java 堆，
 * 而 {@code estimateMemoryBytes()} 只算 Arrow 向量 ⇒ 预算闸门看不见，堆先炸。
 * 数组化后同样 2M 行约 24MB，且如实计入预算。</p>
 *
 * <p>本探针守住三件事（改回装箱/漏接链会立刻 FAIL）：
 * <ol>
 *   <li><b>一对多正确</b>：同键多行全部按插入顺序取回，不同键互不串行；</li>
 *   <li><b>probe 命中</b>：{@code probe()} 找到正确组，未命中返回 -1；</li>
 *   <li><b>内存如实计入且有界</b>：20 万唯一键后 {@code estimateMemoryBytes()} 精确包含三个 int 数组，
 *       且索引字节摊到每行 ≤ 32B（装箱实现的保守下限约 46B/组 + 20B/行）。</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/11
 */
public final class JoinHashTableRowIndexProbeTest {

    private static int pass = 0;
    private static int fail = 0;

    private static final Schema SCHEMA = new Schema(List.of(
            new Field("k", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("p", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    public static void main(String[] args) throws Exception {
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            oneToMany(alloc);
            probeHitAndMiss(alloc);
            memoryAccounting(alloc);
        }
        System.out.printf("%n========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) throw new AssertionError("JoinHashTable 行索引探针失败 " + fail + " 项");
    }

    // ---------------------------------------------------------------- S1 一对多

    private static void oneToMany(BufferAllocator alloc) {
        System.out.println("---------- S1 一对多：同键多行按插入顺序取回、不同键隔离 ----------");
        // k = [1, 2, 1, 3, 1]，p = 行号 —— 键 1 的 build 行号应为 0,2,4
        int[] keys = {1, 2, 1, 3, 1};
        JoinHashTable t = new JoinHashTable(alloc, List.of("k"),
                List.of(SCHEMA.getFields().get(0)), SCHEMA);
        try (VectorSchemaRoot root = buildRoot(alloc, keys)) {
            Page page = Page.fromRoot(root);
            for (int r = 0; r < keys.length; r++) t.insertOrAppend(page, r);

            expect(t.buildRowCount() == 5, "buildRowCount = 5（实际 " + t.buildRowCount() + "）");
            expect(t.size() == 3, "唯一键组数 = 3（实际 " + t.size() + "）");

            List<Integer> rowsOfKey1 = chain(t, probeGroup(t, page, 0));   // 用第 0 行（k=1）探测
            expect(rowsOfKey1.equals(List.of(0, 2, 4)),
                    "键 1 的 build 行号 = [0,2,4]（实际 " + rowsOfKey1 + "）");

            List<Integer> rowsOfKey2 = chain(t, probeGroup(t, page, 1));   // k=2 在行 1
            expect(rowsOfKey2.equals(List.of(1)), "键 2 的 build 行号 = [1]（实际 " + rowsOfKey2 + "）");

            List<Integer> rowsOfKey3 = chain(t, probeGroup(t, page, 3));
            expect(rowsOfKey3.equals(List.of(3)), "键 3 的 build 行号 = [3]（实际 " + rowsOfKey3 + "）");

            // 深拷贝正确性：行号 2 的 payload 必须是原始行 2 的 p=2
            try (VectorSchemaRoot one = t.extractSingleRow(2)) {
                expect(((IntVector) one.getVector("k")).get(0) == 1, "extractSingleRow(2).k = 1");
                expect(((IntVector) one.getVector("p")).get(0) == 2, "extractSingleRow(2).p = 2");
            }
        } catch (Throwable e) {
            System.out.println("  [!!] 运行异常: " + e);
            e.printStackTrace(System.out);
            fail++;
        } finally {
            t.close();
        }
    }

    // ---------------------------------------------------------------- S2 probe 命中/未命中

    private static void probeHitAndMiss(BufferAllocator alloc) {
        System.out.println("---------- S2 probe：命中返回组、未命中返回 -1 ----------");
        int[] keys = {7, 8, 9};
        JoinHashTable t = new JoinHashTable(alloc, List.of("k"),
                List.of(SCHEMA.getFields().get(0)), SCHEMA);
        try (VectorSchemaRoot root = buildRoot(alloc, keys)) {
            Page page = Page.fromRoot(root);
            for (int r = 0; r < keys.length; r++) t.insertOrAppend(page, r);

            int hit = t.probe(page, new int[]{0}, 1);   // k=8
            expect(hit >= 0 && !chain(t, hit).isEmpty() && chain(t, hit).get(0) == 1,
                    "probe(k=8) 命中行 1（实际组 " + hit + "）");
            try (VectorSchemaRoot missRoot = buildRoot(alloc, new int[]{99})) {
                expect(t.probe(Page.fromRoot(missRoot), new int[]{0}, 0) == -1,
                        "未命中的键 probe 返回 -1");
            }
        } catch (Throwable e) {
            System.out.println("  [!!] 运行异常: " + e);
            e.printStackTrace(System.out);
            fail++;
        } finally {
            t.close();
        }
    }

    // ---------------------------------------------------------------- S3 内存计入 + 有界

    private static void memoryAccounting(BufferAllocator alloc) throws Exception {
        System.out.println("---------- S3 内存：20 万唯一键，索引数组如实计入且有界 ----------");
        final int rows = 200_000;
        JoinHashTable t = new JoinHashTable(alloc, List.of("k"),
                List.of(SCHEMA.getFields().get(0)), SCHEMA);
        try {
            int[] keys = new int[rows];
            for (int i = 0; i < rows; i++) keys[i] = i;   // 最优情形：高基数、无重复
            try (VectorSchemaRoot root = buildRoot(alloc, keys)) {
                Page page = Page.fromRoot(root);
                for (int r = 0; r < rows; r++) t.insertOrAppend(page, r);

                expect(t.buildRowCount() == rows, "buildRowCount = " + rows + "（实际 " + t.buildRowCount() + "）");
                expect(t.size() == rows, "唯一键组数 = " + rows + "（实际 " + t.size() + "）");

                int headLen = arrayLen(t, JoinHashTable.class, "groupHead");
                int tailLen = arrayLen(t, JoinHashTable.class, "groupTail");
                int nextLen = arrayLen(t, JoinHashTable.class, "nextRow");
                expect(headLen >= rows && tailLen >= rows && nextLen >= rows,
                        "三个索引数组均覆盖全部行（head=" + headLen + ", tail=" + tailLen
                                + ", next=" + nextLen + "）");

                long indexBytes = 4L * (headLen + tailLen + nextLen);
                long baseBytes = arrowBytes(t)
                        + 4L * arrayLen(t, RowHashTable.class, "buckets")
                        + 4L * arrayLen(t, RowHashTable.class, "rowHashes");
                long reported = t.estimateMemoryBytes();
                expect(reported - baseBytes == indexBytes,
                        "estimateMemoryBytes 精确含索引数组（报告-向量/桶/hash = "
                                + (reported - baseBytes) + "，期望 " + indexBytes + "）");

                double perRow = indexBytes / (double) rows;
                System.out.printf("  索引数组合计 %.2f MB，摊薄 %.1f B/行%n",
                        indexBytes / 1048576.0, perRow);
                expect(perRow <= 32.0, "索引摊薄 ≤ 32 B/行（实际 " + String.format("%.1f", perRow) + "）");

                // 抽样校验：末行仍能按链取回自己
                List<Integer> last = chain(t, probeGroup(t, page, rows - 1));
                expect(last.size() == 1 && last.get(0) == rows - 1,
                        "末行 probe 链 = [" + (rows - 1) + "]（实际 " + last + "）");
            }
        } finally {
            t.close();
        }
    }

    // ---------------------------------------------------------------- 工具

    private static List<Integer> chain(JoinHashTable t, int groupIdx) {
        List<Integer> out = new ArrayList<>();
        for (int r = t.firstMatchingRow(groupIdx); r >= 0; r = t.nextMatchingRow(r)) out.add(r);
        return out;
    }

    private static int probeGroup(JoinHashTable t, Page page, int row) {
        return t.probe(page, new int[]{0}, row);
    }

    private static VectorSchemaRoot buildRoot(BufferAllocator alloc, int[] keys) {
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

    private static long arrowBytes(JoinHashTable t) {
        long total = 0;
        for (FieldVector v : t.getKeyVectors()) total += v.getBufferSize();
        List<FieldVector> build = t.getBuildVectors();
        if (build != null) for (FieldVector v : build) total += v.getBufferSize();
        return total;
    }

    private static int arrayLen(Object target, Class<?> owner, String name) throws Exception {
        java.lang.reflect.Field f = null;
        for (Class<?> c = owner; c != null && f == null; c = c.getSuperclass()) {
            try { f = c.getDeclaredField(name); } catch (NoSuchFieldException ignored) { }
        }
        if (f == null) throw new NoSuchFieldException(name);
        f.setAccessible(true);
        int[] arr = (int[]) f.get(target);
        f.setAccessible(false);
        return arr.length;
    }

    private static void expect(boolean ok, String what) {
        if (ok) { pass++; System.out.println("  [PASS] " + what); }
        else { fail++; System.out.println("  [FAIL] " + what); }
    }
}
