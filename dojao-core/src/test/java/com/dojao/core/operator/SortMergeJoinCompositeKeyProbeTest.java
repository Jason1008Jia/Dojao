package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * 离线探针：<b>排序归并 join 的多列（复合）键在「两边都溢写 ⇒ 走流式归并」时不得退化成单列键</b>。
 *
 * <p>线上事故：{@code INNER JOIN ... ON yj.business_month = tag.business_month AND
 * yj.customer_id = tag.customer_id} 被规划成 SORT_MERGE 且两侧都溢写。流式归并路径旧实现只取
 * {@code leftKeyColumns.getFirst()} ⇒ 实际按 {@code business_month} 单列 join，同一月份的任意两行
 * 都被判为匹配 ⇒ 输出是「同月行的笛卡尔积」（578146 行 × 14513 行），结果流十几分钟仍不结束，
 * 计划卡在 DistributedJoin 算不出结果。</p>
 *
 * <p>探针构造键 {@code (m, c)}：大量行共享 {@code m}、{@code c} 各不相同。只用 {@code m} 比较时输出行数
 * 会放大一个数量级，且每行左右两侧的 {@code c} 不相等 —— 两种错误都能被逐行断言抓住。</p>
 *
 * <p>预算压到 2MB（单槽 1.6MB 阈值）强制两侧溢写，确保走的是 {@code streamingMergeJoin}
 * （{@code leftCompacted && rightCompacted}）而不是索引路径。</p>
 */
public class SortMergeJoinCompositeKeyProbeTest {

    private static final int PAGE_ROWS = 5_000;
    private static final String PAYLOAD = "x".repeat(200);
    /** 2MB 预算 × 0.8 = 1.6MB 单槽阈值：20k 行 × ~200B 必然溢写。 */
    private static final long OPERATOR_MEMORY = 2L * 1024 * 1024;
    private static final long DIRECT_LIMIT = 256L * 1024 * 1024;

    private static int failures = 0;

    public static void main(String[] args) throws Exception {
        Path spill = Files.createTempDirectory("dojao-smj-composite-probe");
        System.setProperty("query.spill.dir", spill.toString());
        System.setProperty("query.direct.limit", Long.toString(DIRECT_LIMIT));

        System.out.println("========== 排序归并 join：多列键（两侧溢写）探针 ==========\n");
        try {
            testInnerCompositeKey();
            System.out.println();
            testInnerCompositeKeyInMemory();
            System.out.println();
            testLeftCompositeKey();
            System.out.println();
            testVarCharSecondKeySpilled();
            System.out.println();
            testVarCharSecondKeyExactMatches();
        } finally {
            try (var walk = Files.walk(spill)) {
                walk.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) { } });
            }
        }

        if (failures > 0) {
            System.out.println("\n[FAIL] 共 " + failures + " 项不符");
            throw new AssertionError("多列键排序归并探针失败 " + failures + " 项");
        }
        System.out.println("\n[PASS] 多列键 join 正确（未退化为单列键）");
    }

    /**
     * INNER：两侧键相同，(m,c) 各出现 20 次 ⇒ 正确输出 = 1000 × 20 × 20 = 400_000。
     * 旧实现按 m 单列比较 ⇒ 100 个 m 组 × (10×20)² = 4_000_000（放大 10 倍）。
     */
    private static void testInnerCompositeKey() {
        final int perSide = 20_000;
        final int keyMod = 1_000;   // 1000 个 (m,c) 键，m = k/10 ∈ [0,99]，c = k%10 ∈ [0,9]
        final long expected = 1_000L * 20 * 20;
        System.out.println("---- 场景 1：INNER，两侧各 " + perSide + " 行 / " + keyMod
                + " 个复合键，期望 " + expected + " 行 ----");
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Source left = new Source(alloc, perSide, keyMod, 10);
            Source right = new Source(alloc, perSide, keyMod, 10);
            SortMergeJoinOperator join = new SortMergeJoinOperator(
                    left, right, List.of("m", "c"), List.of("m", "c"),
                    JoinType.INNER, OPERATOR_MEMORY, 1);
            Result r = runJoin(join);
            System.out.println("  实际输出 = " + r.rows + " 行，键不一致行 = " + r.keyMismatch);
            expect(r.rows == expected, "INNER 输出行数 = " + expected + "（实际 " + r.rows + "）");
            expect(r.keyMismatch == 0, "每行左右两侧 (m,c) 完全相等（不一致 " + r.keyMismatch + " 行）");
        } catch (Throwable t) {
            System.out.println("  [!!] 运行异常: " + t);
            t.printStackTrace(System.out);
            failures++;
        }
    }

    /**
     * 内存路径（零拷贝 sortedIndices，不溢写）：同一个 {@link FileIterator} 也按单列键读过，
     * 这里用 256MB 预算确保不溢写，防「只修了溢写路径、内存路径仍按第一列 join」。
     */
    private static void testInnerCompositeKeyInMemory() {
        final int perSide = 20_000;
        final int keyMod = 1_000;
        final long expected = 1_000L * 20 * 20;
        System.out.println("---- 场景 2：INNER（纯内存路径），期望 " + expected + " 行 ----");
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Source left = new Source(alloc, perSide, keyMod, 10);
            Source right = new Source(alloc, perSide, keyMod, 10);
            SortMergeJoinOperator join = new SortMergeJoinOperator(
                    left, right, List.of("m", "c"), List.of("m", "c"),
                    JoinType.INNER, 256L * 1024 * 1024, 1);
            Result r = runJoin(join);
            System.out.println("  实际输出 = " + r.rows + " 行，键不一致行 = " + r.keyMismatch);
            expect(r.rows == expected, "INNER（内存）输出行数 = " + expected + "（实际 " + r.rows + "）");
            expect(r.keyMismatch == 0, "每行左右两侧 (m,c) 完全相等（不一致 " + r.keyMismatch + " 行）");
        } catch (Throwable t) {
            System.out.println("  [!!] 运行异常: " + t);
            t.printStackTrace(System.out);
            failures++;
        }
    }

    /**
     * LEFT：右表只有 c ∈ [0,4]（500 个键 × 20 行）。
     * 正确输出 = 匹配 500 × 20 × 20 + 左孤儿 500 × 20 = 210_000，其中 10_000 行右列全 NULL。
     */
    private static void testLeftCompositeKey() {
        final int leftRows = 20_000;
        final int rightRows = 10_000;
        final long expected = 500L * 20 * 20 + 500L * 20;
        final long expectedOrphans = 500L * 20;
        System.out.println("---- 场景 3：LEFT（右表只含 c∈[0,4]），期望 " + expected
                + " 行（其中孤儿 " + expectedOrphans + "） ----");
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Source left = new Source(alloc, leftRows, 1_000, 10);
            // 500 个键：m = k/5 ∈ [0,99]，c = k%5 ∈ [0,4]，每键 20 行
            Source right = new Source(alloc, rightRows, 500, 5);
            SortMergeJoinOperator join = new SortMergeJoinOperator(
                    left, right, List.of("m", "c"), List.of("m", "c"),
                    JoinType.LEFT, OPERATOR_MEMORY, 1);
            Result r = runJoin(join);
            System.out.println("  实际输出 = " + r.rows + " 行，右列 NULL 行 = " + r.rightNullRows
                    + "，键不一致行 = " + r.keyMismatch);
            expect(r.rows == expected, "LEFT 输出行数 = " + expected + "（实际 " + r.rows + "）");
            expect(r.rightNullRows == expectedOrphans,
                    "未匹配左行 = " + expectedOrphans + "（实际 " + r.rightNullRows + "）");
            expect(r.keyMismatch == 0, "匹配行两侧 (m,c) 完全相等（不一致 " + r.keyMismatch + " 行）");
            expect(r.badNullness == 0,
                    "右 payload NULL ⇔ 未匹配（不一致 " + r.badNullness + " 行）");
        } catch (Throwable t) {
            System.out.println("  [!!] 运行异常: " + t);
            t.printStackTrace(System.out);
            failures++;
        }
    }

    /**
     * 场景 4：<b>第二键列是 VARCHAR</b>（Arrow 15 的 {@code Text} 不是 {@code Comparable}），两侧溢写。
     *
     * <p>旧实现 {@code compareComposite} 用「{@code instanceof Comparable} 才比较」，VARCHAR 列被整列
     * 跳过 ⇒ 只要 {@code id} 相等就判为同一个 key。这里左 tag 全为 {@code east}、右 tag 全为
     * {@code buy}，正确输出 0 行；旧实现会输出 100 组 id × 200×200 = 4_000_000 行（线上 4 表 join
     * 卡死的形态）。</p>
     */
    private static void testVarCharSecondKeySpilled() {
        final int perSide = 20_000;
        System.out.println("---- 场景 4：复合键 (id, tag) 第二列为 VARCHAR 且两侧 tag 全不同（溢写），期望 0 行 ----");
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Operator left = new GeneratedVarCharSource(alloc, perSide, 100, "east");
            Operator right = new GeneratedVarCharSource(alloc, perSide, 100, "buy");
            SortMergeJoinOperator join = new SortMergeJoinOperator(
                    left, right, List.of("id", "tag"), List.of("id", "tag"),
                    JoinType.INNER, OPERATOR_MEMORY, 1);
            VarCharResult r = runVarCharJoin(join);
            System.out.println("  实际输出 = " + r.rows + " 行");
            expect(r.rows == 0, "VARCHAR 第二列不相等 ⇒ 无匹配（实际 " + r.rows + " 行）");
        } catch (Throwable t) {
            System.out.println("  [!!] 运行异常: " + t);
            t.printStackTrace(System.out);
            failures++;
        }
    }

    /**
     * 场景 5：与随机探针同形的 users/behavior 数据，唯一正确的 INNER 匹配是 {@code (4, "")}。
     * 旧实现会额外输出 {@code (1,east)×(1,*) } 与 {@code (2,east)×(2,*)} ⇒ 4 行。
     */
    private static void testVarCharSecondKeyExactMatches() {
        System.out.println("---- 场景 5：users/behavior 形状，期望且仅期望 (4,\"\") 匹配 ----");
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Operator left = new FixedVarCharSource(alloc,
                    new Integer[]{1, 2, 3, 4},
                    new String[]{"east", "east", "west", ""});
            Operator right = new FixedVarCharSource(alloc,
                    new Integer[]{1, 1, 1, 2, 2, 3, 7, null, 4},
                    new String[]{"app_launch", "buy", "buy", "buy", "app_launch", "buy", "buy", "", ""});
            SortMergeJoinOperator join = new SortMergeJoinOperator(
                    left, right, List.of("id", "tag"), List.of("id", "tag"),
                    JoinType.INNER, 256L * 1024 * 1024, 1);
            VarCharResult r = runVarCharJoin(join);
            System.out.println("  实际输出 = " + r.rows + " 行，id=" + r.ids);
            expect(r.rows == 1 && r.ids.equals("[4]"), "仅 (4,\"\") 匹配 1 行（实际 " + r.ids + "）");
        } catch (Throwable t) {
            System.out.println("  [!!] 运行异常: " + t);
            t.printStackTrace(System.out);
            failures++;
        }
    }

    // ==================== 工具 ====================

    private record VarCharResult(long rows, String ids) { }

    /** 逐页读取 [id int, tag varchar, payload varchar] + [id int, tag varchar, payload varchar]。 */
    private static VarCharResult runVarCharJoin(SortMergeJoinOperator join) throws Exception {
        long rows = 0;
        StringBuilder ids = new StringBuilder("[");
        join.open();
        try {
            Page p;
            while (!(p = join.next()).isEOF()) {
                VectorSchemaRoot root = p.getRoot();
                if (root != null && root.getRowCount() > 0) {
                    IntVector lid = (IntVector) root.getVector(0);
                    VarCharVector ltag = (VarCharVector) root.getVector(1);
                    IntVector rid = (IntVector) root.getVector(3);
                    VarCharVector rtag = (VarCharVector) root.getVector(4);
                    int n = root.getRowCount();
                    for (int i = 0; i < n; i++) {
                        if (lid.isNull(i) || rid.isNull(i) || ltag.isNull(i) || rtag.isNull(i)
                                || lid.get(i) != rid.get(i)
                                || !new String(ltag.get(i), StandardCharsets.UTF_8)
                                        .equals(new String(rtag.get(i), StandardCharsets.UTF_8))) {
                            failures++;
                            System.out.println("  [!!] 假匹配行：左侧 " + lid.get(i) + "/"
                                    + new String(ltag.get(i), StandardCharsets.UTF_8)
                                    + " 右侧 " + rid.get(i) + "/"
                                    + new String(rtag.get(i), StandardCharsets.UTF_8));
                        }
                        if (ids.length() < 200) ids.append(rows == 0 ? "" : ", ").append(lid.get(i));
                        rows++;
                    }
                }
                p.close();
            }
        } finally {
            join.close();
        }
        return new VarCharResult(rows, ids.append(']').toString());
    }

    /** 生成 [id int, tag varchar, payload varchar]：id = idx % idMod，tag 恒定。 */
    private static final class GeneratedVarCharSource implements Operator {
        private final BufferAllocator alloc;
        private final int total;
        private final int idMod;
        private final String tag;
        private int emitted;

        GeneratedVarCharSource(BufferAllocator alloc, int total, int idMod, String tag) {
            this.alloc = alloc;
            this.total = total;
            this.idMod = idMod;
            this.tag = tag;
        }

        @Override public void open() { }

        @Override
        public Page next() {
            if (emitted >= total) return Page.eof();
            int n = Math.min(PAGE_ROWS, total - emitted);
            IntVector id = new IntVector("id", alloc);
            VarCharVector tagV = new VarCharVector("tag", alloc);
            VarCharVector payload = new VarCharVector("payload", alloc);
            id.allocateNew(n);
            tagV.allocateNew((long) n * 16, n);
            payload.allocateNew((long) n * PAYLOAD.length(), n);
            byte[] tb = tag.getBytes(StandardCharsets.UTF_8);
            byte[] pb = PAYLOAD.getBytes(StandardCharsets.UTF_8);
            for (int r = 0; r < n; r++) {
                id.setSafe(r, (emitted + r) % idMod);
                tagV.setSafe(r, tb);
                payload.setSafe(r, pb);
            }
            id.setValueCount(n);
            tagV.setValueCount(n);
            payload.setValueCount(n);
            emitted += n;
            return Page.partitioned(new VectorSchemaRoot(List.of(id, tagV, payload)), 0);
        }

        @Override public void close() { }
    }

    /** 固定小数据集 [id int, tag varchar, payload varchar]，一页输出；id 可为 null。 */
    private static final class FixedVarCharSource implements Operator {
        private final BufferAllocator alloc;
        private final Integer[] ids;
        private final String[] tags;
        private boolean done;

        FixedVarCharSource(BufferAllocator alloc, Integer[] ids, String[] tags) {
            this.alloc = alloc;
            this.ids = ids;
            this.tags = tags;
        }

        @Override public void open() { }

        @Override
        public Page next() {
            if (done) return Page.eof();
            done = true;
            int n = ids.length;
            IntVector id = new IntVector("id", alloc);
            VarCharVector tagV = new VarCharVector("tag", alloc);
            VarCharVector payload = new VarCharVector("payload", alloc);
            id.allocateNew(n);
            tagV.allocateNew((long) n * 16, n);
            payload.allocateNew((long) n * PAYLOAD.length(), n);
            byte[] pb = PAYLOAD.getBytes(StandardCharsets.UTF_8);
            for (int r = 0; r < n; r++) {
                if (ids[r] == null) id.setNull(r); else id.setSafe(r, ids[r]);
                tagV.setSafe(r, tags[r].getBytes(StandardCharsets.UTF_8));
                payload.setSafe(r, pb);
            }
            id.setValueCount(n);
            tagV.setValueCount(n);
            payload.setValueCount(n);
            return Page.partitioned(new VectorSchemaRoot(List.of(id, tagV, payload)), 0);
        }

        @Override public void close() { }
    }

    private record Result(long rows, long keyMismatch, long rightNullRows, long badNullness) { }

    /** 逐页断言：左右两侧 m/c 相等；LEFT 时右 payload NULL 与键 NULL 一致。 */
    private static Result runJoin(SortMergeJoinOperator join) throws Exception {
        long rows = 0, keyMismatch = 0, rightNullRows = 0, badNullness = 0;
        join.open();
        try {
            Page p;
            while (!(p = join.next()).isEOF()) {
                VectorSchemaRoot root = p.getRoot();
                if (root != null && root.getRowCount() > 0) {
                    // 输出 = 左 [m,c,payload] + 右 [m,c,payload]
                    IntVector lm = (IntVector) root.getVector(0);
                    IntVector lc = (IntVector) root.getVector(1);
                    IntVector rm = (IntVector) root.getVector(3);
                    IntVector rc = (IntVector) root.getVector(4);
                    FieldVector rPayload = root.getVector(5);
                    int n = root.getRowCount();
                    for (int i = 0; i < n; i++) {
                        boolean rightNull = rm.isNull(i);
                        if (rightNull) {
                            rightNullRows++;
                        } else if (lm.get(i) != rm.get(i) || lc.get(i) != rc.get(i)) {
                            keyMismatch++;
                        }
                        if (rightNull != rPayload.isNull(i)) badNullness++;
                    }
                    rows += n;
                }
                p.close();
            }
        } finally {
            join.close();
        }
        return new Result(rows, keyMismatch, rightNullRows, badNullness);
    }

    private static void expect(boolean ok, String what) {
        if (!ok) { failures++; System.out.println("  [!!] " + what); }
        else System.out.println("  [ok] " + what);
    }

    /** 列 [m int, c int, payload varchar]：第 idx 行取键 k = idx % keyMod，m = k / cMod，c = k % cMod。 */
    private static final class Source implements Operator {
        private final BufferAllocator alloc;
        private final int total;
        private final int keyMod;
        private final int cMod;
        private int emitted;

        Source(BufferAllocator alloc, int total, int keyMod, int cMod) {
            this.alloc = alloc;
            this.total = total;
            this.keyMod = keyMod;
            this.cMod = cMod;
        }

        @Override public void open() { }

        @Override
        public Page next() {
            if (emitted >= total) return Page.eof();
            int n = Math.min(PAGE_ROWS, total - emitted);
            IntVector m = new IntVector("m", alloc);
            IntVector c = new IntVector("c", alloc);
            VarCharVector payload = new VarCharVector("payload", alloc);
            m.allocateNew(n);
            c.allocateNew(n);
            payload.allocateNew((long) n * PAYLOAD.length(), n);
            byte[] pb = PAYLOAD.getBytes(StandardCharsets.UTF_8);
            for (int r = 0; r < n; r++) {
                int k = (emitted + r) % keyMod;
                m.setSafe(r, k / cMod);
                c.setSafe(r, k % cMod);
                payload.setSafe(r, pb);
            }
            m.setValueCount(n);
            c.setValueCount(n);
            payload.setValueCount(n);
            emitted += n;
            return Page.partitioned(new VectorSchemaRoot(List.of(m, c, payload)), 0);
        }

        @Override public void close() { }
    }
}
