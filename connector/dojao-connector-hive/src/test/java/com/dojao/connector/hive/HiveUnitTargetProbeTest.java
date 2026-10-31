package com.dojao.connector.hive;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <b>自适应读取单元目标大小</b>的离线探针（纯本地，无 HDFS / 无 HMS，毫秒级）。
 *
 * <p>背景：{@code planUnits} 的合并上限原来恒为 {@code hive.compact.target.bytes}（默认 128MB），
 * 于是「总量 &lt; 上限」的小分区会被整段并成 <b>1 个单元</b> ⇒
 * {@code groups = min(并行度, 单元数) = 1} ⇒ 并行度全废、读退化成单线程串行
 * （实测 82MB 分区 / 约 10.5s，这是 54s 查询修掉列裁剪后剩下的最大一块）。</p>
 *
 * <p>本探针钉死两件事：① {@link HiveConnector#unitTarget(long, int, long)} 的取值表；
 * ② 用同一份 {@code planUnits}（反射直击，不再复制一份合并逻辑）验证同一批文件在新旧目标下
 * 单元数的差异 —— 旧口径 2 个 41MB 文件 → 1 单元（缺陷复现），新口径 → 2 单元。</p>
 *
 * <p>所有测试文件长度都 &lt; {@code hive.split.file.min.bytes}（默认 1GB），因此
 * {@code planUnits} 恒走「合并小文件」分支、绝不碰 {@code splitBigFile}（那条路要连 HDFS）。
 * 这也是 {@code meta} 可以传 {@code null} 的原因。</p>
 *
 * @author jiaxin
 * @date 2026/9/27
 */
public class HiveUnitTargetProbeTest {

    private static final long MB = 1024L * 1024L;
    private static final long COMPACT = 128L * MB;   // 对应 dfs.blocksize 默认值

    private static int pass = 0;
    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        System.err.println("========== Hive 自适应读取单元目标大小 探针 ==========");
        testUnitTargetTable();
        testPlanUnitsEndToEnd();
        System.err.printf("========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) {
            System.exit(1);
        }
    }

    // ==================== 1. 取值表 ====================

    private static void testUnitTargetTable() {
        System.err.println("---------- 1. unitTarget 取值表 ----------");

        long t82 = HiveConnector.unitTarget(82 * MB, 8, COMPACT);
        System.err.println("  unitTarget(82MB, 8) = " + (t82 / MB) + "MB（原始 " + t82 + "）");
        check("82MB ÷ 8 并行度 → 约 10MB（取总量/并行度）", t82 == 82 * MB / 8);
        check("该目标让每个 41MB 文件都超过它 → 各自成单元", 41 * MB >= t82);

        check("大批量（4GB ÷ 8 = 512MB）→ 仍被夹回 128MB（常规表口径不变）",
                HiveConnector.unitTarget(4L * 1024 * MB, 8, COMPACT) == COMPACT);
        check("恰好 128MB × 8 = 1GB → 128MB（与旧口径重合的临界点）",
                HiveConnector.unitTarget(8 * COMPACT, 8, COMPACT) == COMPACT);

        check("极小数据（1MB ÷ 8）→ 落到下限 8MB（不放大单元数）",
                HiveConnector.unitTarget(1 * MB, 8, COMPACT) == 8 * MB);
        check("64MB ÷ 8 = 8MB → 正好等于下限",
                HiveConnector.unitTarget(64 * MB, 8, COMPACT) == 8 * MB);

        check("并行度 ≤ 1 → 固定目标（无并行可谈）",
                HiveConnector.unitTarget(82 * MB, 1, COMPACT) == COMPACT);
        check("总量未知（0）→ 固定目标",
                HiveConnector.unitTarget(0L, 8, COMPACT) == COMPACT);
    }

    // ==================== 2. planUnits 端到端（反射直击同一份合并逻辑） ====================

    private static void testPlanUnitsEndToEnd() throws Exception {
        System.err.println("---------- 2. planUnits 单元数（同一份实现，只换 target） ----------");
        HiveConnector conn = new HiveConnector(HiveVersion.HIVE3, "hive",
                Map.of("hive.metastore.uris", "thrift://127.0.0.1:9083"));
        try {
            Method planUnits = HiveConnector.class.getDeclaredMethod(
                    "planUnits", HiveTableMeta.class, List.class, long.class);
            planUnits.setAccessible(true);
            Constructor<?> fe = Class.forName("com.dojao.connector.hive.HiveConnector$FileEntry")
                    .getDeclaredConstructor(String.class, long.class);
            fe.setAccessible(true);

            // ① 本查询的真实形态：2 个 ~41MB 文件组成 82MB 分区
            List<Object> twoFiles = List.of(
                    fe.newInstance("hdfs://n/part-0.orc", 41 * MB),
                    fe.newInstance("hdfs://n/part-1.orc", 41 * MB));

            int oldUnits = unitCount(planUnits, conn, twoFiles, COMPACT);
            int newTarget = (int) HiveConnector.unitTarget(82 * MB, 8, COMPACT);
            int newUnits = unitCount(planUnits, conn, twoFiles, newTarget);
            System.err.printf("  2 个 ~41MB 文件：旧目标 128MB → %d 单元；新目标 %dMB → %d 单元%n",
                    oldUnits, newTarget / MB, newUnits);
            check("旧口径（128MB）把 82MB 分区并成 1 个单元（缺陷复现）", oldUnits == 1);
            check("新口径把 2 个文件拆成 2 个单元（并行度用得上）", newUnits == 2);

            // ② 小文件海量：不能被自适应目标放大成一堆单元
            List<Object> tiny = new ArrayList<>();
            for (int i = 0; i < 1000; i++) tiny.add(fe.newInstance("hdfs://n/tiny-" + i, 10 * 1024L));
            int tinyTarget = (int) HiveConnector.unitTarget(1000 * 10 * 1024L, 8, COMPACT);
            int tinyUnits = unitCount(planUnits, conn, tiny, tinyTarget);
            System.err.printf("  1000 个 10KB 文件（共约 9.8MB）：目标 %dMB → %d 单元%n",
                    tinyTarget / MB, tinyUnits);
            check("小文件海量不被放大成一堆单元（下限 8MB 兜住：≈total/8MB → 2 单元，远不是 1000）",
                    tinyUnits == 2);

            // ②b 总量低于下限：8MB 下限把单元数钉死在 1（不因并行度去切本来就很小的数据）
            List<Object> under = new ArrayList<>();
            for (int i = 0; i < 300; i++) under.add(fe.newInstance("hdfs://n/u-" + i, 10 * 1024L));
            int underTarget = (int) HiveConnector.unitTarget(300 * 10 * 1024L, 8, COMPACT);
            check("总量 3MB（< 下限）→ 仍合成 1 个单元", unitCount(planUnits, conn, under, underTarget) == 1);

            // ③ 中等规模：旧口径 2 单元 → 新口径 10 单元（并行度 8 才用得上）
            List<Object> mid = new ArrayList<>();
            for (int i = 0; i < 20; i++) mid.add(fe.newInstance("hdfs://n/m-" + i, 8 * MB));
            int midTarget = (int) HiveConnector.unitTarget(160 * MB, 8, COMPACT);
            int midOld = unitCount(planUnits, conn, mid, COMPACT);
            int midNew = unitCount(planUnits, conn, mid, midTarget);
            System.err.printf("  20 个 8MB 文件：旧 128MB → %d 单元；新 %dMB → %d 单元%n",
                    midOld, midTarget / MB, midNew);
            check("中等规模下单元数从 2 提到 10（> 并行度 8，切片数才能到 8）", midOld == 2 && midNew == 10);

            // ④ 单个大文件（< 切割阈值）：独占一个单元，绝不因目标小而丢失
            List<Object> big = List.of(fe.newInstance("hdfs://n/one-200mb.orc", 200 * MB));
            check("单个 200MB 文件在新目标下仍独占 1 个单元（丢不了数据）",
                    unitCount(planUnits, conn, big, (int) HiveConnector.unitTarget(200 * MB, 8, COMPACT)) == 1);
        } finally {
            conn.closeRead();
        }
    }

    /** 反射调 {@code planUnits} 并数单元数（{@code meta} 恒为 null —— 文件都 < 1GB，走不到需要 meta 的切割分支）。 */
    private static int unitCount(Method planUnits, HiveConnector conn, List<Object> files, long target)
            throws Exception {
        Object units = planUnits.invoke(conn, null, files, target);
        return ((List<?>) units).size();
    }

    private static void check(String what, boolean ok) {
        if (ok) {
            pass++;
            System.err.println("[PASS] " + what);
        } else {
            fail++;
            System.err.println("[FAIL] " + what);
        }
    }
}
