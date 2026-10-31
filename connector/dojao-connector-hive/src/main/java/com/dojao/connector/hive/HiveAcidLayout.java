package com.dojao.connector.hive;

import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <b>Hive ACID（事务表）目录布局</b>的纯解析/分类逻辑 —— 不碰 HDFS、不碰 HMS，只认名字。
 *
 * <p>Hive 事务表的目录结构（一个分区目录下）：</p>
 * <pre>
 *   base_&lt;writeId&gt;/                  全量基线（一次 compaction 的产物），只含 insert 记录
 *   delta_&lt;min&gt;_&lt;max&gt;_&lt;stmt&gt;/       增量（insert / update 的新值），写 id 落在 [min, max]
 *   delete_delta_&lt;min&gt;_&lt;max&gt;_&lt;stmt&gt;/  删除增量（只含 delete 记录）
 *   _orc_acid_version                  版本标记文件（被 isDataFileName 以 _ 前缀跳过）
 *   &lt;bucketId&gt;_&lt;copyId&gt;  / bucket_&lt;N&gt;  桶文件，位于上面各目录内
 * </pre>
 *
 * <p>每类目录都要按 {@link ValidReaderWriteIdList}（读快照）过滤：<b>base</b> 目录用
 * {@code isValidBase(w)}（只认已闭合的写事务）；<b>delta</b>/<b>delete_delta</b> 用
 * {@code isWriteIdRangeValid(min,max) != NONE}（区间内没有任何可见写事务就整目录丢掉）。
 * 这套判定与 Hive / Impala 的 reader 一致，不自己发明。</p>
 *
 * <p><b>为什么按 (分区目录, 桶号) 分组</b>：ACID 的行身份
 * {@code RecordIdentifier = (originalTransaction, bucketId, rowId)} 只在<b>同一桶</b>内保证唯一 ——
 * 同一个 {@code rowId} 在不同桶、不同分区都可能有。行级合并必须把<b>一个桶的全部文件</b>
 * （base + 全部 delta + 全部 delete_delta）交给<b>同一个</b> reader 处理，否则跨分片的删除
 * 看不到、更新覆盖不到。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/7
 */
public final class HiveAcidLayout {

    static final String BASE_PREFIX = "base_";
    static final String DELTA_PREFIX = "delta_";
    static final String DELETE_DELTA_PREFIX = "delete_delta_";
    /** Hive 4 的可见性元数据目录，非数据，跳过。 */
    static final String VISIBILITY_PREFIX = "visibility_";

    /** 文件来源类别。合并顺序：先原始（最老）、再 base、最后 delta。 */
    public enum Kind {
        /** 非事务时代遗留的普通 ORC 文件（直接躺在分区目录下，无 ACID 列）。 */
        ORIGINAL,
        /** {@code base_<w>/} 下的基线文件。 */
        BASE,
        /** {@code delta_*} / {@code delete_delta_*} 下的增量文件。 */
        DELTA
    }

    /** 一个 ACID 数据文件：{@code root} 是它所属的<b>分区目录</b>（不是文件自己的父目录）。 */
    public record AcidFile(String root, String path, long bytes, Kind kind, long writeId) {
    }

    /** 一个 (分区目录, 桶号) 分组：该桶的<b>全部</b>文件，已按「原始 → base → delta」排序。 */
    public record BucketGroup(String root, int bucketId, List<AcidFile> files) {
    }

    private HiveAcidLayout() {
    }

    // ==================== 目录名解析（纯函数） ====================

    /** {@code base_<writeId>} → write id；非 base 目录 → {@code null}。 */
    public static Long baseWriteId(String dirName) {
        if (dirName == null || !dirName.startsWith(BASE_PREFIX)) return null;
        return parseLong(dirName.substring(BASE_PREFIX.length()));
    }

    /** {@code delta_<min>_<max>_<stmt>} / {@code delete_delta_...} → {@code {min, max}}；否则 {@code null}。 */
    public static long[] deltaRange(String dirName) {
        if (dirName == null) return null;
        String rest;
        if (dirName.startsWith(DELETE_DELTA_PREFIX)) {
            rest = dirName.substring(DELETE_DELTA_PREFIX.length());
        } else if (dirName.startsWith(DELTA_PREFIX)) {
            rest = dirName.substring(DELTA_PREFIX.length());
        } else {
            return null;
        }
        String[] parts = rest.split("_");
        if (parts.length < 2) return null;
        Long min = parseLong(parts[0]);
        Long max = parseLong(parts[1]);
        if (min == null || max == null) return null;
        return new long[]{min, max};
    }

    /** 是否 delete_delta 目录。 */
    public static boolean isDeleteDelta(String dirName) {
        return dirName != null && dirName.startsWith(DELETE_DELTA_PREFIX);
    }

    // ==================== 读快照可见性 ====================

    /**
     * <b>ACID 读快照</b> —— 屏蔽 Hive2（txnId）与 Hive3（writeId）的快照模型差异。
     *
     * <p>两版的目录布局「按名字」完全一致（{@code base_<id>} / {@code delta_<min>_<max>_<stmt>} /
     * {@code delete_delta_...}），差别只在<b>那个 id 是写 id 还是事务 id</b>，以及「可见」的判定源
     * 是 {@link ValidReaderWriteIdList}（Hive3 {@code getValidWriteIds}）还是
     * {@link ValidTxnList}（Hive2 {@code get_open_txns_info}）。此接口把「目录名里的 id 是否可见」
     * 抽成一个判定，ACID 分类/分组/合并逻辑对版本无感知。</p>
     */
    public interface Visibility {

        /** {@code base_<id>} 目录是否可见。 */
        boolean baseVisible(long id);

        /** {@code delta_<min>_<max>} / {@code delete_delta_...} 目录是否含可见记录。 */
        boolean deltaVisible(long min, long max);

        /** 日志用的人类可读描述。 */
        String describe();

        /** 拿不到快照（HMS 过老 / 非事务表）→ 一律可见：宁多读已提交增量，不丢可见行。 */
        Visibility ALWAYS = new Visibility() {
            @Override public boolean baseVisible(long id) { return true; }
            @Override public boolean deltaVisible(long min, long max) { return true; }
            @Override public String describe() { return "无快照（不过滤）"; }
        };

        /** Hive3 写 id 快照：base 用 {@code isValidBase}，增量区间用 {@code isWriteIdRangeValid}。 */
        static Visibility writeIds(ValidReaderWriteIdList v) {
            if (v == null) return ALWAYS;
            return new Visibility() {
                @Override public boolean baseVisible(long id) { return v.isValidBase(id); }
                @Override public boolean deltaVisible(long min, long max) {
                    return v.isWriteIdRangeValid(min, max) != ValidWriteIdList.RangeResponse.NONE;
                }
                @Override public String describe() {
                    long[] inv = v.getInvalidWriteIds();
                    return "writeId(高水位=" + v.getHighWatermark() + ", 无效="
                            + (inv == null ? 0 : inv.length) + ")";
                }
            };
        }

        /** Hive2 事务 id 快照：base 用 {@code isTxnValid}，增量区间用 {@code isTxnRangeValid}。 */
        static Visibility txns(ValidTxnList v) {
            if (v == null) return ALWAYS;
            return new Visibility() {
                @Override public boolean baseVisible(long id) { return v.isTxnValid(id); }
                @Override public boolean deltaVisible(long min, long max) {
                    return v.isTxnRangeValid(min, max) != ValidTxnList.RangeResponse.NONE;
                }
                @Override public String describe() {
                    long[] inv = v.getInvalidTransactions();
                    return "txn(高水位=" + v.getHighWatermark() + ", 无效="
                            + (inv == null ? 0 : inv.length) + ")";
                }
            };
        }
    }

    /** 该 base 目录在当前 Hive3 写快照下是否可见。{@code valid == null} → 一律可见。 */
    public static boolean baseVisible(ValidReaderWriteIdList valid, long writeId) {
        return Visibility.writeIds(valid).baseVisible(writeId);
    }

    /** 该 delta 目录在当前 Hive3 写快照下是否含可见记录。{@code valid == null} → 一律可见。 */
    public static boolean deltaVisible(ValidReaderWriteIdList valid, long min, long max) {
        return Visibility.writeIds(valid).deltaVisible(min, max);
    }

    // ==================== 分组 ====================

    /**
     * 把一枚枚 ACID 文件按 (分区目录, 桶号) 分组；组内按「原始 → base → delta」、写 id 升序、
     * 路径字典序排序 —— 顺序确定，结果可复现。
     *
     * <p>桶号取自文件名（复用 {@link HiveConnector#bucketIdOf}，现代 {@code 000000_0} 与旧式
     * {@code bucket_00000} 都认）。解析不出桶号的（罕见：非分桶的原始文件）归到桶号 {@code -1}
     * 的单独组，仍会被读，只是退化成串行。</p>
     */
    public static List<BucketGroup> groupByBucket(List<AcidFile> files) {
        Map<String, List<AcidFile>> grouped = new LinkedHashMap<>();
        Map<String, Integer> bucketOfKey = new HashMap<>();
        for (AcidFile f : files) {
            Integer parsed = HiveConnector.bucketIdOf(f.path());
            int bucket = parsed == null ? -1 : parsed;
            String key = f.root() + "#" + bucket;
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(f);
            bucketOfKey.put(key, bucket);
        }
        List<BucketGroup> out = new ArrayList<>(grouped.size());
        for (Map.Entry<String, List<AcidFile>> e : grouped.entrySet()) {
            List<AcidFile> list = e.getValue();
            list.sort(Comparator
                    .comparingInt((AcidFile f) -> f.kind().ordinal())
                    .thenComparingLong(AcidFile::writeId)
                    .thenComparing(AcidFile::path));
            out.add(new BucketGroup(list.get(0).root(), bucketOfKey.get(e.getKey()), List.copyOf(list)));
        }
        return out;
    }

    // ==================== 工具 ====================

    /**
     * 从一个 ACID 数据文件路径反推它所属的<b>分区目录</b>（ACID 根）——
     * {@code .../dt=2020/base_0000010/000000_0} → {@code .../dt=2020}；
     * {@code .../dt=2020/000000_0} → {@code .../dt=2020}。
     *
     * <p>Worker 只拿到文件路径清单（切片载荷里没有目录结构），靠它把「哪些文件同属一个桶」重新
     * 拼回来。</p>
     */
    public static String acidRoot(String filePath) {
        String dir = parentDir(filePath);
        if (dir == null) return null;
        String name = baseName(dir);
        if (name != null && (name.startsWith(BASE_PREFIX) || name.startsWith(DELTA_PREFIX)
                || name.startsWith(DELETE_DELTA_PREFIX))) {
            return parentDir(dir);
        }
        return dir;
    }

    static String parentDir(String path) {
        int slash = path.lastIndexOf('/');
        return slash <= 0 ? null : path.substring(0, slash);
    }

    private static String baseName(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }

    private static Long parseLong(String s) {
        if (s == null || s.isEmpty()) return null;
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) return null;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
