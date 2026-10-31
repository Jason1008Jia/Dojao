package com.dojao.common.util;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * <b>分区裁剪</b> —— 引擎侧的通用能力，<b>不专属某个连接器</b>：任何「有分区概念、能列举出分区」
 * 的数据源都能用它把不匹配的分区整个跳过，从而减少扫描量（Hive 目录 / Iceberg·Delta 元数据 /
 * 自带分区目录的 CSV 之类都适用）。
 *
 * <p><b>连接器接入三部曲</b>（无需任何 SPI / 协议改动）：</p>
 * <ol>
 *   <li>{@link #extract} 解析 {@code ReadRequest.filterExpression()} —— 引擎已经把 WHERE 下推文案
 *       交到手里，这里把它变成「分区列 → 约束」；</li>
 *   <li>{@link #prune} 把这些约束作用到自己的分区清单上（取值器用 {@link #byColumnOrder} 适配
 *       位置型分区值）；</li>
 *   <li>只在保留下来的分区上列举文件 / 开 reader —— 被剪掉的分区连目录都不列。</li>
 * </ol>
 *
 * <p><b>落点必须是「列举扫描单元之前」</b>（各连接器的 {@code planSplits}）。落在读数据时才过滤
 * 就只是白读一遍，省不下扫描量。数据库自身会按 WHERE 裁剪原生分区的连接器（MySQL / PostgreSQL
 * 把谓词编进 SQL）<b>无需</b>接这套。</p>
 *
 * <p>支持的合取项（顶层 {@code AND} 连接的各项，可混合）：</p>
 * <ul>
 *   <li>{@code 分区列 = 字面量}、{@code 分区列 IN (字面量, …)}</li>
 *   <li>{@code 分区列 != 字面量} / {@code 分区列 <> 字面量}（排除值）</li>
 *   <li>{@code 分区列 < <= > >= 字面量}（★按分区列的<b>声明类型</b>比较，见下）</li>
 *   <li>{@code 分区列 BETWEEN 字面量 AND 字面量}（等价于两个范围界的合取）</li>
 * </ul>
 *
 * <p><b>★范围谓词必须按声明类型比较，不能一律按字符串比。</b>分区值在 HMS 里都是字符串
 * （{@code Partition.getValues()}），而「字符串序 = 业务序」只在部分类型成立：定长零填充的
 * {@code date} / {@code timestamp} 成立、{@code string} 成立（Hive 的语义本就是字节序），
 * 但数值型分区列（如 {@code hour}）字典序下 {@code "10" < "9"} —— 一刀下去就是<b>静默丢数据</b>。
 * 故本类按 {@link PartitionColumn#type()} 建比较器：数值类型解析成 {@link BigDecimal}，
 * {@code date} / {@code timestamp} 解析成 {@link LocalDate} / {@link LocalDateTime}，
 * {@code string}/{@code varchar}/{@code char} 走字典序。<b>类型未知、或任一侧解析不出来 →
 * 该条范围约束一律「不剪」</b>（保守放行，宁可多扫几个分区）。</p>
 *
 * <p><b>安全铁律：裁剪只许「删」分区，绝不许「留」错分区。</b>认不出的合取项（{@code OR}、
 * {@code LIKE}、函数、列对列比较、非分区列、解析异常……）都<b>只跳过该项</b>，既不报错也不阻断
 * 其它项生效 —— 于是最坏情况只是「少剪一点」（多扫几个分区），永远不会因为解析失误而漏读数据。
 * 同一分区列出现多处约束时取<b>合取</b>（交集）：{@code dt='a' AND dt='b'} 自然剪成 0 个分区。</p>
 *
 * <p>纯函数、无 I/O，可离线探针验证。</p>
 */
public final class PartitionPruner {

    private PartitionPruner() {
    }

    /**
     * 一个分区列：名字 + Hive 声明类型。
     *
     * @param name 分区列名（大小写不敏感比对，返回值保留原始大小写）
     * @param type 声明类型（如 {@code string} / {@code int} / {@code date} / {@code decimal(10,2)}）；
     *             {@code null}/空 = 未知 ⇒ 该列只做等值裁剪，范围谓词一律不剪
     */
    public record PartitionColumn(String name, String type) {

        public PartitionColumn(String name) {
            this(name, null);
        }
    }

    /** 某个分区列上的合取约束（等值集 ∧ 排除集 ∧ 若干范围界）。 */
    public static final class ColumnConstraint {

        private final Set<String> allowed;    // = / IN；null = 无等值约束；空集 = 无值可满足
        private final Set<String> excluded;   // != / <>
        private final List<Bound> bounds;     // 范围界（可多条，取合取）
        private final ValueCmp cmp;           // 该列类型对应的三值比较器；null = 不支持范围比较

        private ColumnConstraint(Set<String> allowed, Set<String> excluded, List<Bound> bounds, ValueCmp cmp) {
            this.allowed = allowed;
            this.excluded = excluded;
            this.bounds = bounds;
            this.cmp = cmp;
        }

        /**
         * 等值 / {@code IN} 命中的<b>允许值集合</b>（原文，未按声明类型转换）；{@code null} = 该列
         * 没有等值约束（只有 {@code !=} / 范围 / 无）。供「桶裁剪」等需要知道<b>具体命中值</b>
         * 的消费方读取 —— 分区裁剪只看 {@link #matches(String)}，不需要本方法。
         */
        public Set<String> allowedValues() {
            return allowed;
        }

        /**
         * 该分区值是否<b>可能</b>满足谓词。返回 {@code false} = 这个分区可以整个不扫。
         *
         * <p>拿不准一律返回 {@code true}（多扫一个分区只是慢，剪错就是丢数据）：分区值解析不出
         * （如数值列上的 {@code __HIVE_DEFAULT_PARTITION__}）、分区值为 {@code null} 等情形都走这条。</p>
         */
        public boolean matches(String value) {
            if (value == null) return true;
            if (allowed != null && !allowed.contains(value)) return false;
            if (excluded != null && excluded.contains(value)) return false;
            for (Bound b : bounds) {
                if (!b.op().holds(cmp.compare(value, b.literal()))) return false;
            }
            return true;
        }

        /**
         * 该分区取值是否<b>确定</b>满足本约束 —— 「兑现」判定，与 {@link #matches(String)} 恰成镜像：
         * {@code matches} 答「能不能剪掉这个分区」（拿不准 → true = 不剪），本方法答「能不能断言这个
         * 分区内<b>每一行</b>都满足」（拿不准 → false = 不断言）。
         *
         * <p>一个分区只由各分区列的取值定义 ⇒ 列取值确定满足 ⇒ 该分区内全部行确定满足该谓词。
         * 这正是「分区裁剪已兑现某条谓词 ⇒ 引擎不必再求值它」（见 {@link #enforcedConjuncts}）的依据。</p>
         *
         * <p><b>拿不准一律 false</b>：值缺、类型不支持范围比较、字面量解析不出，都不足以断言。</p>
         */
        public boolean enforcedFor(String value) {
            if (value == null) return false;                                        // 值缺 → 拿不准
            if (allowed == null && excluded == null && bounds.isEmpty()) return false; // 无实质约束 → 无可兑现
            if (allowed != null && !allowed.contains(value)) return false;          // 确定不满足
            if (excluded != null && excluded.contains(value)) return false;         // 确定不满足
            for (Bound b : bounds) {
                Integer c = cmp == null ? null : cmp.compare(value, b.literal());
                if (c == null || !b.op().holds(c)) return false;                    // 拿不准 / 不满足
            }
            return true;
        }

        @Override
        public String toString() {
            List<String> parts = new ArrayList<>();
            if (allowed != null) {
                parts.add(allowed.size() == 1 ? "=" + allowed.iterator().next() : "IN" + allowed);
            }
            if (excluded != null) parts.add("!=" + excluded);
            for (Bound b : bounds) parts.add(b.op().symbol() + b.literal());
            return String.join(" AND ", parts);
        }
    }

    /**
     * 抽取分区列约束。
     *
     * @param filter           下推的过滤表达式（连接器方言 SQL；{@code null}/空 = 不裁剪）
     * @param partitionColumns 分区列清单（保序，与 SPI {@code getPartitionColumns} 一致）
     * @return 分区列名（<b>原样大小写</b>）→ 约束；<b>空 map = 无法裁剪（不剪）</b>
     */
    public static Map<String, ColumnConstraint> extract(String filter, List<PartitionColumn> partitionColumns) {
        Map<String, ColumnConstraint> out = new LinkedHashMap<>();
        if (filter == null || filter.isBlank() || partitionColumns == null || partitionColumns.isEmpty()) {
            return out;
        }
        // 规范化列名（小写）→ 原始分区列名 / 该列类型的比较器：大小写不敏感比对但不丢原始大小写
        Map<String, String> byNorm = new LinkedHashMap<>();
        Map<String, ValueCmp> cmpByNorm = new LinkedHashMap<>();
        for (PartitionColumn c : partitionColumns) {
            if (c == null || c.name() == null || c.name().isBlank()) continue;
            String norm = normalizeIdent(c.name());
            byNorm.putIfAbsent(norm, c.name());
            cmpByNorm.putIfAbsent(norm, comparatorFor(c.type()));
        }

        Map<String, Builder> acc = new LinkedHashMap<>();
        // 迭代剥括号 + 按顶层 AND 反复切分：`((dt='a') AND (dt='b')) AND x` 也能拆到原子合取项。
        ArrayDeque<String> work = new ArrayDeque<>();
        work.add(filter);
        while (!work.isEmpty()) {
            String cur = stripOuterParens(work.poll().trim());
            if (cur.isEmpty()) continue;
            // ★BETWEEN 的 AND 属于语法本身，必须先于「按顶层 AND 切分」识别，否则会被切成两半。
            Between between = parseBetween(cur);
            if (between != null) {
                merge(acc, byNorm, cmpByNorm, between.terms());
                // 上界字面量之后可能还挂着外层合取项（`dt BETWEEN 'a' AND 'b' AND hour >= 9`）→ 交回继续解析
                if (!between.rest().isBlank()) work.add(between.rest());
                continue;
            }
            List<String> parts = splitOnTopLevelAnd(cur);
            if (parts.size() > 1) {
                for (String p : parts) work.add(p);
                continue;
            }
            merge(acc, byNorm, cmpByNorm, parseConstraint(cur));
        }
        for (Map.Entry<String, Builder> e : acc.entrySet()) {
            out.put(e.getKey(), e.getValue().build());
        }
        return out;
    }

    // ============================================================================
    // ★通用裁剪：任何「能列举出分区 + 知道每个分区的分区列取值」的数据源都能用
    // ============================================================================

    /**
     * <b>把约束作用到分区清单上</b> —— 返回保留下来的分区，其余连元数据都不用取、文件也不用列。
     *
     * <p>这是「有分区的数据源都能省扫描量」的通用落地点：连接器只需两件事 ——
     * ① 用 {@link #extract} 把 {@code request.filterExpression()} 解析成约束；
     * ② 给出「分区对象 → 分区列名到值」的取值器（值按分区列顺序排列的形态可直接用
     * {@link #byColumnOrder}，Hive 的 {@code Partition.getValues()} 就是这种）。</p>
     *
     * <p>本方法<b>不做任何 I/O</b>，只做判定，因此可以在列举扫描单元之前调用：被剪掉的分区
     * 既不会出现在切片里，也不会被列目录 / 开文件 —— 这才是省下扫描量的地方。</p>
     *
     * <p><b>拿不准则保留</b>（约束无法判定、分区缺某分区列的取值、名字对不上……）：丢弃是
     * 「静默少读数据」，保留只是「多扫一个分区 + 让下游把问题暴露出来」。列名比对大小写不敏感。</p>
     *
     * @param partitions  待裁剪的分区清单（{@code null} → 空列表；空 → 原样返回）
     * @param values      分区 → 「分区列名 → 值」取值器
     * @param constraints {@link #extract} 的结果；{@code null}/空 = 不裁剪（原样返回）
     * @return 保留下来的分区（顺序与入参一致）
     */
    public static <T> List<T> prune(List<T> partitions, PartitionValues<T> values,
                                    Map<String, ColumnConstraint> constraints) {
        if (partitions == null) return List.of();
        if (partitions.isEmpty() || constraints == null || constraints.isEmpty()) return partitions;

        Map<String, ColumnConstraint> normConstraints = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnConstraint> e : constraints.entrySet()) {
            if (e.getKey() != null) normConstraints.put(normalizeIdent(e.getKey()), e.getValue());
        }

        List<T> kept = new ArrayList<>(partitions.size());
        for (T p : partitions) {
            if (partitionSurvives(values.valuesOf(p), normConstraints)) kept.add(p);
        }
        return kept;
    }

    /**
     * <b>挑出「已被分区裁剪兑现」的合取项</b> —— 即本次保留下来的<b>每个分区的每一行</b>都必然满足
     * 的谓词。连接器按这些谓词裁掉了不匹配的分区之后，引擎再把它们求值一遍纯属重复劳动，可安全剔除
     * （数据源不兑现谓词下推时尤其值钱：省掉的就是扫描之上那一层逐行 {@code Filter}）。
     *
     * <p><b>三条同时成立才认定兑现：</b></p>
     * <ol>
     *   <li><b>分区清单非空</b> —— 兑现的前提是「确实按这些分区裁剪过」。HMS 未登记分区（连接器回退
     *       扫表根目录，等于没裁）时清单为空，绝不能丢谓词；</li>
     *   <li>该合取项只落在<b>恰好一个</b>分区列上（多列项如 {@code dt='a' AND hour=9} 各自成项）；</li>
     *   <li>该合取项对<b>每一个</b>保留分区的取值都 {@link ColumnConstraint#enforcedFor 确定成立}。</li>
     * </ol>
     *
     * <p><b>★判定一律用该分区列的「合并」约束（与 {@link #prune} 同一份 {@link #extract} 结果）。</b>
     * 裁剪是按合并约束（同列多处约束取合取）做的，兑现判定若改用「单项自己的约束」就会与裁剪口径分叉。
     * 合并后确定成立 ⇒ 其中每一项都确定成立（合取强于各合取项），故逐项剔除安全；且判定只会更保守、
     * 更不会误剔。</p>
     *
     * @param filter           完整过滤表达式（与 {@link #prune} 用同一份，保证约束合并口径一致）
     * @param partitionColumns 分区列清单（含声明类型）
     * @param conjuncts        候选合取项（原文；须来自 {@code filter} 的顶层 AND 拆分）
     * @param partitions       本次实际保留的分区（空 = 没裁过 → 一律不兑现）
     * @param values           分区 → 「分区列名 → 值」取值器
     * @return 被兑现的合取项（原文，∈ {@code conjuncts}）；无可兑现项返回空列表
     */
    public static <T> List<String> enforcedConjuncts(String filter, List<PartitionColumn> partitionColumns,
                                                     List<String> conjuncts, List<T> partitions,
                                                     PartitionValues<T> values) {
        if (conjuncts == null || conjuncts.isEmpty()) return List.of();
        if (partitions == null || partitions.isEmpty()) return List.of();     // 没真裁过 → 不兑现
        Map<String, ColumnConstraint> merged = extract(filter, partitionColumns);
        if (merged.isEmpty()) return List.of();
        Map<String, ColumnConstraint> byNorm = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnConstraint> e : merged.entrySet()) {
            byNorm.put(normalizeIdent(e.getKey()), e.getValue());
        }

        List<String> enforced = new ArrayList<>();
        for (String conj : conjuncts) {
            if (conj == null || conj.isBlank()) continue;
            Map<String, ColumnConstraint> own = extract(conj, partitionColumns);
            if (own.size() != 1) continue;                                   // 非「单一分区列」项 → 不兑现
            String colNorm = normalizeIdent(own.keySet().iterator().next());
            ColumnConstraint c = byNorm.get(colNorm);
            if (c == null) continue;
            boolean ok = true;
            for (T p : partitions) {
                if (!c.enforcedFor(valueFor(values.valuesOf(p), colNorm))) {
                    ok = false;
                    break;
                }
            }
            if (ok) enforced.add(conj);
        }
        return enforced;
    }

    /** 从「分区列名 → 值」里按规范化列名取值（大小写不敏感）；给不出该列的值 → {@code null}（= 拿不准）。 */
    private static String valueFor(Map<String, String> values, String normColumn) {
        if (values == null) return null;
        for (Map.Entry<String, String> e : values.entrySet()) {
            if (e.getKey() != null && normalizeIdent(e.getKey()).equals(normColumn)) {
                return e.getValue();
            }
        }
        return null;
    }

    private static boolean partitionSurvives(Map<String, String> rawValues,
                                             Map<String, ColumnConstraint> constraints) {
        if (rawValues == null || rawValues.isEmpty()) return true;   // 取值器给不出东西 → 不剪
        Map<String, String> vals = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : rawValues.entrySet()) {
            if (e.getKey() != null) vals.put(normalizeIdent(e.getKey()), e.getValue());
        }
        for (Map.Entry<String, ColumnConstraint> e : constraints.entrySet()) {
            if (!vals.containsKey(e.getKey())) continue;             // 该分区没给出这个分区列 → 不剪
            if (!e.getValue().matches(vals.get(e.getKey()))) return false;
        }
        return true;
    }

    /** 分区取值器：分区对象 → 「分区列名 → 值」。 */
    @FunctionalInterface
    public interface PartitionValues<T> {
        Map<String, String> valuesOf(T partition);
    }

    /**
     * 常见形态：分区值<b>按分区列顺序</b>排列（Hive 的 {@code Partition.getValues()}）→ 拼成
     * 「分区列名 → 值」。值少于列名的那几个列不会进 map ⇒ {@link #prune} 视为「拿不准」而不剪。
     */
    public static Map<String, String> byColumnOrder(List<String> keys, List<String> values) {
        Map<String, String> out = new LinkedHashMap<>();
        if (keys == null || values == null) return out;
        int n = Math.min(keys.size(), values.size());
        for (int i = 0; i < n; i++) {
            if (keys.get(i) != null) out.put(keys.get(i), values.get(i));
        }
        return out;
    }

    // ============================================================================
    // 解析：合取项 → 原子约束
    // ============================================================================

    private static void merge(Map<String, Builder> acc, Map<String, String> byNorm,
                              Map<String, ValueCmp> cmpByNorm, List<Term> terms) {
        for (Term t : terms) {
            String norm = normalizeIdent(t.column());
            String orig = byNorm.get(norm);
            if (orig == null) continue;               // 非分区列 → 忽略（不影响其它分区约束）
            acc.computeIfAbsent(orig, k -> new Builder(cmpByNorm.get(norm))).add(t);
        }
    }

    /**
     * {@code 分区列 BETWEEN 字面量 AND 字面量} → 两条范围界 + 「解析后剩下的部分」。
     *
     * <p>下界之后紧跟着的那个 AND 才是 BETWEEN 自己的（{@code dt BETWEEN 'a' AND 'b' AND x > 1}）；
     * 上界只取<b>一个</b>字面量，后面的内容属于外层合取，交回调用方继续解析，不能囫囵吞掉。</p>
     *
     * <p>上界不是字面量（{@code dt BETWEEN 'a' AND x}）时只保留下界 —— 无论上界是什么，
     * {@code dt >= 'a'} 都是必要条件，剪得保守但绝不剪错。</p>
     *
     * @return 认不出 BETWEEN 返回 {@code null}（调用方继续按顶层 AND 切分）
     */
    private static Between parseBetween(String s) {
        int at = indexOfTopLevelKeyword(s, "between");
        if (at <= 0) return null;
        String col = parseColumnRef(s.substring(0, at));
        if (col == null) return null;
        String rest = s.substring(at + "between".length()).trim();
        int and = indexOfTopLevelAnd(rest, 0);
        if (and < 0) return null;
        String lo = parseLiteral(rest.substring(0, and));
        if (lo == null) return null;                  // 下界不是字面量 → 整条不认识

        String upper = rest.substring(and + 3);
        Lit hi = parseLeadingLiteral(upper);
        if (hi == null) {
            return new Between(List.of(Term.range(col, Op.GE, lo)), upper);
        }
        return new Between(List.of(Term.range(col, Op.GE, lo), Term.range(col, Op.LE, hi.value())),
                upper.substring(hi.end()));
    }

    /** 从串首解析<b>一个</b>字面量（可能后面还跟着别的表达式）。 */
    private static Lit parseLeadingLiteral(String s) {
        String t = s.trim();
        if (t.isEmpty()) return null;
        int lead = s.indexOf(t);                      // 前导空白长度
        if (t.charAt(0) == '\'') {
            int end = skipQuoted(t, 0, '\'');
            if (end > t.length() || t.charAt(end - 1) != '\'') return null;   // 引号没闭合
            String val = parseLiteral(t.substring(0, end));
            return val == null ? null : new Lit(val, lead + end);
        }
        int i = 0;
        if (t.charAt(i) == '+' || t.charAt(i) == '-') i++;
        boolean digit = false;
        boolean dot = false;
        while (i < t.length()) {
            char ch = t.charAt(i);
            if (Character.isDigit(ch)) {
                digit = true;
                i++;
            } else if (ch == '.' && !dot) {
                dot = true;
                i++;
            } else {
                break;
            }
        }
        return digit ? new Lit(t.substring(0, i), lead + i) : null;
    }

    /** BETWEEN 的解析结果：范围界 + 尚未消费的剩余文本。 */
    private record Between(List<Term> terms, String rest) {
    }

    /** 串首字面量：值 + 它在原串中的结束下标。 */
    private record Lit(String value, int end) {
    }

    /** 单条比较：{@code =} / {@code != <>} / {@code < <= > >=} / {@code IN (…)}；认不出返回空。 */
    private static List<Term> parseConstraint(String raw) {
        String s = stripOuterParens(raw.trim());
        if (s.isEmpty()) return List.of();

        OpHit hit = findTopLevelComparison(s);
        if (hit != null) {
            String col = parseColumnRef(s.substring(0, hit.at()));
            if (col == null) return List.of();
            String lit = parseLiteral(s.substring(hit.at() + hit.op().length()));
            if (lit == null) return List.of();    // 右操作数不是字面量（列对列 / 函数 / 子查询）→ 不剪
            return switch (hit.op()) {
                case "=" -> List.of(Term.eq(col, Set.of(lit)));
                case "!=", "<>" -> List.of(Term.ne(col, Set.of(lit)));
                case "<" -> List.of(Term.range(col, Op.LT, lit));
                case "<=" -> List.of(Term.range(col, Op.LE, lit));
                case ">" -> List.of(Term.range(col, Op.GT, lit));
                case ">=" -> List.of(Term.range(col, Op.GE, lit));
                default -> List.of();
            };
        }

        int inPos = indexOfTopLevelKeyword(s, "in");
        if (inPos > 0) {
            String col = parseColumnRef(s.substring(0, inPos));
            if (col == null) return List.of();
            Set<String> vals = parseInList(s.substring(inPos + 2).trim());
            return vals == null ? List.of() : List.of(Term.eq(col, vals));
        }
        return List.of();
    }

    /** 解析 {@code ( lit, lit, … )}；右括号后仅允许空白，否则返回 null。 */
    private static Set<String> parseInList(String s) {
        if (s.isEmpty() || s.charAt(0) != '(') return null;
        int close = matchingCloseParen(s, 0);
        if (close < 0) return null;
        if (!s.substring(close + 1).trim().isEmpty()) return null;   // ')' 后有杂串 → 不敢认（可能是 OR …）
        String inner = s.substring(1, close);
        Set<String> vals = new LinkedHashSet<>();
        for (String item : splitOnTopLevelComma(inner)) {
            String lit = parseLiteral(item.trim());
            if (lit == null) return null;                            // 任一元素不是纯字面量 → 整个 IN 放弃
            vals.add(lit);
        }
        return vals.isEmpty() ? null : vals;
    }

    // ============================================================================
    // 按声明类型建值比较器（三值：null = 无法比较 → 不剪）
    // ============================================================================

    /**
     * 分区列类型 → 值比较器。返回 {@code null} = 该类型不支持范围比较（等值裁剪照旧）。
     *
     * <p>{@code boolean} / array / map / struct 等无「序」或本类不认识的类型一律返回 null ——
     * 宁可不剪。</p>
     */
    private static ValueCmp comparatorFor(String type) {
        if (type == null || type.isBlank()) return null;
        String t = type.trim().toLowerCase(Locale.ROOT);
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();     // 去掉 decimal(10,2) / varchar(64) 的参数
        return switch (t) {
            // Hive 的 string 比较就是字节序；ASCII（含日期串）下与 Java 的 code-unit 序一致
            case "string", "varchar", "char" -> (a, b) -> Integer.signum(a.compareTo(b));
            case "tinyint", "smallint", "int", "integer", "bigint", "float", "double", "decimal", "numeric"
                    -> PartitionPruner::compareNumeric;
            case "date" -> PartitionPruner::compareDate;
            case "timestamp" -> PartitionPruner::compareTimestamp;
            default -> null;
        };
    }

    /** 数值型分区列：两侧都解析成 {@link BigDecimal} 才比（解析不出 → null = 不剪）。 */
    private static Integer compareNumeric(String value, String literal) {
        try {
            return new BigDecimal(value.trim()).compareTo(new BigDecimal(literal.trim()));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer compareDate(String value, String literal) {
        LocalDate a = parseDate(value);
        LocalDate b = parseDate(literal);
        return (a == null || b == null) ? null : a.compareTo(b);
    }

    private static Integer compareTimestamp(String value, String literal) {
        LocalDateTime a = parseTimestamp(value);
        LocalDateTime b = parseTimestamp(literal);
        return (a == null || b == null) ? null : a.compareTo(b);
    }

    /** ISO 日期；分区值/字面量不是 {@code yyyy-MM-dd} 就返回 null（→ 不剪，而不是猜）。 */
    private static LocalDate parseDate(String s) {
        try {
            return LocalDate.parse(s.trim());
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /** {@code 2026-09-11 00:00:00} / {@code 2026-09-11 00:00} / {@code 2026-09-11}（含 {@code T} 分隔）。 */
    private static LocalDateTime parseTimestamp(String s) {
        String t = s.trim().replace('T', ' ');
        try {
            return LocalDateTime.parse(t, TS_FMT);
        } catch (DateTimeParseException ignored) {
            // 退到「只有日期」的写法
        }
        LocalDate d = parseDate(t);
        return d == null ? null : d.atStartOfDay();
    }

    private static final DateTimeFormatter TS_FMT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm")
            .optionalStart().appendPattern(":ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .optionalEnd()
            .toFormatter();

    // ============================================================================
    // 约束模型
    // ============================================================================

    /** 一个合取项解析出的原子约束：等值集 / 排除集 / 一个范围界，三者只居其一。 */
    private record Term(String column, Set<String> values, Set<String> excluded, Op op, String boundLit) {

        static Term eq(String column, Set<String> values) {
            return new Term(column, values, null, null, null);
        }

        static Term ne(String column, Set<String> values) {
            return new Term(column, null, values, null, null);
        }

        static Term range(String column, Op op, String literal) {
            return new Term(column, null, null, op, literal);
        }
    }

    /** 范围界的方向。 */
    private enum Op {
        LT("<"), LE("<="), GT(">"), GE(">=");

        private final String symbol;

        Op(String symbol) {
            this.symbol = symbol;
        }

        String symbol() {
            return symbol;
        }

        /** {@code cmp} = 分区值与字面量的比较结果；{@code null}（无法比较）⇒ 恒 true（不剪）。 */
        boolean holds(Integer cmp) {
            if (cmp == null) return true;
            return switch (this) {
                case LT -> cmp < 0;
                case LE -> cmp <= 0;
                case GT -> cmp > 0;
                case GE -> cmp >= 0;
            };
        }
    }

    private record Bound(Op op, String literal) {
    }

    /**
     * 三值比较：{@code null} = 无法比较（类型不支持 / 解析失败）⇒ 调用方必须「不剪」。
     */
    @FunctionalInterface
    private interface ValueCmp {
        Integer compare(String partitionValue, String literal);
    }

    /** 同一分区列上的多条原子约束取合取：等值集求交、排除集求并、范围界全部保留。 */
    private static final class Builder {

        private final ValueCmp cmp;
        private Set<String> allowed;
        private Set<String> excluded;
        private final List<Bound> bounds = new ArrayList<>();

        Builder(ValueCmp cmp) {
            this.cmp = cmp;
        }

        void add(Term t) {
            if (t.values() != null) {
                if (allowed == null) {
                    allowed = new LinkedHashSet<>(t.values());
                } else {
                    allowed.retainAll(t.values());            // 交集（可能变空 = 无可满足分区）
                }
            }
            if (t.excluded() != null) {
                if (excluded == null) {
                    excluded = new LinkedHashSet<>(t.excluded());
                } else {
                    excluded.addAll(t.excluded());
                }
            }
            // 类型不支持范围比较 → 丢掉这条（等价于「不剪」）；等值/排除约束照旧生效
            if (t.op() != null && cmp != null) {
                bounds.add(new Bound(t.op(), t.boundLit()));
            }
        }

        ColumnConstraint build() {
            return new ColumnConstraint(allowed, excluded, bounds, cmp);
        }
    }

    // ============================================================================
    // 列引用 / 字面量
    // ============================================================================

    /** 列引用：{@code dt} / {@code t.dt} / 带反引号或双引号的限定名；返回最后一段（去引号）。 */
    private static String parseColumnRef(String s) {
        s = s.trim();
        if (s.isEmpty()) return null;
        int dot = lastTopLevelDot(s);
        String last = dot >= 0 ? s.substring(dot + 1) : s;
        last = unquote(last.trim());
        if (last.isEmpty()) return null;
        char c0 = last.charAt(0);
        if (!(Character.isLetter(c0) || c0 == '_')) return null;
        for (int i = 1; i < last.length(); i++) {
            char ch = last.charAt(i);
            if (!(Character.isLetterOrDigit(ch) || ch == '_')) return null;
        }
        // 限定符本身也必须是合法段（简单校验，防把 `a + b` 当列名）
        if (dot >= 0 && parseColumnRef(s.substring(0, dot)) == null) return null;
        return last;
    }

    /** 字面量：单引号字符串（{@code ''} 转义还原）或数值（含正负号 / 小数）。 */
    private static String parseLiteral(String s) {
        s = s.trim();
        if (s.length() >= 2 && s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
            String inner = s.substring(1, s.length() - 1);
            if (inner.indexOf('\'') >= 0) {
                // 只接受 '' 转义；出现成对单引号则还原，否则保守放弃
                if (!inner.replace("''", "").contains("'")) return inner.replace("''", "'");
                return null;
            }
            return inner;
        }
        // 数值字面量
        int i = 0;
        if (i < s.length() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
        boolean digit = false;
        boolean dotSeen = false;
        for (; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isDigit(ch)) {
                digit = true;
            } else if (ch == '.' && !dotSeen) {
                dotSeen = true;
            } else {
                return null;
            }
        }
        return digit ? s : null;
    }

    // ============================================================================
    // 词法工具（顶层 = 括号深度 0 且不在引号内）
    // ============================================================================

    /** 第一个顶层比较运算符（{@code <= >= <> != = < >}，两字符的优先）；找不到返回 null。 */
    private static OpHit findTopLevelComparison(String s) {
        int depth = 0;
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = skipQuoted(s, i, '\'');
                continue;
            }
            if (c == '"' || c == '`') {
                i = skipQuoted(s, i, c);
                continue;
            }
            if (c == '(') {
                depth++;
                i++;
                continue;
            }
            if (c == ')') {
                if (depth > 0) depth--;
                i++;
                continue;
            }
            if (depth == 0) {
                char nx = (i + 1 < n) ? s.charAt(i + 1) : ' ';
                if (c == '<' && nx == '=') return new OpHit(i, "<=");
                if (c == '<' && nx == '>') return new OpHit(i, "<>");
                if (c == '>' && nx == '=') return new OpHit(i, ">=");
                if (c == '!' && nx == '=') return new OpHit(i, "!=");
                if (c == '=') return new OpHit(i, "=");
                if (c == '<') return new OpHit(i, "<");
                if (c == '>') return new OpHit(i, ">");
            }
            i++;
        }
        return null;
    }

    private record OpHit(int at, String op) {
    }

    /** 从 {@code from} 起的第一个顶层 {@code AND}（词边界）；找不到返回 -1。 */
    private static int indexOfTopLevelAnd(String s, int from) {
        int depth = 0;
        int i = from;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = skipQuoted(s, i, '\'');
                continue;
            }
            if (c == '"' || c == '`') {
                i = skipQuoted(s, i, c);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (depth > 0) depth--;
            } else if (depth == 0 && matchesKeyword(s, i, "and")) {
                return i;
            }
            i++;
        }
        return -1;
    }

    /** 按顶层 AND 切分（不切引号 / 括号内）。 */
    private static List<String> splitOnTopLevelAnd(String s) {
        List<String> parts = new ArrayList<>();
        int start = 0;
        while (true) {
            int at = indexOfTopLevelAnd(s, start);
            if (at < 0) {
                parts.add(s.substring(start));
                return parts;
            }
            parts.add(s.substring(start, at));
            start = at + 3;
        }
    }

    private static List<String> splitOnTopLevelComma(String s) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = skipQuoted(s, i, '\'');
                continue;
            }
            if (c == '"' || c == '`') {
                i = skipQuoted(s, i, c);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (depth > 0) depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
            i++;
        }
        parts.add(s.substring(start));
        return parts;
    }

    /** 顶层关键字（词边界，大小写不敏感）位置；找不到返回 -1。 */
    private static int indexOfTopLevelKeyword(String s, String kw) {
        int depth = 0;
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = skipQuoted(s, i, '\'');
                continue;
            }
            if (c == '"' || c == '`') {
                i = skipQuoted(s, i, c);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (depth > 0) depth--;
            } else if (depth == 0 && matchesKeyword(s, i, kw)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    /** 从 {@code i} 起是否匹配关键字 {@code kw}（大小写不敏感）且前后都是词边界。 */
    private static boolean matchesKeyword(String s, int i, String kw) {
        int end = i + kw.length();
        if (end > s.length()) return false;
        if (!s.regionMatches(true, i, kw, 0, kw.length())) return false;
        if (i > 0 && isIdentChar(s.charAt(i - 1))) return false;
        return end >= s.length() || !isIdentChar(s.charAt(end));
    }

    private static boolean isIdentChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private static int skipQuoted(String s, int i, char quote) {
        int n = s.length();
        int j = i + 1;
        while (j < n) {
            char c = s.charAt(j);
            if (c == '\\' && j + 1 < n) {
                j += 2;
                continue;
            }
            if (c == quote) {
                if (j + 1 < n && s.charAt(j + 1) == quote) {   // 双写转义
                    j += 2;
                    continue;
                }
                return j + 1;
            }
            j++;
        }
        return n;
    }

    private static int matchingCloseParen(String s, int open) {
        int depth = 0;
        int i = open;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = skipQuoted(s, i, '\'');
                continue;
            }
            if (c == '"' || c == '`') {
                i = skipQuoted(s, i, c);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
            i++;
        }
        return -1;
    }

    private static int lastTopLevelDot(String s) {
        int depth = 0;
        int last = -1;
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = skipQuoted(s, i, '\'');
                continue;
            }
            if (c == '"' || c == '`') {
                i = skipQuoted(s, i, c);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (depth > 0) depth--;
            } else if (c == '.' && depth == 0) {
                last = i;
            }
            i++;
        }
        return last;
    }

    private static String stripOuterParens(String s) {
        String cur = s.trim();
        while (cur.length() >= 2 && cur.charAt(0) == '(') {
            int close = matchingCloseParen(cur, 0);
            if (close != cur.length() - 1) break;
            cur = cur.substring(1, cur.length() - 1).trim();
        }
        return cur;
    }

    private static String unquote(String s) {
        if (s.length() >= 2) {
            char c0 = s.charAt(0);
            char c1 = s.charAt(s.length() - 1);
            if ((c0 == '`' || c0 == '"') && c1 == c0) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }

    private static String normalizeIdent(String s) {
        return unquote(s.trim()).toLowerCase(Locale.ROOT);
    }
}
