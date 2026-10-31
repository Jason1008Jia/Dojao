package com.example.dojao.csvdir;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 连接器侧的极简谓词求值器 —— <b>用来兑现 {@code request.filterExpression()} 的契约</b>。
 *
 * <p><b>为什么这个类必须存在（这是写自定义连接器最容易踩的坑）。</b>连接器声明
 * {@link CsvDirConnector#supportsPredicatePushdown()} 为 {@code true} 之后，Dojao 的规划器会把能下推的
 * WHERE 谓词交给连接器，并且<b>把本地 Filter 算子消掉</b>（见 {@code Planner} 里
 * {@code rel.filter().ifPresent(f -> scan.pushdown(...))} 之后只有 residual 才补 Filter）。
 * 也就是说：<b>声明了能过滤，过滤结果就完全由连接器负责</b>。若"读到了但不理它"，查询不会报错，
 * 只会<b>静默多返回行</b> —— 这是最坏的一类错误。做不到过滤的连接器请把能力位返回 {@code false}
 * （默认值），引擎会自己保留 Filter。</p>
 *
 * <p><b>本示例支持</b>：顶层 {@code AND} 连接的若干条件，每个条件是
 * {@code 列 比较符 字面量}（{@code = != <> < <= > >=}）或 {@code 列 IS [NOT] NULL}。
 * <b>其余形态</b>（{@code OR} / {@code LIKE} / {@code IN} / {@code BETWEEN} / 表达式……）
 * 一律抛 {@link UnsupportedOperationException}：<b>宁可报错，不可漏过滤</b>。
 * 真实连接器按需扩展（多数数据库其实应该把谓词整体翻译成 SQL 交给自己数据源执行，
 * 而不是像这里一样在本地逐行求值）。</p>
 *
 * <p>比较语义：本示例把所有列都声明为 {@code STRING}，因此这里也按<b>字符串</b>比较
 * （{@code id > 5} 是字典序）。真实连接器应结合列真实类型做类型化比较。</p>
 *
 * @author 张三
 * @version 1.0
 * @date 2026/9/20
 */
final class SimpleFilter {

    /** 单个条件：列 + 比较符 + 字面量（{@code literal} 为 null 表示 IS NULL 形态）。 */
    private record Cond(String column, String op, String literal, boolean isNullTest) {

        boolean test(Map<String, String> row) {
            String value = row.get(column);
            if (isNullTest) {
                boolean isNull = value == null;
                return "IS NULL".equals(op) == isNull;
            }
            if (value == null) {
                return false;   // SQL 三值逻辑：与 NULL 的比较不成立
            }
            int cmp = value.compareTo(literal);
            return switch (op) {
                case "=" -> cmp == 0;
                case "!=", "<>" -> cmp != 0;
                case "<" -> cmp < 0;
                case "<=" -> cmp <= 0;
                case ">" -> cmp > 0;
                case ">=" -> cmp >= 0;
                default -> throw new IllegalStateException("未知比较符: " + op);
            };
        }
    }

    private final List<Cond> conditions;

    private SimpleFilter(List<Cond> conditions) {
        this.conditions = conditions;
    }

    /** 空过滤器（无谓词）。 */
    static SimpleFilter none() {
        return new SimpleFilter(List.of());
    }

    /**
     * 解析引擎下推的谓词表达式。无法解析的形态直接抛错 —— 见类注释：漏过滤比报错危险得多。
     */
    static SimpleFilter parse(String expression) {
        List<Cond> conditions = new ArrayList<>();
        for (String part : splitTopLevelAnd(expression)) {
            conditions.add(parseCondition(part));
        }
        return new SimpleFilter(conditions);
    }

    /** 逐行判断是否通过全部条件。 */
    boolean test(Map<String, String> row) {
        for (Cond c : conditions) {
            if (!c.test(row)) {
                return false;
            }
        }
        return true;
    }

    /** 是否没有条件 —— 调用方据此省掉"每行拼 Map"的开销。 */
    boolean isEmpty() {
        return conditions.isEmpty();
    }

    // ==================== 解析 ====================

    /** 按顶层 {@code AND} 切分（忽略括号内与引号内的 AND）。 */
    private static List<String> splitTopLevelAnd(String expression) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        char quote = 0;
        int start = 0;
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && isAndKeywordAt(expression, i)) {
                addPart(parts, expression.substring(start, i));
                i += 3;
                start = i + 1;
            }
        }
        addPart(parts, expression.substring(start));
        return parts;
    }

    private static void addPart(List<String> parts, String part) {
        String trimmed = part.trim();
        if (!trimmed.isEmpty()) {
            parts.add(trimmed);
        }
    }

    /** {@code i} 处是否为独立的 AND 关键字（词边界判断，避免把列名 brand 里的 and 当成连接词）。 */
    private static boolean isAndKeywordAt(String s, int i) {
        if (i + 3 > s.length() || !s.regionMatches(true, i, "AND", 0, 3)) {
            return false;
        }
        boolean leftOk = i == 0 || isBoundary(s.charAt(i - 1));
        boolean rightOk = i + 3 == s.length() || isBoundary(s.charAt(i + 3));
        return leftOk && rightOk;
    }

    private static boolean isBoundary(char c) {
        return Character.isWhitespace(c) || c == '(' || c == ')';
    }

    private static Cond parseCondition(String text) {
        String s = text.trim();
        // 去掉整体包裹的括号（引擎下推时可能带括号）
        while (s.startsWith("(") && s.endsWith(")") && balanced(s.substring(1, s.length() - 1))) {
            s = s.substring(1, s.length() - 1).trim();
        }
        String upper = s.toUpperCase(Locale.ROOT);

        int isIndex = upper.indexOf(" IS NULL");
        int isNotIndex = upper.indexOf(" IS NOT NULL");
        if (isNotIndex >= 0) {
            return new Cond(column(s.substring(0, isNotIndex)), "IS NOT NULL", null, true);
        }
        if (isIndex >= 0 && upper.endsWith("IS NULL")) {
            return new Cond(column(s.substring(0, isIndex)), "IS NULL", null, true);
        }

        // 比较符按「长的先试」避免 >= 被切成 >
        for (String op : List.of(">=", "<=", "<>", "!=", "=", ">", "<")) {
            int idx = s.indexOf(op);
            if (idx > 0) {
                String col = column(s.substring(0, idx));
                String literal = literal(s.substring(idx + op.length()).trim());
                return new Cond(col, op, literal, false);
            }
        }
        throw new UnsupportedOperationException(
                "示例连接器无法本地求值该谓词：[" + text + "]。"
                        + "可下推谓词必须被连接器兑现（引擎已消掉本地 Filter），"
                        + "不认识的形态请扩展本类，或让连接器拒绝该下推。");
    }

    /** 列名规范化：去掉限定符与反引号/双引号。 */
    private static String column(String raw) {
        String c = raw.trim();
        int dot = c.lastIndexOf('.');
        if (dot >= 0) {
            c = c.substring(dot + 1);
        }
        c = c.trim();
        if (c.length() >= 2 && ((c.startsWith("`") && c.endsWith("`"))
                || (c.startsWith("\"") && c.endsWith("\"")))) {
            c = c.substring(1, c.length() - 1);
        }
        if (c.isEmpty()) {
            throw new UnsupportedOperationException("谓词里没解析出列名: [" + raw + "]");
        }
        return c;
    }

    /** 字面量规范化：去掉外层单引号并还原 {@code ''} 转义。 */
    private static String literal(String raw) {
        String v = raw.trim();
        if (v.length() >= 2 && v.startsWith("'") && v.endsWith("'")) {
            return v.substring(1, v.length() - 1).replace("''", "'");
        }
        return v;
    }

    /** 判断去掉外层括号后是否仍然配平（避免把 {@code (a) OR (b)} 误当成整体括号）。 */
    private static boolean balanced(String inner) {
        int depth = 0;
        char quote = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"') quote = c;
            else if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth < 0) return false;
            }
        }
        return depth == 0;
    }
}
