package com.dojao.grammar.optimize;

import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * 将 LogicalPlan 格式化为可读的树形文本 —— EXPLAIN 的输出核心。
 *
 * <p>输出示例：
 * <pre>
 * == Optimized Logical Plan ==
 * With
 * ├── CTE team_clean [team_code, team_name, ...]
 * │   └── Select
 * │       └── Scan: mysql.ball.nba_team_info [team_code, team_name, ...]
 * │           Filter (pushed): flag = 1 AND create_time IS NOT NULL
 * └── Select
 *     ├── Scan: team_clean (tc) [conference, division, ...]
 *     ├── Join: INNER ON tc.city_code = cs.city_code
 *     └── Filter: cs.city_team_count >= 2
 * </pre>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public final class PlanTextFormatter {

    private PlanTextFormatter() {}

    /** 格式化整个计划为树形文本。 */
    public static String format(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        sb.append("== Optimized Logical Plan ==\n");
        formatNode(sb, plan, "", true);
        return sb.toString();
    }

    /** 格式化计划节点（递归）。lastInParent 表示该节点是其父的最后一个子节点。 */
    private static void formatNode(StringBuilder sb, LogicalPlan plan,
                                    String prefix, boolean lastInParent) {
        if (plan == null) {
            sb.append(prefix).append("null\n");
            return;
        }

        switch (plan) {
            case LogicalPlan.With w -> formatWith(sb, w, prefix, lastInParent);
            case LogicalPlan.Select s -> formatSelect(sb, s, prefix, lastInParent);
            case LogicalPlan.Explain e -> formatExplain(sb, e, prefix, lastInParent);
            case LogicalPlan.Insert i -> formatInsert(sb, i, prefix, lastInParent);
            case LogicalPlan.SetOperation so -> formatSetOp(sb, so, prefix, lastInParent);
            case LogicalPlan.CreateTableAsSelect ctas -> formatCtasp(sb, ctas, prefix, lastInParent);
            case LogicalPlan.CreateView cv ->
                writeLine(sb, prefix, "CreateView: " + String.join(".", cv.qualifiedName()));
            case LogicalPlan.Values v -> writeLine(sb, prefix, "Values");
            case LogicalPlan.Unsupported u ->
                writeLine(sb, prefix, "Unsupported: " + u.statementType());
            default -> writeLine(sb, prefix, plan.getClass().getSimpleName());
        }
    }

    // ==================== With ====================

    private static void formatWith(StringBuilder sb, LogicalPlan.With w,
                                    String prefix, boolean lastInParent) {
        writeLine(sb, prefix, "With");
        String childPrefix = prefix + (lastInParent ? "    " : "│   ");
        List<LogicalPlan.CteDef> ctes = w.ctes();

        for (int i = 0; i < ctes.size(); i++) {
            LogicalPlan.CteDef cte = ctes.get(i);
            boolean lastCte = (i == ctes.size() - 1);
            String marker = lastCte ? "└── " : "├── ";

            writeLine(sb, childPrefix, marker + "CTE " + cte.name()
                + " " + cte.columns());

            // CTE 内部查询
            String cteChildPrefix = childPrefix + (lastCte ? "    " : "│   ");
            formatNode(sb, cte.query(), cteChildPrefix + "└── ", true);
        }

        // 主查询是 With 的最后一个子节点
        formatNode(sb, w.query(), childPrefix, true);
    }

    // ==================== Select ====================

    private static void formatSelect(StringBuilder sb, LogicalPlan.Select s,
                                      String prefix, boolean lastInParent) {
        writeLine(sb, prefix, "Select");

        String childPrefix = prefix + (lastInParent ? "    " : "│   ");

        // 收集所有子节点文本
        List<String> children = new ArrayList<>();

        // FROM / Scan
        for (LogicalPlan.ResolvedRelation rel : s.fromRelations()) {
            String alias = rel.alias().map(a -> " (" + a + ")").orElse("");
            String tableName = rel.catalog().isEmpty()
                ? rel.table()
                : rel.catalog() + "." + rel.schema() + "." + rel.table();
            children.add("Scan: " + tableName + alias + " " + rel.columns());

            // 下推的 filter（紧跟在对应的 Scan 后面，作为子项）
            if (rel.filter().isPresent() && !isNullExpr(rel.filter().get())) {
                children.add("  Filter (pushed): " + rel.filter().get().text());
            }
        }

        // JOIN
        for (LogicalPlan.JoinInfo join : s.joins()) {
            children.add("Join: " + join.joinType() + " ON " + join.condition().text());
        }

        // WHERE
        s.where().ifPresent(w -> {
            if (!isNullExpr(w)) {
                children.add("Filter: " + w.text());
            }
        });

        // GROUP BY
        if (!s.groupBy().isEmpty()) {
            children.add("GroupBy: " + s.groupBy().stream()
                .map(Expression::text)
                .reduce((a, b) -> a + ", " + b).orElse(""));
        }

        // HAVING
        s.having().ifPresent(h -> {
            if (!isNullExpr(h)) {
                children.add("Having: " + h.text());
            }
        });

        // Project (select items)
        if (!s.selectItems().isEmpty()) {
            List<String> itemStrs = new ArrayList<>();
            for (LogicalPlan.SelectItem item : s.selectItems()) {
                String alias = item.alias().map(a -> " AS " + a).orElse("");
                String exprText;
                if (item.expression() instanceof Expression.WindowFunction wf) {
                    exprText = formatWindowFunction(wf);
                } else {
                    exprText = item.expression().text();
                }
                itemStrs.add(exprText + alias);
            }
            children.add("Project: " + String.join(", ", itemStrs));
        }

        // 打印子节点
        for (int i = 0; i < children.size(); i++) {
            boolean last = (i == children.size() - 1);
            String marker = last ? "└── " : "├── ";
            writeLine(sb, childPrefix, marker + children.get(i));
        }
    }

    // ==================== Explain ====================

    private static void formatExplain(StringBuilder sb, LogicalPlan.Explain e,
                                       String prefix, boolean lastInParent) {
        StringBuilder header = new StringBuilder("Explain");
        if (e.analyze()) header.append(" ANALYZE");
        if (e.verbose()) header.append(" VERBOSE");
        writeLine(sb, prefix, header.toString());
        String childPrefix = prefix + (lastInParent ? "    " : "│   ");
        formatNode(sb, e.statement(), childPrefix + "└── ", true);
    }

    // ==================== Insert ====================

    private static void formatInsert(StringBuilder sb, LogicalPlan.Insert i,
                                      String prefix, boolean lastInParent) {
        String target = i.catalog() + "." + i.schema() + "." + i.table();
        writeLine(sb, prefix, "Insert: " + target);
        String childPrefix = prefix + (lastInParent ? "    " : "│   ");
        formatNode(sb, i.query(), childPrefix + "└── ", true);
    }

    // ==================== SetOperation ====================

    private static void formatSetOp(StringBuilder sb, LogicalPlan.SetOperation so,
                                     String prefix, boolean lastInParent) {
        String header = so.opType().name() + (so.distinct() ? " DISTINCT" : " ALL");
        writeLine(sb, prefix, header);
        String childPrefix = prefix + (lastInParent ? "    " : "│   ");
        formatNode(sb, so.left(), childPrefix + "├── ", false);
        formatNode(sb, so.right(), childPrefix + "└── ", true);
    }

    // ==================== CTAS ====================

    private static void formatCtasp(StringBuilder sb, LogicalPlan.CreateTableAsSelect ctas,
                                     String prefix, boolean lastInParent) {
        writeLine(sb, prefix, "CreateTableAsSelect: "
            + String.join(".", ctas.qualifiedName()));
        String childPrefix = prefix + (lastInParent ? "    " : "│   ");
        formatNode(sb, ctas.query(), childPrefix + "└── ", true);
    }

    // ==================== 窗口函数格式化 ====================

    private static String formatWindowFunction(Expression.WindowFunction wf) {
        StringBuilder s = new StringBuilder(wf.functionName()).append("(");
        s.append(String.join(", ", wf.args())).append(")");
        s.append(" OVER (");
        if (!wf.partitionBy().isEmpty()) {
            s.append("PARTITION BY ").append(String.join(", ", wf.partitionBy()));
        }
        if (!wf.orderBy().isEmpty()) {
            if (!wf.partitionBy().isEmpty()) s.append(" ");
            s.append("ORDER BY ").append(String.join(", ", wf.orderBy()));
        }
        wf.windowFrame().ifPresent(f -> s.append(" ").append(f));
        s.append(")");
        return s.toString();
    }

    // ==================== 工具方法 ====================

    private static void writeLine(StringBuilder sb, String prefix, String text) {
        sb.append(prefix).append(text).append("\n");
    }

    private static boolean isNullExpr(Expression expr) {
        if (expr == null) return true;
        if (expr instanceof Expression.OpaqueExpr oe) {
            String t = oe.originalText().trim();
            return t.isEmpty() || "null".equalsIgnoreCase(t);
        }
        return false;
    }
}
