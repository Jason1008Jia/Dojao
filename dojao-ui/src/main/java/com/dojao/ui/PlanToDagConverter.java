package com.dojao.ui;

import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 将 LogicalPlan 转换为 Job/Stage 模型的 DAG（模拟分布式计算）。
 *
 * <p>规则：
 * <ul>
 *   <li>所有 Table Scan 属于 Stage 0，逐条声明为 "Stage 0: Scan 0", "Stage 0: Scan 1"...</li>
 *   <li>只有 Shuffle 操作（JOIN / GROUP BY / Window）才会切出新 Stage（从 Stage 1 开始递增）</li>
 *   <li>Filter / Where 合入 Scan 或就近的 Shuffle Stage 描述，不单独成 Stage</li>
 *   <li>CTE 定义内部的 Stage 作为独立节点，最终查询引用 CTE 输出而非重复建 Scan</li>
 * </ul>
 *
 * @author jiaxin
 * @version 4.0
 * @date 2026/7/5
 */
public final class PlanToDagConverter {

    private PlanToDagConverter() {}

    public static DagData convert(LogicalPlan plan) {
        List<DagData.Node> nodes = new ArrayList<>();
        List<DagData.Edge> edges = new ArrayList<>();
        AtomicInteger nodeIdCounter = new AtomicInteger(0);
        AtomicInteger scanCounter = new AtomicInteger(0);   // Stage 0 内 Scan 编号
        AtomicInteger stageNum = new AtomicInteger(1);      // Shuffle Stage 从 1 开始
        Map<String, String> cteOutputs = new LinkedHashMap<>();

        String jobLabel = buildJobLabel(plan);

        if (plan instanceof LogicalPlan.With w) {
            convertWith(w, nodes, edges, nodeIdCounter, scanCounter, stageNum, cteOutputs);
        } else if (plan instanceof LogicalPlan.Explain e) {
            if (e.statement() instanceof LogicalPlan.With w2) {
                convertWith(w2, nodes, edges, nodeIdCounter, scanCounter, stageNum, cteOutputs);
            } else {
                buildQuery(e.statement(), null, nodes, edges, nodeIdCounter,
                    scanCounter, stageNum, cteOutputs);
            }
        } else {
            buildQuery(plan, null, nodes, edges, nodeIdCounter,
                scanCounter, stageNum, cteOutputs);
        }

        return new DagData(jobLabel, nodes, edges);
    }

    // ==================== WITH / CTE ====================

    private static void convertWith(LogicalPlan.With w,
                                    List<DagData.Node> nodes, List<DagData.Edge> edges,
                                    AtomicInteger nodeIdCounter, AtomicInteger scanCounter,
                                    AtomicInteger stageNum, Map<String, String> cteOutputs) {
        // 1) 先构建每个 CTE 内部的 Stage 链，记录输出 Stage ID
        for (LogicalPlan.CteDef cte : w.ctes()) {
            String lastId = buildQuery(cte.query(), cte.name(), nodes, edges,
                nodeIdCounter, scanCounter, stageNum, cteOutputs);
            if (lastId != null) {
                cteOutputs.put(cte.name(), lastId);
            }
        }
        // 2) 最终查询：CTE 引用 → 复用已记录的 Stage ID
        buildQuery(w.query(), null, nodes, edges, nodeIdCounter,
            scanCounter, stageNum, cteOutputs);
    }

    // ==================== 构建单个查询 ====================

    /** 返回该查询最后一个 Stage 的节点 ID（供 CTE 输出追踪）。 */
    private static String buildQuery(LogicalPlan plan, String cteLabel,
                                     List<DagData.Node> nodes, List<DagData.Edge> edges,
                                     AtomicInteger nodeIdCounter, AtomicInteger scanCounter,
                                     AtomicInteger stageNum, Map<String, String> cteOutputs) {
        return switch (plan) {
            case LogicalPlan.Select s ->
                buildSelect(s, cteLabel, nodes, edges, nodeIdCounter, scanCounter, stageNum, cteOutputs);
            case LogicalPlan.Explain e ->
                buildQuery(e.statement(), cteLabel, nodes, edges, nodeIdCounter,
                    scanCounter, stageNum, cteOutputs);
            default -> null;
        };
    }

    private static String buildSelect(LogicalPlan.Select s, String cteLabel,
                                       List<DagData.Node> nodes, List<DagData.Edge> edges,
                                       AtomicInteger nodeIdCounter, AtomicInteger scanCounter,
                                       AtomicInteger stageNum, Map<String, String> cteOutputs) {
        List<LogicalPlan.JoinInfo> joins = s.joins();
        boolean hasJoin = !joins.isEmpty();
        boolean hasGroupBy = !s.groupBy().isEmpty();
        boolean hasWhere = s.where().isPresent() && !isNullExpr(s.where().get());
        int relationCount = s.fromRelations().size();

        // 窗口函数收集
        List<Expression.WindowFunction> windows = new ArrayList<>();
        for (LogicalPlan.SelectItem item : s.selectItems()) {
            if (item.expression() instanceof Expression.WindowFunction wf) {
                windows.add(wf);
            }
        }
        boolean hasWindow = !windows.isEmpty();
        boolean hasDownstream = hasJoin || hasGroupBy || hasWindow;

        // ============ 输入：Scan（Stage 0）或 CTE 引用 ============
        List<String> inputIds = new ArrayList<>();
        for (LogicalPlan.ResolvedRelation rel : s.fromRelations()) {
            // CTE 引用 → 复用 CTE 输出 Stage 节点
            if (rel.catalog().isEmpty() && cteOutputs.containsKey(rel.table())) {
                inputIds.add(cteOutputs.get(rel.table()));
                continue;
            }
            // 实体表 → Stage 0: Scan N
            StringBuilder sb = new StringBuilder();
            String alias = rel.alias().map(a -> " (" + a + ")").orElse("");
            String tableName = rel.catalog().isEmpty()
                ? rel.table()
                : rel.catalog() + "." + rel.schema() + "." + rel.table();
            sb.append("├── Scan: ").append(tableName).append(alias).append("\n");
            sb.append("│   └── columns: ").append(rel.columns()).append("\n");
            if (rel.filter().isPresent() && !isNullExpr(rel.filter().get())) {
                sb.append("│   └── pushdown filter: ")
                    .append(rel.filter().get().text()).append("\n");
            }

            boolean whereAbsorbed = false;
            if (relationCount == 1 && !hasDownstream && hasWhere) {
                sb.append("├── filter: ").append(s.where().get().text()).append("\n");
                whereAbsorbed = true;
            }
            if (relationCount == 1 && !hasDownstream) {
                appendProject(s, sb);
            }

            int si = scanCounter.getAndIncrement();
            String prefix = cteLabel != null ? "[" + cteLabel + "] " : "";
            String id = nextId(nodeIdCounter);
            nodes.add(new DagData.Node(id,
                prefix + "Stage 0: Scan " + si + "\n" + sb, "stage"));
            inputIds.add(id);
            if (whereAbsorbed) hasWhere = false;
        }

        // 无 Shuffle = 纯 Scan + Filter，输出即 Scan 节点本身
        String lastId = inputIds.isEmpty() ? null
            : inputIds.get(inputIds.size() - 1);
        List<String> prevIds = new ArrayList<>(inputIds);

        // 多表无 Join → Merge Stage（合并输出）
        if (inputIds.size() > 1 && !hasJoin && !hasGroupBy && !hasWindow) {
            StringBuilder sb = new StringBuilder();
            if (hasWhere) {
                sb.append("├── filter: ").append(s.where().get().text()).append("\n");
            }
            appendProject(s, sb);

            int sn = stageNum.getAndIncrement();
            String prefix = cteLabel != null ? "[" + cteLabel + "] " : "";
            String id = nextId(nodeIdCounter);
            nodes.add(new DagData.Node(id,
                prefix + "Stage " + sn + ": Result\n" + sb, "stage"));
            for (String pid : inputIds) edges.add(new DagData.Edge(pid, id));
            return id;
        }

        // ============ Stage: Shuffle Join ============
        if (hasJoin) {
            StringBuilder sb = new StringBuilder();
            for (LogicalPlan.JoinInfo join : joins) {
                sb.append("├── ").append(join.joinType())
                    .append(" JOIN ON ").append(join.condition().text()).append("\n");
                sb.append("│   └── left: ").append(join.leftAlias())
                    .append(", right: ").append(join.rightAlias()).append("\n");
            }
            if (hasWhere) {
                sb.append("├── filter: ").append(s.where().get().text()).append("\n");
                hasWhere = false;
            }
            if (!hasGroupBy && !hasWindow) {
                appendProject(s, sb);
            }

            int sn = stageNum.getAndIncrement();
            String prefix = cteLabel != null ? "[" + cteLabel + "] " : "";
            String id = nextId(nodeIdCounter);
            nodes.add(new DagData.Node(id,
                prefix + "Stage " + sn + ": Shuffle Join\n" + sb, "stage"));
            for (String pid : prevIds) edges.add(new DagData.Edge(pid, id));
            prevIds = List.of(id);
            lastId = id;
        }

        // ============ Stage: Shuffle Agg ============
        if (hasGroupBy) {
            String gbText = s.groupBy().stream()
                .map(Expression::text)
                .reduce((a, b) -> a + ", " + b).orElse("");
            StringBuilder sb = new StringBuilder();
            if (hasWhere) {
                sb.append("├── filter: ").append(s.where().get().text()).append("\n");
                hasWhere = false;
            }
            sb.append("├── GROUP BY ").append(gbText).append("\n");
            if (s.having().isPresent() && !isNullExpr(s.having().get())) {
                sb.append("├── HAVING ").append(s.having().get().text()).append("\n");
            }
            if (!hasWindow) {
                appendProject(s, sb);
            }

            int sn = stageNum.getAndIncrement();
            String prefix = cteLabel != null ? "[" + cteLabel + "] " : "";
            String id = nextId(nodeIdCounter);
            nodes.add(new DagData.Node(id,
                prefix + "Stage " + sn + ": Shuffle Agg\n" + sb, "stage"));
            for (String pid : prevIds) edges.add(new DagData.Edge(pid, id));
            prevIds = List.of(id);
            lastId = id;
        }

        // ============ Stage: Shuffle Window ============
        if (hasWindow) {
            StringBuilder sb = new StringBuilder();
            if (hasWhere) {
                sb.append("├── filter: ").append(s.where().get().text()).append("\n");
                hasWhere = false;
            }
            for (Expression.WindowFunction wf : windows) {
                sb.append("├── ").append(wf.functionName()).append("() OVER (");
                if (!wf.partitionBy().isEmpty()) {
                    sb.append("PARTITION BY ")
                        .append(String.join(", ", wf.partitionBy()));
                }
                if (!wf.orderBy().isEmpty()) {
                    if (!wf.partitionBy().isEmpty()) sb.append(" ");
                    sb.append("ORDER BY ").append(String.join(", ", wf.orderBy()));
                }
                sb.append(")\n");
            }
            appendProject(s, sb);

            int sn = stageNum.getAndIncrement();
            String prefix = cteLabel != null ? "[" + cteLabel + "] " : "";
            String id = nextId(nodeIdCounter);
            nodes.add(new DagData.Node(id,
                prefix + "Stage " + sn + ": Shuffle Window\n" + sb, "stage"));
            for (String pid : prevIds) edges.add(new DagData.Edge(pid, id));
            prevIds = List.of(id);
            lastId = id;
        }

        return lastId;
    }

    // ==================== 标签 ====================

    private static String buildJobLabel(LogicalPlan plan) {
        if (plan instanceof LogicalPlan.With w) {
            return buildJobLabel(w.query());
        }
        if (plan instanceof LogicalPlan.Explain e) {
            return buildJobLabel(e.statement());
        }
        if (plan instanceof LogicalPlan.Select s) {
            StringBuilder sb = new StringBuilder("Job: SELECT ");
            if (!s.selectItems().isEmpty()) {
                int show = Math.min(s.selectItems().size(), 4);
                for (int i = 0; i < show; i++) {
                    if (i > 0) sb.append(", ");
                    LogicalPlan.SelectItem item = s.selectItems().get(i);
                    String alias = item.alias().map(a -> " AS " + a).orElse("");
                    sb.append(item.expression().text()).append(alias);
                }
                if (s.selectItems().size() > show) sb.append(", ...");
            }
            return sb.toString();
        }
        return "Job: " + plan.getClass().getSimpleName();
    }

    // ==================== 辅助 ====================

    private static void appendProject(LogicalPlan.Select s, StringBuilder sb) {
        if (s.selectItems().isEmpty()) return;
        sb.append("│\n");
        int show = Math.min(s.selectItems().size(), 6);
        for (int i = 0; i < show; i++) {
            LogicalPlan.SelectItem item = s.selectItems().get(i);
            String alias = item.alias().map(a -> " AS " + a).orElse("");
            sb.append("├── ").append(item.expression().text())
                .append(alias).append("\n");
        }
        if (s.selectItems().size() > show) {
            sb.append("├── ...(+").append(s.selectItems().size() - show).append(" more)\n");
        }
    }

    private static String nextId(AtomicInteger counter) {
        return "n" + counter.getAndIncrement();
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
