package com.dojao.core.resource;

import com.dojao.core.planner.SqlTextUtil;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 扫描表提取器 —— 从逻辑计划中提取全部被扫描的基表，供初始并行度决策做元数据探查。
 *
 * <p>沿 SELECT 的 fromRelations 下钻：普通表直接产出 {@link TableRef}；子查询（含 CTE / 视图）
 * 递归展开；INSERT / CTAS / CREATE VIEW 取其查询体；集合运算取两侧。表函数（TABLE(...)）与
 * VALUES 无基表，跳过。同表多次引用（join / 重复 CTE）去重。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class ScanTableExtractor {

    private ScanTableExtractor() {
    }

    /** 一次基表扫描：表引用 + 该 relation 上已下推的过滤谓词（连接器方言 SQL 文本；无过滤 → {@code null}）。 */
    public record ScannedTable(TableRef table, String filter) {
    }

    /** 提取逻辑计划中全部被扫描的基表（去重，保持出现顺序）。 */
    public static List<TableRef> extract(LogicalPlan plan) {
        return extractScans(plan).stream().map(ScannedTable::table).toList();
    }

    /**
     * 提取逻辑计划中全部被扫描的基表 + 各表 relation 上的<b>已下推过滤谓词</b>（同表去重，首次出现者保留）。
     *
     * <p>供监控的 Scan 进度条分母做<b>过滤感知</b>估算：分母反映过滤后行数而非整表量，
     * 否则 {@code WHERE id between 3 and 8} 这类把行数从百万级压到个位数的谓词会把进度条永远钉在 0。
     * 谓词此时已由 {@code Optimizer}（PredicatePushdown）落到 relation 上。</p>
     */
    public static List<ScannedTable> extractScans(LogicalPlan plan) {
        List<ScannedTable> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        collectScans(plan, out, seen);
        return out;
    }

    private static void collectScans(LogicalPlan plan, List<ScannedTable> out, Set<String> seen) {
        if (plan == null) {
            return;
        }
        if (plan instanceof LogicalPlan.Select sel) {
            for (LogicalPlan.ResolvedRelation rel : sel.fromRelations()) {
                if (rel.subquery().isPresent()) {
                    collectScans(rel.subquery().get(), out, seen);
                } else if (rel.table() != null && !rel.table().isBlank()) {
                    TableRef ref = new TableRef(rel.catalog(), rel.schema(), rel.table());
                    if (seen.add(ref.qualifiedName())) {
                        out.add(new ScannedTable(ref, renderFilter(rel)));
                    }
                }
            }
        } else if (plan instanceof LogicalPlan.Insert ins) {
            collectScans(ins.query(), out, seen);
        } else if (plan instanceof LogicalPlan.CreateTableAsSelect ctas) {
            collectScans(ctas.query(), out, seen);
        } else if (plan instanceof LogicalPlan.CreateView cv) {
            collectScans(cv.query(), out, seen);
        } else if (plan instanceof LogicalPlan.With w) {
            for (LogicalPlan.CteDef cte : w.ctes()) {
                collectScans(cte.query(), out, seen);
            }
            collectScans(w.query(), out, seen);
        } else if (plan instanceof LogicalPlan.SetOperation so) {
            collectScans(so.left(), out, seen);
            collectScans(so.right(), out, seen);
        } else if (plan instanceof LogicalPlan.Explain ex) {
            collectScans(ex.statement(), out, seen);
        }
        // Values / DDL / TableFunction 等：无基表扫描
    }

    /** relation 上的下推谓词 → 连接器方言 SQL 文本（去限定符，与 Planner 下推渲染同款）；无 → {@code null}。 */
    private static String renderFilter(LogicalPlan.ResolvedRelation rel) {
        return rel.filter().map(f -> SqlTextUtil.stripQualifiers(f.text())).orElse(null);
    }
}
