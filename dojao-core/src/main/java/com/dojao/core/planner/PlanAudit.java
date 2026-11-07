package com.dojao.core.planner;

import com.dojao.core.operator.DistributedAggregateWindowOperator;
import com.dojao.core.operator.DistributedAggregationOperator;
import com.dojao.core.operator.DistributedChainedOperator;
import com.dojao.core.operator.DistributedDerivedJoinOperator;
import com.dojao.core.operator.DistributedJoinAggregationOperator;
import com.dojao.core.operator.DistributedJoinOperator;
import com.dojao.core.operator.FragmentSpec;
import com.dojao.core.operator.HashAggregationOperator;
import com.dojao.core.operator.WindowChainCarrier;
import com.dojao.core.operator.WindowFunctionOperator;
import com.dojao.core.operator.WindowFunctionSpec;
import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * 规划期<b>计划完整性审计</b>（保险丝）—— 把「逻辑计划里有窗口，物理计划里却少了」这类
 * <b>静默丢阶段</b>从线上错误变成规划期可见。
 *
 * <p>病根背景：分布式计划靠「形状匹配 + 从松散字段重建算子/DTO」拼出来，重建时静默丢掉
 * 别的阶段（如 {@code WindowStep} 拆前 4 字段重建丢 filter/computed/outputColumns；
 * 载体算子从 {@code links} 重建丢 sources/aliases）。这类丢失在运行期表现为静默 NULL / 少行，
 * 极难定位。本审计对逻辑与物理两侧的窗口函数计数做逐项比对，并校验窗口阶段自身的完整性，
 * 让「丢一组窗口」在规划期直接暴露。</p>
 *
 * <p><b>判定口径</b>：</p>
 * <ul>
 *   <li>{@link #windowLoss}：逻辑计划的窗口函数数 &gt; 物理计划承载的窗口函数数 ⇒ 违规
 *       （递归进入子查询 / CTE 体 / 集合运算两侧；同一 {@link LogicalPlan} 实例只计一次，
 *       避免 CTE 多引用被重复计数）；</li>
 *   <li>{@link #stageIntegrity}：任何 {@link WindowChainCarrier} 的阶段必须「非空函数、
 *       {@code numPartitions >= 1}、{@code outputColumns} 包含本阶段函数输出列」——这是
 *       字段级重建丢负载的直接信号；</li>
 *   <li>{@link #aggregateDeficit}：逻辑计划的<b>聚合个数</b>不能多于物理计划承载的聚合阶段数
 *       （{@link DistributedAggregationOperator} 主/副聚合、
 *       {@link DistributedJoinAggregationOperator}、
 *       {@link DistributedJoinOperator}/{@link DistributedChainedOperator} 的 followUp、
 *       {@link DistributedAggregateWindowOperator}、
 *       {@link DistributedDerivedJoinOperator}（内层聚合 + 两级聚合的外层聚合）、
 *       {@link HashAggregationOperator}；partial merge 与上游是同一逻辑聚合，不重复计数；
 *       终端二级聚合 {@code terminalPartialAggregate} 则是独立阶段，单独计数）。
 *       「多表 join 之上的聚合被外层提取器顶掉」的静默 NULL 正是内层聚合阶段整体消失。</li>
 * </ul>
 *
 * <p><b>模式</b>（系统属性 {@link #PROPERTY}）：{@code warn}（默认，打印告警）/
 * {@code strict}（违规即抛 {@link IllegalStateException}）/ {@code off}（关闭）。
 * 开启 {@code -Dplanner.distributed.strict=true} 时等同 strict（与集中回退开关一致）。</p>
 *
 * @author jiaxin
 * @date 2026/11/5
 */
public final class PlanAudit {

    /** 审计模式系统属性：{@code warn}（默认）/ {@code strict}（违规即抛）/ {@code off}（关闭）。 */
    public static final String PROPERTY = "dojao.plan.audit";

    /** 分布式严格开关：开启后任何回退 / 审计违规都直接抛错（CI / 探针用）。 */
    public static final String STRICT_PROPERTY = "planner.distributed.strict";

    private PlanAudit() { }

    /** 一条审计违规。 */
    public record Violation(String kind, String node, String detail) {
        @Override
        public String toString() {
            return "[" + kind + "] " + node + "：" + detail;
        }
    }

    /** 逻辑计划中窗口函数总数（递归子查询 / CTE 体 / 集合运算；按实例去重）。 */
    public static int expectedWindowFunctions(LogicalPlan plan) {
        return expectedWindowFunctions(plan, Map.of());
    }

    /**
     * @param skip 不参与计数的查询体（<b>物化 CTE</b>：其物理计划独立于主计划树、惰性规划，
     *             从主根无法统计 —— 跳过以免误报；非物化 CTE 正常递归计数）
     */
    public static int expectedWindowFunctions(LogicalPlan plan, Map<LogicalPlan, ?> skip) {
        return expectedWindowFunctions(plan, skip, new IdentityHashMap<>());
    }

    private static int expectedWindowFunctions(LogicalPlan plan, Map<LogicalPlan, ?> skip,
                                               Map<LogicalPlan, Boolean> seen) {
        if (plan == null || skip.containsKey(plan) || seen.put(plan, Boolean.TRUE) != null) return 0;
        if (plan instanceof LogicalPlan.Select s) {
            int n = 0;
            for (LogicalPlan.SelectItem item : s.selectItems()) {
                if (item.expression() instanceof Expression.WindowFunction) n++;
                n += item.windowCalls().size();
            }
            for (LogicalPlan.ResolvedRelation rel : s.fromRelations()) {
                if (rel.subquery().isPresent()) {
                    n += expectedWindowFunctions(rel.subquery().get(), skip, seen);
                }
            }
            for (Expression.ScalarSubquery sq : s.scalarSubqueries()) {
                n += expectedWindowFunctions(sq.inner(), skip, seen);
            }
            return n;
        }
        if (plan instanceof LogicalPlan.SetOperation so) {
            return expectedWindowFunctions(so.left(), skip, seen)
                    + expectedWindowFunctions(so.right(), skip, seen);
        }
        if (plan instanceof LogicalPlan.Insert ins) {
            return expectedWindowFunctions(ins.query(), skip, seen);
        }
        if (plan instanceof LogicalPlan.CreateTableAsSelect ctas) {
            return expectedWindowFunctions(ctas.query(), skip, seen);
        }
        if (plan instanceof LogicalPlan.CreateView cv) {
            return expectedWindowFunctions(cv.query(), skip, seen);
        }
        if (plan instanceof LogicalPlan.Explain e) {
            return expectedWindowFunctions(e.statement(), skip, seen);
        }
        return 0;
    }

    /** 物理计划实际承载的窗口函数总数（分布式窗口链 + 单机 WindowFunctionOperator）。 */
    public static int physicalWindowFunctions(PhysicalNode root) {
        if (root == null) return 0;
        int n = 0;
        if (root.operator() instanceof WindowChainCarrier carrier) {
            for (FragmentSpec.WindowStep step : carrier.windowSteps()) {
                n += step.functions().size();
            }
        } else if (root.operator() instanceof WindowFunctionOperator wfo) {
            n += wfo.getFunctionSpecs().size();
        }
        for (PhysicalNode child : root.children()) {
            n += physicalWindowFunctions(child);
        }
        return n;
    }

    /** 逻辑侧有、物理侧少的窗口函数数（&gt; 0 = 有窗口阶段被静默丢弃）。 */
    public static int windowLoss(LogicalPlan logical, PhysicalNode root) {
        return windowLoss(logical, root, Map.of());
    }

    public static int windowLoss(LogicalPlan logical, PhysicalNode root, Map<LogicalPlan, ?> skip) {
        return Math.max(0, expectedWindowFunctions(logical, skip) - physicalWindowFunctions(root));
    }

    /**
     * 物理计划承载的<b>聚合函数规格个数</b>（与逻辑侧 {@code AggregationSpec} 计数同口径）。
     *
     * <p>计数单位是「聚合规格」而不是算子：一个聚合阶段可含多个函数（{@code count(*), sum(x)}），
     * 一个算子也可承载两段（{@link DistributedAggregationOperator} 主聚合 + follow-up）。
     * {@link PartialAggregationMergeOperator} 是上游分布式聚合的 final 段，与上游是
     * <b>同一逻辑聚合</b>，不重复计数；反之，{@link DistributedDerivedJoinOperator} 的
     * {@code outerAggregations}（两级聚合派生体）与载体终端
     * {@code terminalPartialAggregate}（窗口链 / join 之后下沉的二次聚合）都是<b>独立阶段</b>，
     * 必须计入，否则守恒检查会误报缺聚合。</p>
     */
    public static int physicalAggregateCount(PhysicalNode root) {
        if (root == null) return 0;
        int n = 0;
        Object op = root.operator();
        if (op instanceof DistributedAggregationOperator dao) {
            n += dao.aggregations().size();
            if (dao.followUp() != null) n += dao.followUp().aggregations().size();
        } else if (op instanceof DistributedJoinAggregationOperator djao) {
            n += djao.aggregations().size();
            if (djao.followUp() != null) n += djao.followUp().aggregations().size();
            n += derivedSourceAggregates(djao.sources());
        } else if (op instanceof DistributedJoinOperator djo) {
            n += aggregateStepSize(djo.followUp());
            if (djo.partialFollowUp() != null) n += djo.partialFollowUp().aggregations().size();
            n += derivedSourceAggregates(djo.sources());
        } else if (op instanceof DistributedChainedOperator dco) {
            n += aggregateStepSize(dco.followUp());
            if (dco.partialFollowUp() != null) n += dco.partialFollowUp().aggregations().size();
        } else if (op instanceof DistributedAggregateWindowOperator dawo) {
            n += dawo.aggregations().size();
        } else if (op instanceof DistributedDerivedJoinOperator ddjo) {
            n += ddjo.aggregations().size();
            n += ddjo.outerAggregations().size();   // 两级聚合派生体：外层聚合也是独立阶段
        } else if (op instanceof HashAggregationOperator hao) {
            n += hao.getAggregations().size();
        }
        // 窗口链终端二级聚合（外层全局聚合下沉，与上述载体自身阶段互斥叠加）。
        if (op instanceof com.dojao.core.operator.AbstractDistributedOperator ado
                && ado.terminalPartialAggregate() != null) {
            n += ado.terminalPartialAggregate().aggregations().size();
        }
        for (PhysicalNode child : root.children()) {
            n += physicalAggregateCount(child);
        }
        return n;
    }

    private static int aggregateStepSize(FragmentSpec.FollowUp followUp) {
        return followUp instanceof FragmentSpec.AggregateStep step
                ? step.aggregations().size() : 0;
    }

    /**
     * 派生源（join 侧的子查询聚合体）承载的聚合规格数 —— 每个派生源都是<b>独立的</b>逻辑聚合
     * 阶段（{@code FROM t, (SELECT k, sum(v) … GROUP BY k) d}），与载体自身的 followUp /
     * 外层聚合互不重叠。漏计会让「join + 派生聚合源」在 strict 审计下误报缺聚合。
     */
    private static int derivedSourceAggregates(List<StageScheduler.DerivedSource> sources) {
        if (sources == null) return 0;
        int n = 0;
        for (StageScheduler.DerivedSource source : sources) {
            if (source != null && source.aggregations() != null) {
                n += source.aggregations().size();
            }
        }
        return n;
    }

    /**
     * 逻辑侧聚合数 - 物理侧聚合阶段数（{@code > 0 = 有聚合阶段被静默丢弃}）。
     *
     * <p>multi-table join + 聚合的静默错误（外层 {@code sum(alias)} 恒 NULL、
     * {@code count(*)} 退化成明细行数）就是「内层聚合阶段整体消失」—— 阶段数守恒是最直接的指纹。</p>
     */
    public static int aggregateDeficit(LogicalPlan logical, PhysicalNode root) {
        return aggregateDeficit(logical, root, Map.of());
    }

    public static int aggregateDeficit(LogicalPlan logical, PhysicalNode root,
                                       Map<LogicalPlan, ?> skip) {
        return Math.max(0, Planner.logicalAggregateCount(logical, skip)
                - physicalAggregateCount(root));
    }

    /** 深度优先遍历，收集全部违规（只读，无副作用）。 */
    public static List<Violation> violations(LogicalPlan logical, PhysicalNode root) {
        return violations(logical, root, Map.of());
    }

    public static List<Violation> violations(LogicalPlan logical, PhysicalNode root,
                                             Map<LogicalPlan, ?> skip) {
        List<Violation> out = new ArrayList<>();
        int expected = expectedWindowFunctions(logical, skip);
        int actual = physicalWindowFunctions(root);
        if (expected > actual) {
            out.add(new Violation("窗口丢失", "物理计划",
                    "逻辑计划有 " + expected + " 个窗口函数，物理计划仅承载 " + actual
                            + " 个 —— 有窗口阶段被重建时丢弃"));
        }
        int aggregateExpected = Planner.logicalAggregateCount(logical, skip);
        int aggregateActual = physicalAggregateCount(root);
        if (aggregateExpected > aggregateActual) {
            out.add(new Violation("聚合丢失", "物理计划",
                    "逻辑计划有 " + aggregateExpected + " 个聚合，物理计划仅承载 "
                            + aggregateActual + " 个聚合阶段 —— 有聚合阶段被重建时丢弃"));
        }
        stageIntegrity(root, out);
        return out;
    }

    /** 窗口阶段完整性：函数非空 / 分区数合法 / 输出列覆盖函数输出。 */
    private static void stageIntegrity(PhysicalNode node, List<Violation> out) {
        if (node == null) return;
        if (node.operator() instanceof WindowChainCarrier carrier) {
            // 注意：载体算子（DistributedJoin / Chained …）恒实现本接口，但<不携带窗口>时
            // steps 为空是合法状态（它就是普通 join / 聚合链）—— 只有非空阶段才校验字段。
            List<FragmentSpec.WindowStep> steps = carrier.windowSteps();
            if (steps != null) {
                for (int i = 0; i < steps.size(); i++) {
                    FragmentSpec.WindowStep step = steps.get(i);
                    String label = node.name() + "#w" + i;
                    if (step.functions().isEmpty()) {
                        out.add(new Violation("窗口阶段", label, "窗口步骤无函数"));
                    }
                    if (step.numPartitions() < 1) {
                        out.add(new Violation("窗口阶段", label,
                                "numPartitions=" + step.numPartitions() + "（应 >= 1）"));
                    }
                    if (!step.outputColumns().isEmpty()) {
                        for (WindowFunctionSpec fn : step.functions()) {
                            if (fn.outputColumn() != null
                                    && !step.outputColumns().contains(fn.outputColumn())) {
                                out.add(new Violation("窗口阶段", label,
                                        "outputColumns 缺窗口输出列 " + fn.outputColumn()
                                                + "（下游会找不到列）"));
                            }
                        }
                    }
                }
            }
        }
        for (PhysicalNode child : node.children()) {
            stageIntegrity(child, out);
        }
    }

    /**
     * 按 {@link #PROPERTY} 模式执行审计：{@code off} 关闭 / {@code strict}（或
     * {@code planner.distributed.strict=true}）违规即抛 / {@code warn}（默认）打印告警。
     */
    public static void audit(LogicalPlan logical, PhysicalNode root) {
        audit(logical, root, Map.of());
    }

    public static void audit(LogicalPlan logical, PhysicalNode root, Map<LogicalPlan, ?> skip) {
        String mode = System.getProperty(PROPERTY, "warn");
        if ("off".equalsIgnoreCase(mode)) return;
        List<Violation> violations = violations(logical, root, skip);
        if (violations.isEmpty()) return;
        StringBuilder sb = new StringBuilder("[plan-audit] ").append(violations.size())
                .append(" violation(s):");
        for (Violation v : violations) {
            sb.append("\n  - ").append(v);
        }
        if ("strict".equalsIgnoreCase(mode) || Boolean.getBoolean(STRICT_PROPERTY)) {
            throw new IllegalStateException(sb.toString());
        }
        System.err.println(sb);
    }
}
