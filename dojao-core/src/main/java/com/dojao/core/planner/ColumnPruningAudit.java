package com.dojao.core.planner;

import com.dojao.core.operator.ScanColumnAware;
import com.dojao.core.operator.ScanColumnAware.ScanColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * 规划期<b>列裁剪审计</b>（保险丝）—— 遍历物理计划树，让每个 {@link ScanColumnAware} 算子自述
 * 「我扫了哪些基表、请求了哪些列」，与「<b>空列集 = 连接器语义读全表</b>」的静默降级约定对照，
 * 把「Planner 漏传列集」这类 bug 从<b>静默 10× I/O</b> 变成<b>规划期报错 / 告警</b>。
 *
 * <p>历史教训：{@code Planner.tryBuildChainedWindow} 构造 {@link com.dojao.core.operator.DistributedChainedOperator}
 * 时用了无列集重载 ⇒ 派发的 {@code ScanStep} 列集为空 ⇒ Hive 宽表全列读取，且不报任何错。本审计让这类
 * 漏传在规划期即暴露（该路径的兄弟 {@code tryBuildChainedAggregation} 早已传列集，属实现不一致）。</p>
 *
 * <p><b>判定口径</b>（{@link ScanColumn#violation()}）：仅当扫描被声明为 {@code derived=false}
 * 且 {@code optional=false}（即「要求必须带列集的基表扫描」）而列集为 {@code null}/空时判违规。
 * <ul>
 *   <li>{@code derived}（列由上游流水线喂入的派生源）→ 不判违规；</li>
 *   <li>{@code optional}（列随 {@code ReadRequest} 携带、空 = 连接器语义读全表 —— 如全局聚合
 *       {@code COUNT(*)} 本就无需扫列）→ 拿不到「应当裁剪」的证据，不判违规。</li>
 * </ul>
 * 因此审计<b>零误报</b>：只抓「显式列集字段被丢空」这一确定性缺陷。</p>
 *
 * <p><b>模式</b>（系统属性 {@link #PROPERTY}）：{@code warn}（默认，打印告警）/ {@code strict}
 * （违规即抛 {@link IllegalStateException}）/ {@code off}（关闭）。默认 warn 保证生产安全。</p>
 *
 * @author jiaxin
 */
public final class ColumnPruningAudit {

    /** 审计模式系统属性：{@code warn}（默认）/ {@code strict}（违规即抛）/ {@code off}（关闭）。 */
    public static final String PROPERTY = "dojao.columnPruning.audit";

    private ColumnPruningAudit() { }

    /** 一条违规：某算子的某基表扫描「要求有列却拿到空列集」（空 = 连接器读全表 ⇒ 静默 10× I/O）。 */
    public record Violation(String nodeName, String relation) {
        @Override
        public String toString() {
            return nodeName + " 扫描 " + relation
                    + " 的列集为空（应已列裁剪 ⇒ 疑似 Planner 漏传列集，将退化为读全表）";
        }
    }

    /** 深度优先遍历物理计划树，收集全部违规（只读，无副作用）。 */
    public static List<Violation> violations(PhysicalNode root) {
        List<Violation> out = new ArrayList<>();
        collect(root, out);
        return out;
    }

    private static void collect(PhysicalNode node, List<Violation> out) {
        if (node == null) return;
        if (node.operator() instanceof ScanColumnAware aware) {
            for (ScanColumn scan : aware.scanColumnSets()) {
                if (scan.violation()) {
                    out.add(new Violation(node.name(), scan.relation()));
                }
            }
        }
        for (PhysicalNode child : node.children()) {
            collect(child, out);
        }
    }

    /**
     * 按 {@link #PROPERTY} 模式执行审计：{@code off} 关闭 / {@code strict} 违规即抛 /
     * {@code warn}（默认）打印告警。无违规则静默返回。
     */
    public static void audit(PhysicalNode root) {
        String mode = System.getProperty(PROPERTY, "warn");
        if ("off".equalsIgnoreCase(mode)) return;
        List<Violation> violations = violations(root);
        if (violations.isEmpty()) return;
        StringBuilder sb = new StringBuilder("[列裁剪审计] 发现 ").append(violations.size())
                .append(" 处空列集基表扫描（连接器将读全表）：");
        for (Violation v : violations) {
            sb.append("\n  - ").append(v);
        }
        if ("strict".equalsIgnoreCase(mode)) {
            throw new IllegalStateException(sb.toString());
        }
        System.err.println(sb);
    }
}
