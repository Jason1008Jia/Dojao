package com.dojao.core.operator;

import java.util.List;

/**
 * 携带<b>窗口分组链</b>的分布式算子 —— 步骤按执行顺序排列，末组即终端组
 * （其输出不再被任何后续窗口消费）。
 *
 * <p>Planner 的窗口过滤下推 / EXPLAIN 能力判断<b>只依赖本接口</b>，不再对具体算子
 * {@code instanceof} 分支。新增携带窗口的分布式算子只要实现本接口，就自动获得
 * 「外层 {@code WHERE 窗口函数列 op N}」的终端组下推能力，不会因为漏接分支而静默失效。</p>
 *
 * <p>安全边界：只有<b>终端组</b>可以承接外层过滤。中间组的输出还会喂给后续窗口组，
 * 提前删行会改变后续 {@code RANK} / 聚合结果 —— 该规则由
 * {@code Planner.terminalChainFilter} 单点实现。</p>
 *
 * @author jiaxin
 * @date 2026/11/4
 */
public interface WindowChainCarrier {

    /** 窗口分组链（顺序即执行顺序；空 = 不携带窗口）。 */
    List<FragmentSpec.WindowStep> windowSteps();

    /** 用新步骤替换整条窗口链（下推时只重建末组，其余原样）。 */
    void replaceWindowSteps(List<FragmentSpec.WindowStep> steps);
}
