package com.dojao.core.operator;

import java.util.List;

/**
 * CASE WHEN 派生列 —— 把聚合参数里的 {@code CASE WHEN ... END} / {@code IF(...)} 表达式改写为
 * 预计算的派生列，聚合改写为对派生列聚合（SUM / COUNT / MIN / MAX / AVG 统一）。
 *
 * <p>纯传输 DTO（可序列化，片段派发用），不引用任何运行期算子。字段：</p>
 * <ul>
 *   <li>{@code derivedColumn}：合成派生列名（如 {@code __case_0}），Worker 侧重建为列头；</li>
 *   <li>{@code branches}：按顺序求值的条件分支，每支 = 条件文本 + 结果值。条件支持
 *       {@code col op literal}（如 {@code rating >= 5}）或 {@code col IN (lit, ...)}；结果值支持
 *       数值字面量、列引用（如 {@code a.rating}，取该列逐行值）或算术/函数表达式
 *       （如 {@code rating-1}、{@code rating*2}，逐行求值）。
 *       <b>空列表</b>表示无条件的纯表达式派生列（如 {@code SUM(a+b)} / {@code SUM(price*0.8)}），
 *       此时每行结果恒取 {@code elseValue}。</li>
 *   <li>{@code elseValue}：兜底结果（数值字面量 / 列引用 / 表达式）；{@code null} 表示 NULL
 *       （无 ELSE 分支 → 全不匹配行为 NULL）。空 {@code branches} 时即整列的表达式源。</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/8/21
 */
public record CaseProjection(String derivedColumn, List<CaseBranch> branches, String elseValue) {

    /** 单个 CASE WHEN 分支：{@code WHEN <condition> THEN <value>}。 */
    public record CaseBranch(String condition, String value) {
    }
}
