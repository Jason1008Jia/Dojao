package com.dojao.core.operator;

/**
 * GROUP BY 表达式派生列 —— 把 {@code GROUP BY <表达式>}（如 {@code year(created_at)} /
 * {@code floor(rating/10)} / {@code CASE WHEN ... END}）改写为预计算的派生列，表达式作为该列的
 * 逐行求值源，分组键引用派生列名聚合。
 *
 * <p>纯传输 DTO（可序列化，片段派发用），不引用任何运行期算子。字段：</p>
 * <ul>
 *   <li>{@code derivedColumn}：合成派生列名 —— 命中 SELECT 别名取别名，否则取表达式归一化文本
 *       （如 {@code year(created_at)}，小写去空白，与 {@code count(*)} 同款约定）。Worker 侧重建为列头；</li>
 *   <li>{@code exprText}：{@code OpaqueExpr} 原始 SQL 表达式文本（{@link
 *       com.dojao.core.operator.expr.ExprParser} 递归下降解析成 AST 后逐行求值）。</li>
 * </ul>
 *
 * <p><b>关键差异于 CASE 派生列</b>：group key 是<b>分区键</b>，派生列必须在 Worker <b>Producer 侧、
 * HashPartitionOperator 之前</b>物化（否则同值行散到不同分区 → 计数错误）；CASE 聚合参数派生列
 * （{@link CaseProjection}）只在 Consumer 本地聚合侧算即可。</p>
 *
 * @author jiaxin
 * @date 2026/9/6
 */
public record GroupKeyExpression(String derivedColumn, String exprText) {
}
