package com.dojao.ui.monitor;

/**
 * 一个可用的 SQL 函数（内置 / 自定义），SQL 编辑器补全用。
 *
 * <p>中性 DTO：ui 层只依赖本记录，内置目录由 dojao-core 汇总、自定义来自 SQLite 元数据，
 * 两者都在 server 侧适配成此形状（契约留 ui、实现进 server 的分层约定，同 {@link MonitorColumn}）。</p>
 *
 * @param name    函数名（小写，SQL 大小写不敏感）
 * @param source  来源：{@code builtin} | {@code custom}
 * @param kind    类别：{@code scalar} | {@code aggregate} | {@code window}（自定义函数无此区分 → 空串）
 * @param backend 自定义函数的实现语言（{@code java} | {@code python}），内置函数为空串
 * @param detail  说明：内置取 {@code Function.display()}，自定义取函数注释（可能为空串）
 * @author jiaxin
 * @date 2026/9/29
 */
public record MonitorFunction(
        String name,
        String source,
        String kind,
        String backend,
        String detail) {
}
