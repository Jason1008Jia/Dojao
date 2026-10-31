package com.dojao.ui.monitor;

import java.util.List;

/**
 * 查询结果预览分页 —— 「SQL 页」结果表格一次拉取的一页数据。
 *
 * <p>{@code columns} 列名、{@code rows} 单元格均为字符串（前端直接填表）；
 * {@code truncated} 为 true 表示结果集超过预览上限被截断，前端提示「仅预览前 N 行」。
 * 查询未完成时 {@code state}=RUNNING（无 rows）；失败时 {@code state}=FAILED 且 error 非空。</p>
 *
 * <p>{@code sourceRows} = 扫描源表的行数<b>估计</b>（连接器元数据，非精确 count）：单表查询
 * 时就是「这个表有多少行」，供前端展示「共约 N 行，预览前 M 行」。取不到时为 0。</p>
 *
 * <p>{@code error} = 失败时的<b>一行</b> cause 链（{@code A <- B <- C}），贴在错误卡主区；
 * {@code errorDetail} = 失败时的<b>完整多行堆栈</b>（{@code Errors.stackTrace}），供前端折叠展开，
 * 便于人或 AI 定位到具体代码行。成功 / 运行中为 null。</p>
 *
 * <p>{@code wallMs} = 查询执行墙钟耗时（毫秒，从 Worker/引擎开始执行到结束，不含排队与前端轮询间隔）；
 * 未开始 / 未知 / 非本控制台提交时为 0。AI 的 {@code execute_sql(includeTiming=true)} 用它做性能评估。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record MonitorResult(
        String queryId,
        String state,
        List<String> columns,
        List<List<String>> rows,
        long totalRows,
        long sourceRows,
        boolean truncated,
        String error,
        String errorDetail,
        long wallMs) {
}
