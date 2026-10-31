package com.dojao.ui.monitor;

/**
 * SQL 异步提交响应 —— 用户从「SQL 页」提交一条查询后立即拿到的受理回执。
 *
 * <p>{@code queryId} 立即可用于轮询任务状态 / 取消；{@code message} 为受理信息
 * 或语法校验错误（受理失败时 {@code state}=REJECTED）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record MonitorSubmit(String queryId, String state, String message) {
}
