package com.dojao.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 异常信息渲染 —— 把「外壳无信息量」的包装异常还原成<b>一整条 cause 链</b>。
 *
 * <p>连接器 / 算子层惯于把底层异常包一层再抛，且外壳消息里不带真因：
 * {@code new RuntimeException("Failed to list schemas", e)}、{@code Operator TableSink[...] next failed}。
 * 调用方（Web UI 的目录树、客户端看到的查询错误）若只回 {@code e.getMessage()}，用户拿到的是一句
 * 「Failed to list schemas」——真因（{@code Access denied for user 'root'@'aliyun001'}、
 * {@code Unknown database 'metastore'}、{@code Communications link failure}、
 * {@code Table 'tmp1' already exists}）全在链.bottom，只能靠猜。</p>
 *
 * <p>统一在这里渲染：{@code A <- B <- C}，与 {@code MasterQueryFlightProducer} 的
 * {@code log.error(sql, e)} 打印的栈互为表里（日志给全栈，回给用户的给一行）。</p>
 */
public final class Errors {

    private Errors() {
    }

    /**
     * 渲染整条 cause 链：{@code java.lang.RuntimeException: Failed to list schemas <- java.sql.SQLException: Access denied ...}。
     * 相邻重复的 cause（包装层没加信息）只保留一次；{@code null} 入参返回 {@code "unknown"}。
     */
    public static String describe(Throwable t) {
        if (t == null) return "unknown";
        StringBuilder sb = new StringBuilder(t.toString());
        String prev = sb.toString();
        for (Throwable cause = t.getCause(); cause != null; cause = cause.getCause()) {
            String s = cause.toString();
            if (!s.equals(prev)) {
                sb.append(" <- ").append(s);
                prev = s;
            }
            if (cause.getCause() == cause) break;   // 自引用 cause（防御性）
        }
        return sb.toString();
    }

    /**
     * 渲染<b>完整多行堆栈</b>（顶层异常 + 各层 {@code Caused by:} + 全部栈帧）。
     *
     * <p>与 {@link #describe(Throwable)} 互补：{@code describe} 给「一行能贴进提示条」的 cause 链，
     * 本方法给「贴进可折叠面板、交给人或 AI 定位到具体代码行」的全量文本。格式即 JDK 惯用的
     * {@code printStackTrace} 输出，工具链（IDE / 日志分析 / LLM）最认这个。</p>
     *
     * <p>{@code null} 入参返回 {@code "unknown"}；输出恒以换行结尾。</p>
     */
    public static String stackTrace(Throwable t) {
        if (t == null) return "unknown";
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            t.printStackTrace(pw);
        }
        return sw.toString();
    }
}
