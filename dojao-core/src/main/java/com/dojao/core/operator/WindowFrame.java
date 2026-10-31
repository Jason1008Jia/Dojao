package com.dojao.core.operator;

import java.util.Locale;

/**
 * 窗口 ROWS 帧描述 —— 相对当前行的偏移区间 {@code [i - startBack, i - endBack]}（均在分区内）。
 *
 * <p>例如 SQL {@code SUM(x) OVER (ORDER BY t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)} 对应
 * {@code new WindowFrame(2, 0)}：帧包含当前行及其前两行，共 3 行。</p>
 *
 * <p><b>仅承载「无需未来行」的帧</b>：{@code startBack}/{@code endBack} 均为 {@code >= 0} 的有限回溯。
 * 运行/默认帧（{@code UNBOUNDED PRECEDING ~ CURRENT ROW}）用 {@code null} 表示，不使用本类型。
 * 结束边界落在 {@code FOLLOWING}（含 {@code UNBOUNDED FOLLOWING}）的帧需要未来行，
 * 与流式 + 溢写模型冲突，解析时直接拒绝。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/16
 */
public record WindowFrame(int startBack, int endBack) {

    /** 单个帧最多回溯的行数上限：防止超大 {@code PRECEDING} 常量把每个函数的环形缓冲撑爆。 */
    public static final int MAX_FRAME_ROWS = 1_000_000;

    public WindowFrame {
        if (startBack < 0 || endBack < 0) {
            throw new IllegalArgumentException("窗口帧偏移不能为负: startBack=" + startBack + " endBack=" + endBack);
        }
        if (startBack < endBack) {
            throw new IllegalArgumentException("窗口帧起始不能在结束之后: startBack=" + startBack + " endBack=" + endBack);
        }
    }

    /** 工厂：{@code startBack PRECEDING ~ endBack PRECEDING/CURRENT ROW}（endBack=0 即 CURRENT ROW）。 */
    public static WindowFrame rows(int startBack, int endBack) {
        return new WindowFrame(startBack, endBack);
    }

    /** 帧包含的行数（未截断时）。 */
    public int size() {
        return startBack - endBack + 1;
    }

    // ==================== 文本解析 ====================

    /**
     * 解析窗口帧子句文本（来自语义分析器捕获的 {@code windowFrame} 原文）。
     *
     * @return {@code null} 表示「运行/默认帧」（无需按帧计算，走原运行聚合路径）或命名窗口引用；
     *         否则返回需要按帧计算的 {@link WindowFrame}
     * @throws UnsupportedOperationException 帧形态不受支持（如 {@code FOLLOWING} 结束边界、非默认 {@code RANGE}、
     *                                       非整数偏移常量、超大 {@code PRECEDING}）
     */
    public static WindowFrame parse(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        String t = text.trim();
        // 命名窗口（WINDOW w AS (...)）的帧定义当前未解析 ⇒ 保持既有「运行帧」行为，不误判为报错。
        if (t.regionMatches(true, 0, "WINDOW_REF:", 0, "WINDOW_REF:".length())) {
            return null;
        }
        String[] p = t.toUpperCase(Locale.ROOT)
                .replace('(', ' ').replace(')', ' ')
                .trim().split("\\s+");
        if (p.length == 0) {
            return null;
        }
        if ("RANGE".equals(p[0])) {
            if (isDefaultRange(p)) {
                return null;   // RANGE UNBOUNDED PRECEDING [AND CURRENT ROW] 与默认运行帧等价
            }
            throw new UnsupportedOperationException("窗口帧: " + text + "（RANGE 帧暂不支持，请改用 ROWS）");
        }
        if (!"ROWS".equals(p[0])) {
            throw new UnsupportedOperationException("窗口帧: " + text);
        }

        Bound start;
        Bound end;
        if (p.length >= 2 && "BETWEEN".equals(p[1])) {
            int and = indexOf(p, "AND", 2);
            if (and < 0) {
                throw new UnsupportedOperationException("窗口帧: " + text + "（缺少 AND）");
            }
            start = parseBound(p, 2, and, text);
            end = parseBound(p, and + 1, p.length, text);
        } else {
            // ROWS <bound> ⇒ 等价于 ROWS BETWEEN <bound> AND CURRENT ROW
            start = parseBound(p, 1, p.length, text);
            end = new Bound(0, false, false);
        }
        return resolveFrame(start, end, text);
    }

    private static WindowFrame resolveFrame(Bound start, Bound end, String text) {
        if (start.following() || end.following()) {
            throw new UnsupportedOperationException(
                    "窗口帧: " + text + "（结束/起始边界为 FOLLOWING，需要未来行，当前流式算子不支持）");
        }
        if (start.unbounded()) {
            if (!end.unbounded() && end.n() == 0) {
                return null;   // UNBOUNDED PRECEDING ~ CURRENT ROW = 运行帧
            }
            throw new UnsupportedOperationException(
                    "窗口帧: " + text + "（UNBOUNDED PRECEDING 只支持结束于 CURRENT ROW）");
        }
        int startBack = start.n();
        int endBack = end.n();
        if (startBack < endBack) {
            throw new UnsupportedOperationException("窗口帧: " + text + "（起始边界在结束边界之后）");
        }
        if (startBack + 1 > MAX_FRAME_ROWS) {
            throw new UnsupportedOperationException(
                    "窗口帧: " + text + "（单帧回溯行数超过上限 " + MAX_FRAME_ROWS + "）");
        }
        return new WindowFrame(startBack, endBack);
    }

    private static Bound parseBound(String[] p, int from, int to, String text) {
        if (from >= to) {
            throw new UnsupportedOperationException("窗口帧: " + text + "（边界缺失）");
        }
        String first = p[from];
        if ("UNBOUNDED".equals(first)) {
            if (from + 1 < to && "PRECEDING".equals(p[from + 1])) {
                return new Bound(0, true, false);
            }
            if (from + 1 < to && "FOLLOWING".equals(p[from + 1])) {
                return new Bound(0, true, true);
            }
            throw new UnsupportedOperationException("窗口帧: " + text + "（UNBOUNDED 后须跟 PRECEDING/FOLLOWING）");
        }
        if ("CURRENT".equals(first)) {
            return new Bound(0, false, false);
        }
        int n;
        try {
            n = Integer.parseInt(first);
        } catch (NumberFormatException e) {
            throw new UnsupportedOperationException("窗口帧: " + text + "（偏移须为整数字面量）");
        }
        if (from + 1 >= to) {
            throw new UnsupportedOperationException("窗口帧: " + text + "（偏移后须跟 PRECEDING/FOLLOWING）");
        }
        String dir = p[from + 1];
        if ("PRECEDING".equals(dir)) {
            return new Bound(n, false, false);
        }
        if ("FOLLOWING".equals(dir)) {
            return new Bound(n, false, true);
        }
        throw new UnsupportedOperationException("窗口帧: " + text + "（偏移后须跟 PRECEDING/FOLLOWING）");
    }

    private static boolean isDefaultRange(String[] p) {
        String s = String.join(" ", p);
        return "RANGE UNBOUNDED PRECEDING".equals(s)
                || "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".equals(s);
    }

    private static int indexOf(String[] p, String token, int from) {
        for (int i = from; i < p.length; i++) {
            if (token.equals(p[i])) return i;
        }
        return -1;
    }

    /** 解析出的单个帧边界。{@code unbounded}/{@code following} 二选一或都不选（有限偏移/CURRENT ROW）。 */
    private record Bound(int n, boolean unbounded, boolean following) { }
}
