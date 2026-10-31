package com.dojao.ui.ai;

import com.dojao.ui.ai.AiExportFormats.ChartData;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Arc2D;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 把 {@link ChartData}（柱状 / 折线 / 面积 / 饼图 / 散点，可混合、可双 Y 轴）用 Java2D 画成位图，
 * 以及把文本 / 表格内容排版成 A4 位图页面（供 {@link AiPdfWriter} 装进 PDF）。
 *
 * <p>刻意<b>不引入任何第三方依赖</b>：只用 JDK 自带的 {@code java.awt} / {@code javax.imageio}，
 * 离线构建与分发包体积不受影响。所有方法都是纯函数（无共享可变状态），可在无头环境运行。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/18
 */
final class AiChartRenderer {

    private AiChartRenderer() {
    }

    /** 图表图片默认尺寸（PNG / JPG 导出）。 */
    static final int CHART_WIDTH = 1200;
    static final int CHART_HEIGHT = 720;

    /** PDF 页面像素尺寸（A4 @150dpi）。 */
    static final int PDF_WIDTH = 1240;
    static final int PDF_HEIGHT = 1754;
    /** PDF 横向页面尺寸（图表页）。 */
    static final int PDF_LAND_WIDTH = 1754;
    static final int PDF_LAND_HEIGHT = 1240;

    private static final Color TEXT = new Color(0x1F2937);
    private static final Color MUTED = new Color(0x6B7280);
    private static final Color GRID = new Color(0xE5E7EB);
    private static final Color AXIS = new Color(0x9CA3AF);
    private static final Color BORDER = new Color(0xD1D5DB);
    private static final Color HEADER_BG = new Color(0xF3F4F6);

    private static final Color[] PALETTE = {
            new Color(0x4E79A7), new Color(0xF28E2B), new Color(0xE15759),
            new Color(0x76B7B2), new Color(0x59A14F), new Color(0xEDC948),
            new Color(0xB07AA1), new Color(0xFF9DA7)
    };

    private static final Font TITLE_FONT = new Font(Font.SANS_SERIF, Font.BOLD, 24);
    private static final Font LEGEND_FONT = new Font(Font.SANS_SERIF, Font.PLAIN, 14);
    private static final Font AXIS_FONT = new Font(Font.SANS_SERIF, Font.PLAIN, 13);
    private static final Font DOC_TITLE_FONT = new Font(Font.SANS_SERIF, Font.BOLD, 30);
    private static final Font DOC_BODY_FONT = new Font(Font.SANS_SERIF, Font.PLAIN, 16);
    private static final Font TABLE_FONT = new Font(Font.SANS_SERIF, Font.PLAIN, 14);
    private static final Font TABLE_HEADER_FONT = new Font(Font.SANS_SERIF, Font.BOLD, 14);

    private static final int LINE_H = 23;
    private static final int TABLE_LINE_H = 20;
    private static final int MARGIN = 64;

    // ==================== 图表 ====================

    /** 画一张图表（默认 1200×720）。 */
    static BufferedImage renderChart(ChartData chart) {
        return renderChart(chart, CHART_WIDTH, CHART_HEIGHT);
    }

    /** 画一张图表到指定尺寸（RGB，白底）。 */
    static BufferedImage renderChart(ChartData chart, int width, int height) {
        int w = Math.max(480, width);
        int h = Math.max(320, height);
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        try {
            setup(g);
            g.setColor(Color.WHITE);
            g.fillRect(0, 0, w, h);
            if (chart == null) {
                return img;
            }
            if ("pie".equals(chart.type())) {
                drawPie(g, chart, w, h);
            } else {
                drawCartesian(g, chart, w, h);
            }
        } finally {
            g.dispose();
        }
        return img;
    }

    private static void drawCartesian(Graphics2D g, ChartData chart, int w, int h) {
        List<String> cats = chart.categories() == null ? List.of() : chart.categories();
        List<ChartData.Series> series = chart.series() == null ? List.of() : chart.series();
        int n = cats.size();

        int top = 24;
        if (chart.title() != null && !chart.title().isBlank()) {
            g.setFont(TITLE_FONT);
            g.setColor(TEXT);
            drawCentered(g, chart.title().strip(), w / 2, top + 20, TITLE_FONT);
            top += 42;
        }
        top = drawLegend(g, series, w, top + 4);

        boolean dual = chart.dualAxis() || series.stream().anyMatch(ChartData.Series::y2);
        int left = 86;
        int right = dual ? 86 : 34;
        int bottom = 64;
        int plotTop = top + 14;
        int plotW = Math.max(80, w - left - right);
        int plotH = Math.max(60, h - plotTop - bottom);

        double[] primary = axisRange(series, false);
        double[] secondary = dual ? axisRange(series, true) : new double[]{0, 1};

        // 网格 + 主轴刻度
        g.setFont(AXIS_FONT);
        FontMetrics fm = g.getFontMetrics();
        int ticks = 5;
        for (int t = 0; t <= ticks; t++) {
            double v = primary[0] + (primary[1] - primary[0]) * t / ticks;
            int y = valueY(v, primary, plotTop, plotH);
            g.setColor(GRID);
            g.drawLine(left, y, left + plotW, y);
            g.setColor(MUTED);
            String label = num(v);
            g.drawString(label, left - 10 - fm.stringWidth(label), y + fm.getAscent() / 2 - 2);
        }
        if (dual) {
            for (int t = 0; t <= ticks; t++) {
                double v = secondary[0] + (secondary[1] - secondary[0]) * t / ticks;
                int y = valueY(v, secondary, plotTop, plotH);
                g.setColor(MUTED);
                g.drawString(num(v), left + plotW + 10, y + fm.getAscent() / 2 - 2);
            }
        }
        // 坐标轴
        g.setColor(AXIS);
        g.drawLine(left, plotTop, left, plotTop + plotH);
        g.drawLine(left, plotTop + plotH, left + plotW, plotTop + plotH);

        // X 轴类目
        if (n > 0) {
            int step = Math.max(1, (int) Math.ceil(n / 12.0));
            int groupW = plotW / n;
            for (int i = 0; i < n; i += step) {
                String label = cats.get(i) == null ? "" : cats.get(i);
                label = fit(fm, label, Math.max(28, groupW * step - 6));
                int cx = left + (int) ((i + 0.5) * groupW);
                g.setColor(MUTED);
                g.drawString(label, cx - fm.stringWidth(label) / 2, plotTop + plotH + 22);
            }
        }

        // 柱状（按类目分组，所有柱序列共享组内槽位）
        List<Integer> barIdx = new ArrayList<>();
        for (int k = 0; k < series.size(); k++) {
            if ("bar".equals(series.get(k).type())) {
                barIdx.add(k);
            }
        }
        if (!barIdx.isEmpty() && n > 0) {
            int groupW = plotW / n;
            double inner = groupW * 0.72;
            double slot = inner / barIdx.size();
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < barIdx.size(); j++) {
                    ChartData.Series s = series.get(barIdx.get(j));
                    Double v = value(s, i);
                    if (v == null) {
                        continue;
                    }
                    double[] range = s.y2() ? secondary : primary;
                    int y0 = valueY(v, range, plotTop, plotH);
                    int yBase = valueY(clamp0(0, range), range, plotTop, plotH);
                    int x = (int) (left + i * groupW + (groupW - inner) / 2 + j * slot + slot * 0.1);
                    int bw = Math.max(2, (int) (slot * 0.8));
                    g.setColor(color(barIdx.get(j)));
                    g.fillRect(x, Math.min(y0, yBase), bw, Math.max(1, Math.abs(y0 - yBase)));
                }
            }
        }

        // 面积 / 折线 / 散点
        for (int k = 0; k < series.size(); k++) {
            ChartData.Series s = series.get(k);
            String type = s.type() == null ? "bar" : s.type();
            if ("bar".equals(type)) {
                continue;
            }
            double[] range = s.y2() ? secondary : primary;
            Color c = color(k);
            if ("area".equals(type)) {
                Path2D.Double area = new Path2D.Double();
                boolean open = false;
                int base = valueY(clamp0(0, range), range, plotTop, plotH);
                int firstX = 0;
                int lastX = 0;
                for (int i = 0; i < n; i++) {
                    Double v = value(s, i);
                    if (v == null) {
                        continue;
                    }
                    int x = pointX(i, n, left, plotW);
                    int y = valueY(v, range, plotTop, plotH);
                    if (!open) {
                        area.moveTo(x, base);
                        area.lineTo(x, y);
                        open = true;
                        firstX = x;
                    } else {
                        area.lineTo(x, y);
                    }
                    lastX = x;
                }
                if (open) {
                    area.lineTo(lastX, base);
                    area.lineTo(firstX, base);
                    area.closePath();
                    g.setColor(new Color(c.getRed(), c.getGreen(), c.getBlue(), 80));
                    g.fill(area);
                }
            }
            if ("scatter".equals(type)) {
                g.setColor(c);
                for (int i = 0; i < n; i++) {
                    Double v = value(s, i);
                    if (v == null) {
                        continue;
                    }
                    int x = pointX(i, n, left, plotW);
                    int y = valueY(v, range, plotTop, plotH);
                    g.fillOval(x - 5, y - 5, 10, 10);
                }
                continue;
            }
            g.setColor(c);
            g.setStroke(new BasicStroke(2.4f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
            Path2D.Double line = new Path2D.Double();
            boolean open = false;
            for (int i = 0; i < n; i++) {
                Double v = value(s, i);
                if (v == null) {
                    open = false;
                    continue;
                }
                int x = pointX(i, n, left, plotW);
                int y = valueY(v, range, plotTop, plotH);
                if (!open) {
                    line.moveTo(x, y);
                    open = true;
                } else {
                    line.lineTo(x, y);
                }
            }
            g.draw(line);
            g.setStroke(new BasicStroke(1f));
        }

        if (n == 0) {
            g.setFont(LEGEND_FONT);
            g.setColor(MUTED);
            drawCentered(g, "无数据", w / 2, plotTop + plotH / 2, LEGEND_FONT);
        }
    }

    private static void drawPie(Graphics2D g, ChartData chart, int w, int h) {
        List<String> cats = chart.categories() == null ? List.of() : chart.categories();
        List<ChartData.Series> series = chart.series() == null ? List.of() : chart.series();

        int top = 20;
        if (chart.title() != null && !chart.title().isBlank()) {
            g.setFont(TITLE_FONT);
            g.setColor(TEXT);
            drawCentered(g, chart.title().strip(), w / 2, top + 20, TITLE_FONT);
            top += 46;
        }

        ChartData.Series s = series.isEmpty() ? null : series.get(0);
        List<Double> values = new ArrayList<>();
        double total = 0;
        if (s != null && s.data() != null) {
            for (Double d : s.data()) {
                double v = d == null || !Double.isFinite(d) || d < 0 ? 0 : d;
                values.add(v);
                total += v;
            }
        }
        int cx = (int) (w * 0.35);
        int cy = top + (h - top) / 2;
        int r = (int) (Math.min(w * 0.30, (h - top) * 0.40));
        if (total <= 0 || r < 20) {
            g.setFont(LEGEND_FONT);
            g.setColor(MUTED);
            drawCentered(g, "无数据", w / 2, h / 2, LEGEND_FONT);
            return;
        }
        double start = 90;
        for (int i = 0; i < values.size(); i++) {
            double v = values.get(i);
            if (v <= 0) {
                continue;
            }
            double ext = -360 * v / total;
            g.setColor(color(i));
            g.fill(new Arc2D.Double(cx - r, cy - r, r * 2.0, r * 2.0, start, ext, Arc2D.PIE));
            if (v / total >= 0.06) {
                double mid = Math.toRadians(start + ext / 2);
                int lx = (int) (cx + Math.cos(mid) * r * 0.62);
                int ly = (int) (cy - Math.sin(mid) * r * 0.62);
                String pct = String.format(Locale.ROOT, "%.0f%%", 100 * v / total);
                g.setFont(AXIS_FONT);
                g.setColor(Color.WHITE);
                g.drawString(pct, lx - g.getFontMetrics().stringWidth(pct) / 2,
                        ly + g.getFontMetrics().getAscent() / 2);
            }
            start += ext;
        }

        // 图例（右侧，名称 + 数值 + 占比）
        g.setFont(LEGEND_FONT);
        FontMetrics fm = g.getFontMetrics();
        int lx = (int) (w * 0.64);
        int ly = top + 24;
        for (int i = 0; i < values.size() && i < 24; i++) {
            double v = values.get(i);
            String name = i < cats.size() && cats.get(i) != null ? cats.get(i) : "项" + (i + 1);
            String text = name + "  " + num(v) + "（"
                    + String.format(Locale.ROOT, "%.1f%%", 100 * v / total) + "）";
            g.setColor(color(i));
            g.fillRect(lx, ly - 11, 12, 12);
            g.setColor(TEXT);
            g.drawString(fit(fm, text, w - lx - 24), lx + 20, ly);
            ly += 24;
        }
    }

    // ==================== PDF 文档页面 ====================

    /**
     * 把内容 / 表格 / 图表排成 PDF 页面位图：图表（若有）占一张横向页，文本与表格按 A4 纵向分页。
     * 无任何可排版内容时返回空列表（调用方报错）。
     */
    static List<BufferedImage> documentPages(String title, String content,
                                             List<String> columns, List<List<String>> rows,
                                             ChartData chart) {
        List<BufferedImage> pages = new ArrayList<>();
        if (chart != null) {
            pages.add(renderChart(chart, PDF_LAND_WIDTH, PDF_LAND_HEIGHT));
        }
        boolean hasContent = content != null && !content.isBlank();
        boolean hasTable = columns != null && !columns.isEmpty();
        if (hasContent || hasTable) {
            pages.addAll(textPages(title, content, columns, rows));
        }
        return pages;
    }

    private static List<BufferedImage> textPages(String title, String content,
                                                 List<String> columns, List<List<String>> rows) {
        Doc doc = new Doc();
        if (title != null && !title.isBlank()) {
            FontMetrics fm = doc.g.getFontMetrics(DOC_TITLE_FONT);
            for (String line : wrap(title.strip(), fm, PDF_WIDTH - 2 * MARGIN)) {
                doc.line(line, DOC_TITLE_FONT);
            }
            doc.y += 12;
        }
        if (content != null && !content.isBlank()) {
            FontMetrics fm = doc.g.getFontMetrics(DOC_BODY_FONT);
            for (String raw : content.strip().replace("\r\n", "\n").replace('\r', '\n').split("\n", -1)) {
                if (raw.isEmpty()) {
                    doc.line("", DOC_BODY_FONT);
                    continue;
                }
                for (String line : wrap(raw, fm, PDF_WIDTH - 2 * MARGIN)) {
                    doc.line(line, DOC_BODY_FONT);
                }
            }
            doc.y += 10;
        }
        if (columns != null && !columns.isEmpty()) {
            drawTable(doc, columns, rows == null ? List.of() : rows);
        }
        return doc.finish();
    }

    /** 画表格（表头 + 数据行），行放不下时自动换页。 */
    private static void drawTable(Doc doc, List<String> columns, List<List<String>> rows) {
        int n = columns.size();
        int colW = Math.max(60, (PDF_WIDTH - 2 * MARGIN) / n);
        drawTableRow(doc, columns, colW, true);
        for (List<String> row : rows) {
            List<String> cells = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                String v = row != null && i < row.size() && row.get(i) != null ? row.get(i) : "";
                cells.add(v.replace('\n', ' ').replace('\r', ' '));
            }
            drawTableRow(doc, cells, colW, false);
        }
        doc.y += 10;
    }

    private static void drawTableRow(Doc doc, List<String> cells, int colW, boolean header) {
        Font font = header ? TABLE_HEADER_FONT : TABLE_FONT;
        FontMetrics fm = doc.g.getFontMetrics(font);
        List<List<String>> wrapped = new ArrayList<>(cells.size());
        int maxLines = 1;
        for (String v : cells) {
            List<String> lines = wrap(v, fm, colW - 16);
            if (lines.size() > 6) {
                lines = new ArrayList<>(lines.subList(0, 6));
            }
            wrapped.add(lines);
            maxLines = Math.max(maxLines, lines.size());
        }
        int rowH = maxLines * TABLE_LINE_H + 12;
        doc.ensure(rowH);
        int n = cells.size();
        if (header) {
            doc.g.setColor(HEADER_BG);
            doc.g.fillRect(MARGIN, doc.y, colW * n, rowH);
        }
        doc.g.setColor(BORDER);
        for (int c = 0; c <= n; c++) {
            doc.g.drawLine(MARGIN + c * colW, doc.y, MARGIN + c * colW, doc.y + rowH);
        }
        doc.g.drawLine(MARGIN, doc.y, MARGIN + n * colW, doc.y);
        doc.g.drawLine(MARGIN, doc.y + rowH, MARGIN + n * colW, doc.y + rowH);
        doc.g.setFont(font);
        doc.g.setColor(TEXT);
        int ascent = doc.g.getFontMetrics(font).getAscent();
        for (int c = 0; c < n; c++) {
            List<String> lines = wrapped.get(c);
            for (int li = 0; li < lines.size(); li++) {
                doc.g.drawString(lines.get(li), MARGIN + c * colW + 8,
                        doc.y + 8 + ascent + li * TABLE_LINE_H);
            }
        }
        doc.y += rowH;
    }

    /** 文档排版游标：持有当前页与纵向位置，需要时自动换页。 */
    private static final class Doc {
        private final List<BufferedImage> pages = new ArrayList<>();
        private BufferedImage img;
        private Graphics2D g;
        private int y = MARGIN;

        Doc() {
            startPage();
        }

        private void startPage() {
            if (g != null) {
                g.dispose();
                pages.add(img);
            }
            img = newPage(PDF_WIDTH, PDF_HEIGHT);
            g = img.createGraphics();
            setup(g);
            y = MARGIN;
        }

        /** 保证当前页还能放下 {@code needed} 像素，放不下则换页。 */
        void ensure(int needed) {
            if (y + needed > PDF_HEIGHT - MARGIN) {
                startPage();
            }
        }

        void line(String text, Font font) {
            ensure(LINE_H);
            g.setFont(font);
            g.setColor(TEXT);
            g.drawString(text, MARGIN, y + g.getFontMetrics().getAscent());
            y += LINE_H;
        }

        List<BufferedImage> finish() {
            if (g != null) {
                g.dispose();
                pages.add(img);
                g = null;
            }
            return pages;
        }
    }

    // ==================== 助手 ====================

    private static BufferedImage newPage(int w, int h) {
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        try {
            g.setColor(Color.WHITE);
            g.fillRect(0, 0, w, h);
        } finally {
            g.dispose();
        }
        return img;
    }

    private static void setup(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setColor(TEXT);
    }

    private static int drawLegend(Graphics2D g, List<ChartData.Series> series, int w, int y) {
        if (series == null || series.isEmpty()) {
            return y;
        }
        g.setFont(LEGEND_FONT);
        FontMetrics fm = g.getFontMetrics();
        List<String> names = new ArrayList<>();
        int total = 0;
        for (ChartData.Series s : series) {
            String name = s.name() == null ? "" : s.name();
            names.add(name);
            total += 18 + fm.stringWidth(name) + 18;
        }
        int x = Math.max(16, w / 2 - total / 2);
        for (int i = 0; i < names.size(); i++) {
            g.setColor(color(i));
            g.fillRect(x, y - 11, 12, 12);
            g.setColor(TEXT);
            g.drawString(names.get(i), x + 17, y);
            x += 18 + fm.stringWidth(names.get(i)) + 18;
        }
        return y + 20;
    }

    private static double[] axisRange(List<ChartData.Series> series, boolean y2) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        boolean any = false;
        for (ChartData.Series s : series) {
            if (s.y2() != y2 || s.data() == null) {
                continue;
            }
            for (Double d : s.data()) {
                if (d == null || !Double.isFinite(d)) {
                    continue;
                }
                min = Math.min(min, d);
                max = Math.max(max, d);
                any = true;
            }
        }
        if (!any) {
            return new double[]{0, 1};
        }
        min = Math.min(min, 0);
        max = Math.max(max, 0);
        if (min == max) {
            max = min + 1;
        }
        return new double[]{min, max};
    }

    private static double clamp0(double v, double[] range) {
        return Math.max(range[0], Math.min(range[1], v));
    }

    private static int valueY(double v, double[] range, int plotTop, int plotH) {
        double span = range[1] - range[0];
        if (span <= 0) {
            span = 1;
        }
        double ratio = (v - range[0]) / span;
        return plotTop + (int) Math.round(plotH * (1 - ratio));
    }

    private static int pointX(int i, int n, int left, int plotW) {
        return left + (int) Math.round((i + 0.5) * plotW / Math.max(1, n));
    }

    private static Double value(ChartData.Series s, int i) {
        if (s.data() == null || i >= s.data().size()) {
            return null;
        }
        Double d = s.data().get(i);
        return d == null || !Double.isFinite(d) ? null : d;
    }

    private static Color color(int i) {
        return PALETTE[Math.floorMod(i, PALETTE.length)];
    }

    private static void drawCentered(Graphics2D g, String text, int cx, int baseline, Font font) {
        g.setFont(font);
        g.drawString(text, cx - g.getFontMetrics().stringWidth(text) / 2, baseline);
    }

    /** 数值刻度：整数不带小数，大数用千分位。 */
    private static String num(double v) {
        if (Math.abs(v) >= 10_000) {
            return String.format(Locale.ROOT, "%,.0f", v);
        }
        if (Math.abs(v - Math.rint(v)) < 1e-9) {
            return String.format(Locale.ROOT, "%.0f", v);
        }
        return String.format(Locale.ROOT, "%.2f", v);
    }

    /** 超宽文本截断加省略号。 */
    private static String fit(FontMetrics fm, String s, int maxWidth) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        if (fm.stringWidth(s) <= maxWidth) {
            return s;
        }
        String ellipsis = "...";
        int i = s.length();
        while (i > 0 && fm.stringWidth(s.substring(0, i) + ellipsis) > maxWidth) {
            i--;
        }
        return s.substring(0, i) + ellipsis;
    }

    /** 按像素宽度折行（逐码点累加，兼容中文）。 */
    private static List<String> wrap(String text, FontMetrics fm, int maxWidth) {
        List<String> out = new ArrayList<>();
        if (text == null) {
            return out;
        }
        String s = text.replace("\r\n", "\n").replace('\r', '\n');
        for (String raw : s.split("\n", -1)) {
            if (raw.isEmpty()) {
                out.add("");
                continue;
            }
            StringBuilder cur = new StringBuilder();
            int i = 0;
            while (i < raw.length()) {
                int cp = raw.codePointAt(i);
                String ch = new String(Character.toChars(cp));
                if (cur.length() > 0 && fm.stringWidth(cur.toString() + ch) > maxWidth) {
                    out.add(cur.toString());
                    cur.setLength(0);
                }
                cur.append(ch);
                i += Character.charCount(cp);
            }
            out.add(cur.toString());
        }
        return out;
    }
}
