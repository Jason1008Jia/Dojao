package com.dojao.ui.ai;

import com.dojao.ai.AiConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * 探针：{@link AiAgentService#buildChartSpec} 图表规格构建（纯函数，离线）。
 *
 * <p>覆盖：类型归一化 / 列解析（忽略大小写）/ 数值与文本单元格 / 多序列 / 截断标记 /
 * <b>混合图（每条序列各自图形 seriesTypes + 主副 Y 轴 seriesAxes）</b> /
 * <b>{@link AiAgentService#parseDataSet} 结果集 JSON 解析（对象 {columns,rows} / 行对象数组）</b> /
 * <b>服务端结果集缓存 + 引用（{@code cacheResult}/{@code lookupResultRef}，{@code data="rs3"}/"last" 复用）</b> /
 * 以及各类非法输入抛 {@link IllegalArgumentException} 或解析失败返回 null。</p>
 *
 * <p><b>判别力</b>：把 {@code indexOfColumn} 的大小写匹配去掉，「OS 大小写」用例 FAIL；
 * 让非法类型不再抛错，「未知类型」用例 FAIL；把 {@code resolveSeriesTypes} 改成恒回退基础类型
 * ⇒ 混合图的「逐条图形」用例 FAIL；去掉顶层 {@code dualAxis} 写入 ⇒ 副轴用例 FAIL；
 * 把 {@code parseDataSet} 改成恒返回 null ⇒ 结果集解析的正向用例 FAIL（8 项）；
 * 把 {@code looksLikeRef} 改成恒 false ⇒ 引用解析/复用用例 FAIL（13 项）；
 * 把 heatmap / graph 分支钉成 {@code false &&} ⇒ 对应分类/节点/边用例 FAIL；
 * 去掉 {@code resolveSeriesTypes} 的非直角类型守卫 ⇒「非直角类型不支持 seriesTypes」FAIL；
 * 去掉 {@code buildChartData} 的 {@code EXPORT_CHART_TYPES} 守卫 ⇒ 导出拒绝用例 FAIL。</p>
 */
public final class AiChartSpecProbeTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        AiAgentService svc = new AiAgentService(new AiConfig("http://x", "k", "m", 0.2), null, null);

        // ---- bar：单序列 ----
        String bar = svc.buildChartSpec("bar", "各 OS 订单", List.of("os", "order_cnt", "purchase_events"),
                List.of(List.of("Android", "120", "300"), List.of("iOS", "80", "210")),
                "os", List.of("order_cnt"), false, 2);
        JsonNode b = M.readTree(bar);
        check("bar.type", "bar".equals(b.path("type").asText()));
        check("bar.title", "各 OS 订单".equals(b.path("title").asText()));
        check("bar.categories", b.path("categories").size() == 2
                && "Android".equals(b.path("categories").get(0).asText()));
        check("bar.series[0].name", "order_cnt".equals(b.path("series").get(0).path("name").asText()));
        check("bar.series[0].data 数值化", b.path("series").get(0).path("data").get(0).asDouble() == 120.0);
        check("bar.totalRows 透传", b.path("totalRows").asLong() == 2 && !b.path("truncated").asBoolean());

        // ---- 多序列 ----
        String multi = svc.buildChartSpec("line", null, List.of("os", "a", "b"),
                List.of(List.of("Android", "1", "2")), "os", List.of("a", "b"), false, 1);
        check("多序列 series 数量", M.readTree(multi).path("series").size() == 2);

        // ---- 列名忽略大小写 ----
        String ci = svc.buildChartSpec("pie", null, List.of("OS", "Order_Cnt"),
                List.of(List.of("Android", "5")), "os", List.of("order_cnt"), false, 1);
        check("列名忽略大小写", "pie".equals(M.readTree(ci).path("type").asText()));

        // ---- 类型别名 + 大小写 ----
        check("别名 column→bar", "bar".equals(AiAgentService.normalizeChartType("column")));
        check("别名 donut→pie", "pie".equals(AiAgentService.normalizeChartType("DONUT")));
        check("AREA 归一化", "area".equals(AiAgentService.normalizeChartType("AREA")));
        check("未知名→null", AiAgentService.normalizeChartType("radar") == null);

        // ---- 非数值单元格保持原文本 ----
        String txt = svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "N/A")), "k", List.of("v"), false, 1);
        check("非数值保持文本", "N/A".equals(M.readTree(txt).path("series").get(0).path("data").get(0).asText()));

        // ---- 截断标记透传 ----
        String tr = svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), true, 999);
        JsonNode t = M.readTree(tr);
        check("truncated/totalRows 透传", t.path("truncated").asBoolean() && t.path("totalRows").asLong() == 999);

        // ---- 混合图：柱状 + 折线，副轴 ----
        String mix = svc.buildChartSpec("bar", "混合", List.of("dt", "order_cnt", "refund_rate"),
                List.of(List.of("2026-01-01", "120", "0.05"), List.of("2026-01-02", "80", "0.12")),
                "dt", List.of("order_cnt", "refund_rate"),
                List.of("bar", "line"), List.of("y", "y2"), false, 2);
        JsonNode mx = M.readTree(mix);
        check("混合图顶层 type 保留默认", "bar".equals(mx.path("type").asText()));
        check("混合图 序列0.type=bar", "bar".equals(mx.path("series").get(0).path("type").asText()));
        check("混合图 序列1.type=line", "line".equals(mx.path("series").get(1).path("type").asText()));
        check("混合图 序列1.axis=y2", "y2".equals(mx.path("series").get(1).path("axis").asText()));
        check("混合图 序列0 未写 axis（默认主图）", mx.path("series").get(0).path("axis").isMissingNode());
        check("混合图 dualAxis=true", mx.path("dualAxis").asBoolean());
        check("混合图 数值化不变", mx.path("series").get(1).path("data").get(0).asDouble() == 0.05);

        // ---- 只给 seriesTypes（不指定轴）→ 逐条图形、单轴 ----
        String typesOnly = svc.buildChartSpec("line", null, List.of("dt", "a", "b"),
                List.of(List.of("d1", "1", "2")), "dt", List.of("a", "b"),
                List.of("line", "area"), null, false, 1);
        JsonNode to = M.readTree(typesOnly);
        check("seriesTypes 生效：序列1=area", "area".equals(to.path("series").get(1).path("type").asText()));
        check("未指定 seriesAxes 时无 dualAxis", !to.path("dualAxis").asBoolean());

        // ---- 缺省 seriesTypes → 每条序列回退基础类型 ----
        String fallback = svc.buildChartSpec("scatter", null, List.of("dt", "a", "b"),
                List.of(List.of("d1", "1", "2")), "dt", List.of("a", "b"), false, 1);
        JsonNode fb = M.readTree(fallback);
        check("缺省 seriesTypes 回退基础类型", "scatter".equals(fb.path("series").get(1).path("type").asText()));
        check("轴归一化别名 right→y2", "y2".equals(AiAgentService.normalizeSeriesAxis("RIGHT")));
        check("轴归一化未知→null", AiAgentService.normalizeSeriesAxis("z9") == null);

        // ---- 结果集 JSON（data 参数）：复用已算好的数据出图 ----
        AiAgentService.ResultSet ds = svc.parseDataSet(
                "{\"columns\":[\"dt\",\"cnt\"],\"rows\":[[\"2026-01-01\",\"120\"],[\"2026-01-02\",\"80\"]]}");
        check("data 对象形式：列解析", ds != null && List.of("dt", "cnt").equals(ds.columns()));
        check("data 对象形式：行解析", ds != null && ds.rows().size() == 2
                && "120".equals(ds.rows().get(0).get(1)));
        check("data 行不足补空串", cellOf(svc, "{\"columns\":[\"a\",\"b\"],\"rows\":[[\"x\"]]}", 0, 1).isEmpty());
        check("data null 单元格→空串", cellOf(svc, "{\"columns\":[\"a\"],\"rows\":[[null]]}", 0, 0).isEmpty());

        // 行对象数组形式
        AiAgentService.ResultSet ja = svc.parseDataSet(
                "[{\"os\":\"Android\",\"n\":\"3\"},{\"os\":\"iOS\",\"n\":\"5\"}]");
        check("data 行对象数组：列取自键", ja != null && List.of("os", "n").equals(ja.columns()));
        check("data 行对象数组：值解析", ja != null && "5".equals(ja.rows().get(1).get(1)));

        // 解析出的结果集可直接喂 buildChartSpec（与 sql 路径同形）
        String fromData = ds == null ? "" : svc.buildChartSpec("bar", null, ds.columns(), ds.rows(),
                "dt", List.of("cnt"), false, ds.rows().size());
        JsonNode fd = M.readTree(fromData.isEmpty() ? "{}" : fromData);
        check("data→spec：categories", fd.path("categories").size() > 0
                && "2026-01-01".equals(fd.path("categories").get(0).asText()));
        check("data→spec：数值化", fd.path("series").size() > 0
                && fd.path("series").get(0).path("data").size() > 0
                && fd.path("series").get(0).path("data").get(0).asDouble() == 120.0);

        // 非法结果集 → null
        check("data 非 JSON→null", svc.parseDataSet("not json") == null);
        check("data 对象缺 rows→null", svc.parseDataSet("{\"columns\":[\"a\"]}") == null);
        check("data 空数组→null", svc.parseDataSet("[]") == null);
        check("data 标量数组→null", svc.parseDataSet("[1,2,3]") == null);
        check("data 空串→null", svc.parseDataSet("  ") == null);

        // ---- 服务端结果集缓存 + 引用（execute_sql 缓存，render_chart 用 id / "last" 复用，免重算免回传）----
        String v = "alice";
        String ref1 = svc.cacheResult(v, List.of("dt", "cnt"), List.of(List.of("2026-01-01", "120")));
        String ref2 = svc.cacheResult(v, List.of("dt", "cnt"), List.of(List.of("2026-01-02", "80")));
        check("cache 返回引用 id rs1", "rs1".equals(ref1));
        check("cache 递增 id rs2", "rs2".equals(ref2));
        check("cache 空列不缓存→null", svc.cacheResult(v, List.of(), List.of()) == null);
        AiAgentService.CachedResult c1 = svc.lookupResultRef(v, "rs1");
        check("引用 rs1 命中且列正确", c1 != null && List.of("dt", "cnt").equals(c1.columns())
                && "120".equals(c1.rows().get(0).get(1)));
        AiAgentService.CachedResult lastRef = svc.lookupResultRef(v, "last");
        check("引用 last 取最新(rs2)", lastRef != null && "rs2".equals(lastRef.id()));
        AiAgentService.CachedResult prevRef = svc.lookupResultRef(v, "previous");
        check("引用 previous 取最新", prevRef != null && "rs2".equals(prevRef.id()));
        check("引用可带 # 前缀", svc.lookupResultRef(v, "#rs1") != null);
        check("引用大小写不敏感", svc.lookupResultRef(v, "RS1") != null);
        check("looksLikeRef: last/rs3/#RS3 真", AiAgentService.looksLikeRef("last")
                && AiAgentService.looksLikeRef("rs3") && AiAgentService.looksLikeRef("#RS3"));
        check("looksLikeRef: JSON/数字 假", !AiAgentService.looksLikeRef("{\"a\":1}")
                && !AiAgentService.looksLikeRef("3"));
        check("未命中引用→null", svc.lookupResultRef(v, "rs99") == null);
        check("跨 viewer 隔离", svc.lookupResultRef("bob", "rs1") == null);

        // 端到端：render_chart 用缓存引用出图，且不触发任何 SQL 提交（monitor 为 null 也能跑）
        List<String> emitted = new ArrayList<>();
        String msg = svc.renderChart("bar", null, "rs1", "dt", List.of("cnt"),
                null, null, null, v, emitted::add);
        JsonNode es = emitted.isEmpty() ? M.createObjectNode() : M.readTree(emitted.get(0));
        check("render_chart 引用出图：已 emit", emitted.size() == 1);
        check("render_chart 引用出图：categories", es.path("categories").size() > 0
                && "2026-01-01".equals(es.path("categories").get(0).asText()));
        check("render_chart 引用出图：数值化", es.path("series").size() > 0
                && es.path("series").get(0).path("data").size() > 0
                && es.path("series").get(0).path("data").get(0).asDouble() == 120.0);
        check("render_chart 引用出图：文案含『来自传入结果集』", msg.contains("来自传入结果集"));

        List<String> emitted2 = new ArrayList<>();
        svc.renderChart("bar", null, "last", "dt", List.of("cnt"), null, null, null, v, emitted2::add);
        check("render_chart data=last 可出图", emitted2.size() == 1
                && M.readTree(emitted2.get(0)).path("categories").size() > 0
                && "2026-01-02".equals(M.readTree(emitted2.get(0)).path("categories").get(0).asText()));

        List<String> emitted3 = new ArrayList<>();
        String miss = svc.renderChart("bar", null, "rs99", "dt", List.of("cnt"), null, null, null, v, emitted3::add);
        check("render_chart 引用未命中→不出图且提示", emitted3.isEmpty() && miss.contains("未找到"));

        // 淘汰：每 viewer 只保留最近 RESULT_CACHE_MAX(10) 份
        for (int i = 0; i < 12; i++) {
            svc.cacheResult("carol", List.of("a"), List.of(List.of(String.valueOf(i))));
        }
        check("缓存淘汰：最旧被清", svc.lookupResultRef("carol", "rs1") == null);
        check("缓存淘汰：最新仍在", svc.lookupResultRef("carol", "last") != null);

        // ==================== 新增 2D 图表类型 ====================
        List<String> cols = List.of("province", "city", "sales", "ratio", "target");
        List<List<String>> data = List.of(
                List.of("广东", "深圳", "120", "0.4", "广州"),
                List.of("广东", "广州", "80", "0.2", "深圳"),
                List.of("北京", "北京", "50", "0.9", "深圳"));

        // ---- 类型归一化 / 中文别名 ----
        check("funnel 归一化", "funnel".equals(AiAgentService.normalizeChartType("FUNNEL")));
        check("heatmap 中文别名", "heatmap".equals(AiAgentService.normalizeChartType("热力图")));
        check("treemap 中文别名", "treemap".equals(AiAgentService.normalizeChartType("矩形树图")));
        check("tree 别名", "tree".equals(AiAgentService.normalizeChartType("树图")));
        check("graph 中文别名", "graph".equals(AiAgentService.normalizeChartType("关系图")));
        check("dataset 别名 table", "dataset".equals(AiAgentService.normalizeChartType("table")));
        check("雷达图仍未知（未在本次范围）", AiAgentService.normalizeChartType("radar") == null);

        // ---- 漏斗图：category + values[0] ----
        JsonNode funnel = spec(svc.buildChartSpec("funnel", null, cols, data, "province",
                List.of("sales"), false, 3));
        check("funnel.type", "funnel".equals(funnel.path("type").asText()));
        check("funnel 序列图形", "funnel".equals(funnel.path("series").get(0).path("type").asText()));
        check("funnel categories", funnel.path("categories").size() == 3
                && "广东".equals(funnel.path("categories").get(0).asText()));

        // ---- 矩形树图 / 树图：复用 category + values[0] ----
        JsonNode treemap = spec(svc.buildChartSpec("treemap", null, cols, data, "province",
                List.of("sales"), false, 3));
        check("treemap.type", "treemap".equals(treemap.path("type").asText()));
        check("treemap 数据已数值化", treemap.path("series").get(0).path("data").get(0).asDouble() == 120.0);
        JsonNode tree = spec(svc.buildChartSpec("tree", "根", cols, data, "province",
                List.of("sales"), false, 3));
        check("tree.type", "tree".equals(tree.path("type").asText()));

        // ---- 热力图：X 分类 + Y 分类 + 数值 → [xIdx, yIdx, value] ----
        JsonNode heatmap = spec(svc.buildChartSpec("heatmap", null, cols, data, "province",
                List.of("sales"), null, null, "city", null, false, 3));
        check("heatmap.type", "heatmap".equals(heatmap.path("type").asText()));
        check("heatmap X 分类去重", heatmap.path("categories").size() == 2
                && "广东".equals(heatmap.path("categories").get(0).asText()));
        check("heatmap Y 分类去重", heatmap.path("yCategories").size() == 3
                && "深圳".equals(heatmap.path("yCategories").get(0).asText())
                && "广州".equals(heatmap.path("yCategories").get(1).asText()));
        JsonNode hp0 = heatmap.path("series").get(0).path("data").get(0);
        check("heatmap 三元组 [x,y,v]", hp0.size() == 3 && hp0.get(0).asInt() == 0
                && hp0.get(1).asInt() == 0 && hp0.get(2).asDouble() == 120.0);
        check("heatmap 序列图形", "heatmap".equals(heatmap.path("series").get(0).path("type").asText()));

        // ---- 关系图：category=源、target=目标、values[0]=权重 ----
        JsonNode graph = spec(svc.buildChartSpec("graph", null, cols, data, "province",
                List.of("ratio"), null, null, null, "target", false, 3));
        check("graph.type", "graph".equals(graph.path("type").asText()));
        check("graph nodes 去重（广东/北京/广州/深圳）", graph.path("nodes").size() == 4);
        check("graph links 数量", graph.path("links").size() == 3);
        JsonNode firstLink = at(graph.path("links"), 0);
        check("graph 边权重", firstLink.path("value").asDouble() == 0.4
                && "广东".equals(firstLink.path("source").asText())
                && "广州".equals(firstLink.path("target").asText()));
        JsonNode graphNoWeight = null;
        try {
            graphNoWeight = spec(svc.buildChartSpec("graph", null, cols, data, "province",
                    null, null, null, null, "target", false, 2));
        } catch (RuntimeException ignored) {
            // 留 null，下面按 FAIL 计（避免探针整体中断）
        }
        check("graph 无 values 时权重默认 1", graphNoWeight != null
                && at(graphNoWeight.path("links"), 1).path("value").asDouble() == 1.0);

        // ---- 数据集：原始 columns + rows，无需 category / values ----
        JsonNode dataset = spec(svc.buildChartSpec("dataset", null, cols, data, null, null,
                false, 3));
        check("dataset.type", "dataset".equals(dataset.path("type").asText()));
        check("dataset columns", dataset.path("columns").size() == 5
                && "province".equals(dataset.path("columns").get(0).asText()));
        check("dataset rows", dataset.path("rows").size() == 3
                && "120".equals(dataset.path("rows").get(0).get(2).asText()));

        // render_chart 端到端：dataset 用缓存引用直接出图
        List<String> dsEmit = new ArrayList<>();
        String dsMsg = svc.renderChart("dataset", null, "rs1", null, null,
                null, null, null, v, dsEmit::add);
        JsonNode dsSpec = dsEmit.isEmpty() ? M.createObjectNode() : M.readTree(dsEmit.get(0));
        check("render_chart dataset 已 emit", dsEmit.size() == 1
                && "dataset".equals(dsSpec.path("type").asText()));
        check("render_chart dataset 回执", dsMsg.contains("数据集表格"));

        // ---- 非法输入 ----
        rejects("未知类型", () -> svc.buildChartSpec("pie3d", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), false, 1));
        rejects("category 列不存在", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "nope", List.of("v"), false, 1));
        rejects("values 列不存在", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("nope"), false, 1));
        rejects("values 为空", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of(), false, 1));
        rejects("无列", () -> svc.buildChartSpec("bar", null, List.of(), List.of(), "k", List.of("v"), false, 0));
        rejects("seriesTypes 长度不符", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), List.of("bar", "line"), null, false, 1));
        rejects("seriesTypes 含非法图形", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), List.of("pie"), null, false, 1));
        rejects("seriesAxes 长度不符", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), null, List.of("y", "y2"), false, 1));
        rejects("seriesAxes 含非法轴", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), null, List.of("z9"), false, 1));
        rejects("heatmap 缺 yCategory", () -> svc.buildChartSpec("heatmap", null, List.of("k", "y", "v"),
                List.of(List.of("a", "b", "1")), "k", List.of("v"), null, null, null, null, false, 1));
        rejects("heatmap yCategory 列不存在", () -> svc.buildChartSpec("heatmap", null, List.of("k", "v"),
                List.of(List.of("a", "1")), "k", List.of("v"), null, null, "nope", null, false, 1));
        rejects("graph 缺 target", () -> svc.buildChartSpec("graph", null, List.of("a", "b", "w"),
                List.of(List.of("x", "y", "1")), "a", List.of("w"), null, null, null, null, false, 1));
        rejects("graph target 列不存在", () -> svc.buildChartSpec("graph", null, List.of("a", "b", "w"),
                List.of(List.of("x", "y", "1")), "a", List.of("w"), null, null, null, "nope", false, 1));
        rejects("非直角类型不支持 seriesTypes", () -> svc.buildChartSpec("funnel", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), List.of("bar"), null, false, 1));
        rejects("seriesTypes 不允许 funnel", () -> svc.buildChartSpec("bar", null, List.of("k", "v"),
                List.of(List.of("x", "1")), "k", List.of("v"), List.of("funnel"), null, false, 1));
        rejects("图片/PDF 导出不支持 funnel", () -> AiAgentService.buildChartData("funnel", null,
                List.of("k", "v"), List.of(List.of("x", "1")), "k", List.of("v"), null, null));
        rejects("图片/PDF 导出不支持 heatmap", () -> AiAgentService.buildChartData("heatmap", null,
                List.of("k", "y", "v"), List.of(List.of("a", "b", "1")), "k", List.of("v"), null, null));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    /** 取数组中第 idx 个元素；缺失 / 非数组 → MissingNode（令断言 FAIL 而非 NPE）。 */
    private static JsonNode at(JsonNode arr, int idx) {
        return (arr != null && arr.isArray() && idx >= 0 && idx < arr.size()) ? arr.get(idx) : M.missingNode();
    }

    /** 把 spec JSON 解析成树；解析失败抛错（探针直接暴露问题）。 */
    private static JsonNode spec(String json) throws Exception {
        return M.readTree(json);
    }

    /** 解析结果集后取某单元格；解析失败 → 返回非空哨兵，令检查 FAIL 而非 NPE。 */
    private static String cellOf(AiAgentService svc, String json, int r, int c) {
        AiAgentService.ResultSet ds = svc.parseDataSet(json);
        if (ds == null || r >= ds.rows().size() || c >= ds.rows().get(r).size()) {
            return "\u0000MISSING";
        }
        return ds.rows().get(r).get(c);
    }

    private static void rejects(String name, Runnable r) {
        try {
            r.run();
            check("非法输入应抛错: " + name, false);
        } catch (IllegalArgumentException e) {
            check("非法输入已抛错: " + name, true);
        }
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
