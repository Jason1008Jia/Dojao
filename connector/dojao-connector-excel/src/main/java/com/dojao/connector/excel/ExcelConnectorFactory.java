package com.dojao.connector.excel;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Excel 连接器 SPI 工厂，由 ServiceLoader 自动发现（{@code CREATE CATALOG ... USING excel}）。
 *
 * <p><b>层级映射</b>：</p>
 * <ul>
 *   <li>catalog → {@code catalog.excel.path} 指向的目录（或单个工作簿文件）；</li>
 *   <li>schema → 工作簿（文件名去掉 {@code .xlsx/.xlsm}），如 {@code sales.xlsx} → {@code sales}；</li>
 *   <li>table → 工作表（sheet 名），如 {@code SELECT * FROM excel_catalog.sales.Sheet1}。</li>
 * </ul>
 *
 * <p><b>分区</b>：Excel 读取<b>恒为单分区</b>（{@link ExcelConnector#planSplits} 固定返回一片）。
 * 一个工作簿是一份 ZIP、一张表是一条 XML 流，拆成多片只会让每个 Worker 各自解压一遍；
 * 需要并行的是下游的聚合 / join，而不是这张表本身。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
public class ExcelConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return ExcelConnector.TYPE;
    }

    @Override
    public String displayName() {
        return "Excel";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.excel.path",
                "必填。工作簿文件（.xlsx / .xlsm）或包含工作簿的目录；目录下每个工作簿成为一个 schema");
        props.put("catalog.excel.header",
                "可选，默认 true：每张表的第一个非空行作为列名；false 时列名为 col1、col2 …");
        props.put("catalog.excel.sampleRows",
                "可选，默认 200：类型推断的采样行数（bigint / double / boolean / datetime / string）");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String path = props.get("path");
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Excel catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.excel.path'。可用属性: " + describeProperties());
        }
        Path parsed;
        try {
            parsed = Paths.get(path.trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Excel catalog '" + catalogName
                    + "' 的 'catalog.excel.path' 不是合法路径: " + path, e);
        }
        if (!parsed.isAbsolute()) {
            // 相对路径在各进程的工作目录里解释不同 —— 集群下 Master 与 Worker 会读到不同的文件
            throw new IllegalArgumentException("Excel catalog '" + catalogName
                    + "' 的 'catalog.excel.path' 必须是绝对路径: " + path
                    + "（相对路径在 Master / Worker 上会指向不同位置）");
        }
        String header = props.get("header");
        if (header != null && !header.isBlank() && !isBoolean(header)) {
            throw new IllegalArgumentException("Excel catalog '" + catalogName
                    + "' 的 'catalog.excel.header' 只能是 true / false: " + header);
        }
        String sample = props.get("sampleRows");
        if (sample != null && !sample.isBlank()) {
            try {
                if (Integer.parseInt(sample.trim()) < 0) throw new NumberFormatException();
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Excel catalog '" + catalogName
                        + "' 的 'catalog.excel.sampleRows' 必须是非负整数: " + sample);
            }
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        boolean header = !"false".equalsIgnoreCase(trim(props.get("header"))
                ) && !"0".equals(trim(props.get("header")));
        int sampleRows = ExcelConnector.DEFAULT_SAMPLE_ROWS;
        String sample = trim(props.get("sampleRows"));
        if (!sample.isEmpty()) {
            sampleRows = Integer.parseInt(sample);
        }
        return new ExcelConnector(catalogName, Paths.get(props.get("path").trim()),
                header, sampleRows);
    }

    private static boolean isBoolean(String value) {
        String v = value.trim().toLowerCase(Locale.ROOT);
        return "true".equals(v) || "false".equals(v) || "1".equals(v) || "0".equals(v)
                || "yes".equals(v) || "no".equals(v);
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }
}
