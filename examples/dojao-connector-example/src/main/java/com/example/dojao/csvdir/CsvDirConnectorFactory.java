package com.example.dojao.csvdir;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ConnectorRegistry;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 示例连接器的工厂 —— 第三方扩展的入口类。
 *
 * <p>必须实现 {@link #type()} 与 {@link #create}，并在
 * {@code src/main/resources/META-INF/services/com.dojao.spi.service.ConnectorFactory} 里登记本类全限定名，
 * 引擎启动时才能发现它（见 {@link ConnectorRegistry}）。</p>
 *
 * <p>其余方法都有默认实现，这里演示三个「让人用得更顺」的可选覆写：
 * {@link #displayName()}（给人看的名字）、{@link #describeProperties()}（属性说明，
 * Web UI 拿它渲染建 catalog 表单）、{@link #validate}（落库前的静态准入检查）。</p>
 *
 * @author 张三
 * @version 1.0
 * @date 2026/9/20
 */
public final class CsvDirConnectorFactory implements ConnectorFactory {

    /** {@code CREATE CATALOG ... USING csvdir} 里的那个词，全局唯一、大小写敏感。 */
    @Override
    public String type() {
        return "csvdir";
    }

    @Override
    public String displayName() {
        return "CSV 目录（示例）";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> describe = new LinkedHashMap<>();
        describe.put("catalog.csvdir.dir", "必填。数据根目录，布局为 <dir>/<schema>/<table>/part-*.csv");
        describe.put("catalog.csvdir.delimiter", "可选。字段分隔符，单个字符，默认 \",\"");
        return describe;
    }

    /**
     * 落库前的静态检查：只验属性在不在、格式对不对，<b>不碰数据源</b>
     * （目录存不存在、能不能读，交给 {@link #create} 在构造时 fail-fast —— 那一步同样发生在落库之前）。
     */
    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String dir = props.get("dir");
        if (dir == null || dir.isBlank()) {
            throw new IllegalArgumentException(
                    "catalog '" + catalogName + "' 缺少必填属性 catalog.csvdir.dir，可用属性："
                            + describeProperties());
        }
        String delimiter = props.get("delimiter");
        if (delimiter != null && delimiter.length() != 1) {
            throw new IllegalArgumentException(
                    "属性 catalog.csvdir.delimiter 必须是单个字符，当前为 [" + delimiter + "]");
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        Path root = Path.of(props.get("dir").trim()).toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException(
                    "catalog '" + catalogName + "' 的 dir 不是可读目录: " + root);
        }
        String delimiter = props.getOrDefault("delimiter", ",");
        return new CsvDirConnector(catalogName, root, delimiter.charAt(0));
    }
}
