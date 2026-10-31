package com.dojao.spi.service;

import java.util.Map;

/**
 * 连接器工厂 SPI —— 每种连接器类型一个实现，由 {@link java.util.ServiceLoader} 发现
 * （声明在 {@code META-INF/services/com.dojao.spi.service.ConnectorFactory}）。
 *
 * <p>这是第三方扩展的入口：写一个实现 + 一行 services 声明，打成 jar 丢进
 * {@code lib/connectors/} 即可，<b>不必改 Dojao 任何代码</b>（加载规则见
 * {@link ConnectorRegistry}）。</p>
 *
 * <p>只有 {@link #type()} 和 {@link #create} 是必须实现的，其余都有默认实现 ——
 * 契约刻意做得小，让「能跑起来」的成本最低；{@link #displayName()} /
 * {@link #describeProperties()} 这类只是让人用得更顺，不实现不影响功能。</p>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/7/09
 */
public interface ConnectorFactory {

    /**
     * 连接器类型标识（如 {@code "mysql"} / {@code "hive3"}）——
     * {@code CREATE CATALOG ... USING <type>} 里的那个词，全局唯一、大小写敏感。
     *
     * <p>本类型也是 catalog 属性键的命名空间：属性一律写成
     * {@code catalog.<type>.<属性名>}（如 {@code catalog.mysql.jdbcUrl}），
     * 归一化见 {@link com.dojao.spi.util.CatalogProperties#normalize}。</p>
     */
    String type();

    /** 按 catalog 名与属性构造连接器实例。属性来自 {@code CREATE CATALOG ... WITH (...)}。 */
    DojaoConnector create(String catalogName, Map<String, String> properties);

    /**
     * 展示名（{@code SHOW CATALOGS} / Web UI / CLI 里给人看的名字）。
     * 默认取 {@link #type()}；想让用户看到 {@code "MySQL"} 而不是 {@code "mysql"} 就覆写它。
     */
    default String displayName() {
        return type();
    }

    /**
     * 本工厂编译时针对的 SPI 契约版本。
     *
     * <p>用途是让「插件太老」这件事<b>当场可见</b>：{@link ConnectorRegistry} 加载时会比对
     * {@link ConnectorRegistry#API_VERSION}，高于当前服务端版本 → 记一条失败记录并拒绝注册，
     * 而不是等某个方法签名悄悄对不上才在运行时炸。</p>
     */
    default int apiVersion() {
        return ConnectorRegistry.API_VERSION;
    }

    /**
     * 本连接器认识的属性说明（属性键 → 一句话说明）：{@code jdbcUrl} 该长什么样、
     * 哪些必填、有哪些可选项。
     *
     * <p>引擎拿它拼错误提示（属性写错时直接列出可用项），Web UI 拿它渲染建 catalog 的表单。
     * 返回空 Map = 未声明，功能不受影响，只是用户得自己翻文档。</p>
     */
    default Map<String, String> describeProperties() {
        return Map.of();
    }

    /**
     * 落库前的准入校验（可选钩子）—— 属性缺失 / 写法不对时<b>尽快</b>抛
     * {@link IllegalArgumentException}，别等到落库后第一次查询才暴露。
     *
     * <p>默认空实现。注意这里<b>只做静态检查</b>（键是否存在、格式是否合法），
     * 不要去连数据源 —— 真正的构造与连通性由 {@link #create} 负责，
     * 那一步已经在落库之前跑（见 {@code MetadataExecutor} 的 CREATE CATALOG 顺序）。</p>
     */
    default void validate(String catalogName, Map<String, String> properties) {
    }
}
