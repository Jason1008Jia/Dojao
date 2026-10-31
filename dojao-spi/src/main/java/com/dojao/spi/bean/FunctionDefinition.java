package com.dojao.spi.bean;

import java.util.Map;
import java.util.Optional;

/**
 * 函数定义 —— CREATE FUNCTION 的持久化形态，也是多语言 {@code FunctionBackend} 的统一输入。
 *
 * <p>{@code type} 为<b>开放集合</b>：java / python 已实装；rust / go / r / julia … 由注册进
 * {@code BackendRegistry} 的后端解析，引擎本身零改动。</p>
 *
 * @param name       函数名（v1 全限定；可与 catalog 绑定）
 * @param type       开放集合：java | python | rust | go | r | julia | …
 * @param jarPath    jar 路径（java）；其他语言为 code_path（源码/包/二进制）
 * @param className  实现入口：java 全限定类名；其他语言 module.ClassName（点分）
 * @param properties 原始 WITH 属性（type 专属：model_path / sha256 / endpoint …）
 * @param comment    函数说明
 * @param createdAt  创建时间
 * @param updatedAt  更新时间
 * @param owner      归属创建者（Web 控制台登录用户名）；CLI/JDBC 或未登录创建 = {@code null}（全局）。
 *                   仅当 {@code WITH (is_private='true')} 时用于可见性隔离（见 {@link #isPrivate()}）。
 */
public record FunctionDefinition(
        String name,
        String type,
        String jarPath,
        String className,
        Map<String, String> properties,
        String comment,
        String createdAt,
        String updatedAt,
        String owner
) {

    /** 向后兼容：无 owner 的 8 参构造（CLI / 未登录 / 旧调用点 → 归属为空 = 全局）。 */
    public FunctionDefinition(String name, String type, String jarPath, String className,
                              Map<String, String> properties, String comment,
                              String createdAt, String updatedAt) {
        this(name, type, jarPath, className, properties, comment, createdAt, updatedAt, null);
    }

    /** 读取 WITH 属性，缺失返回 empty。 */
    public Optional<String> property(String key) {
        return properties == null ? Optional.empty() : Optional.ofNullable(properties.get(key));
    }

    /** 代码/jar 路径的语义别名（非 java 后端读它拿到 code_path）。 */
    public String codePath() {
        return jarPath;
    }

    /** 是否标记为私有：{@code WITH (is_private='true')}（大小写不敏感）。 */
    public boolean isPrivate() {
        String v = property("is_private").orElse(null);
        return v != null && "true".equalsIgnoreCase(v.trim());
    }
}
