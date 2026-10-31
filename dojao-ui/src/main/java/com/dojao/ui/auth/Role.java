package com.dojao.ui.auth;

/**
 * 控制台用户角色 —— 决定 AI 对话的能力边界与提示词（前端顶栏也会展示角色标签）。
 *
 * <p>三个角色：</p>
 * <ul>
 *   <li>{@link #ADMIN}：管理员 —— 开发者与分析师能力的并集，且可创建 / 删除用户；</li>
 *   <li>{@link #DEVELOPER}：开发者 —— 数据能力 + 集群测试、压测、日志分析、查询/算子诊断；</li>
 *   <li>{@link #ANALYST}：分析师 —— 数据探查、报表、分析与挖掘，不能使用集群开发类工具。</li>
 * </ul>
 *
 * <p>旧版本的 {@code USER} 角色统一迁移为 {@link #ANALYST}（最小权限）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/17
 */
public enum Role {

    /** 管理员：全部能力，含用户管理。 */
    ADMIN,

    /** 开发者：数据能力 + 集群测试 / 压测 / 日志分析 / 查询与算子诊断。 */
    DEVELOPER,

    /** 分析师：数据探查 / 报表 / 数据分析与挖掘（无集群开发能力）。 */
    ANALYST;

    /** 宽松解析（大小写不敏感，未知值回退 {@link #ANALYST}，不抛异常）。 */
    public static Role of(String name) {
        if (name == null) {
            return ANALYST;
        }
        String v = name.trim().toUpperCase();
        if ("USER".equals(v)) {
            return ANALYST;   // 旧版本角色：普通用户 → 分析师
        }
        try {
            return valueOf(v);
        } catch (IllegalArgumentException e) {
            return ANALYST;
        }
    }

    /**
     * 严格解析（大小写不敏感；未知值抛 {@link IllegalArgumentException}）。
     * 用于管理员新建 / 修改用户时的入参校验 —— 不能把拼错的角色静默降级。
     */
    public static Role parse(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("角色不能为空（可选：ADMIN / DEVELOPER / ANALYST）");
        }
        String v = name.trim().toUpperCase();
        if ("USER".equals(v)) {
            return ANALYST;
        }
        try {
            return valueOf(v);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("未知角色: " + name + "（可选：ADMIN / DEVELOPER / ANALYST）");
        }
    }

    /** 是否管理员。 */
    public boolean isAdmin() {
        return this == ADMIN;
    }

    /** 是否可以使用集群开发类工具（集群状态 / 节点日志 / 查询诊断 / 算子诊断）。 */
    public boolean canUseClusterTools() {
        return this != ANALYST;
    }
}
