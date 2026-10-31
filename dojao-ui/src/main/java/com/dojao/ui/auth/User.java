package com.dojao.ui.auth;

/**
 * 控制台用户 —— 对外表示不含口令哈希（哈希只在 {@link AuthStore} 内部比对，绝不出库到 REST）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/17
 */
public record User(String username, Role role, String displayName) {

    public boolean isAdmin() {
        return role == Role.ADMIN;
    }
}
