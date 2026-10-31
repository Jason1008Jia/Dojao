package com.dojao.server.function;

/**
 * Sidecar 失败分类 —— 语言无关的运行时失败归一（对应设计文档 §7.7 失败分类表）。
 *
 * <p>把五层感知（进程层 / 控制面探活 / 业务层 status / 看门狗 / 协议）归一到语义化异常，
 * 让上层（FunctionManager / 查询执行）能给出可诊断的错误信息。不绑定任何具体语言 ——
 * Python / Rust / Go … 的 sidecar 失败都走本异常。</p>
 */
public final class SidecarException extends RuntimeException {

    /** 失败类别（与 §7.7 失败分类表一一对应）。 */
    public enum Kind {
        /** spawn 失败（运行时 / 依赖缺失，如无 python / pyarrow）→ 该节点该运行时不可用 */
        NO_RUNTIME_ENV,
        /** 进程启动后探活失败 / 进程即退 */
        STARTUP_FAILED,
        /** 运行中进程退出 / Flight 连接重置 */
        CRASHED,
        /** 资源耗尽被杀（退出码 137 / 信号） */
        RESOURCE_EXHAUSTED,
        /** 用户函数代码异常（message 携带 traceback，§7.7 第④层业务感知） */
        USER_CODE_ERROR,
        /** 协议版本不兼容（sidecar 实现的版本不在引擎兼容区间） */
        PROTOCOL_MISMATCH,
        /** 无响应超时（看门狗，§7.7 第⑤层）→ 判 LOST */
        WATCHDOG_TIMEOUT
    }

    private final Kind kind;

    public SidecarException(Kind kind, String message) {
        super(kind + ": " + message);
        this.kind = kind;
    }

    public SidecarException(Kind kind, String message, Throwable cause) {
        super(kind + ": " + message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
