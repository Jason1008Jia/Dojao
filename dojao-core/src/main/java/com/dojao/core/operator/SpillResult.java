package com.dojao.core.operator;

/**
 * 溢写操作结果 —— 描述一次 {@link OperatorMemoryHandle#spill(long)} 调用的结果。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/19
 */
public record SpillResult(
        /** 实际释放的内存字节数（溢写后腾出的堆外 Arrow 内存） */
        long bytesFreed,
        /** 本次溢写写入磁盘的字节数 */
        long bytesSpilled,
        /** 溢写是否完成：true = 已达到目标 / 没有更多可溢写数据 */
        boolean complete) {

    /** 无操作结果 —— 没有可溢写的数据或不需要溢写。 */
    public static SpillResult noOp() {
        return new SpillResult(0, 0, true);
    }

    /** 部分溢写结果 —— 释放了一些内存但未达到目标。 */
    public static SpillResult partial(long bytesFreed, long bytesSpilled) {
        return new SpillResult(bytesFreed, bytesSpilled, false);
    }

    /** 完全溢写结果 —— 已达到目标释放量。 */
    public static SpillResult completed(long bytesFreed, long bytesSpilled) {
        return new SpillResult(bytesFreed, bytesSpilled, true);
    }
}
