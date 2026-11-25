package com.dojao.core.operator;

import com.dojao.common.data.Page;

/**
 * 类描述：TODO 实现这个接口的算子，将会变成可观察的算子，这里会在webui中可视化展示出来
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11 23:45
 */
public interface MeasurableOperator extends Operator {
    // 1. 基础钩子，在 open/next/close 时自动记录
    void onStart();
    void onFinish();

    /** 标记算子失败（查询失败时由 QueryManager 统一调用，将未完成算子置为 FAILED）。 */
    void markFailed(Throwable t);

    // 2. 返回当前统计信息的快照
    OperatorStats getStats();

    // 3. 内存管理钩子，供资源管理器调用
    long getCurrentMemoryBytes();
    void addSpilledBytes(long bytes); // 由溢写管理器回调

    /**
     * 采样本算子溢写目录的实际磁盘占用，返回峰值字节数（无溢写的算子恒 0）。
     *
     * <p>默认取统计里已记录的峰值：可溢写算子覆盖为真实目录遍历（归并重写 / 删除旧文件后
     * 读数会回落，峰值才与 {@code du} 对得上），无目录的算子无需实现。</p>
     */
    default long sampleSpillDiskBytes() {
        return getStats().getPeakSpillDiskBytes();
    }

    // 4. 性能和吞吐量更新
    void recordInput(Page page); // 在 next() 中调用
    void recordOutput(Page page); // 在输出页时调用
    void recordCPUTime(long nanos); // 在关键路径上记录


}
