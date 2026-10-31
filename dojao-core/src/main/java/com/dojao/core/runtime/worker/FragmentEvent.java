package com.dojao.core.runtime.worker;

import com.dojao.common.data.Page;

/**
 * 片段执行结果信封 —— 消费者片段把 join 输出页 / 终止事件回传给 Master。
 *
 * <p>与 {@link ScanEvent}（扫描任务回传）对称的第三种回传信封：任务终止二选一，
 * <b>失败显式回传、绝不静默</b>：
 * <ul>
 *   <li>{@link FragmentData}：一页 join 输出数据（页所有权随信封移交 Master）；</li>
 *   <li>{@link FragmentProgress}：心跳 —— 任务仍在推进（上游 shuffle 持续收页）但长时间无结果回传，
 *       防止 Master 结果看门狗误杀阻塞式算子（聚合 Phase 1 / 溢写 / 排序）的慢查询；</li>
 *   <li>{@link FragmentFinished}：正常跑完（所有数据页先于本事件入队）；</li>
 *   <li>{@link FragmentFailed}：执行异常（Master 收到即 fail-fast，不得当成数据完整）。</li>
 * </ul>
 *
 * <p>单机伪分布式版跨线程传递 {@link Page} 引用；跨进程版由传输层（Arrow Flight）
 * 序列化 Arrow batch（与 ScanEvent 同构）。</p>
 *
 * @author jiaxin
 * @version 2.1
 * @date 2026/8/16
 */
public sealed interface FragmentEvent
        permits FragmentEvent.FragmentData, FragmentEvent.FragmentProgress,
        FragmentEvent.FragmentFinished, FragmentEvent.FragmentFailed {

    /** 一页 join 输出数据。 */
    record FragmentData(String taskId, Page page) implements FragmentEvent {
    }

    /**
     * 心跳：任务仍在推进但长时间无结果回传。Master 看门狗收到即重置计时（不当作数据 / 完成）。
     * 由 Worker 心跳线程在「有输入 shuffle 收页 + 超过心跳间隔无回传」时发送（见 FragmentExecutor）。
     */
    record FragmentProgress(String taskId) implements FragmentEvent {
    }

    /**
     * 片段正常跑完（所有数据页已先入队）。
     *
     * @param rowCount 本分区写走的行数（分布式写路径 runAndSink 回传；SELECT 回传路径恒 0）。
     *                 数字不是数据 —— 经 ResultEventMsg FINISHED metadata 传输，不进结果数据面。
     */
    record FragmentFinished(String taskId, long rowCount) implements FragmentEvent {
        /** SELECT 回传路径（无写目标）：不关心行数。 */
        public FragmentFinished(String taskId) {
            this(taskId, 0);
        }
    }

    /** 片段执行失败（Master 收到即 fail-fast）。 */
    record FragmentFailed(String taskId, String cause) implements FragmentEvent {
    }
}
