package com.dojao.core.operator;

import com.dojao.common.data.Page;

import java.util.concurrent.BlockingQueue;

/**
 * 分区数据源 —— {@link HashJoinOperator} 对上游分区器依赖的最小接口
 * （{@code partitionQueue / numPartitions / distributorFailure / open / close}）。
 *
 * <p>这是"单机 / 分布式双模"的接缝：同一份 join 算子代码，两种输入来源。
 * <ul>
 *   <li>{@link HashPartitionOperator}（发送端）：后台分发线程把上游数据灌进本地分区队列
 *       （单机）或经 {@code ExchangeChannel} 推到远程 Worker（分布式）；</li>
 *   <li>{@link ShuffleReader}（分布式接收端）：接收各发送方推来的数据页，缓冲进本地有界队列。</li>
 * </ul>
 *
 * <p>{@link HashJoinOperator} 只依赖该接口 —— 数据来自本地分区器还是跨机 shuffle，
 * 对它是不可见的；单机路径行为完全不变。
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/8/14
 */
public interface PartitionedSource {

    /** 分区数。 */
    int numPartitions();

    /** 指定分区的输出队列（并行 join 工作线程按分区号消费）。 */
    BlockingQueue<Page> partitionQueue(int pid);

    /** 源 / 分发失败信息（无异常返回 null；消费端据此 rethrow，防止静默缺数据）。 */
    Throwable distributorFailure();

    /** 打开数据源（HashPartition 启动后台分发线程；ShuffleReader 由发送方驱动，可 no-op）。 */
    void open() throws Exception;

    /** 关闭并释放资源。 */
    void close() throws Exception;
}
