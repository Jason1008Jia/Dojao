package com.dojao.server.master;

import java.util.List;
import java.util.Map;

/**
 * 整查询实时快照 —— {@link QueryManager#snapshot()} 每次采集返回，不可变。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public record QuerySnapshot(
        String queryId,
        String sql,
        QueryState state,
        long submitTimeNanos,
        long startTimeNanos,
        long endTimeNanos,
        /** 从开始到当前/结束的墙钟毫秒。 */
        long wallMs,
        /** 根算子输出行数（= 最终结果行数）。 */
        long totalRowsOut,
        /** 当前重试次数（1 起）。 */
        int attempts,
        int retryLimit,
        /** 计划树顺序的算子快照（第 0 个为根算子）。 */
        List<OperatorSnapshot> operators,
        /** 已记录的算子失败（operatorId → 失败信息）。 */
        Map<String, String> operatorFailures) {

    /** 全部算子的输入行数合计。 */
    public long totalRowsIn() {
        return operators.stream().mapToLong(OperatorSnapshot::rowsIn).sum();
    }

    /** 是否仍处于活跃执行（可被客户端轮询）。 */
    public boolean isActive() {
        return state == QueryState.SUBMITTED || state == QueryState.RUNNING;
    }
}
