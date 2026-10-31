package com.dojao.server.master;

/**
 * Job 提交请求 —— CLI / JDBC / WEB 各渠道提交查询的统一入参。
 *
 * @param sql          查询 SQL
 * @param user         提交用户
 * @param channel      提交渠道
 * @param parallelism  请求并行度（&lt;=0 表示由分配策略按集群资源自行决定）
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public record JobRequest(String sql, String user, JobChannel channel, int parallelism) {

    /** 便捷构造：并行度交分配策略决定。 */
    public static JobRequest of(String sql, String user, JobChannel channel) {
        return new JobRequest(sql, user, channel, 0);
    }
}
