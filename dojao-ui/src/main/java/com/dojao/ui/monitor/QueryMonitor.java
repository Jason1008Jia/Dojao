package com.dojao.ui.monitor;

import java.util.List;

/**
 * 查询监控契约 —— dojao-ui 唯一依赖的外部能力面，实现在 dojao-server。
 *
 * <p>模块边界：{@code dojao-server → dojao-ui}，ui 不得反向依赖 server。故监控/
 * SQL 页需要的全部「读 Master 实时执行信息 + 异步提交查询」能力抽象成此接口，
 * 由 {@code MasterServer.start()} 注入实现（{@code WebUiModule.bind}），DojaoUiApp
 * 只面对这层薄契约与中性 DTO，不感知 server 任何类型。</p>
 *
 * <p>所有实现需线程安全：REST 处理器并发调用。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public interface QueryMonitor {

    /**
     * 任务列表（含最近完成/失败的历史，按 queryId 时间戳倒序）。监控页「任务列表」。
     */
    List<MonitorJob> listQueries();

    /**
     * 单查询完整快照（算子 DAG + 每算子任务进度）。点开一个任务后渲染执行图。
     * 未知 queryId 返回 operators 为空的 {@link MonitorQuery}。
     */
    MonitorQuery snapshot(String queryId);

    /**
     * 集群总览：Master 自身 + 健康 Worker + 活动/最近查询。监控页「集群概览」。
     */
    MonitorCluster cluster();

    /**
     * 各节点资源时间序列（CPU / 内存 / 堆 / 线程数），监控页折线图。
     * 每节点最多返回 {@code maxPoints} 个最新的点（时间升序）。启动后逐步积累，无历史回填。
     */
    MonitorHistory history(int maxPoints);

    /**
     * 异步提交一条 SQL（真实走查询执行线，立即返回受理 queryId，后台异步执行）。
     * SQL 页「执行」。语法/校验失败 → state=REJECTED 且 message 带原因。
     */
    default MonitorSubmit submit(String sql) {
        return submit(sql, null);
    }

    /**
     * 异步提交一条 SQL（带视角归属）。
     *
     * @param owner 登录用户名；{@code null} = 全局（CLI/JDBC/admin）。用于 catalog / 函数隔离。
     */
    MonitorSubmit submit(String sql, String owner);

    /**
     * 取消一条进行中的查询（置 CANCELED，执行循环检测后停拉并释放）。SQL 页「取消」。
     */
    void cancel(String queryId);

    /**
     * 分页拉取已出结果的查询预览页（偏移量 + 单页行数）。查询未完成返回 RUNNING 态。
     * SQL 页「结果表格」。
     */
    MonitorResult result(String queryId, int offset, int limit);

    /**
     * 已注册的 catalog 名称列表。首页「catalog → schema → table」目录树顶层。
     */
    default List<String> listCatalogs() {
        return listCatalogs(null);
    }

    /**
     * 对指定视角可见的 catalog 名称列表（全局 + 归属该视角的私有 catalog）。
     *
     * @param viewer 登录用户名；{@code null} = 看全部（admin / 无登录）。
     */
    List<String> listCatalogs(String viewer);

    /**
     * 指定 catalog 下的 schema 列表。目录树展开 catalog 时调用。
     */
    List<String> listSchemas(String catalog);

    /**
     * 指定 catalog.schema 下的 table 列表。目录树展开 schema 时调用。
     */
    List<String> listTables(String catalog, String schema);

    /**
     * 指定表的列信息（列名 / 类型 / 注释 / 可空）。目录树展开 table 时调用。
     */
    List<MonitorColumn> listColumns(String catalog, String schema, String table);

    /**
     * 全部可用函数（内置 + 自定义），SQL 编辑器补全用。按名字排序。
     */
    default List<MonitorFunction> listFunctions() {
        return listFunctions(null);
    }

    /**
     * 对指定视角可见的函数（内置 + 自定义，按 is_private 过滤），SQL 编辑器补全用。
     *
     * @param viewer 登录用户名；{@code null} = 看全部（admin / 无登录）。
     */
    List<MonitorFunction> listFunctions(String viewer);
}
