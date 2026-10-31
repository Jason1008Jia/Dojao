package com.dojao.core.rpc;

/**
 * Arrow Flight 控制面 action 类型常量（Master 查询入口）。
 *
 * <p>客户端（dojao-jdbc / 测试 Client）与 Master（{@code MasterQueryFlightProducer}）
 * 通过 {@code doAction(EXECUTE_QUERY, QueryRequest bytes)} 提交 SQL。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/24
 */
public final class FlightActions {

    /** Client submits SQL query, Master returns results（请求体 = QueryRequest，响应体 = QueryResponse）。 */
    public static final String EXECUTE_QUERY = "execute_query";

    private FlightActions() {
    }
}
