package com.dojao.cli;

import com.dojao.core.rpc.FlightActions;
import com.dojao.core.rpc.QueryRequest;
import com.dojao.core.rpc.QueryResponse;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.memory.RootAllocator;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 直连 Master 的 Arrow Flight 查询客户端：{@code doAction("execute_query", QueryRequest)}，
 * 逐块消费 {@link QueryResponse}（每块 10 万行，见 QueryServiceImpl）并<b>边收边推</b>给
 * {@link RowSink}，不在客户端物化全量结果 —— 客户端内存与结果行数无关。
 *
 * <p>与 JDBC 驱动（{@code DojaoStatement}）的唯一差别：JDBC 只读首个 {@link Result}（丢块，
 * 已知限制），这里循环到 {@code hasNext()} 为 false。</p>
 *
 * @author jiaxin
 * @date 2026/9/17
 */
final class FlightQueryClient implements AutoCloseable {

    /** 结果接收器：列头只推一次（首块），之后逐行推。 */
    interface RowSink {
        /** 列头到达。 */
        void begin(List<String> columns);

        /** 一行数据到达。 */
        void row(List<String> values);
    }

    /** 一次查询的统计。 */
    static final class Stats {
        /** 服务端 queryId（用于 /api/query/{id}/cancel 等外部接口）。 */
        String queryId = "";
        /** 收到的总行数。 */
        long rows;
        /** 端到端耗时（毫秒，含传输）。 */
        long elapsedMs;
    }

    /** 面向用户的查询失败（消息已友好化，直接打印即可）。 */
    static final class QueryException extends RuntimeException {
        QueryException(String message) {
            super(message);
        }
    }

    private final String host;
    private final int port;
    private final RootAllocator allocator;
    private final FlightClient client;

    FlightQueryClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        try {
            this.client = FlightClient.builder()
                    .location(Location.forGrpcInsecure(host, port))
                    .allocator(allocator)
                    .maxInboundMessageSize(Integer.getInteger("DojaoMaxMessageBytes", 512 << 20))
                    .build();
        } catch (RuntimeException e) {
            allocator.close();
            throw new QueryException("无法连接 Master " + host + ":" + port + " —— " + rootMessage(e));
        }
    }

    /**
     * 启动握手：真发一次 RPC（{@code listActions}），确认 Master 在线 —— 客户端启动即调用，
     * <b>Master 没起来直接失败退出</b>，而不是等用户敲完 SQL 才发现连不上。
     *
     * <p>{@code FlightClient} 是懒连接（构造不建连），所以必须有一次真实往返才能判定可达。
     * {@code listActions} 是 Master 已实现且<b>无副作用</b>的控制面调用（不产生查询、不落监控），
     * 适合做探活。超时 {@code -Dsystem.cli.connectTimeoutMs}（默认 5000ms）。</p>
     *
     * @throws QueryException 连不上（服务未启动 / 端口不对 / 超时）
     */
    void connect() {
        int timeoutMs = Integer.getInteger("system.cli.connectTimeoutMs", 5000);
        try {
            for (ActionType ignored
                    : client.listActions(CallOptions.timeout(timeoutMs, TimeUnit.MILLISECONDS))) {
                // 只为触发一次真实 RPC，忽略 Master 声明的 action 列表
            }
        } catch (FlightRuntimeException e) {
            throw new QueryException(friendly(e));
        }
    }

    /**
     * 执行一条 SQL（无参查询 / 写语句 / DDL 均可），结果逐块推给 {@code sink}。
     *
     * @return 本次查询统计（queryId / 行数 / 耗时）
     * @throws QueryException 连接失败或服务端执行失败
     */
    Stats execute(String sql, RowSink sink) {
        Stats stats = new Stats();
        long startNanos = System.nanoTime();
        QueryRequest request = QueryRequest.newBuilder().setSql(sql).build();
        try {
            Iterator<Result> results = client.doAction(
                    new Action(FlightActions.EXECUTE_QUERY, request.toByteArray()));
            boolean first = true;
            while (results.hasNext()) {
                byte[] body = results.next().getBody();
                QueryResponse resp = body == null || body.length == 0
                        ? QueryResponse.getDefaultInstance()
                        : QueryResponse.parseFrom(body);
                if (stats.queryId.isEmpty() && !resp.getQueryId().isEmpty()) {
                    stats.queryId = resp.getQueryId();
                }
                if (first) {
                    first = false;
                    if (!resp.getColumnsList().isEmpty()) {
                        List<String> cols = new java.util.ArrayList<>(resp.getColumnsCount());
                        for (com.dojao.core.rpc.Column c : resp.getColumnsList()) {
                            cols.add(c.getName());
                        }
                        sink.begin(cols);
                    }
                }
                for (com.dojao.core.rpc.Row row : resp.getRowsList()) {
                    sink.row(row.getValuesList());
                    stats.rows++;
                }
            }
        } catch (FlightRuntimeException e) {
            throw new QueryException(friendly(e));
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new QueryException("响应解析失败：" + e.getMessage());
        } finally {
            stats.elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        }
        return stats;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception ignored) {
            // 关闭失败无需打扰用户
        } finally {
            allocator.close();
        }
    }

    String host() {
        return host;
    }

    int port() {
        return port;
    }

    // ==================== 错误友好化 ====================

    private String friendly(FlightRuntimeException e) {
        String desc = e.status().description();
        switch (e.status().code()) {
            case UNAVAILABLE:
                return "无法连接 Master " + host + ":" + port
                        + "（服务未启动或端口不对）。原始错误：" + desc;
            case TIMED_OUT:
                return "查询超时：" + desc;
            case CANCELLED:
                return "查询被取消：" + desc;
            case INTERNAL:
                return "执行失败：" + desc;
            default:
                return "查询失败(" + e.status().code() + ")：" + desc;
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
