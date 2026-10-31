package com.dojao.server.master;

import com.dojao.common.util.Errors;
import com.dojao.core.rpc.FlightActions;
import com.dojao.core.rpc.QueryRequest;
import com.dojao.core.rpc.QueryResponse;
import com.dojao.grammar.analyzer.exception.SemanticException;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master 查询入口的 Arrow Flight 生产者 —— 客户端经 {@code doAction("execute_query")} 提交 SQL，
 * 请求体 = {@link QueryRequest}（protobuf bytes），响应体 = {@link QueryResponse}；
 * SELECT 结果<b>分块流式回传</b>（多次 {@code onNext}，客户端从 doAction 的
 * {@code Iterator<Result>} 逐块读取 = 背压），EXPLAIN / DDL / 写语句为单块。
 *
 * <p>SemanticException → INVALID_ARGUMENT，其它异常 → INTERNAL。其余 FlightProducer 方法
 * （listFlights/getStream/acceptPut 等）均按未实现处理，本服务只暴露 doAction 控制面。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/24
 */
public final class MasterQueryFlightProducer implements FlightProducer {

    private static final Logger log = LoggerFactory.getLogger(MasterQueryFlightProducer.class);

    private final QueryServiceImpl queryService;

    public MasterQueryFlightProducer(QueryServiceImpl queryService) {
        this.queryService = queryService;
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        if (!FlightActions.EXECUTE_QUERY.equals(action.getType())) {
            listener.onError(CallStatus.UNIMPLEMENTED
                    .withDescription("未知 action: " + action.getType()).toRuntimeException());
            return;
        }
        try {
            QueryRequest request = QueryRequest.parseFrom(action.getBody());
            // SELECT 分块实时流式回传：executeQuery 每攒满一块就回调一次 onNext（master 内存有界 =
            // 有界结果队列 + 当前一块，客户端逐块消费即背压）；EXPLAIN / DDL / 写语句为单块。
            queryService.executeQuery(request, response ->
                    listener.onNext(new Result(response.toByteArray())));
            listener.onCompleted();
        } catch (SemanticException e) {
            log.warn("SQL 语义错误: {}", e.getMessage());
            listener.onError(CallStatus.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).toRuntimeException());
        } catch (Exception e) {
            log.error("查询执行失败", e);
            // 回给客户端整条 cause 链（日志里是完整栈）：外壳消息（"Failed to list schemas" /
            // "Operator X next failed"）没有信息量，真因在链底（Access denied / Unknown database /
            // Table 'x' already exists …）—— 见 Errors.describe。
            listener.onError(CallStatus.INTERNAL
                    .withDescription(Errors.describe(e)).toRuntimeException());
        }
    }

    // ==================== 未使用：本服务只暴露 doAction ====================

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("Master 查询走 doAction，不用 getFlightInfo").toRuntimeException();
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Master 查询走 doAction，不用 getStream").toRuntimeException());
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return () -> ackStream.onError(CallStatus.UNIMPLEMENTED
                .withDescription("Master 查询走 doAction，不用 acceptPut").toRuntimeException());
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onNext(new ActionType(FlightActions.EXECUTE_QUERY, "Client submits SQL query, Master returns results"));
        listener.onCompleted();
    }
}
