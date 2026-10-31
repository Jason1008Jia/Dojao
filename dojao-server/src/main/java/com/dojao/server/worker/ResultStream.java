package com.dojao.server.worker;

import com.dojao.common.data.Page;
import com.dojao.core.operator.RowAccumulator;
import com.dojao.core.rpc.control.ControlActions;
import com.dojao.core.rpc.control.ResultEventMsg;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker → Master 的<b>结果回传流</b>（doExchange "result/{taskId}"，单生产者）。
 *
 * <p>镜像 {@code NetworkExchangeChannel.SenderStream} 的数据面，但<b>单执行线程 + 心跳线程</b>：
 * 结果由单个任务执行线程生产（{@code FragmentExecutor.runAndStream} / 扫描回传），另有心跳线程
 * 在阻塞算子期间穿插 {@link #progress()} —— 发送方法全部 synchronized 串行（Flight 客户端写端
 * 非线程安全）。数据发送直接同步 {@code putNext}，gRPC 出站流控窗口耗尽即阻塞 = 背压（Master
 * 消费慢 → 本线程阻塞 → 上游算子阻塞）。每批：Arrow batch = 数据页（FINISHED/FAILED 为 0 行
 * 空批，PROGRESS 为 metadata-only 无 body），metadata = {@link ResultEventMsg} 承载 kind / cause。</p>
 *
 * <p><b>惰性建流</b>：构造只登记 client/taskId，<b>不建流</b>——首条数据 / 终止事件 / 心跳才
 * {@code client.doExchange(...)}（见 {@link #listener()}）。与交换 {@code SenderStream} 的
 * "临发前建流"一致：建流后立即 start + putNext，数据即刻流出，杜绝"派发时建流 + 聚合/扫描
 * 闲置十几秒 → gRPC 静默取消"（Master 侧开流 ~10ms 即 "client cancelled"）。阻塞式算子的心跳
 * 每 {@code DojaoHeartbeatMs} 一发，建流后流持续活跃，不会被静默取消。Producer 片段从不回传
 * → 从不建流，{@link #close()} 对未建流 no-op。</p>
 *
 * <p><b>页所有权</b>：{@link #sendPage} 把源页拷入流根后立即 {@code page.close()}（所有权移交），
 * 调用方不得再 close —— 与交换发送端语义一致。</p>
 *
 * <p><b>客户端所有权</b>：构造传入的 {@code client} 是 {@code NetworkMasterGateway.openResultClient}
 * 建的<b>结果回传专用缓存客户端</b>（本 Worker 唯一一条、所有 result 流共享；分配器与流根同 root，
 * 零拷贝安全）。本类<b>只使用不关闭</b>——{@link #close()} 仅 half-close 请求流，客户端由 Worker
 * 停止时 {@code NetworkMasterGateway.close()} 统一关闭（half-close 仍会把已入队的 FINISHED 送抵 Master）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
final class ResultStream {

    private static final Logger log = LoggerFactory.getLogger(ResultStream.class);

    private final FlightClient client;
    private final String taskId;
    private final BufferAllocator allocator;
    private final AtomicBoolean closed = new AtomicBoolean();
    /** 流的复用根（首条 DATA 定 schema；无数据的流用空 schema）。 */
    private volatile VectorSchemaRoot streamRoot;
    /** 发送侧失败（putNext 抛错）→ 后续 send 立即抛。 */
    private volatile Throwable failure;
    /** 惰性打开的写端：首条数据 / 终止事件才建流（见 {@link #listener()}）。 */
    private volatile FlightClient.ClientStreamListener listener;
    /** 建流互斥锁（首个 send 线程唯一建流，单生产者场景串行无竞态）。 */
    private final Object streamLock = new Object();

    ResultStream(FlightClient client, String taskId, BufferAllocator allocator) {
        this.client = client;
        this.taskId = taskId;
        this.allocator = allocator;
    }

    /**
     * 惰性取 doExchange 写端：首条数据 / 终止事件才建流 —— <b>镜像交换 {@code SenderStream} 的
     * "临发前建流"</b>。派发时只构造对象不建流，避免"建流后长期闲置（聚合 / 扫描要十几秒）被
     * gRPC 静默取消"（Master 侧表现为 doExchange 开流后 ~10ms 即 "client cancelled"，一页未送断流）；
     * 首条 send 建流后立即 start + putNext，数据即刻流出，与可用交换通道同构。
     */
    private FlightClient.ClientStreamListener listener() {
        FlightClient.ClientStreamListener l = listener;
        if (l == null) {
            synchronized (streamLock) {
                l = listener;
                if (l == null) {
                    l = client.doExchange(
                            FlightDescriptor.path(ControlActions.RESULT_PREFIX, taskId)).getWriter();
                    listener = l;
                }
            }
        }
        return l;
    }

    /**
     * 心跳（无 body）：metadata-only 消息（{@code putMetadata}），<b>不建流 schema、不锁 root</b>。
     * 可在首个数据页到达前（阻塞式聚合 Phase 1 / 溢写 / 排序期间）安全发送 —— 流 schema 由首个
     * DATA / 终止事件经 {@code start(root)} 建立，心跳只是穿插的 metadata，接收端 {@code
     * getLatestMetadata()} 即取到。任务执行线程与心跳线程并发发送，故与 sendPage/sendControl
     * 同为 synchronized 串行。
     */
    synchronized void progress() {
        checkFailure();
        try {
            listener();   // 惰性建流：心跳是首事件时打开 doExchange
            putMetadata(ResultEventMsg.newBuilder().setKind(ResultEventMsg.Kind.PROGRESS).build());
        } catch (RuntimeException e) {
            log.warn("ResultStream progress 失败: {}", e.toString());
            failure = e;
            throw e;
        }
    }

    /** 一页数据：拷入流根 → putNext(DATA) → 关闭源页（全量拷贝）。 */
    synchronized void sendPage(Page page) {
        checkFailure();
        VectorSchemaRoot src = page.getRoot();
        try {
            if (streamRoot == null) {
                streamRoot = VectorSchemaRoot.create(src.getSchema(), allocator);
                streamRoot.allocateNew();
                listener().start(streamRoot);
            }
            int rc = page.getRowCount();
            for (int col = 0; col < src.getFieldVectors().size(); col++) {
                FieldVector s = src.getVector(col);
                FieldVector d = streamRoot.getVector(col);
                d.setInitialCapacity(rc);
                d.allocateNew();
                RowAccumulator.copyColumnBatch(s, d, 0, 0, rc);
                d.setValueCount(rc);
            }
            streamRoot.setRowCount(rc);
            putNext(ResultEventMsg.newBuilder()
                    .setKind(ResultEventMsg.Kind.DATA)
                    .setBatchIndex(page.getBatchIndex())
                    .build());
        } catch (RuntimeException e) {
            log.warn("ResultStream sendPage 失败 rows={} cols={}", page.getRowCount(),
                    page.getRoot().getFieldVectors().size(), e);
            failure = e;
            throw e;
        } finally {
            page.close();   // 拷贝完成，源页所有权移交
        }
    }

    /** 正常完成：0 行空批 + FINISHED(rowCount) → 关闭流。rowCount = 分布式写本分区写入行数。 */
    synchronized void finished(long rowCount) {
        sendControl(ResultEventMsg.newBuilder()
                .setKind(ResultEventMsg.Kind.FINISHED).setRowCount(rowCount).build());
        close();
    }

    /** 无行数完成（扫描 / SELECT 回传路径，行数恒 0）。 */
    synchronized void finished() {
        finished(0);
    }

    /** 失败：0 行空批 + FAILED(cause) → 关闭流。 */
    synchronized void failed(String cause) {
        sendControl(ResultEventMsg.newBuilder()
                .setKind(ResultEventMsg.Kind.FAILED).setCause(cause).build());
        close();
    }

    /**
     * 关闭流（任务线程被中断 / 取消时由派发 handler 的 finally 调用，主动收尾避免流悬挂）。
     * 幂等：只 half-close 请求流（{@code listener.completed()}），<b>不关客户端</b>——本流持有的
     * 是 {@code NetworkMasterGateway.openResultClient} 建的<b>缓存共享客户端</b>（本 Worker 所有
     * result 流共用一条），由 Worker 停止时 {@code NetworkMasterGateway.close()} 统一关闭。
     * half-close 会把已入队的 FINISHED 消息与请求结束标记送抵 Master。
     */
    synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // 从未建流（Producer 片段从不回传 / 无数据即失败前直接收尾）→ 无需 half-close，no-op
        FlightClient.ClientStreamListener l = listener;
        if (l != null) {
            try {
                l.completed();
            } catch (Throwable ignored) {
            }
        }
        // 释放复用根的 Arrow buffer：每条结果流一个 root，不关会随任务数累积在受管直接内存里
        // （监控页「直接内存」迟迟不回落）。completed() 之后 Flight 已不再引用 root。
        VectorSchemaRoot root = streamRoot;
        streamRoot = null;
        if (root != null) {
            try {
                root.close();
            } catch (Throwable ignored) {
            }
        }
    }

    private synchronized void sendControl(ResultEventMsg control) {
        checkFailure();
        try {
            if (streamRoot == null) {
                streamRoot = VectorSchemaRoot.create(new Schema(List.of()), allocator);
                listener().start(streamRoot);
            }
            streamRoot.setRowCount(0);
            putNext(control);
        } catch (RuntimeException e) {
            log.warn("ResultStream sendControl 失败 kind={}", control.getKind(), e);
            failure = e;
            throw e;
        }
    }

    private void putNext(ResultEventMsg control) {
        byte[] ctrl = control.toByteArray();
        ArrowBuf metaBuf = allocator.buffer(ctrl.length);
        metaBuf.setBytes(0, ctrl);
        metaBuf.writerIndex(ctrl.length);
        // putNext 接管 metaBuf 所有权（写入后由 Flight 负责释放），勿再 close —— 否则双释放 RefCnt 为负
        listener.putNext(metaBuf);   // gRPC 流控背压（阻塞）
    }

    /** metadata-only 心跳：只发 app_metadata、无 Arrow body，不依赖流 schema（首个数据页前可用）。 */
    private synchronized void putMetadata(ResultEventMsg control) {
        byte[] ctrl = control.toByteArray();
        ArrowBuf metaBuf = allocator.buffer(ctrl.length);
        metaBuf.setBytes(0, ctrl);
        metaBuf.writerIndex(ctrl.length);
        // putMetadata 接管 metaBuf 所有权（同 putNext），勿再 close
        listener.putMetadata(metaBuf);   // gRPC 流控背压（阻塞）
    }

    private void checkFailure() {
        if (failure != null) {
            throw new RuntimeException("result 流已失败: " + failure);
        }
    }
}
