package com.dojao.server.function;

import com.dojao.core.operator.BatchAggregateFunction;
import com.dojao.spi.function.CollectAggregateFunction;
import com.dojao.core.function.scalar.BatchScalarFunction;
import com.dojao.core.rpc.udf.UdfControl;
import com.dojao.core.rpc.udf.UdfMeta;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.ScalarFunction;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.List;

/**
 * Python 函数桥 —— SPI 代理，引擎只认 {@link ScalarFunction} / {@link AggregateFunction} /
 * {@link CollectAggregateFunction}，不感知 Python（无痕）。
 *
 * <p>把 SPI 的<b>逐行</b>调用转成对 sidecar 的 <b>按批 Flight doExchange</b>（§7.7 请求-响应完成语义），
 * 用户 Python 异常以 {@link SidecarException.Kind#USER_CODE_ERROR} 穿透（traceback 在 message）。</p>
 *
 * <p><b>sidecar 按 function_ref（Python 约定 module.ClassName）解析函数</b>：控制包
 * {@code UdfControl.function_ref} 填 {@link #className}（sidecar FunctionHub 只认 module.ClassName 导入）；
 * SQL 函数名（def.name）仅用于 Java 侧注册表键与 SPI {@link #functionName()} 展示，不参与 sidecar 解析。</p>
 *
 * <p><b>批量捷径（性能墙消除）</b>：同时实现 {@link BatchScalarFunction} / {@link BatchAggregateFunction}
 * （core 的批量扩展接口，SPI 逐行契约不动）。引擎在批量边界上检测到桥实现批量接口后，整批调用一次
 * Flight doExchange —— Python 标量投影/过滤从"每行一次往返"降到"每 batch 一次"；Python 聚合从"每行
 * 一次 ACCUMULATE"降到"每 group 每 page 一次"。聚合状态用 {@link PyState}（可变 holder）：sidecar
 * 累加后返回的新状态写回 holder，满足 SPI "原地修改 state" 契约，跨批累加正确。</p>
 *
 * <p><b>collect-evaluate 型</b>：本桥还实现 {@link CollectAggregateFunction} —— 构造时带装饰器声明的
 * 输入列 + 输出 Arrow 类型（{@code @collect_aggregate} 元数据）。引擎已按 key 分组，{@link #evaluateBatch}
 * 收到的是每 key 一行的 (key 列 + list 值列) batch（值列 = List，每行 = 该 key 攒齐的全部值），一次
 * {@code COLLECT_EVALUATE} 发往 sidecar，sidecar 逐行取 {@code list.value(row)} 逐 key 调 evaluate
 * （无切组），返回 N 行结果列（每 key 一行）。</p>
 */
public final class PythonFunctionBridge implements ScalarFunction, BatchScalarFunction,
        AggregateFunction<PythonFunctionBridge.PyState>, BatchAggregateFunction<PythonFunctionBridge.PyState>,
        CollectAggregateFunction {

    /** 聚合状态：不透明字节的可变 holder（sidecar 返回的新状态原地写回）。 */
    public static final class PyState {
        byte[] bytes;

        PyState(byte[] bytes) {
            this.bytes = bytes;
        }
    }

    private final SidecarPool manager;
    private final BufferAllocator allocator;
    private final String functionName;
    private final String className;
    private final List<String> collectInputs;
    private final ArrowType collectOutputType;

    /** 非 collect 型构造（标量/增量聚合）：collect 元数据留空。 */
    public PythonFunctionBridge(SidecarPool manager, String functionName, String className) {
        this(manager, functionName, className, List.of(), new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE));
    }

    /** collect-evaluate 型构造：输入列 + 输出 Arrow 类型来自 sidecar 回传的结构化签名（PythonBackend 解析）。 */
    public PythonFunctionBridge(SidecarPool manager, String functionName, String className,
                                List<String> collectInputs, ArrowType collectOutputType) {
        this.manager = manager;
        this.allocator = manager.allocator();
        this.functionName = functionName;
        this.className = className;
        this.collectInputs = collectInputs;
        this.collectOutputType = collectOutputType;
    }

    // ==================== 标量（1→1） ====================

    @Override
    public Object evaluate(Object[] inputs) {
        try (VectorSchemaRoot input = ArrowRowCodec.encodeRowRoot(allocator, inputs)) {
            UdfControl control = UdfControl.newBuilder()
                    .setFunctionRef(className)
                    .setOp(UdfControl.Op.EVAL)
                    .build();
            return invoke(control, input);
        }
    }

    /**
     * 批量标量求值（{@link BatchScalarFunction}）—— 整批一次 Flight 往返，消除逐行性能墙。
     *
     * <p>sidecar EVAL 已按"批进批出"执行（{@code fn.evaluate(batch)} 返回整批 RecordBatch）；
     * 结果整批复制到独立向量返回（列名 = outputName），行数必须与输入一致（fail-fast 防下游错位）。</p>
     */
    @Override
    public FieldVector evaluateBatch(VectorSchemaRoot root, BufferAllocator allocator, String outputName) {
        int n = root.getRowCount();
        UdfControl control = UdfControl.newBuilder()
                .setFunctionRef(className)
                .setOp(UdfControl.Op.EVAL)
                .build();
        SidecarPool.InvokeBatch r = invokeBatchRaw(control, root);
        try (VectorSchemaRoot result = r.resultRoot()) {
            int outRows = result.getRowCount();
            if (outRows != n) {
                throw new IllegalStateException(
                        "Python 标量函数批量求值结果行数 " + outRows + " != 输入行数 " + n
                                + "（函数 " + functionName + "）—— 批量函数须返回与输入等长的结果列");
            }
            FieldVector src = result.getVector(0);
            FieldVector out = new Field(outputName, src.getField().getFieldType(), src.getField().getChildren())
                    .createVector(allocator);
            out.setInitialCapacity(n);
            out.allocateNew();
            TransferPair tp = src.makeTransferPair(out);
            for (int p = 0; p < n; p++) tp.copyValueSafe(p, p);
            out.setValueCount(n);
            return out;
        }
    }

    // ==================== 聚合（CREATE_STATE / ACCUMULATE / MERGE / EVALUATE） ====================

    @Override
    public PyState createState() {
        UdfControl control = UdfControl.newBuilder()
                .setFunctionRef(className)
                .setOp(UdfControl.Op.CREATE_STATE)
                .build();
        return new PyState(invokeMeta(control, null).getOpaqueState().toByteArray());
    }

    @Override
    public void accumulate(PyState state, Object[] inputs) {
        try (VectorSchemaRoot input = ArrowRowCodec.encodeRowRoot(allocator, inputs)) {
            UdfControl control = UdfControl.newBuilder()
                    .setFunctionRef(className)
                    .setOp(UdfControl.Op.ACCUMULATE)
                    .setOpaqueState(ByteString.copyFrom(state.bytes))
                    .build();
            state.bytes = invokeMeta(control, input).getOpaqueState().toByteArray(); // 原地写回新状态
        }
    }

    /**
     * 批量聚合累加（{@link BatchAggregateFunction}）—— 一组 N 行一次 Flight 往返。
     *
     * <p>sidecar ACCUMULATE 契约本就是"整批并入 state"（{@code fn.accumulate(state, batch)} 接收
     * 整批 RecordBatch）；一次调用把整组并入，语义等价于逐行 {@link #accumulate} N 次。结果仅取
     * metadata 里的新状态写回 holder，返回的批根（空）随后关闭。</p>
     */
    @Override
    public void accumulateBatch(PyState state, VectorSchemaRoot batch) {
        UdfControl control = UdfControl.newBuilder()
                .setFunctionRef(className)
                .setOp(UdfControl.Op.ACCUMULATE)
                .setOpaqueState(ByteString.copyFrom(state.bytes))
                .build();
        SidecarPool.InvokeBatch r = invokeBatchRaw(control, batch);
        try (VectorSchemaRoot ignored = r.resultRoot()) {
            state.bytes = r.meta().getOpaqueState().toByteArray(); // 原地写回新状态
        }
    }

    @Override
    public void merge(PyState target, PyState source) {
        UdfControl control = UdfControl.newBuilder()
                .setFunctionRef(className)
                .setOp(UdfControl.Op.MERGE)
                .setOpaqueState(ByteString.copyFrom(target.bytes))
                .setSourceState(ByteString.copyFrom(source.bytes))
                .build();
        target.bytes = invokeMeta(control, null).getOpaqueState().toByteArray(); // 原地写回合并后状态
    }

    @Override
    public Object evaluate(PyState state) {
        UdfControl control = UdfControl.newBuilder()
                .setFunctionRef(className)
                .setOp(UdfControl.Op.EVALUATE)
                .setOpaqueState(ByteString.copyFrom(state.bytes))
                .build();
        return invoke(control, null);
    }

    // ==================== Function 元数据 ====================

    @Override
    public String functionName() {
        return functionName;
    }

    @Override
    public String display() {
        return "Python 函数: " + className + "（sidecar）";
    }

    // ==================== CollectAggregateFunction（collect-evaluate 型） ====================

    @Override
    public List<String> inputColumns() {
        return collectInputs;
    }

    @Override
    public ArrowType outputType() {
        return collectOutputType;
    }

    /**
     * collect 批量求值：{@code batch} = [key 列..., list<值列>...]（每 key 一行，引擎已按 key 分组，
     * 值列 = List 列，每行 = 该 key 攒齐的全部值）。一次 COLLECT_EVALUATE 发往 sidecar
     * （逐行取 list.value(row) → 逐 key 调用户 evaluate，无切组）→ N 行结果列。
     */
    @Override
    public FieldVector evaluateBatch(VectorSchemaRoot batch, int keyColumnCount,
                                     BufferAllocator allocator, String outputName) {
        UdfControl control = UdfControl.newBuilder()
                .setFunctionRef(className)
                .setOp(UdfControl.Op.COLLECT_EVALUATE)
                .setKeyColumnCount(keyColumnCount)
                .build();
        SidecarPool.InvokeBatch r = invokeBatchRaw(control, batch);
        try (VectorSchemaRoot result = r.resultRoot()) {
            int outRows = result.getRowCount();
            FieldVector src = result.getVector(0);
            FieldVector out = new Field(outputName, src.getField().getFieldType(), src.getField().getChildren())
                    .createVector(allocator);
            out.setInitialCapacity(outRows);
            out.allocateNew();
            TransferPair tp = src.makeTransferPair(out);
            for (int p = 0; p < outRows; p++) tp.copyValueSafe(p, p);
            out.setValueCount(outRows);
            return out;
        }
    }

    // ==================== 内部 ====================

    /** 标量结果走这里：解码首值返回。 */
    private Object invoke(UdfControl control, VectorSchemaRoot input) {
        return check(invokeRaw(control, input)).value();
    }

    /** 聚合状态走这里：只取返回 metadata（新状态）。 */
    private UdfMeta invokeMeta(UdfControl control, VectorSchemaRoot input) {
        return check(invokeRaw(control, input)).meta();
    }

    private SidecarPool.InvokeResult invokeRaw(UdfControl control, VectorSchemaRoot input) {
        try {
            return manager.acquire().invoke(control, input);
        } catch (FlightRuntimeException e) {
            throw mapFlightError(e);
        }
    }

    /** 批量调用：invokeBatch 内部已对 UdfMeta.status=ERROR fail-fast（抛 USER_CODE_ERROR），传输层失败在此分类。 */
    private SidecarPool.InvokeBatch invokeBatchRaw(UdfControl control, VectorSchemaRoot input) {
        try {
            return manager.acquire().invokeBatch(control, input);
        } catch (FlightRuntimeException e) {
            throw mapFlightError(e);
        }
    }

    /** §7.7 第④层业务感知：UdfMeta.status=ERROR → 用户代码异常（traceback 在 message）。 */
    private static SidecarPool.InvokeResult check(SidecarPool.InvokeResult r) {
        if (r.meta().getStatus() == UdfMeta.Status.ERROR) {
            throw new SidecarException(SidecarException.Kind.USER_CODE_ERROR, r.meta().getMessage());
        }
        return r;
    }

    /** 传输层失败分类（§7.7 看门狗 + 进程崩溃兜底）。Arrow 15：FlightStatusCode + CallStatus.TIMED_OUT。 */
    private static SidecarException mapFlightError(FlightRuntimeException e) {
        CallStatus status = e.status();
        if (status.code() == FlightStatusCode.UNAVAILABLE) {
            return new SidecarException(SidecarException.Kind.CRASHED,
                    "sidecar 连接不可用: " + status.description(), e);
        }
        if (status.code() == FlightStatusCode.TIMED_OUT) {
            return new SidecarException(SidecarException.Kind.WATCHDOG_TIMEOUT,
                    "sidecar 调用超时（看门狗）", e);
        }
        return new SidecarException(SidecarException.Kind.CRASHED,
                "Flight 调用失败: " + status.code() + " " + status.description(), e);
    }
}
