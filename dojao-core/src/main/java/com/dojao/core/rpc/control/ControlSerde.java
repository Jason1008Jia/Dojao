package com.dojao.core.rpc.control;

import com.dojao.core.operator.CaseProjection;
import com.dojao.core.operator.FragmentSpec;
import com.dojao.core.operator.GroupKeyExpression;
import com.dojao.core.operator.JoinType;
import com.dojao.core.operator.WindowFilterConfig;
import com.dojao.core.operator.WindowFrame;
import com.dojao.core.operator.WindowFunctionSpec;
import com.dojao.core.operator.WindowFunctionType;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
import com.dojao.common.memory.AllocatorUsage;
import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.Split;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 真分布式控制面编解码 —— 域对象 ↔ {@code control.proto} 消息（纯序列化，零网络依赖）。
 *
 * <p>覆盖四组载荷：
 * <ul>
 *   <li><b>节点 / 资源</b>：{@link WorkerNode} ↔ {@code WorkerNodeMsg}，
 *       {@link ResourceSummary} ↔ {@code ResourceSummaryMsg}，{@link WorkerState} ↔ 枚举；</li>
 *   <li><b>读取请求</b>：{@link ReadRequest} ↔ {@code ReadRequestMsg}
 *       （{@code Optional} 字段用空串 / 空 repeated 表示未设）；</li>
 *   <li><b>片段</b>：{@link FragmentSpec} 全家族（Producer / Consumer / AggregateConsumer /
 *       WindowConsumer / ChainedJoinConsumer / MiddleJoinConsumer + 各步骤）↔
 *       {@code FragmentSpecMsg}（oneof 路由），聚合 / 窗口函数以字符串名传输、
 *       Worker 侧按名重建（与伪分布式同构，不引用运行期算子）；</li>
 *   <li><b>窗口默认值</b>：{@link WindowFunctionSpec#defaultValue()}（{@code Object}，可空）
 *       ↔ {@code WindowFunctionSpecMsg} 的 {@code default_value_variant} oneof。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class ControlSerde {

    private ControlSerde() {
    }

    // ==================== 节点 & 资源 & 状态 ====================

    public static WorkerNodeMsg toNode(WorkerNode n) {
        return WorkerNodeMsg.newBuilder()
                .setWorkerId(n.workerId())
                .setHost(n.host())
                .setPort(n.port())
                .setVersion(n.version())
                .setStartTimeNanos(n.startTimeNanos())
                .build();
    }

    public static WorkerNode fromNode(WorkerNodeMsg m) {
        return new WorkerNode(m.getWorkerId(), m.getHost(), m.getPort(),
                m.getVersion(), m.getStartTimeNanos());
    }

    public static ResourceSummaryMsg toResource(ResourceSummary r) {
        return ResourceSummaryMsg.newBuilder()
                .setCpuCores(r.cpuCores())
                .setCpuUsagePct(r.cpuUsagePct())
                .setTotalMemoryBytes(r.totalMemoryBytes())
                .setUsedMemoryBytes(r.usedMemoryBytes())
                .setTotalDiskBytes(r.totalDiskBytes())
                .setFreeDiskBytes(r.freeDiskBytes())
                .setMaxConcurrency(r.maxConcurrency())
                .setTimestampNanos(r.timestampNanos())
                .setHeapUsedBytes(r.heapUsedBytes())
                .setHeapMaxBytes(r.heapMaxBytes())
                .setThreadCount(r.threadCount())
                .setDirectUsedBytes(r.directUsedBytes())
                .setDirectBudgetBytes(r.directBudgetBytes())
                .setDiskReadBytes(r.diskReadBytes())
                .setDiskWriteBytes(r.diskWriteBytes())
                .setProcessRssBytes(r.processRssBytes())
                .addAllAllocators(r.allocators().stream()
                        .map(a -> AllocatorUsageMsg.newBuilder()
                                .setCategory(a.category())
                                .setAllocatedBytes(a.allocatedBytes())
                                .setPeakBytes(a.peakBytes())
                                .setLimitBytes(a.limitBytes())
                                .build())
                        .toList())
                .build();
    }

    public static ResourceSummary fromResource(ResourceSummaryMsg m) {
        return new ResourceSummary(m.getCpuCores(), m.getCpuUsagePct(),
                m.getTotalMemoryBytes(), m.getUsedMemoryBytes(),
                m.getTotalDiskBytes(), m.getFreeDiskBytes(),
                m.getMaxConcurrency(), m.getTimestampNanos(),
                m.getHeapUsedBytes(), m.getHeapMaxBytes(), m.getThreadCount(),
                m.getDirectUsedBytes(), m.getDirectBudgetBytes(),
                m.getAllocatorsList().stream()
                        .map(u -> new AllocatorUsage(u.getCategory(), u.getAllocatedBytes(),
                                u.getPeakBytes(), u.getLimitBytes()))
                        .toList(),
                m.getDiskReadBytes(), m.getDiskWriteBytes(), m.getProcessRssBytes());
    }

    public static WorkerStateMsg toState(WorkerState s) {
        return WorkerStateMsg.valueOf(s.name());
    }

    public static WorkerState fromState(WorkerStateMsg m) {
        return WorkerState.valueOf(m.name());
    }

    // ==================== 读取请求 ====================

    public static ReadRequestMsg toRead(ReadRequest r) {
        ReadRequestMsg.Builder b = ReadRequestMsg.newBuilder()
                .setCatalog(r.catalog())
                .setSchema(r.schema())
                .setTable(r.table())
                .setOffset(r.offset())
                .setLimit(r.limit());
        r.columns().ifPresent(b::addAllColumns);
        r.filterExpression().ifPresent(b::setFilterExpression);
        return b.build();
    }

    public static ReadRequest fromRead(ReadRequestMsg m) {
        List<String> columns = m.getColumnsList();
        String filter = m.getFilterExpression();
        return new ReadRequest(m.getCatalog(), m.getSchema(), m.getTable(),
                columns.isEmpty() ? Optional.empty() : Optional.of(columns),
                filter.isEmpty() ? Optional.empty() : Optional.of(filter),
                m.getOffset(), m.getLimit());
    }

    // ==================== 片段步骤 ====================

    private static ScanStepMsg toScanStep(FragmentSpec.ScanStep s) {
        ScanStepMsg.Builder b = ScanStepMsg.newBuilder()
                .setCatalog(s.catalog())
                .setRequest(toRead(s.request()));
        // 切片私有载荷（连接器自定义字节）：空 = 整表切片（谓词型连接器 / 单切片扫描的常态）
        if (s.hasSplit()) {
            b.setSplitPayload(ByteString.copyFrom(s.split().payload()));
        }
        return b.build();
    }

    private static FragmentSpec.ScanStep fromScanStep(ScanStepMsg m) {
        // 空字节 → Split.WHOLE（Split.of 归一化），与老 Master 派发的载荷兼容
        return new FragmentSpec.ScanStep(m.getCatalog(), fromRead(m.getRequest()),
                Split.of(m.getSplitPayload().toByteArray()));
    }

    private static PartitionStepMsg toPartitionStep(FragmentSpec.PartitionStep p) {
        return PartitionStepMsg.newBuilder()
                .setExchangeId(p.exchangeId())
                .addAllKeyColumns(p.keyColumns())
                .setNumPartitions(p.numPartitions())
                .addAllOwnerWorkerIds(p.ownerWorkerIds())
                .addAllExchangeIds(p.exchangeIds())
                // 枚举名与 proto 值一一对应（HASH / REPLICATE / FIXED）；缺省 = DIST_HASH → 旧接收方按哈希执行
                .setDistribution(DistributionMsg.valueOf("DIST_" + p.distribution().name()))
                .setFixedBucket(p.fixedBucket())
                .build();
    }

    private static FragmentSpec.PartitionStep fromPartitionStep(PartitionStepMsg m) {
        // 缺省字段（DIST_HASH）→ 旧发送方的载荷仍按哈希分区还原，优雅降级
        String name = m.getDistribution().name();
        FragmentSpec.Distribution dist = FragmentSpec.Distribution.HASH;
        if (name.startsWith("DIST_")) {
            dist = FragmentSpec.Distribution.valueOf(name.substring("DIST_".length()));
        }
        return new FragmentSpec.PartitionStep(m.getExchangeId(), m.getKeyColumnsList(),
                m.getNumPartitions(), m.getOwnerWorkerIdsList(), m.getExchangeIdsList(),
                dist, m.getFixedBucket());
    }

    private static ExchangeInStepMsg toExchangeIn(FragmentSpec.ExchangeInStep e) {
        ExchangeInStepMsg.Builder b = ExchangeInStepMsg.newBuilder()
                .setExchangeId(e.exchangeId())
                .setNumPartitions(e.numPartitions())
                .setNumSenders(e.numSenders());
        boolean[] owned = e.owned();
        for (boolean o : owned) {
            b.addOwned(o);
        }
        return b.build();
    }

    private static FragmentSpec.ExchangeInStep fromExchangeIn(ExchangeInStepMsg m) {
        boolean[] owned = new boolean[m.getOwnedCount()];
        for (int i = 0; i < owned.length; i++) {
            owned[i] = m.getOwned(i);
        }
        return new FragmentSpec.ExchangeInStep(m.getExchangeId(),
                m.getNumPartitions(), m.getNumSenders(), owned);
    }

    private static JoinStepMsg toJoinStep(FragmentSpec.JoinStep j) {
        JoinStepMsg.Builder b = JoinStepMsg.newBuilder()
                .addAllLeftKeyColumns(j.leftKeyColumns())
                .addAllRightKeyColumns(j.rightKeyColumns())
                .setJoinType(JoinTypeMsg.valueOf(j.joinType().name()))
                .setMemoryLimit(j.memoryLimit())
                .setNumPartitions(j.numPartitions())
                .setAlgorithm(JoinAlgorithmMsg.valueOf(j.algorithm().name()));
        byte[] ipc = ArrowSchemaIpc.encode(j.declaredRightFields());
        if (ipc.length > 0) {
            b.setRightSchemaIpc(ByteString.copyFrom(ipc));
        }
        byte[] leftIpc = ArrowSchemaIpc.encode(j.declaredLeftFields());
        if (leftIpc.length > 0) {
            b.setLeftSchemaIpc(ByteString.copyFrom(leftIpc));
        }
        return b.build();
    }

    private static FragmentSpec.JoinStep fromJoinStep(JoinStepMsg m) {
        // 枚举名与 proto 值一一对应（HASH / SORT_MERGE）；缺省字段 = HASH → 旧发送方优雅降级
        return new FragmentSpec.JoinStep(m.getLeftKeyColumnsList(), m.getRightKeyColumnsList(),
                JoinType.valueOf(m.getJoinType().name()), m.getMemoryLimit(), m.getNumPartitions(),
                FragmentSpec.JoinAlgorithm.valueOf(m.getAlgorithm().name()),
                ArrowSchemaIpc.decode(m.getRightSchemaIpc().toByteArray()),
                ArrowSchemaIpc.decode(m.getLeftSchemaIpc().toByteArray()));
    }

    private static AggregationDtoMsg toAggregationDto(FragmentSpec.AggregationDto a) {
        // collect-evaluate 型 UDAF 的 inputColumn 为 null（增量型才用），实际收集列走
        // collect_input_columns；proto3 string builder 不接受 null → nz() 空串序列化，
        // worker 侧 isEmpty 还原 null。
        return AggregationDtoMsg.newBuilder()
                .setInputColumn(nz(a.inputColumn()))
                .setOutputColumn(a.outputColumn())
                .setAggregateName(a.aggregateName())
                .addAllCollectInputColumns(a.collectInputColumns())
                .build();
    }

    private static FragmentSpec.AggregationDto fromAggregationDto(AggregationDtoMsg m) {
        return new FragmentSpec.AggregationDto(
                m.getInputColumn().isEmpty() ? null : m.getInputColumn(),
                m.getOutputColumn(), m.getAggregateName(), m.getCollectInputColumnsList());
    }

    private static CaseProjectionMsg toCaseProjection(CaseProjection c) {
        CaseProjectionMsg.Builder b = CaseProjectionMsg.newBuilder()
                .setDerivedColumn(c.derivedColumn())
                .setElseValue(nz(c.elseValue()));
        for (CaseProjection.CaseBranch br : c.branches()) {
            b.addBranches(CaseBranchMsg.newBuilder()
                    .setCondition(br.condition())
                    .setValue(nz(br.value()))
                    .build());
        }
        return b.build();
    }

    private static CaseProjection fromCaseProjection(CaseProjectionMsg m) {
        List<CaseProjection.CaseBranch> branches = new ArrayList<>();
        for (CaseBranchMsg br : m.getBranchesList()) {
            // 分支结果值同属可空字符串字段（WHEN…THEN NULL / IF(cond, NULL, x) 计划期已 normalizeNull
            // 归一化为 null）；写侧 nz() 编成空串，此处必须还原 —— 否则 NULL → "" → compileValue("")
            // 把空串当"幽灵列引用"（column=""）→ 非命中行 root.getVector("") 返回 null → NPE。
            String value = br.getValue().isEmpty() ? null : br.getValue();
            branches.add(new CaseProjection.CaseBranch(br.getCondition(), value));
        }
        // elseValue 为可空字段（null = 无 ELSE → NULL）；toCaseProjection 用 nz() 把 null 编成空串，
        // 此处必须还原 —— 否则 ELSE-less CASE 到 Worker 侧 elseValue="" → compileValue("") 把空串当
        // 列引用（column=""）→ evaluate 对不命中行 root.getVector("") 返回 null → NPE。
        String elseValue = m.getElseValue().isEmpty() ? null : m.getElseValue();
        return new CaseProjection(m.getDerivedColumn(), branches, elseValue);
    }

    private static GroupKeyExpressionMsg toGroupKeyExpression(GroupKeyExpression g) {
        return GroupKeyExpressionMsg.newBuilder()
                .setDerivedColumn(g.derivedColumn())
                .setExprText(g.exprText())
                .build();
    }

    private static GroupKeyExpression fromGroupKeyExpression(GroupKeyExpressionMsg m) {
        return new GroupKeyExpression(m.getDerivedColumn(), m.getExprText());
    }

    // ==================== 多维聚合展开（ROLLUP / CUBE / GROUPING SETS） ====================

    private static ExpandStepMsg toExpandStep(FragmentSpec.ExpandStep e) {
        ExpandStepMsg.Builder b = ExpandStepMsg.newBuilder().addAllDimColumns(e.dimColumns());
        for (List<String> set : e.groupingSets()) {
            b.addGroupingSets(ExpandSetMsg.newBuilder().addAllMembers(set).build());
        }
        return b.build();
    }

    private static FragmentSpec.ExpandStep fromExpandStep(ExpandStepMsg m) {
        List<List<String>> sets = new ArrayList<>(m.getGroupingSetsCount());
        for (ExpandSetMsg s : m.getGroupingSetsList()) {
            sets.add(new ArrayList<>(s.getMembersList()));
        }
        return new FragmentSpec.ExpandStep(new ArrayList<>(m.getDimColumnsList()), sets);
    }

    private static AggregateStepMsg toAggregateStep(FragmentSpec.AggregateStep a) {
        AggregateStepMsg.Builder b = AggregateStepMsg.newBuilder()
                .addAllKeyColumns(a.keyColumns())
                .setMemoryLimit(a.memoryLimit())
                .setNumPartitions(a.numPartitions());
        for (FragmentSpec.AggregationDto dto : a.aggregations()) {
            b.addAggregations(toAggregationDto(dto));
        }
        for (CaseProjection c : a.caseProjections()) {
            b.addCaseProjections(toCaseProjection(c));
        }
        return b.build();
    }

    private static FragmentSpec.AggregateStep fromAggregateStep(AggregateStepMsg m) {
        return new FragmentSpec.AggregateStep(
                m.getKeyColumnsList(),
                m.getAggregationsList().stream().map(ControlSerde::fromAggregationDto).toList(),
                m.getCaseProjectionsList().stream().map(ControlSerde::fromCaseProjection).toList(),
                m.getMemoryLimit(), m.getNumPartitions());
    }

    private static WindowFunctionSpecMsg toWindowFunction(WindowFunctionSpec w) {
        WindowFunctionSpecMsg.Builder b = WindowFunctionSpecMsg.newBuilder()
                .setType(WindowFunctionTypeMsg.valueOf(w.type().name()))
                .setInputColumn(nz(w.inputColumn()))
                .setOutputColumn(nz(w.outputColumn()))
                .setOffset(w.offset());
        Object dv = w.defaultValue();
        if (dv != null) {
            if (dv instanceof Long || dv instanceof Integer || dv instanceof Short || dv instanceof Byte) {
                b.setDefaultInt(((Number) dv).longValue());
            } else if (dv instanceof Number n) {
                b.setDefaultDouble(n.doubleValue());
            } else if (dv instanceof Boolean bool) {
                b.setDefaultBool(bool);
            } else {
                b.setDefaultString(String.valueOf(dv));
            }
        }
        b.setFunctionName(nz(w.functionName()));
        if (w.frame() != null) {
            b.setFrame(WindowFrameMsg.newBuilder()
                    .setStartBack(w.frame().startBack())
                    .setEndBack(w.frame().endBack())
                    .build());
        }
        return b.build();
    }

    private static WindowFunctionSpec fromWindowFunction(WindowFunctionSpecMsg m) {
        Object dv = null;
        switch (m.getDefaultValueVariantCase()) {
            case DEFAULT_INT -> dv = m.getDefaultInt();
            case DEFAULT_DOUBLE -> dv = m.getDefaultDouble();
            case DEFAULT_STRING -> dv = m.getDefaultString();
            case DEFAULT_BOOL -> dv = m.getDefaultBool();
            default -> dv = null;
        }
        WindowFrame frame = m.hasFrame()
                ? new WindowFrame(m.getFrame().getStartBack(), m.getFrame().getEndBack())
                : null;
        return new WindowFunctionSpec(
                WindowFunctionType.valueOf(m.getType().name()),
                m.getInputColumn().isEmpty() ? null : m.getInputColumn(),
                m.getOutputColumn(),
                m.getOffset(), dv,
                m.getFunctionName().isEmpty() ? null : m.getFunctionName(),
                frame);
    }

    private static WindowStepMsg toWindowStep(FragmentSpec.WindowStep w) {
        WindowStepMsg.Builder b = WindowStepMsg.newBuilder()
                .addAllPartitionBy(w.partitionBy())
                .addAllOrderBy(w.orderBy())
                .addAllAscending(w.ascending())
                .setMemoryLimit(w.memoryLimit())
                .setNumPartitions(w.numPartitions());
        for (WindowFunctionSpec f : w.functions()) {
            b.addFunctions(toWindowFunction(f));
        }
        if (w.filter() != null) b.setFilter(toWindowFilter(w.filter()));
        return b.build();
    }

    private static WindowFilterMsg toWindowFilter(WindowFilterConfig f) {
        return WindowFilterMsg.newBuilder()
                .setFunctionType(WindowFunctionTypeMsg.valueOf(f.functionType().name()))
                .setTargetValue(f.targetValue())
                .setAggregateColIndex(f.aggregateColIndex())
                .setNoOrderBy(f.noOrderBy())
                .setOp(WindowFilterOpMsg.valueOf("WINDOW_FILTER_" + f.operator().name()))
                .build();
    }

    private static WindowFilterConfig fromWindowFilter(WindowFilterMsg m) {
        WindowFunctionType type = WindowFunctionType.valueOf(m.getFunctionType().name());
        WindowFilterConfig.FilterOp op =
                WindowFilterConfig.FilterOp.valueOf(m.getOp().name().substring("WINDOW_FILTER_".length()));
        return new WindowFilterConfig(type, m.getTargetValue(),
                m.getAggregateColIndex(), m.getNoOrderBy(), op);
    }

    private static FragmentSpec.WindowStep fromWindowStep(WindowStepMsg m) {
        return new FragmentSpec.WindowStep(m.getPartitionByList(), m.getOrderByList(),
                m.getAscendingList(),
                m.getFunctionsList().stream().map(ControlSerde::fromWindowFunction).toList(),
                m.getMemoryLimit(), m.getNumPartitions(),
                m.hasFilter() ? fromWindowFilter(m.getFilter()) : null);
    }

    private static SinkStepMsg toSinkStep(FragmentSpec.SinkStep s) {
        SinkStepMsg.Builder b = SinkStepMsg.newBuilder()
                .setCatalog(s.catalog())
                .setSchema(s.schema())
                .setTable(s.table());
        if (s.targetColumns() != null) {
            b.addAllTargetColumns(s.targetColumns());
        }
        return b.build();
    }

    private static FragmentSpec.SinkStep fromSinkStep(SinkStepMsg m) {
        List<String> cols = m.getTargetColumnsList();
        return new FragmentSpec.SinkStep(m.getCatalog(), m.getSchema(), m.getTable(),
                cols.isEmpty() ? null : cols);
    }

    /** 片内 top-N 截断尾 ↔ proto（可空；空 = 不下推）。 */
    public static SortLimitStepMsg toSortLimitStep(FragmentSpec.SortLimitStep t) {
        return SortLimitStepMsg.newBuilder()
                .addAllOrderByKeys(t.orderByKeys())
                .addAllAscending(t.ascending())
                .setOffset(t.offset())
                .setLimit(t.limit())
                .setMemoryLimit(t.memoryLimit())
                .build();
    }

    /** proto ↔ 片内 top-N 截断尾（可空；空 = 不下推）。 */
    public static FragmentSpec.SortLimitStep fromSortLimitStep(SortLimitStepMsg m) {
        return new FragmentSpec.SortLimitStep(m.getOrderByKeysList(), m.getAscendingList(),
                m.getOffset(), m.getLimit(), m.getMemoryLimit());
    }

    // ==================== 片段（oneof 路由） ====================

    public static FragmentSpecMsg toFragment(FragmentSpec spec) {
        FragmentSpecMsg.Builder b = FragmentSpecMsg.newBuilder();
        switch (spec) {
            case FragmentSpec.Producer p -> {
                ProducerMsg.Builder pb = ProducerMsg.newBuilder()
                        .setFragmentId(p.fragmentId())
                        .setSelfWorkerId(p.selfWorkerId())
                        .setScan(toScanStep(p.scan()))
                        .setPartition(toPartitionStep(p.partition()));
                for (GroupKeyExpression g : p.groupKeyExpressions()) {
                    pb.addGroupKeyExpressions(toGroupKeyExpression(g));
                }
                if (p.expand() != null) {
                    pb.setExpand(toExpandStep(p.expand()));
                }
                b.setProducer(pb.build());
            }
            case FragmentSpec.Consumer c -> {
                ConsumerMsg.Builder cb = ConsumerMsg.newBuilder()
                        .setFragmentId(c.fragmentId())
                        .setSelfWorkerId(c.selfWorkerId())
                        .setLeftIn(toExchangeIn(c.leftIn()))
                        .setRightIn(toExchangeIn(c.rightIn()))
                        .setJoin(toJoinStep(c.join()));
                if (c.sink() != null) {
                    cb.setSink(toSinkStep(c.sink()));
                }
                if (c.tail() != null) {
                    cb.setTail(toSortLimitStep(c.tail()));
                }
                b.setConsumer(cb.build());
            }
            case FragmentSpec.AggregateConsumer a -> {
                AggregateConsumerMsg.Builder ab = AggregateConsumerMsg.newBuilder()
                        .setFragmentId(a.fragmentId())
                        .setSelfWorkerId(a.selfWorkerId())
                        .setIn(toExchangeIn(a.in()))
                        .setAggregate(toAggregateStep(a.aggregate()));
                if (a.followUp() != null) {
                    ab.setFollowUp(toAggregateStep(a.followUp()));
                }
                if (a.sink() != null) {
                    ab.setSink(toSinkStep(a.sink()));
                }
                if (a.tail() != null) {
                    ab.setTail(toSortLimitStep(a.tail()));
                }
                b.setAggregateConsumer(ab.build());
            }
            case FragmentSpec.WindowConsumer w -> {
                WindowConsumerMsg.Builder wb = WindowConsumerMsg.newBuilder()
                        .setFragmentId(w.fragmentId())
                        .setSelfWorkerId(w.selfWorkerId())
                        .setIn(toExchangeIn(w.in()))
                        .setWindow(toWindowStep(w.window()));
                if (w.sink() != null) {
                    wb.setSink(toSinkStep(w.sink()));
                }
                if (w.tail() != null) {
                    wb.setTail(toSortLimitStep(w.tail()));
                }
                b.setWindowConsumer(wb.build());
            }
            case FragmentSpec.ChainedJoinConsumer cj -> {
                ChainedJoinConsumerMsg.Builder mb = ChainedJoinConsumerMsg.newBuilder()
                        .setFragmentId(cj.fragmentId())
                        .setSelfWorkerId(cj.selfWorkerId())
                        .setLeftIn(toExchangeIn(cj.leftIn()))
                        .setRightIn(toExchangeIn(cj.rightIn()))
                        .setJoin(toJoinStep(cj.join()));
                switch (cj.followUp()) {
                    case FragmentSpec.WindowStep w -> mb.setWindow(toWindowStep(w));
                    case FragmentSpec.AggregateStep a -> mb.setAggregate(toAggregateStep(a));
                }
                if (cj.sink() != null) {
                    mb.setSink(toSinkStep(cj.sink()));
                }
                if (cj.tail() != null) {
                    mb.setTail(toSortLimitStep(cj.tail()));
                }
                b.setChainedJoinConsumer(mb.build());
            }
            case FragmentSpec.MiddleJoinConsumer mj -> {
                MiddleJoinConsumerMsg.Builder mjb = MiddleJoinConsumerMsg.newBuilder()
                        .setFragmentId(mj.fragmentId())
                        .setSelfWorkerId(mj.selfWorkerId())
                        .setLeftIn(toExchangeIn(mj.leftIn()))
                        .setRightIn(toExchangeIn(mj.rightIn()))
                        .setJoin(toJoinStep(mj.join()))
                        .setNextPartition(toPartitionStep(mj.nextPartition()));
                if (mj.expand() != null) {
                    mjb.setExpand(toExpandStep(mj.expand()));
                }
                b.setMiddleJoinConsumer(mjb.build());
            }
            case FragmentSpec.MiddleAggregateConsumer ma -> {
                MiddleAggregateConsumerMsg.Builder mb = MiddleAggregateConsumerMsg.newBuilder()
                        .setFragmentId(ma.fragmentId())
                        .setSelfWorkerId(ma.selfWorkerId())
                        .setIn(toExchangeIn(ma.in()))
                        .setAggregate(toAggregateStep(ma.aggregate()))
                        .setNextPartition(toPartitionStep(ma.nextPartition()));
                if (ma.filterAfterAggregate() != null && !ma.filterAfterAggregate().isBlank()) {
                    mb.setFilterAfterAggregate(ma.filterAfterAggregate());
                }
                b.setMiddleAggregateConsumer(mb.build());
            }
            case FragmentSpec.MiddleWindowConsumer mw -> {
                MiddleWindowConsumerMsg m = MiddleWindowConsumerMsg.newBuilder()
                        .setFragmentId(mw.fragmentId())
                        .setSelfWorkerId(mw.selfWorkerId())
                        .setIn(toExchangeIn(mw.in()))
                        .setWindow(toWindowStep(mw.window()))
                        .setNextPartition(toPartitionStep(mw.nextPartition()))
                        .build();
                b.setMiddleWindowConsumer(m);
            }
        }
        return b.build();
    }

    public static FragmentSpec fromFragment(FragmentSpecMsg msg) {
        switch (msg.getSpecCase()) {
            case PRODUCER -> {
                ProducerMsg m = msg.getProducer();
                List<GroupKeyExpression> gks = m.getGroupKeyExpressionsList().stream()
                        .map(ControlSerde::fromGroupKeyExpression).toList();
                FragmentSpec.Producer producer = new FragmentSpec.Producer(m.getFragmentId(),
                        m.getSelfWorkerId(), fromScanStep(m.getScan()), fromPartitionStep(m.getPartition()), gks);
                return m.hasExpand() ? producer.withExpand(fromExpandStep(m.getExpand())) : producer;
            }
            case CONSUMER -> {
                ConsumerMsg m = msg.getConsumer();
                FragmentSpec.Consumer c = new FragmentSpec.Consumer(m.getFragmentId(), m.getSelfWorkerId(),
                        fromExchangeIn(m.getLeftIn()), fromExchangeIn(m.getRightIn()),
                        fromJoinStep(m.getJoin()));
                if (m.hasSink()) c = c.withSink(fromSinkStep(m.getSink()));
                if (m.hasTail()) c = c.withTail(fromSortLimitStep(m.getTail()));
                return c;
            }
            case AGGREGATE_CONSUMER -> {
                AggregateConsumerMsg m = msg.getAggregateConsumer();
                FragmentSpec.AggregateConsumer a = new FragmentSpec.AggregateConsumer(
                        m.getFragmentId(), m.getSelfWorkerId(),
                        fromExchangeIn(m.getIn()), fromAggregateStep(m.getAggregate()));
                if (m.hasFollowUp()) a = a.withFollowUp(fromAggregateStep(m.getFollowUp()));
                if (m.hasSink()) a = a.withSink(fromSinkStep(m.getSink()));
                if (m.hasTail()) a = a.withTail(fromSortLimitStep(m.getTail()));
                return a;
            }
            case WINDOW_CONSUMER -> {
                WindowConsumerMsg m = msg.getWindowConsumer();
                FragmentSpec.WindowConsumer w = new FragmentSpec.WindowConsumer(
                        m.getFragmentId(), m.getSelfWorkerId(),
                        fromExchangeIn(m.getIn()), fromWindowStep(m.getWindow()));
                if (m.hasSink()) w = w.withSink(fromSinkStep(m.getSink()));
                if (m.hasTail()) w = w.withTail(fromSortLimitStep(m.getTail()));
                return w;
            }
            case CHAINED_JOIN_CONSUMER -> {
                ChainedJoinConsumerMsg m = msg.getChainedJoinConsumer();
                FragmentSpec.FollowUp followUp = switch (m.getFollowUpCase()) {
                    case WINDOW -> fromWindowStep(m.getWindow());
                    case AGGREGATE -> fromAggregateStep(m.getAggregate());
                    default -> throw new IllegalArgumentException("ChainedJoinConsumer 缺 follow_up: " + m);
                };
                FragmentSpec.ChainedJoinConsumer cj = new FragmentSpec.ChainedJoinConsumer(
                        m.getFragmentId(), m.getSelfWorkerId(),
                        fromExchangeIn(m.getLeftIn()), fromExchangeIn(m.getRightIn()),
                        fromJoinStep(m.getJoin()), followUp);
                if (m.hasSink()) cj = cj.withSink(fromSinkStep(m.getSink()));
                if (m.hasTail()) cj = cj.withTail(fromSortLimitStep(m.getTail()));
                return cj;
            }
            case MIDDLE_JOIN_CONSUMER -> {
                MiddleJoinConsumerMsg m = msg.getMiddleJoinConsumer();
                FragmentSpec.MiddleJoinConsumer mjc = new FragmentSpec.MiddleJoinConsumer(
                        m.getFragmentId(), m.getSelfWorkerId(),
                        fromExchangeIn(m.getLeftIn()), fromExchangeIn(m.getRightIn()),
                        fromJoinStep(m.getJoin()), fromPartitionStep(m.getNextPartition()));
                return m.hasExpand() ? mjc.withExpand(fromExpandStep(m.getExpand())) : mjc;
            }
            case MIDDLE_AGGREGATE_CONSUMER -> {
                MiddleAggregateConsumerMsg m = msg.getMiddleAggregateConsumer();
                String filter = m.getFilterAfterAggregate();
                return new FragmentSpec.MiddleAggregateConsumer(m.getFragmentId(), m.getSelfWorkerId(),
                        fromExchangeIn(m.getIn()), fromAggregateStep(m.getAggregate()),
                        fromPartitionStep(m.getNextPartition()),
                        filter == null || filter.isBlank() ? null : filter);
            }
            case MIDDLE_WINDOW_CONSUMER -> {
                MiddleWindowConsumerMsg m = msg.getMiddleWindowConsumer();
                return new FragmentSpec.MiddleWindowConsumer(m.getFragmentId(), m.getSelfWorkerId(),
                        fromExchangeIn(m.getIn()), fromWindowStep(m.getWindow()),
                        fromPartitionStep(m.getNextPartition()));
            }
            default -> throw new IllegalArgumentException("FragmentSpecMsg 缺 spec oneof: " + msg);
        }
    }

    // ==================== 函数定义（Master 随片段附带 → Worker 按名重建） ====================

    public static FunctionDefinitionMsg toFunctionDefinition(FunctionDefinition f) {
        FunctionDefinitionMsg.Builder b = FunctionDefinitionMsg.newBuilder()
                .setName(f.name())
                .setType(f.type())
                .setJarPath(f.jarPath())
                .setClassName(f.className());
        if (f.properties() != null) {
            b.putAllProperties(f.properties());
        }
        b.setComment(nz(f.comment()));
        b.setCreatedAt(nz(f.createdAt()));
        b.setUpdatedAt(nz(f.updatedAt()));
        return b.build();
    }

    public static FunctionDefinition fromFunctionDefinition(FunctionDefinitionMsg m) {
        return new FunctionDefinition(
                m.getName(), m.getType(), m.getJarPath(), m.getClassName(),
                m.getPropertiesMap(),
                m.getComment().isEmpty() ? null : m.getComment(),
                m.getCreatedAt().isEmpty() ? null : m.getCreatedAt(),
                m.getUpdatedAt().isEmpty() ? null : m.getUpdatedAt());
    }

    /** null → 空串（proto3 无 null 字符串）。 */
    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
