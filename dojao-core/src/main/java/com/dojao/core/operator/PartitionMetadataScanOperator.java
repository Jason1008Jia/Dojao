package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.DojaoConnector;

import java.util.List;
import java.util.Objects;

/**
 * 分区元数据扫描 —— 不读数据文件，只输出「一行为一个非空分区」的合成页。
 *
 * <p><b>为什么有它</b>：当查询的聚合输入<b>全部是分区列</b>时（
 * {@code min(dt) / max(dt) / count(distinct dt) / count(*)}），逐行扫描数据毫无必要 —— 分区列的值
 * 来自目录名，行数来自文件 footer。本算子把连接器
 * {@link DojaoConnector#scanPartitionMetadata(ReadRequest)} 的合成页作为数据源，
 * 下游 {@link HashAggregationOperator} 照常聚合，结果与全表扫描<b>精确等价</b>。</p>
 *
 * <p><b>页的内容</b>：全部分区列（按声明类型，值 = 该分区目录的 {@code key=value}）+
 * {@link DojaoConnector#PARTITION_ROW_COUNT_COLUMN}（该分区精确行数）。空分区不出现。
 * {@code count(*)} 在规划期被改写成对行数列的 {@code sum_int64}（BIGINT 保真）。</p>
 *
 * <p><b>惰性取数</b>：规划期只做结构性判定（{@link DojaoConnector#supportsPartitionMetadataScan}），
 * 真正的 HMS 分区列举 + footer 读取发生在 {@link #doOpen()} —— {@code EXPLAIN} 不执行算子，
 * 因此不会为「看一眼计划」去读 footer。</p>
 *
 * <p><b>失败语义</b>：运行期连接器返回 {@code null}（HDFS 故障 / 分区被并发删除）说明精确行数
 * 拿不到，此时<b>直接报错</b>而不是换个算法 —— 规划期已把 {@code count(*)} 改写成
 * {@code sum(行数列)}，换回真实扫描会让该改写失去依据（静默算错）。</p>
 *
 * @author jiaxin
 * @date 2026/10/20
 */
public final class PartitionMetadataScanOperator extends AbstractMeasurableOperator {

    private final DojaoConnector connector;
    private final ReadRequest request;
    private final List<String> outputColumns;

    /** 待输出的合成页；首次 {@link #doNext()} 交出所有权后置空。 */
    private Page page;
    private boolean emitted;

    public PartitionMetadataScanOperator(DojaoConnector connector, ReadRequest request,
                                         List<String> outputColumns) {
        super("PartitionMetadataScan", "PartitionMetadataScan", 1);
        this.connector = Objects.requireNonNull(connector, "connector");
        this.request = Objects.requireNonNull(request, "request");
        this.outputColumns = List.copyOf(outputColumns);
    }

    public List<String> getOutputColumns() {
        return outputColumns;
    }

    @Override
    protected void doOpen() {
        page = connector.scanPartitionMetadata(request);
        if (page == null) {
            throw new IllegalStateException("连接器未能提供精确的分区元数据（"
                    + request.catalog() + "." + request.schema() + "." + request.table()
                    + "），分区元数据扫描路径不应在此时失效");
        }
    }

    @Override
    protected Page doNext() {
        if (emitted) return Page.eof();
        emitted = true;
        Page p = page;
        page = null;           // 所有权交给下游：由下游 close
        return p;
    }

    @Override
    protected void doClose() {
        if (page != null) {
            page.close();
            page = null;
        }
    }
}
