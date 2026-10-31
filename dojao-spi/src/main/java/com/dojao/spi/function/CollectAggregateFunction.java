package com.dojao.spi.function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

/**
 * 收集-求值型聚合函数 —— GROUP BY 聚合的 Arrow 原生公开契约（collect-evaluate 形态）。
 *
 * <p>与 {@link AggregateFunction}（增量四阶段：createState/accumulate/merge/evaluate，逐行 Object 状态）
 * 的本质差异：<b>状态全程引擎侧 Arrow 列（可测可溢写）</b>，框架按 key 收集声明的输入列，
 * 实现方只写最后一步 {@link #evaluateBatch}（收该 key 的全部值 → 标量）。无跨技术栈状态往返，
 * 消灭内存黑盒 + O(N²) 传输——「最稳定」的 UDAF 形态，主力推广。</p>
 *
 * <p><b>多语言扩展</b>：本接口是公开 SPI 契约，任何支持 Arrow 数据传输的语言（Python / Rust / Go / …）
 * 都可实现。<b>切组由引擎完成、外部只计算</b>：引擎把 N 个 key 攒齐后的值以"每 key 一行"的
 * 原生 Arrow RecordBatch 交付（值列 = List 列，每行 = 该 key 全部值），实现方逐行取
 * {@code list.value(row)} 即得每 key 值数组、逐 key 求值，返回 N 行标量结果列 —— 零切组代码。
 * List 是 Arrow 标准类型，Java/C++/Go/Rust/Python 全部原生解析，语言 SDK 侧可直接镜像本契约
 * （Python 侧即为 {@code dojao_udf} 的 {@code @collect_aggregate} 装饰器写法）。</p>
 *
 * <p>引擎（{@code CollectHashTable}）负责攒值+分组：每输入列一个共享累积 Arrow 列 + 每 group 行号集合；
 * 溢写 = 按 key 排序提取值子列写 Arrow IPC（纯引擎零外部语言）。Phase 2 按 key 分组拼成
 * [key 列..., list<值列>...] 的 RecordBatch（每 key 一行，值列 = 该 key 攒齐的全部值），
 * 调 {@link #evaluateBatch} 交给实现方，返回 N 行结果列（每 key 一行）。</p>
 */
public interface CollectAggregateFunction extends Function {

    /**
     * 声明的输入列名 —— <b>仅是参数位标签</b>（元数据 / 按位对齐用），<b>不要求与 SQL 列同名</b>。
     *
     * <p>真正收集哪一列由 <b>SQL 调用实参按位</b>决定：声明 {@code ("num",)} 的函数被
     * {@code stddev(discount_amount)} 调用时，收集列是 {@code discount_amount}（位置 = 第 0 个实参），
     * 列名 {@code "num"} 对外挂函数无意义（{@link #evaluateBatch} 按位取列）。数量须等于 SQL 实参个数。</p>
     */
    List<String> inputColumns();

    /** 输出列 Arrow 类型（引擎据此建结果列）。 */
    ArrowType outputType();

    /**
     * 批量求值：把"每 key 一行"的 (key 列 + list 值列) batch 交付实现方，返回 N 行结果列。
     *
     * <p>batch 列序 = [keyColumns..., 收集列...]（收集列 = SQL 实参按位决定的列，见
     * {@link #inputColumns()}），<b>每 key 一行</b>；值列 = Arrow List 列，
     * 每行 = 该 key 攒齐的全部值（连续段）。实现方把该 batch 发给外部（如 sidecar COLLECT_EVALUATE），
     * 外部逐行取 {@code list.value(row)} 得每 key 值数组、逐 key 求值，返回 N 行标量结果 —— 无切组逻辑。</p>
     *
     * @param batch          分组 batch（每 key 一行，调用方持有，实现方不得 close；实现方内部如深拷贝则自管）
     * @param keyColumnCount batch 前缀的 key 列数（其后列 = List 值列）
     * @param allocator     结果列 allocator
     * @param outputName    结果列名
     * @return N 行结果列（每 key 一行），类型须与 {@link #outputType()} 一致；调用方负责 close
     */
    FieldVector evaluateBatch(VectorSchemaRoot batch, int keyColumnCount,
                              BufferAllocator allocator, String outputName);
}
