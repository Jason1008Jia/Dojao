package com.dojao.core.operator;

import com.dojao.spi.function.AggregateFunction;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 批量聚合函数 —— 引擎内部批量累加扩展接口（SPI 之外的批量捷径）。
 *
 * <p>背景：{@link AggregateFunction#accumulate(Object, Object[])} 是逐行契约，多语言桥
 * （如 {@code PythonFunctionBridge}）若逐行调用会退化为"每行一次进程往返"的性能墙。
 * 而 Python 侧聚合契约本就是"把一批并入 state"（{@code accumulate(state, batch)} 接收整批
 * RecordBatch），因此给实现方一个批量累加入口是天然对齐。</p>
 *
 * <p>放 core 而非 SPI：与 {@link com.dojao.core.function.scalar.BatchScalarFunction} 同理，
 * 保持 SPI 纯 Object 逐行契约，批量是引擎内对"可实现批量执行的后端桥"的增强钩子。
 * 内置五函数（count/sum/avg/min/max）不实现 → {@link AggregationHashTable} 走原逐行路径。</p>
 *
 * <p>契约：{@link AggregationHashTable#addPage} 在批量启用时，先把页内行按 group 归组，
 * 再对每个 group 构造只含该聚合输入列的子 batch 调用本方法一次。batch 由调用方创建并负责 close，
 * 实现方不得 close；本方法须把整批（含全部行）并入 state，语义等价于逐行
 * {@link #accumulate(Object, Object[])} 每行一次。</p>
 *
 * <p><b>列名约定</b>：子 batch 的列名归一到参数位 {@code c0..cn-1}（当前每规格一个输入列 ⇒ c0），
 * 与逐行路径的 {@code Object[] inputs} 按位对应、与标量批量路径一致 —— 外挂语言按
 * {@code batch.column("c0")} 取参，不继承上游原始列名。</p>
 *
 * @param <S> 聚合状态类型
 */
public interface BatchAggregateFunction<S> extends AggregateFunction<S> {

    /** 整批并入 state（默认退化为逐行 accumulate，保证未重写的实现类安全）。 */
    default void accumulateBatch(S state, VectorSchemaRoot batch) {
        int n = batch.getRowCount();
        int cols = batch.getFieldVectors().size();
        for (int r = 0; r < n; r++) {
            Object[] row = new Object[cols];
            for (int c = 0; c < cols; c++) {
                row[c] = batch.getVector(c).isNull(r) ? null : batch.getVector(c).getObject(r);
            }
            accumulate(state, row);
        }
    }
}
