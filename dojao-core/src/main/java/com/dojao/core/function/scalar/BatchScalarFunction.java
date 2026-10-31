package com.dojao.core.function.scalar;

import com.dojao.spi.function.ScalarFunction;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 批量标量函数 —— 引擎内部批量求值扩展接口（SPI 之外的批量捷径）。
 *
 * <p>背景：{@link ScalarFunction#evaluate(Object[])} 是逐行契约，多语言桥（如
 * {@code PythonFunctionBridge}）若逐行调用会退化为"每行一次进程往返"的性能墙。
 * 而 Java 侧批量边界已存在 —— {@link ScalarEvaluator#evaluate} 接收整批
 * {@link VectorSchemaRoot}。本接口让实现方能在这条批量边界上一次处理整批，绕过逐行 SPI。</p>
 *
 * <p>放 core 而非 SPI：保持 SPI 的纯 Object 逐行契约（对用户语言 SDK 友好、内置函数零感知）；
 * 批量能力是引擎内部对"可实现批量执行的后端桥"的增强钩子，任何后端桥（Java/Python/R…）
 * 都可实现它。内置 Java 标量函数不实现 → 仍走 {@link ScalarEvaluator} 逐行路径，行为零变化。</p>
 *
 * <p>契约：输入 root 行数 N，必须返回行数恰为 N 的结果向量（与输入逐行对应）。
 * 返回向量由实现方创建、调用方负责 close；输入 root 由调用方管理，实现方不得 close。</p>
 */
public interface BatchScalarFunction extends ScalarFunction {

    /**
     * 整批求值：对输入 root（行数 N）求值，返回行数 N 的结果向量（列名 = outputName）。
     *
     * <p>实现方应保证：<ul>
     *   <li>输出向量与输入同 allocator root（调用方持有的 allocator）；</li>
     *   <li>返回向量的 FieldType 反映实际结果类型（下游按 schema 解析）；</li>
     *   <li>结果行数与输入不一致必须 fail-fast（防下游组装错位）。</li>
     * </ul></p>
     */
    FieldVector evaluateBatch(VectorSchemaRoot root, BufferAllocator allocator, String outputName);
}
