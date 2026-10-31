package com.dojao.spi.function;

import com.dojao.spi.bean.FunctionDefinition;

/**
 * 函数后端 SPI —— 多语言拓展外缘的核心抽象（顶层设计保留的扩展接口）。
 *
 * <p>一种编程语言 = 一个 {@link FunctionBackend} 实现。引擎（Planner / 执行器）只认识
 * {@link ScalarFunction} / {@link AggregateFunction} / {@link TableFunction} / {@link WindowFunction}
 * 这四类 SPI 函数接口（聚合另含 Arrow 原生形态 {@link CollectAggregateFunction}），永远不感知
 * 实现语言 —— 后端把该语言的函数代码 / 模型加载成 <b>标准 SPI 函数对象</b>，进入引擎后与
 * Java 内置函数无区别。这就是"无痕"（用户对语言无感）的机制保证。</p>
 *
 * <h3>宿主模型由各后端自决（不强迫统一）</h3>
 * <ul>
 *   <li>Java → 进程内 {@code URLClassLoader}（最快）；</li>
 *   <li>Python / R / Julia / MATLAB → 独立 Sidecar 子进程（隔离运行时）；</li>
 *   <li>Go / Rust / C++ → 可选 Sidecar 或 JNI / Panama（后端内部细节）。</li>
 * </ul>
 *
 * <h3>新增一种语言（对接清单）</h3>
 * <ol>
 *   <li>实现本接口（或对接现有远程服务端点）；</li>
 *   <li>提供该语言 SDK 脚手架：读写 Arrow IPC + 实现 ScalarFunction / AggregateFunction 语义 + 控制协议收发；</li>
 *   <li>函数代码 / 模型分发：共享 FS（v1）或 Master 控制面下发（v2，源码 / 字节码 / wasm）；</li>
 *   <li>注册进 {@code BackendRegistry}（内置或 ServiceLoader jar）；引擎零改动。</li>
 * </ol>
 *
 * @see com.dojao.spi.function.ScalarFunction
 * @see com.dojao.spi.function.AggregateFunction
 * @see com.dojao.spi.function.CollectAggregateFunction
 * @see com.dojao.spi.function.TableFunction
 * @see com.dojao.spi.function.WindowFunction
 */
public interface FunctionBackend {

    /**
     * 后端类型标识：java | python | rust | go | r | julia | …
     * 与 CREATE FUNCTION 的 {@code type} 属性对应，由 {@code BackendRegistry} 按此分发。
     */
    String type();

    /**
     * 每 worker 节点一次：初始化运行时 / 侧车。首次加载函数前调用，幂等。
     *
     * <p>★ <b>懒启动约定（外挂运行时一律遵守）</b>：后端在进程启动期<b>只</b>注册进
     * {@code BackendRegistry}（把 {@link #type()} 变成已知类型），<b>不在启动期</b> spawn 子进程 /
     * 探活 / 检查环境。运行时是否就绪推迟到 {@link #validate}（CREATE FUNCTION 冒烟校验）或首次
     * {@link #loadScalar}/{@link #loadAggregate}；届时若环境缺失（无 python、缺 pyarrow/protobuf…），
     * 就<b>显式抛出可操作的异常</b>，让用户在自己的操作上第一时间看到原因，而不是压在启动日志里、
     * 或退化成"不支持的函数类型"。这样 Master / Worker 启动也不被外部依赖拖累。</p>
     */
    default void init() {}

    /** 加载一个标量函数（1→1）。加载失败抛异常，由框架 fail-fast。 */
    ScalarFunction loadScalar(FunctionDefinition def);

    /**
     * 加载一个聚合函数（GROUP BY 归约）。
     */
    void loadAggregate(FunctionDefinition def);

    /**
     * 加载一个表函数（1 行 → N 行，FROM TABLE(...)）。加载失败抛异常，由框架 fail-fast。
     */
    TableFunction loadTable(FunctionDefinition def);

    /** 加载一个窗口函数（OVER 窗口）。 */
    WindowFunction loadWindow(FunctionDefinition def);

    /**
     * CREATE FUNCTION 冒烟校验：加载/解析该函数定义，确认代码可解析、契约完整。
     *
     * <p>校验失败（如 Python 装饰器缺阶段注解、类未实现任何函数契约）抛异常，由框架
     * fail-fast —— <b>坏函数不落库、不注册</b>。默认空实现 = 不做额外校验（后端可在
     * 加载时自行保证）。</p>
     *
     * @param def 待校验的函数定义
     * @throws IllegalArgumentException 校验失败（携带可诊断的用户错误信息）
     */
    default void validate(FunctionDefinition def) {
    }

    /** 节点退出回收（关侧车 / 释放类加载器）。 */
    default void close() {}
}
