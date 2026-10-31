package com.dojao.spi.function;

/**
 * Python 聚合函数桥接 —— 通过 Arrow Flight 与 Python Sidecar 通信。
 *
 * <p>引擎只认 {@link AggregateFunction} 这一个聚合接口；此桥把 SPI 调用翻译成
 * 对 Python 守护进程的 Flight 往返，让 Python 实现的聚合函数在引擎里与内置聚合无区别。</p>
 *
 * <h3>执行模型</h3>
 * <pre>
 *   Java Engine                     Python Sidecar
 *   ──────────                      ──────────────
 *   createState()  ─────────────→   CREATE_STATE  → fn.create_state()
 *   accumulate()   ─────────────→   ACCUMULATE    → fn.accumulate(state, batch)
 *   evaluate()     ─────────────→   EVALUATE      → fn.evaluate(state)
 *   merge()        ─────────────→   MERGE         → fn.merge(target, source)
 * </pre>
 *
 * <p>当前为<b>骨架实现</b>—— 真实 Flight 调用见 dojao-server 的 {@code PythonFunctionBridge}。
 * State 序列化/溢写由框架统一管理。
 *
 * @author jiaxin
 * @since 1.2
 */
public class PyAggregateBridge implements AggregateFunction<byte[]> {

    private final String pythonHost;
    private final int pythonPort;
    private final String moduleName;
    private final String className;

    public PyAggregateBridge(String pythonHost, int pythonPort,
                             String moduleName, String className) {
        this.pythonHost = pythonHost;
        this.pythonPort = pythonPort;
        this.moduleName = moduleName;
        this.className = className;
    }

    @Override
    public byte[] createState() {
        // TODO: Arrow Flight → CREATE_STATE → 返回 state bytes
        return new byte[0];
    }

    @Override
    public void accumulate(byte[] state, Object[] inputs) {
        // TODO: Arrow Flight → ACCUMULATE → 原地修改 state
    }

    @Override
    public void merge(byte[] target, byte[] source) {
        // TODO: Arrow Flight → MERGE → 原地修改 target
    }

    @Override
    public Object evaluate(byte[] state) {
        // TODO: Arrow Flight → EVALUATE → 返回结果值
        return null;
    }

    @Override
    public String functionName() {
        return className.toLowerCase();
    }

    @Override
    public String display() {
        return "Python 聚合函数桥接: " + moduleName + "." + className;
    }

    // ---- debug ----

    public String pythonHost() { return pythonHost; }
    public int pythonPort() { return pythonPort; }
    public String moduleName() { return moduleName; }
    public String pythonClassName() { return className; }
}
