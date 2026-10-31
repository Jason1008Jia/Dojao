package com.dojao.spi.service;

import com.dojao.common.data.Page;

/**
 * 单个扫描切片的读取游标 —— 由 {@link DojaoConnector#openSplit} 产出，Worker 在其任务线程内
 * 反复 {@link #next()} 直到 {@link Page#isEOF()}，然后 {@link #close()}。
 *
 * <p><b>它和 {@link DojaoConnector#read(com.dojao.spi.bean.ReadRequest)} 的区别。</b>
 * {@code read} 是有状态的行式游标：同一个 {@code ReadRequest} 连续调用会「接着上次读」，
 * 于是「读到哪儿了」这个状态挂在连接器对象上 —— 一个连接器实例同时只能服务一个切片，
 * 多线程并行扫描时必须每线程一个实例（引擎为此要复制连接器实例，见 {@link DojaoConnector#copy()}）。
 * {@code ScanReader} 把游标状态收进对象自身，一个切片一个 reader，天然可并行，也不再需要
 * 引擎替连接器操心实例复用。</p>
 *
 * <p><b>默认实现是桥接。</b>{@link DojaoConnector#openSplit} 默认返回一个把 {@code next()}
 * 转发到 {@code read(effectiveRequest)} 的 reader（首次返回 EOF 后不再调用），于是所有既有
 * 连接器不需要任何改动就满足本接口；只有真正需要切片私有状态（如 Hive 的多文件游标）的连接器
 * 才覆写 {@code openSplit}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/20
 */
public interface ScanReader extends AutoCloseable {

    /**
     * 读取下一页。返回 {@link Page#eof()} 表示本切片已读完。
     *
     * <p>返回值不应为 {@code null}（无数据用 {@code Page.eof()} 表达）。返回 EOF 后再次调用
     * 应当仍然返回 EOF（幂等），而不是抛异常。</p>
     */
    Page next();

    /**
     * 关闭本切片，释放游标与文件句柄。默认空实现。
     *
     * <p>与 {@link AutoCloseable#close()} 相比不抛受检异常 —— 引擎在 {@code doClose} 里调用，
     * 不希望因清理失败而掩盖查询的主错误。</p>
     */
    @Override
    default void close() {}
}
