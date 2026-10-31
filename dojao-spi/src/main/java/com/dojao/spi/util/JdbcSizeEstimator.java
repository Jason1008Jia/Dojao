package com.dojao.spi.util;

import com.dojao.common.data.Page;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.DojaoConnector;

import java.util.function.ToLongFunction;

/**
 * <b>JDBC 系连接器的数据量估算工具</b> —— 把「元数据 → EXPLAIN + 采样 → 未知」三级降级收敛到一处，
 * 供 MySQL / PostgreSQL / ClickHouse 等 JDBC 连接器复用（它们无共享基类，各自实现 {@code DojaoConnector}）。
 *
 * <p><b>为什么需要三级降级。</b>生产上创建 catalog 的账号未必有读元数据统计的权限
 * （如 ClickHouse 账号读不到 {@code system.tables.total_rows/total_bytes}）。此时若只认元数据、
 * 拿不到就放弃，join 选型会直接退化到最保守的排序归并 —— 安全但不够准。本工具在此之上加一级
 * 「{@code EXPLAIN} 取行数 × 采样几行求平均行长」，只有这一级也失败时才返回未知。</p>
 *
 * <p>三级（{@link #estimate}）：</p>
 * <ol>
 *   <li><b>元数据真实体积</b>（{@code metaBytes > 0}）：无过滤直接返回；有过滤按
 *       {@code EXPLAIN 过滤后行数 / 整表行数} 缩放，缩不动就退回整表体积。</li>
 *   <li><b>EXPLAIN 行数 × 采样平均行长</b>：元数据拿不到时，用 {@code EXPLAIN}（含下推谓词）估行数，
 *       再 {@code LIMIT} 采样几行算平均行长（{@link #sampleAvgRowBytes}），二者相乘。</li>
 *   <li><b>未知</b>：以上都失败 → {@code (-1, -1)}，由调用方退回最保守的可溢写排序归并。</li>
 * </ol>
 *
 * <p><b>字节口径</b>：采样平均行长 = {@code Page.getSizeInBytes() / 行数}，即 Arrow
 * {@code FieldVector.getBufferSize()}（<b>已分配容量</b>）口径，与元数据体积、{@code JoinCardinality}
 * 的 {@code byteSize/rowCount} 同口径。对 power-of-2 分配有小幅<b>高估</b>，属保守方向：估大最多多走
 * 一次可溢写的排序归并，估小才可能撞内存。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/1
 */
public final class JdbcSizeEstimator {

    private JdbcSizeEstimator() {
    }

    /** 估算结果：{@code rowCount} / {@code byteSize}，任一为 {@code -1} 表示未知。 */
    public record Estimate(long rowCount, long byteSize) {
    }

    /**
     * 采样 {@code sampleRows} 行求<b>平均行长（字节）</b>：复制一份连接器 → 读一条
     * {@code LIMIT sampleRows} 的页 → {@code getSizeInBytes()/rowCount}。
     *
     * <p>用 {@link DojaoConnector#copy()} 保证不动调用方实例的读游标；读到的页无论成败都关闭。
     * 表为空 / 读失败 / 连接器不支持读取 → 返回 <b>{@code -1}</b>（未知）。</p>
     *
     * @param sampleRows 采样行数（建议取一个完整 batch，如 {@code CommonConfig.DEFAULT_BATCH_SIZE}，
     *                   以摊薄 Arrow power-of-2 分配带来的高估）
     */
    public static double sampleAvgRowBytes(DojaoConnector conn, String catalog, String schema,
                                           String table, int sampleRows) {
        if (conn == null || sampleRows <= 0) {
            return -1;
        }
        DojaoConnector c = conn.copy();
        try {
            Page page = c.read(ReadRequest.of(catalog, schema, table, 0, sampleRows));
            if (page == null || page.isEOF()) {
                return -1;
            }
            try {
                int n = page.getRowCount();
                return n > 0 ? (double) page.getSizeInBytes() / n : -1;
            } finally {
                page.close();
            }
        } catch (Exception e) {
            return -1;
        } finally {
            try {
                c.closeRead();
            } catch (Exception ignored) {
                // 采样是尽力而为，关闭失败不影响结果
            }
        }
    }

    /**
     * 三级降级估算数据量。
     *
     * @param conn       连接器（{@link #sampleAvgRowBytes} 采样时用 {@code copy()} 复制，不污染调用方游标）
     * @param filter     已下推的过滤谓词（连接器方言 SQL 表达式；{@code null}/空 = 无过滤 → 整表）
     * @param metaRows   连接器已取的元数据行数（{@code -1} = 未知/无权限）
     * @param metaBytes  连接器已取的元数据字节数（{@code -1} = 未知/无权限）
     * @param explainRows 方言 {@code EXPLAIN} 行数估算（入参为 filter，可空 = 整表；不得抛异常，
     *                    取不到返回 {@code -1}）；传 {@code null} 表示该连接器不支持 EXPLAIN
     * @param sampleRows 采样行数（{@code <= 0} 时跳过采样）
     * @return {@link Estimate}；完全探不到时为 {@code (-1, -1)}
     */
    public static Estimate estimate(DojaoConnector conn, String catalog, String schema, String table,
                                    String filter, long metaRows, long metaBytes,
                                    ToLongFunction<String> explainRows, int sampleRows) {
        boolean hasFilter = filter != null && !filter.isBlank();

        // ① 元数据真实体积：有过滤则按选择率缩放（需要过滤前后行数都在册，缺一退回整表）
        if (metaBytes > 0) {
            if (!hasFilter) {
                return new Estimate(metaRows, metaBytes);
            }
            long filtered = explain(filter, explainRows);
            if (metaRows > 0 && filtered > 0 && filtered != metaRows) {
                return new Estimate(filtered, (long) Math.ceil(metaBytes * (double) filtered / metaRows));
            }
            return new Estimate(metaRows, metaBytes);
        }

        // ② EXPLAIN 行数 × 采样平均行长
        long rows = explain(hasFilter ? filter : null, explainRows);
        double avg = sampleAvgRowBytes(conn, catalog, schema, table, sampleRows);
        if (rows > 0 && avg > 0) {
            return new Estimate(rows, (long) Math.ceil(rows * avg));
        }

        // ③ 未知 → 调用方退回最保守的排序归并
        return new Estimate(-1L, -1L);
    }

    /** 安全调 EXPLAIN：不支持 / 抛异常 → {@code -1}。 */
    private static long explain(String filter, ToLongFunction<String> explainRows) {
        if (explainRows == null) {
            return -1L;
        }
        try {
            return explainRows.applyAsLong(filter);
        } catch (Exception e) {
            return -1L;
        }
    }
}
