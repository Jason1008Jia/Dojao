package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * 三槽溢写管线中的槽位抽象。
 *
 * <p>子类实现：
 * <ul>
 *   <li>{@link AggregationHashTable} — 聚合模式（accumulate 聚合函数状态）</li>
 *   <li>{@link RowAccumulator} — 开窗/Join 模式（append 原始行）</li>
 * </ul>
 *
 * @author jiaxin
 * @since 2.0
 */
public interface Slot {
    /** 单行写入（accumulate 或 append） */
    void addRow(Page input, int row);
    /** 批量写入（默认逐行，子类可覆盖优化） */
    default void addPage(Page input) {
        int n = input.getRowCount();
        for (int r = 0; r < n; r++) addRow(input, r);
    }
    /** 按 key 排序后构建溢写根（key列 + 数据列） */
    VectorSchemaRoot buildSortedSpillRoot();
    /** 估算内存占用（字节） */
    long estimateMemoryBytes();
    boolean isEmpty();
    int rowCount();
    void close();
}
