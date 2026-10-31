package com.dojao.core.operator;

import java.util.AbstractList;
import java.util.Arrays;

/**
 * 溢写索引中「一个 key 的段列表」—— 用整型平行数组存储 {@link Segment}，替代原先每 key 一个
 * {@code CopyOnWriteArrayList} + 每段一个 {@code Segment} 对象。
 *
 * <p><b>为什么重构</b>：高基数分组（如 {@code PARTITION BY event_id} 且 event_id 近似唯一）下，
 * 索引条目数与行数同阶 —— 9.53M 个 key 就意味着 9.53M 个 {@code CopyOnWriteArrayList}
 * （对象头 + 写时复制数组）和 9.53M 个 {@code Segment}，索引内存膨胀到与列数据同量级甚至更大，
 * 成为窗口溢写路径的内存主峰（也是「一涉及开窗函数 worker 就挂」的成因之一）。本实现把每个 key
 * 的段压进 3 个 {@code int} 数组，{@link #get(int)} 按需合成 {@code Segment}，不再常驻段对象。</p>
 *
 * <p><b>线程安全</b>：溢写由三槽流水线并发发起（最多 2 个溢写任务同时在跑），同一 key 可能被两个
 * 溢写线程同时追加 ⓘ 故 {@link #addSegment} 用 {@code synchronized}，{@link #size 长度} 置 volatile
 * 保证 Phase 2（全部溢写完成后）读到的可见性。普通 ArrayList 并发 add 会丢段 ⇒ 静默少行。</p>
 *
 * @author jiaxin
 * @date 2026/9/29
 */
final class SegmentList extends AbstractList<Segment> {

    private int[] batch = new int[2];
    private int[] start = new int[2];
    private int[] end = new int[2];
    private volatile int size;

    /** 追加一个段（并发安全）。 */
    synchronized void addSegment(int batchIndex, int startRow, int endRow) {
        if (size == batch.length) {
            int cap = size << 1;
            batch = Arrays.copyOf(batch, cap);
            start = Arrays.copyOf(start, cap);
            end = Arrays.copyOf(end, cap);
        }
        batch[size] = batchIndex;
        start[size] = startRow;
        end[size] = endRow;
        size++;
    }

    @Override
    public Segment get(int index) {
        if (index < 0 || index >= size) throw new IndexOutOfBoundsException("index=" + index);
        return new Segment(batch[index], start[index], end[index]);
    }

    @Override
    public int size() {
        return size;
    }
}
