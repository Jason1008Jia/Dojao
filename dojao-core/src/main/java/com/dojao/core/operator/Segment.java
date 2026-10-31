package com.dojao.core.operator;

/**
 * 溢写文件中的一个段记录：key 在某个 RecordBatch 中的行范围。
 *
 * <p>由 {@link AbstractSpillableOperator#addToIndex} 在溢写时写入，
 * Phase 2 按 key 读取时通过 batchIndex + startRow/endRow 精准定位数据行。</p>
 *
 * @param batchIndex Arrow 批次文件索引（对应 batchFiles.get(batchIndex)）
 * @param startRow   起始行号（含）
 * @param endRow     结束行号（不含）
 * @author jiaxin
 * @since 2.0
 */
public record Segment(int batchIndex, int startRow, int endRow) {}
