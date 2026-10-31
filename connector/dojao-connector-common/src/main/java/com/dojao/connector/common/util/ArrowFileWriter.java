package com.dojao.connector.common.util;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

/**
 * Arrow 页 → 物理文件的写入器统一契约。
 *
 * <p>ORC / Parquet / 分隔文本三种写入器共用这套 API：调用方（Hive 连接器的
 * {@code HiveWriteSession}）只负责按格式挑实现，写入与收尾的动作一致。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public interface ArrowFileWriter extends AutoCloseable {

    /** 写入一页数据（行数 = {@link VectorSchemaRoot#getRowCount()}）。 */
    void write(VectorSchemaRoot root) throws IOException;

    @Override
    void close() throws IOException;
}
