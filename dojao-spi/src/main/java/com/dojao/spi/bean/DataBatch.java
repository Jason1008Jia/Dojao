package com.dojao.spi.bean;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据批次 —— Worker 与连接器之间交换数据的列式批次。
 *
 * <p>数据以 Apache Arrow {@link VectorSchemaRoot} 形式存储（列式、堆外内存），
 * 避免逐行 Java 对象分配带来的 GC 压力。调用方在使用完批次后必须调用
 * {@link #close()} 释放 Arrow 内存。</p>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/7/11
 */
public class DataBatch implements AutoCloseable {

    private final VectorSchemaRoot root;

    public DataBatch(VectorSchemaRoot root) {
        this.root = root;
    }

    /** 列名列表。 */
    public List<String> columns() {
        List<String> names = new ArrayList<>();
        for (var field : root.getSchema().getFields()) {
            names.add(field.getName());
        }
        return names;
    }

    /** 行数。 */
    public int rowCount() {
        return root.getRowCount();
    }

    /** 列数。 */
    public int columnCount() {
        return root.getSchema().getFields().size();
    }

    /** 底层的 Arrow 数据载体，可直接传递给 Dojao 算子。 */
    public VectorSchemaRoot root() {
        return root;
    }

    /** 空批次。 */
    public static DataBatch empty() {
        return new DataBatch(null);
    }

    @Override
    public void close() {
        if (root != null) {
            root.close();
        }
    }
}
