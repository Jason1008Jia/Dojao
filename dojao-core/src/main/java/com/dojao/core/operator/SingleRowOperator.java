package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

/**
 * 无 FROM 的 SELECT 的数据源 —— 输出<b>恰好一行</b>。
 *
 * <p>标准 SQL 允许 {@code SELECT 1}、{@code SELECT concat('a', NULL)} 这类纯表达式查询；Dojao 的
 * 语法层（{@code querySpecification} 的 FROM 子句可选）与语义层本就接受，缺的是物理层数据源。
 * 本算子提供那一行：只带一个内部占位列（{@code __dojao_single}，值 1），让下游
 * {@link ScalarProjectOperator} 能拿到分配器并逐行求值常量表达式；占位列不会出现在最终投影里
 * （SELECT 输出列由表达式求值产生）。</p>
 *
 * <p>{@code SELECT *} 在无 FROM 时无列可展开，由 Planner 在规划期明确拒绝。</p>
 *
 * @author jiaxin
 * @date 2026/10/18
 */
public final class SingleRowOperator extends AbstractMeasurableOperator {

    /** 内部占位列名（不对用户暴露）。 */
    public static final String DUMMY_COLUMN = "__dojao_single";

    private BufferAllocator allocator;
    private boolean emitted;

    public SingleRowOperator() {
        super("SingleRow", "SingleRow", 1);
    }

    @Override
    protected void doOpen() {
        allocator = NodeMemory.shared().newOperatorAllocator(getStats().getOperatorType(),
                ResourceConfig.queryDirectLimit());
    }

    @Override
    protected Page doNext() {
        if (emitted) return Page.eof();
        emitted = true;
        Field field = new Field(DUMMY_COLUMN,
                FieldType.nullable(new ArrowType.Int(32, true)), null);
        IntVector vec = (IntVector) field.createVector(allocator);
        vec.allocateNew(1);
        vec.setSafe(0, 1);
        vec.setValueCount(1);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(field), List.of(vec), 1);
        return Page.fromRoot(root);
    }

    @Override
    protected void doClose() {
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }
}
