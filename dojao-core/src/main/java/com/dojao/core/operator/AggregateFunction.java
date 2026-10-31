package com.dojao.core.operator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * 聚合函数枚举，定义支持的聚合操作及其累加器向量类型。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/15
 */
public enum AggregateFunction {

    COUNT {
        @Override
        public FieldVector createAccumulatorVector(String outputColumn, BufferAllocator allocator) {
            Field field = new Field(outputColumn,
                    FieldType.nullable(new ArrowType.Int(64, true)), null);
            BigIntVector vec = new BigIntVector(field, allocator);
            vec.allocateNew();
            return vec;
        }

        @Override
        public void initializeAccumulator(FieldVector vector, int index) {
            ((BigIntVector) vector).setSafe(index, 0);
        }

        @Override
        public void updateAccumulator(FieldVector vector, int index, FieldVector inputVector, int inputRow) {
            BigIntVector agg = (BigIntVector) vector;
            agg.setSafe(index, agg.get(index) + 1);
        }

        @Override
        public FieldVector[] getAccumulatorVectors() {
            return new FieldVector[1]; // 单 vector slot，由 createAccumulatorVector 填充
        }

        @Override
        public int accumulatorCount() {
            return 1;
        }
    },

    SUM {
        @Override
        public FieldVector createAccumulatorVector(String outputColumn, BufferAllocator allocator) {
            Field field = new Field(outputColumn,
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            Float8Vector vec = new Float8Vector(field, allocator);
            vec.allocateNew();
            return vec;
        }

        @Override
        public void initializeAccumulator(FieldVector vector, int index) {
            ((Float8Vector) vector).setSafe(index, 0.0);
        }

        @Override
        public void updateAccumulator(FieldVector vector, int index, FieldVector inputVector, int inputRow) {
            Float8Vector agg = (Float8Vector) vector;
            double val = toDouble(inputVector, inputRow);
            agg.setSafe(index, agg.get(index) + val);
        }

        @Override
        public FieldVector[] getAccumulatorVectors() {
            return new FieldVector[1];
        }

        @Override
        public int accumulatorCount() {
            return 1;
        }
    },

    AVG {
        @Override
        public FieldVector createAccumulatorVector(String outputColumn, BufferAllocator allocator) {
            // AVG 的累加器是一个 slot，但需要两个 vector：sum + count
            // 实际两个 vector 分别管理，这里返回 sum vector
            Field field = new Field(outputColumn,
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            Float8Vector vec = new Float8Vector(field, allocator);
            vec.allocateNew();
            return vec;
        }

        @Override
        public void initializeAccumulator(FieldVector vector, int index) {
            // 由 AggregationHashTable 管理两个 vector
        }

        @Override
        public void updateAccumulator(FieldVector vector, int index, FieldVector inputVector, int inputRow) {
            // 由 AggregationHashTable 管理两个 vector
        }

        @Override
        public FieldVector[] getAccumulatorVectors() {
            return new FieldVector[2]; // sum + count
        }

        @Override
        public int accumulatorCount() {
            return 2;
        }
    },

    MIN {
        @Override
        public FieldVector createAccumulatorVector(String outputColumn, BufferAllocator allocator) {
            Field field = new Field(outputColumn,
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            Float8Vector vec = new Float8Vector(field, allocator);
            vec.allocateNew();
            return vec;
        }

        @Override
        public void initializeAccumulator(FieldVector vector, int index) {
            ((Float8Vector) vector).setSafe(index, Double.MAX_VALUE);
        }

        @Override
        public void updateAccumulator(FieldVector vector, int index, FieldVector inputVector, int inputRow) {
            Float8Vector agg = (Float8Vector) vector;
            double val = toDouble(inputVector, inputRow);
            if (val < agg.get(index)) {
                agg.setSafe(index, val);
            }
        }

        @Override
        public FieldVector[] getAccumulatorVectors() {
            return new FieldVector[1];
        }

        @Override
        public int accumulatorCount() {
            return 1;
        }
    },

    MAX {
        @Override
        public FieldVector createAccumulatorVector(String outputColumn, BufferAllocator allocator) {
            Field field = new Field(outputColumn,
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            Float8Vector vec = new Float8Vector(field, allocator);
            vec.allocateNew();
            return vec;
        }

        @Override
        public void initializeAccumulator(FieldVector vector, int index) {
            ((Float8Vector) vector).setSafe(index, -Double.MAX_VALUE);
        }

        @Override
        public void updateAccumulator(FieldVector vector, int index, FieldVector inputVector, int inputRow) {
            Float8Vector agg = (Float8Vector) vector;
            double val = toDouble(inputVector, inputRow);
            if (val > agg.get(index)) {
                agg.setSafe(index, val);
            }
        }

        @Override
        public FieldVector[] getAccumulatorVectors() {
            return new FieldVector[1];
        }

        @Override
        public int accumulatorCount() {
            return 1;
        }
    };

    // ==================== 抽象方法 ====================

    /** 创建累加器 Arrow 向量 */
    public abstract FieldVector createAccumulatorVector(String outputColumn, BufferAllocator allocator);

    /** 初始化累加器某行的值（新建 group 时调用） */
    public abstract void initializeAccumulator(FieldVector vector, int index);

    /** 用输入行更新累加器 */
    public abstract void updateAccumulator(FieldVector vector, int index, FieldVector inputVector, int inputRow);

    /** 该聚合函数占用的累加器 vector slot 数（AVG 需要 2 个） */
    public abstract FieldVector[] getAccumulatorVectors();

    /** 返回累加器 vector 的数量 */
    public abstract int accumulatorCount();

    // ==================== 辅助方法 ====================

    protected static double toDouble(FieldVector vector, int index) {
        Object obj = vector.getObject(index);
        if (obj == null) return 0.0;
        if (obj instanceof Number n) return n.doubleValue();
        try {
            return Double.parseDouble(obj.toString());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
