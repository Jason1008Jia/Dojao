package com.dojao.common.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dojao 数据传输的基本单位。
 *
 * <p>Page 是 VectorSchemaRoot 的包装器，携带元数据（Schema、内存大小、溢写信息）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/12
 */
public class Page implements AutoCloseable {

    // ==================== 核心数据 ====================

    private final VectorSchemaRoot root;
    private final int rowCount;
    private final long sizeInBytes;
    private final long batchIndex;
    private final long createdAt;

    // ==================== 元数据 ====================

    private final Optional<Integer> partitionId;
    private final boolean isSpilled;
    private final Optional<String> spillLocation;
    /** 分区结束标记：root 为 null 但 {@link #isEOF()} 为 false（仅表达"该分区数据已发完"）。 */
    private final boolean endOfPartition;
    private final Map<String, Object> metadata;

    // ==================== 构造 ====================

    private Page(VectorSchemaRoot root,
                 long batchIndex,
                 Integer partitionId,
                 boolean isSpilled,
                 String spillLocation) {
        this(root, batchIndex, partitionId, isSpilled, spillLocation, false);
    }

    private Page(VectorSchemaRoot root,
                 long batchIndex,
                 Integer partitionId,
                 boolean isSpilled,
                 String spillLocation,
                 boolean endOfPartition) {
        this.root = root;
        this.rowCount = root != null ? root.getRowCount() : 0;
        this.sizeInBytes = root != null ? estimateSizeInBytes(root) : 0;
        this.batchIndex = batchIndex;
        this.createdAt = System.currentTimeMillis();
        this.partitionId = Optional.ofNullable(partitionId);
        this.isSpilled = isSpilled;
        this.spillLocation = Optional.ofNullable(spillLocation);
        this.endOfPartition = endOfPartition;
        this.metadata = new ConcurrentHashMap<>();
    }

    // ==================== 工厂方法 ====================

    public static Page fromRoot(VectorSchemaRoot root) {
        return new Page(root, 0, null, false, null);
    }

    public static Page fromRoot(VectorSchemaRoot root, long batchIndex) {
        return new Page(root, batchIndex, null, false, null);
    }

    public static Page spillFrom(String spillLocation, VectorSchemaRoot root, long batchIndex) {
        return new Page(root, batchIndex, null, true, spillLocation);
    }

    public static Page partitioned(VectorSchemaRoot root, int partitionId) {
        return new Page(root, 0, partitionId, false, null);
    }

    public static Page eof() {
        return new Page(null, -1, null, false, null);
    }

    /** 分区结束标记：表示 partitionId 所属分区的数据已全部发出（root 为 null，isEOF() 为 false）。 */
    public static Page partitionEnd(int partitionId) {
        return new Page(null, -1, partitionId, false, null, true);
    }

    /**
     * 换分区号（浅包装，共享同一 root，不复制、不 close 源）：把跨机回传后丢失分区号的页
     * 归一到其真实桶下标。调用方须丢弃源页（新页独占 root 生命周期，只 close 一次）。
     */
    public Page withPartitionId(int partitionId) {
        return new Page(root, batchIndex, partitionId, isSpilled, spillLocation.orElse(null));
    }

    // ==================== Getter ====================

    public VectorSchemaRoot getRoot() { return root; }

    public Schema getSchema() {
        return root != null ? root.getSchema() : null;
    }

    public int getRowCount() { return rowCount; }

    public long getSizeInBytes() { return sizeInBytes; }

    public long getBatchIndex() { return batchIndex; }

    public long getCreatedAt() { return createdAt; }

    public Optional<Integer> getPartitionId() { return partitionId; }

    public boolean isSpilled() { return isSpilled; }

    public Optional<String> getSpillLocation() { return spillLocation; }

    public Map<String, Object> getMetadata() { return metadata; }

    public boolean isEOF() { return root == null && !endOfPartition; }

    /** 分区结束标记：该分区的数据已全部发完（区别于 {@link #isEOF()} 的全局结束）。 */
    public boolean isEndOfPartition() { return endOfPartition; }

    public boolean isEmpty() { return rowCount == 0; }

    public double getSizeMB() {
        return sizeInBytes / (1024.0 * 1024.0);
    }

    @SuppressWarnings("unchecked")
    public <T extends FieldVector> T getVector(String columnName) {
        return (T) root.getVector(columnName);
    }

    // ==================== 投影 ====================

    public Page select(List<String> columnNames) {
        if (isEOF()) return this;

        // "*" 在运行时展开为上游全部字段（SELECT * 的投影）。
        // 内部列（外连接同名列副本 __jk_ / 窗口键物化列 __win_key_，见 Planner 约定）不对用户暴露。
        List<String> names = new ArrayList<>();
        for (String name : columnNames) {
            if ("*".equals(name)) {
                for (org.apache.arrow.vector.types.pojo.Field f : root.getSchema().getFields()) {
                    if (f.getName().startsWith("__jk_")
                            || f.getName().startsWith("__win_key_")) continue;
                    names.add(f.getName());
                }
            } else {
                names.add(name);
            }
        }

        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();
        List<FieldVector> vectors = new ArrayList<>();
        List<org.apache.arrow.vector.types.pojo.Field> schemaFields = root.getSchema().getFields();

        // 投影含重复列名时，transfer() 会把同一源 vector 二次掏空导致拿到空 buffer，
        // 必须回退逐格拷贝。
        boolean hasDuplicateName = new HashSet<>(names).size() != names.size();

        // 重名列解析：同一名字第 k 次出现 → 取 schema 中第 k 个同名列。
        // findField(name)+indexOf(field) 永远解析到第一个匹配，join 输出多列同名
        // （如两个 id）时第二个 id 会错误地拿到第一列，这里改为按出现次序匹配。
        Map<String, Integer> occurrence = new HashMap<>();
        for (String name : names) {
            int k = occurrence.merge(name, 1, Integer::sum);
            int idx = -1;
            int seen = 0;
            for (int i = 0; i < schemaFields.size(); i++) {
                if (schemaFields.get(i).getName().equals(name)) {
                    seen++;
                    if (seen == k) { idx = i; break; }
                }
            }
            if (idx < 0) continue;   // 名字不存在（含第 k 次超出实际列数）→ 静默跳过
            org.apache.arrow.vector.types.pojo.Field field = schemaFields.get(idx);
            fields.add(field);

            FieldVector src = root.getVector(idx);
            FieldVector dst = field.createVector(src.getAllocator());
            if (hasDuplicateName) {
                // 拷贝向量数据，避免与原 Page 共享底层 Arrow buffer
                // （否则原 Page 被 close 后，新 Page 就是野指针）
                dst.setInitialCapacity(rowCount);
                dst.allocateNew();
                org.apache.arrow.vector.util.TransferPair transfer = src.makeTransferPair(dst);
                for (int i = 0; i < rowCount; i++) {
                    transfer.copyValueSafe(i, i);
                }
            } else {
                // 零拷贝：整块 buffer 所有权从源转移到目标（源被掏空后 close 安全）。
                // 前提由唯一调用方 ProjectOperator 保证：select 之后立即 close 源 Page。
                // transfer 自带 buffer，无需 allocateNew。
                src.makeTransferPair(dst).transfer();
                // 防御：直接写 buffer 的生产者（HashJoin.assemble 等）不推进 lastSet，
                // 显式置位使下方 setValueCount 的 fillHoles 变 no-op，避免 var offset 被清零。
                if (dst instanceof BaseVariableWidthVector dv) dv.setLastSet(rowCount - 1);
            }
            dst.setValueCount(rowCount);
            vectors.add(dst);
        }

        VectorSchemaRoot newRoot = new VectorSchemaRoot(fields, vectors, rowCount);
        return new Page(newRoot, batchIndex, partitionId.orElse(null),
                isSpilled, spillLocation.orElse(null));
    }

    // ==================== 内存管理 ====================

    /**
     * 把本页 Arrow buffer 的所有权整体移交到 {@code target} 分配器，并关闭本页。同 root 时零拷贝
     * （只转移所有权，不复制数据）；跨 root 时（源 / 目标分配器不共享同一 root）退化为逐格拷贝，
     * 因为 Arrow 的 {@code transfer()} 不允许跨 root 的 buffer 关联。
     *
     * <p>用于<b>跨线程 / 跨生命周期交接</b>：页面交给另一端后可能被延迟 close，而创建它的算子
     * 分配器可能已提前关闭（如分布式片段里 Producer 的 HashPartition、Consumer 的 HashJoin），
     * 导致 Arrow 在关闭分配器时误报 {@code Memory was leaked by query}。移交后页面的 buffer 归属
     * {@code target}，原分配器即可安全关闭。</p>
     *
     * @param target 接管 buffer 所有权的目标分配器（须活得比新页的最终 close 更长）
     * @return 新 Page（buffer 归属 target，调用方接管其生命周期）；EOF / 分区结束标记原样返回
     */
    public Page transferTo(BufferAllocator target) {
        if (root == null) {
            return this;   // EOF / partitionEnd 无数据 buffer，无需转移
        }
        var fields = root.getSchema().getFields();
        List<FieldVector> moved = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            FieldVector src = root.getVector(i);
            FieldVector dst = fields.get(i).createVector(target);
            if (src.getAllocator().getRoot() == target.getRoot()) {
                // 同 root：零拷贝，整块 buffer 所有权从源转移到目标
                src.makeTransferPair(dst).transfer();
                // 直接写 buffer 的生产者不推进 lastSet，显式置位使下方 setValueCount 的 fillHoles 变 no-op，
                // 避免 var offset 被清零（与 select 零拷贝分支同款处理）
                if (rowCount > 0 && dst instanceof BaseVariableWidthVector dv) {
                    dv.setLastSet(rowCount - 1);
                }
            } else {
                // 跨 root：Arrow 的 transfer 要求源/目标分配器共享同一 root（否则抛
                // IllegalStateException），这里退化为逐格拷贝。copyValueSafe 内部更新目的
                // lastSet，无 var offset 清零问题（与 select 的 hasDuplicateName 分支同款）。
                dst.setInitialCapacity(rowCount);
                dst.allocateNew();
                TransferPair copy = src.makeTransferPair(dst);
                for (int r = 0; r < rowCount; r++) {
                    copy.copyValueSafe(r, r);
                }
            }
            dst.setValueCount(rowCount);
            moved.add(dst);
        }
        VectorSchemaRoot newRoot = new VectorSchemaRoot(fields, moved, rowCount);
        Page movedPage = new Page(newRoot, batchIndex, partitionId.orElse(null),
                isSpilled, spillLocation.orElse(null));
        close();   // 源向量已掏空，close 无实际内存释放
        return movedPage;
    }

    @Override
    public void close() {
        if (root != null) {
            root.close();
        }
    }

    private static long estimateSizeInBytes(VectorSchemaRoot root) {
        long total = 0;
        for (FieldVector vector : root.getFieldVectors()) {
            total += vector.getBufferSize();
        }
        return total;
    }

    @Override
    public String toString() {
        if (isEOF()) return "Page[EOF]";
        if (endOfPartition) return "Page[EndOfPartition pid=" + partitionId.orElse(-1) + "]";
        return String.format("Page[rows=%d, size=%.2fMB, batch=%d, spilled=%s, partition=%s]",
                rowCount, getSizeMB(), batchIndex, isSpilled, partitionId.orElse(-1));
    }
}
