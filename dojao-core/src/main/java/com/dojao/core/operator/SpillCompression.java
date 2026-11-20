package com.dojao.core.operator;

import com.dojao.core.config.ResourceConfig;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 溢写 Arrow 文件的压缩编解码统一入口。
 *
 * <p><b>写路径恒不压缩</b>：Arrow IPC 的 LZ4 body compression 走 commons-compress 的
 * {@code FramedLZ4CompressorOutputStream}，在真实溢写批次上吞吐只有 ~0.2MB/s（实测 25 万行
 * VarChar 键 4.5s、40 万行 106s、105 万行 75~143s），而无压缩写同样的批只要 15~35ms。
 * 溢写是 Phase 1 的关键路径，压缩比（~3.7x）远不值这个代价 —— 表现为「溢写一触发查询就卡死」。
 * {@code system.query.spill.compression=true} 只能换来一条告警，不再改变写路径。</p>
 *
 * <p><b>读路径恒带压缩工厂</b>：{@link ArrowFileReader} 按每个 record batch 的元数据自动选择是否解压，
 * 未压缩文件不会触碰工厂；因此历史压缩文件仍可读，混写（老文件解压 / 新文件直读）也安全。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/15
 */
final class SpillCompression {

    private static final Logger log = LoggerFactory.getLogger(SpillCompression.class);

    /** LZ4 / ZSTD codec 提供者（arrow-compression）。 */
    private static final CompressionCodec.Factory FACTORY = CommonsCompressionFactory.INSTANCE;

    private static final AtomicBoolean WARNED = new AtomicBoolean(false);

    private SpillCompression() {
    }

    /** 新建溢写文件 writer：恒写成无压缩 Arrow IPC（压缩写实测吞吐 ~0.2MB/s，见类注释）。 */
    static ArrowFileWriter writer(VectorSchemaRoot root, WritableByteChannel out) {
        if (ResourceConfig.spillCompressionEnabled() && WARNED.compareAndSet(false, true)) {
            log.warn("[溢写] 配置要求开启溢写压缩，但压缩写实测吞吐仅 ~0.2MB/s（百万行批次可达分钟级），"
                    + "已忽略该配置、保持无压缩写入；请把 query.spill.compression 置为 false 消除此告警");
        }
        return new ArrowFileWriter(root, null, out);
    }

    /** 新建溢写文件 reader：恒带压缩工厂，未压缩文件不受影响。 */
    static ArrowFileReader reader(SeekableByteChannel in, BufferAllocator allocator) {
        return new ArrowFileReader(in, allocator, FACTORY);
    }
}
