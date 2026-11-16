package com.dojao.core.operator;

import com.dojao.core.config.ResourceConfig;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;

import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

/**
 * 溢写 Arrow 文件的压缩编解码统一入口（LZ4 frame）。
 *
 * <p>溢写文件是<b>临时中间数据</b>，字符串列占比高、重复度大，裸 Arrow IPC 写盘量往往是内存数据量的
 * 数倍；而大排序 / 窗口的 Phase 1.5 归并每轮都会把全量重写一遍 —— 磁盘 I/O 与盘占用都被放大。
 * 开启压缩后文件按 Arrow IPC body compression 写（每 record batch 独立压缩），
 * {@link ArrowFileReader} 自动识别压缩元数据解压，读写路径与未压缩文件完全一致。</p>
 *
 * <p><b>开关</b>：{@code system.query.spill.compression}（兼容 {@code query.spill.compression}），
 * 只认 {@code true} / {@code false}，默认 {@code true}。压缩算法固定 LZ4 frame。
 * 关闭时 writer 退回旧的无压缩写法；<b>reader 恒带压缩工厂</b> —— 未压缩文件不会触碰工厂
 * （Arrow 仅在记录批带 body compression 元数据时才创建 codec），因此同一进程可安全读取
 * 本次开关生效前遗留的旧文件。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/15
 */
final class SpillCompression {

    /** LZ4 / ZSTD codec 提供者（arrow-compression）；本工程只用 LZ4 frame。 */
    private static final CompressionCodec.Factory FACTORY = CommonsCompressionFactory.INSTANCE;

    private SpillCompression() {
    }

    /** 新建溢写文件 writer：开启压缩时按 LZ4 frame 写，否则保持旧的无压缩格式。 */
    static ArrowFileWriter writer(VectorSchemaRoot root, WritableByteChannel out) {
        if (!ResourceConfig.spillCompressionEnabled()) {
            return new ArrowFileWriter(root, null, out);
        }
        return new ArrowFileWriter(root, null, out, Map.of(), IpcOption.DEFAULT,
                FACTORY, CompressionUtil.CodecType.LZ4_FRAME);
    }

    /** 新建溢写文件 reader：恒带压缩工厂，未压缩文件不受影响。 */
    static ArrowFileReader reader(SeekableByteChannel in, BufferAllocator allocator) {
        return new ArrowFileReader(in, allocator, FACTORY);
    }
}
