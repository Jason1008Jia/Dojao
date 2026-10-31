package com.dojao.core.rpc.control;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Arrow schema ↔ 字节串 —— 把 {@code List<Field>} 塞进 proto 的 {@code bytes} 字段。
 *
 * <p><b>为什么需要它</b>：哈希 join 的 build 侧（右表）只在<b>收到过右表页</b>时才拿得到右列类型，
 * 而 {@code HashPartitionOperator.flushMyBuffers} 对空桶<b>一个页都不发</b>（只发 {@code partitionEnd}）
 * ⇒ 该分区的 join 退化成「只有左列」的短 schema。若此时左表还有行要吐（LEFT JOIN 未匹配行恒有），
 * 同一算子的不同分区就吐出<b>列数不同</b>的页 —— 下游 {@code PartitionBuffer.ensureTransfers} /
 * {@code appendRowsGather} 按 {@code vectors.size()} 取值，当场 {@code IndexOutOfBoundsException}
 * （顺序相反时则静默丢列）。
 *
 * <p>修法是：Master 在规划期<b>本来就知道</b>右表的声明列（连接器元数据），把它随片段一起下发，
 * 每个分区都能独立算出<b>同一份</b>输出 schema，与是否收到数据无关。
 *
 * <p><b>为什么用 Arrow IPC 而不是自定义编码</b>：{@link Field} 可含嵌套类型（List / Struct /
 * Decimal / Timestamp 带时区）与 metadata，逐字段手写编解码必然漏分支；Arrow 自带
 * {@code MessageSerializer.serializeMetadata / deserializeSchema} 是无损且与版本一致的唯一正确选项。
 * 用「元数据消息」而非「完整 IPC 流」：不需要 data body，也不引入额外的 stream 框架。
 *
 * <p>空清单 ⇄ 空字节串（proto3 的 {@code bytes} 默认空），调用方据此还原 {@code null}。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/27
 */
public final class ArrowSchemaIpc {

    private ArrowSchemaIpc() {
    }

    /** 列清单 → Arrow IPC 元数据字节；null / 空 → 空字节串。 */
    public static byte[] encode(List<Field> fields) {
        if (fields == null || fields.isEmpty()) {
            return new byte[0];
        }
        try {
            ByteBuffer bb = MessageSerializer.serializeMetadata(new Schema(fields), IpcOption.DEFAULT);
            byte[] out = new byte[bb.remaining()];
            bb.get(out);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("序列化 join 右表声明 schema 失败", e);
        }
    }

    /** Arrow IPC 元数据字节 → 列清单；空字节串 → null。 */
    public static List<Field> decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            Message msg = Message.getRootAsMessage(ByteBuffer.wrap(bytes));
            Schema schema = MessageSerializer.deserializeSchema(msg);
            return schema == null || schema.getFields().isEmpty() ? null : schema.getFields();
        } catch (Exception e) {
            throw new IllegalStateException("反序列化 join 右表声明 schema 失败", e);
        }
    }
}
