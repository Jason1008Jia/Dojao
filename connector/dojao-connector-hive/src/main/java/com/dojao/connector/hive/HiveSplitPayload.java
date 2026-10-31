package com.dojao.connector.hive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive 分片载荷编解码 —— 把「本分片要读哪些读取单元」编成一段<b>连接器私有字节</b>，
 * 装进 {@link com.dojao.spi.bean.Split#payload()}。单元见 {@link HiveSplitUnit}。
 *
 * <p>这是 v2 的做法。v1 曾把文件清单伪装成 {@code filterExpression} 里的
 * {@code __dojao_files__='p1\u0001p2'} 魔法串，副作用有三：谓词字段被污染（EXPLAIN 里能看到假谓词）、
 * 分隔符要挑控制字符防碰撞、路径里出现引号就会解析崩。现在引擎有了正式的切片 SPI，私有载荷
 * 原样搬运不解释，这些问题一并消失。</p>
 *
 * <p><b>格式 v3</b>：{@code byte 版本号} + {@code int32 单元数} + 每单元
 * {@code writeUTF 路径} + {@code long 起始偏移} + {@code long 字节长度}。</p>
 *
 * <p>v2（只有文件清单）→ v3 加了<b>首字节版本号</b>与区间字段：载荷由 Master 产出、Worker 消费，
 * 两边版本不一致时 v2 解码器会把版本号当成文件数的高位字节读出一个巨大的计数 —— 与其让它
 * 读出一堆乱码路径后崩在别处，不如在这里就明确报「版本不匹配，需同时重建重启」。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/9/20
 */
public final class HiveSplitPayload {

    /** 当前载荷格式版本；不匹配直接报错，不做猜测性兼容。 */
    private static final byte VERSION = 3;

    private HiveSplitPayload() {
    }

    /** 读取单元清单 → 载荷字节。 */
    public static byte[] encode(List<HiveSplitUnit> units) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeByte(VERSION);
            out.writeInt(units.size());
            for (HiveSplitUnit u : units) {
                out.writeUTF(u.path());
                out.writeLong(u.start());
                out.writeLong(u.length());
            }
        } catch (IOException e) {
            // ByteArrayOutputStream 不产生 IO 错误；真出了也只能是编码 bug
            throw new UncheckedIOException("编码 Hive 分片载荷失败", e);
        }
        return bos.toByteArray();
    }

    /** 载荷字节 → 读取单元清单。 */
    public static List<HiveSplitUnit> decode(byte[] payload) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int version = in.readByte();
            if (version != VERSION) {
                throw new IllegalStateException("Hive 分片载荷版本不匹配（读到 " + version
                        + "，期望 " + VERSION + "）—— Master 与 Worker 需同时重建重启");
            }
            int n = in.readInt();
            if (n < 0) throw new IllegalStateException("Hive 分片载荷损坏：单元数 = " + n);
            List<HiveSplitUnit> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                String path = in.readUTF();
                long start = in.readLong();
                long length = in.readLong();
                out.add(new HiveSplitUnit(path, start, length));
            }
            return out;
        } catch (IOException e) {
            throw new IllegalStateException("解码 Hive 分片载荷失败", e);
        }
    }
}
