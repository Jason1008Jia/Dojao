package com.example.dojao.csvdir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 本连接器的切片载荷编解码 —— 「一个切片 = 读哪几个 CSV 文件」。
 *
 * <p><b>为什么需要它。</b>本连接器的切片无法用 SQL 谓词表达（"读 part-003.csv" 不是 WHERE 条件），
 * 所以走 {@link com.dojao.spi.bean.Split} 的不透明载荷：规划期把文件清单编码成字节，引擎原样搬运到
 * Worker，读取时再交还给本连接器解码。载荷格式是<b>连接器私事</b>，引擎既不解析也不理解 ——
 * 第三方连接器想用什么格式就用什么格式（这里是「int 个数 + writeUTF 逐个路径」，够用且无歧义）。</p>
 *
 * <p>格式：{@code [int 文件数][UTF 路径 1][UTF 路径 2]...}（{@link DataOutputStream#writeUTF} 自带
 * 长度前缀，路径里出现任何字符都不会串位）。</p>
 *
 * @author 张三
 * @version 1.0
 * @date 2026/9/20
 */
final class CsvSplitPayload {

    private CsvSplitPayload() {
    }

    /** 把一批文件路径编码成切片载荷字节。 */
    static byte[] encode(List<String> files) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(files.size());
            for (String f : files) {
                out.writeUTF(f);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("编码 CSV 切片载荷失败", e);
        }
        return bos.toByteArray();
    }

    /** 把切片载荷字节还原成文件路径清单。 */
    static List<String> decode(byte[] payload) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int n = in.readInt();
            if (n < 0) {
                throw new IllegalStateException("CSV 切片载荷损坏：文件数 = " + n);
            }
            List<String> files = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                files.add(in.readUTF());
            }
            return files;
        } catch (IOException e) {
            throw new IllegalStateException("解码 CSV 切片载荷失败", e);
        }
    }
}
