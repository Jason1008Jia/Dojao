package com.dojao.ui.ai;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AI 导出文件的临时存储：{@code export_file} 工具把生成的字节落盘并换回一个短 id，
 * 前端点「下载」时经 {@code GET /api/ai/files/{id}} 取回。
 *
 * <p>刻意<b>按登录用户（viewer）隔离</b>：A 用户拿不到 B 用户的文件。文件放在系统临时目录下的
 * 独立子目录，进程退出时删除；单文件 {@link #MAX_FILE_BYTES 10MB}、每用户最多
 * {@link #MAX_FILES_PER_OWNER 20} 份、默认 {@link #DEFAULT_TTL_MILLIS 30 分钟}后清理，
 * 避免长期运行把磁盘占满。</p>
 *
 * <p>文件名在这里不做业务校验（由 {@link AiExportFormats#fileName} 清洗），只负责存储与隔离。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/18
 */
public final class AiFileStore implements AutoCloseable {

    /** 单个导出文件最大字节数。 */
    public static final int MAX_FILE_BYTES = 10 * 1024 * 1024;

    /** 每个用户最多保留的导出文件份数（超出时淘汰最旧的）。 */
    public static final int MAX_FILES_PER_OWNER = 20;

    /** 默认保留时长（30 分钟）。 */
    public static final long DEFAULT_TTL_MILLIS = 30 * 60 * 1000L;

    /** 一份已存储的导出文件。{@code owner} 为 {@code null} 归一到空串（admin / 未登录视角）。 */
    public record StoredFile(String id, String owner, String name, String format, long size,
                             long createdAtMillis, Path path) {
    }

    private final Path dir;
    private final long ttlMillis;
    /** 按写入顺序保存（LinkedHashMap），便于按 owner 淘汰最旧文件。 */
    private final Map<String, StoredFile> files = new LinkedHashMap<>();
    private final AtomicLong seq = new AtomicLong();

    /** 生产用：系统临时目录下的独立子目录。 */
    public AiFileStore() {
        this(createDefaultDir(), DEFAULT_TTL_MILLIS);
    }

    /** 探针用：指定目录与 TTL（包内可见）。 */
    AiFileStore(Path dir, long ttlMillis) {
        this.dir = dir;
        this.ttlMillis = ttlMillis;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new IllegalStateException("创建 AI 导出目录失败: " + e.getMessage(), e);
        }
    }

    private static Path createDefaultDir() {
        try {
            return Files.createTempDirectory("dojao-ai-files");
        } catch (IOException e) {
            throw new IllegalStateException("创建 AI 导出目录失败: " + e.getMessage(), e);
        }
    }

    /**
     * 保存一份文件，返回存储记录（含短 id）。
     *
     * @throws IllegalArgumentException 内容为空 / 超过单文件上限
     */
    public synchronized StoredFile save(String owner, String name, String format, byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("文件内容为空。");
        }
        if (data.length > MAX_FILE_BYTES) {
            throw new IllegalArgumentException("文件过大（" + data.length + " 字节），单文件上限 "
                    + MAX_FILE_BYTES + " 字节。");
        }
        sweep();
        String ow = key(owner);
        // 每用户份数上限：淘汰该用户最旧的一份
        while (countOwned(ow) >= MAX_FILES_PER_OWNER) {
            StoredFile oldest = oldestOwned(ow);
            if (oldest == null) {
                break;
            }
            delete(oldest);
        }
        String id = Long.toString(seq.incrementAndGet(), 36)
                + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        Path path = dir.resolve(id + ".bin");
        try {
            Files.write(path, data);
        } catch (IOException e) {
            throw new IllegalStateException("写入导出文件失败: " + e.getMessage(), e);
        }
        StoredFile f = new StoredFile(id, ow, name, format, data.length, System.currentTimeMillis(), path);
        files.put(id, f);
        return f;
    }

    /** 按 id 取文件；用户不匹配 / 不存在 / 已过期 → {@code null}。 */
    public synchronized StoredFile find(String owner, String id) {
        sweep();
        StoredFile f = id == null ? null : files.get(id);
        if (f == null || !f.owner().equals(key(owner))) {
            return null;
        }
        return f;
    }

    /** 当前存活文件数（探针用）。 */
    public synchronized int size() {
        sweep();
        return files.size();
    }

    /** 清空全部文件与目录。 */
    @Override
    public synchronized void close() {
        for (StoredFile f : files.values()) {
            try {
                Files.deleteIfExists(f.path());
            } catch (IOException ignored) {
                // 尽力删除
            }
        }
        files.clear();
        try {
            Files.deleteIfExists(dir);
        } catch (IOException ignored) {
            // 目录非空 / 被占用时留给系统临时目录清理
        }
    }

    private static String key(String owner) {
        return owner == null ? "" : owner;
    }

    private int countOwned(String owner) {
        int n = 0;
        for (StoredFile f : files.values()) {
            if (f.owner().equals(owner)) {
                n++;
            }
        }
        return n;
    }

    private StoredFile oldestOwned(String owner) {
        for (StoredFile f : files.values()) {
            if (f.owner().equals(owner)) {
                return f;
            }
        }
        return null;
    }

    private void sweep() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, StoredFile>> it = files.entrySet().iterator();
        while (it.hasNext()) {
            StoredFile f = it.next().getValue();
            if (now - f.createdAtMillis() > ttlMillis) {
                it.remove();
                delete(f);
            }
        }
    }

    private void delete(StoredFile f) {
        files.remove(f.id());
        try {
            Files.deleteIfExists(f.path());
        } catch (IOException ignored) {
            // 删除失败不影响主流程（TTL 到了仍会被再次尝试）
        }
    }
}
