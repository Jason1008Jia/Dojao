package com.dojao.core.function;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;

/**
 * 远程函数代码下载缓存：URI → 本地缓存目录（按 URI 的 SHA-1 前 16 位命名），
 * 同 URI 幂等复用。缓存根可经 {@code -Dsystem.function.cache.dir} 覆盖，
 * 默认 {@code <java.io.tmpdir>/dojao-function-cache}。
 */
final class FunctionCodeCache {

    private static final Path ROOT = Path.of(System.getProperty("system.function.cache.dir",
                    System.getProperty("java.io.tmpdir") + File.separator + "dojao-function-cache"))
            .toAbsolutePath().normalize();

    private FunctionCodeCache() {
    }

    /** URI → 缓存文件路径（单文件）或缓存目录（dir=true，下载器按目录组织文件）。 */
    static Path path(String codePath, boolean dir) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(codePath.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            Path base = ROOT.resolve(sb.substring(0, 16));
            Files.createDirectories(base);
            return dir ? base : base.resolve(fileNameOf(codePath));
        } catch (Exception e) {
            throw new IllegalStateException("创建函数代码缓存目录失败: " + ROOT, e);
        }
    }

    /** 从 URI 尾部取文件名（单文件下载落盘名）。 */
    static String fileNameOf(String codePath) {
        String s = codePath;
        int q = s.indexOf('?');
        if (q >= 0) {
            s = s.substring(0, q);
        }
        int slash = s.lastIndexOf('/');
        String name = slash >= 0 ? s.substring(slash + 1) : s;
        if (name.isBlank()) {
            name = "code";
        }
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /** 单文件已缓存（存在且非空）。 */
    static boolean cached(Path p) {
        if (!Files.isRegularFile(p)) {
            return false;
        }
        try {
            return Files.size(p) > 0;
        } catch (IOException e) {
            return false;
        }
    }

    /** 目录已缓存（存在且非空）。 */
    static boolean cachedDir(Path dir) {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        try (var s = Files.list(dir)) {
            return s.findAny().isPresent();
        } catch (IOException e) {
            return false;
        }
    }
}
