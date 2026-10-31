package com.dojao.core.function;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * 本地 / 共享路径读取器。
 *
 * <ul>
 *   <li>{@code file:///D:/udf/my_fns.py} 与裸路径 {@code D:/udf/my_fns.py} → 原样直通；</li>
 *   <li>{@code shared://rest} → {@code <system.function.shared.dir>/rest}（系统属性，默认 {@code <cwd>/shared}，
 *       面向"所有 worker 共享一个目录"的部署，不做拷贝）。</li>
 * </ul>
 */
public final class LocalFunctionPathReader implements FunctionPathReader {

    private final String shareRoot;

    public LocalFunctionPathReader() {
        this(System.getProperty("system.function.shared.dir",
                Path.of("shared").toAbsolutePath().normalize().toString()));
    }

    LocalFunctionPathReader(String shareRoot) {
        this.shareRoot = shareRoot;
    }

    @Override
    public Set<String> schemes() {
        return Set.of("file", "shared");
    }

    @Override
    public ResolvedCode resolve(String codePath) {
        String scheme = PathReaders.schemeOf(codePath);
        String rest;
        if ("shared".equals(scheme)) {
            rest = Path.of(shareRoot).resolve(codePath.substring("shared://".length())).toString();
        } else if ("file".equals(scheme)) {
            rest = fileUriToPath(codePath);
        } else {
            rest = codePath;   // 裸路径
        }
        Path abs = Path.of(rest).toAbsolutePath().normalize();
        if (!Files.exists(abs)) {
            throw new IllegalArgumentException("函数代码路径不存在: " + abs);
        }
        return new ResolvedCode(codePath, abs.toString(), false);
    }

    /** "file:///D:/x" / "file://D:/x" → "D:/x"（Windows 驱动器被 URI 当 host 的情况也处理）。 */
    private static String fileUriToPath(String codePath) {
        try {
            URI uri = URI.create(codePath);
            String host = uri.getHost();
            String path = uri.getPath();
            if (host != null && !host.isBlank()) {
                return host + ":" + path;               // file://D:/x → D:/x
            }
            if (path != null && path.length() >= 3 && path.charAt(0) == '/'
                    && path.charAt(2) == ':') {
                return path.substring(1);               // /D:/x → D:/x
            }
            if (path != null) {
                return path;
            }
        } catch (Exception ignored) {
            // 解析失败回退字符串切片
        }
        return codePath.substring("file://".length());
    }
}
