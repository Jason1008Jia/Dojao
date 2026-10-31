package com.dojao.core.function;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

/**
 * 函数代码路径读取器注册表（仿 {@link BackendRegistry}）。
 *
 * <p>内置：file/shared（本地直通）、http/https（JDK HttpClient）、hdfs（WebHDFS REST）、
 * s3（aws/mc CLI 壳）。用户自定义介质：实现 {@link FunctionPathReader} → {@link #register}
 * 注册，或放在 {@code META-INF/services/com.dojao.core.function.FunctionPathReader} 自动发现。</p>
 */
public final class PathReaders {

    private static final Map<String, FunctionPathReader> READERS = new TreeMap<>();
    private static final LocalFunctionPathReader LOCAL = new LocalFunctionPathReader();

    static {
        register(LOCAL);
        register(new HttpFunctionPathReader());
        register(new WebHdfsFunctionPathReader());
        register(new S3CliFunctionPathReader());
        // SPI 扩展：用户自定义介质
        for (FunctionPathReader reader : ServiceLoader.load(FunctionPathReader.class)) {
            register(reader);
        }
    }

    private PathReaders() {
    }

    /** 注册读取器（每个 scheme 小写）。 */
    public static void register(FunctionPathReader reader) {
        for (String scheme : reader.schemes()) {
            READERS.put(scheme.toLowerCase(), reader);
        }
    }

    /** 已注册的全部 scheme（排序，便于报错信息与测试）。 */
    public static java.util.Set<String> schemes() {
        return READERS.keySet();
    }

    /** 解析 codePath → 本地路径。裸路径（无 scheme）走本地读取器。 */
    public static ResolvedCode resolve(String codePath) {
        if (codePath == null || codePath.isBlank()) {
            throw new IllegalArgumentException("code_path 为空");
        }
        String scheme = schemeOf(codePath);
        FunctionPathReader reader = scheme == null ? LOCAL : READERS.get(scheme);
        if (reader == null) {
            throw new IllegalArgumentException("不支持的 code_path scheme: " + scheme
                    + "（已注册: " + READERS.keySet() + "）");
        }
        return reader.resolve(codePath);
    }

    /** 提取 URI scheme（"file:///x" → "file"）；裸路径 → null。 */
    public static String schemeOf(String codePath) {
        int i = codePath.indexOf("://");
        if (i <= 0) {
            return null;
        }
        return codePath.substring(0, i).toLowerCase();
    }
}
