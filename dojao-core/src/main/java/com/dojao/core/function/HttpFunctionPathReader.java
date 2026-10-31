package com.dojao.core.function;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

import static com.dojao.core.function.FunctionPathErrors.isNetworkUnreachable;
import static com.dojao.core.function.FunctionPathErrors.networkUnreachable;

/**
 * HTTP(S) 读取器 —— JDK {@link HttpClient} 直接下载到本地缓存。
 *
 * <p>天然覆盖 S3 预签名 URL、对象存储公网直链、HTTP 文件服务器、Artifactory 等，
 * 零 SDK。仅支持单文件下载（jar/zip/py/whl 等）；目录请打成归档后走单文件。</p>
 */
public final class HttpFunctionPathReader implements FunctionPathReader {

    private static String httpStatusMessage(int code) {
        if (code == 401 || code == 403) {
            return "无权限访问（需认证/签名）";
        }
        if (code == 404) {
            return "URL 不存在或已失效";
        }
        if (code >= 500) {
            return "服务端错误";
        }
        return "请求被拒绝";
    }

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    @Override
    public Set<String> schemes() {
        return Set.of("http", "https");
    }

    @Override
    public ResolvedCode resolve(String codePath) {
        Path dest = FunctionCodeCache.path(codePath, false);
        if (FunctionCodeCache.cached(dest)) {
            return new ResolvedCode(codePath, dest.toString(), true);
        }
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(codePath))
                    .GET()
                    .timeout(Duration.ofMinutes(2))
                    .build();
            HttpResponse<Path> resp = client.send(req, HttpResponse.BodyHandlers.ofFile(dest));
            if (resp.statusCode() >= 400) {
                throw new IllegalArgumentException("下载函数代码失败: HTTP " + resp.statusCode() + " "
                        + codePath + " —— " + httpStatusMessage(resp.statusCode()));
            }
            return new ResolvedCode(codePath, dest.toString(), true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("下载函数代码中断: " + codePath, e);
        } catch (IOException e) {
            if (isNetworkUnreachable(e)) {
                throw networkUnreachable(codePath, URI.create(codePath).getHost(), e);
            }
            throw new IllegalStateException("下载函数代码失败: " + codePath, e);
        }
    }
}
