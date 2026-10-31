package com.dojao.core.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.dojao.core.function.FunctionPathErrors.isNetworkUnreachable;
import static com.dojao.core.function.FunctionPathErrors.networkUnreachable;

/**
 * HDFS 读取器 —— 走 WebHDFS REST（HDFS 默认开启 {@code dfs.webhdfs.enabled}）。
 *
 * <p>{@code hdfs://namenode:8020/udf/my_fns.py} → 映射到
 * {@code http://namenode:9870/webhdfs/v1/udf/my_fns.py}（Namenode HTTP 端口默认 9870，
 * 用 {@code -Ddojao.webhdfs.base=http://host:port} 覆盖），
 * {@code op=GETFILESTATUS} 判型 → 文件 {@code op=OPEN} / 目录 {@code op=LISTSTATUS} 递归下载。</p>
 *
 * <p>文件 → 下载到缓存 {@code cache/<hash>/<文件名>}；目录 → 递归拉到
 * {@code cache/<hash>/<目录名>/}（保留相对路径），供后端整体加进 PYTHONPATH / classpath。</p>
 */
public final class WebHdfsFunctionPathReader implements FunctionPathReader {

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String webhdfsBase;   // 例 "http://namenode:9870"；空 → 从 hdfs URI 推导

    public WebHdfsFunctionPathReader() {
        this(System.getProperty("system.function.webhdfs.base", "").trim());
    }

    WebHdfsFunctionPathReader(String webhdfsBase) {
        this.webhdfsBase = webhdfsBase.endsWith("/")
                ? webhdfsBase.substring(0, webhdfsBase.length() - 1) : webhdfsBase;
    }

    @Override
    public Set<String> schemes() {
        return Set.of("hdfs");
    }

    @Override
    public ResolvedCode resolve(String codePath) {
        URI uri = URI.create(codePath);
        String hdfsPath = uri.getPath();
        String base = !webhdfsBase.isBlank() ? webhdfsBase : deriveWebhdfsBase(uri);
        String type = fileStatus(base, hdfsPath).path("FileStatus").path("type").asText();
        if ("FILE".equals(type)) {
            Path dest = FunctionCodeCache.path(codePath, false);
            if (!FunctionCodeCache.cached(dest)) {
                download(base, hdfsPath, dest);
            }
            return new ResolvedCode(codePath, dest.toString(), true);
        }
        // DIRECTORY → 递归拉取，保留相对路径
        Path root = FunctionCodeCache.path(codePath, true).resolve(lastSegment(hdfsPath, "code"));
        if (!FunctionCodeCache.cachedDir(root)) {
            List<String[]> files = new ArrayList<>();
            listRecursive(base, hdfsPath, "", files);
            try {
                Files.createDirectories(root);
            } catch (IOException e) {
                throw new IllegalStateException("创建函数代码缓存目录失败: " + root, e);
            }
            for (String[] f : files) {
                download(base, f[0], root.resolve(f[1]));
            }
        }
        return new ResolvedCode(codePath, root.toString(), true);
    }

    /** hdfs://host:8020 → http://host:9870（-Dsystem.function.webhdfs.port 可覆盖）。 */
    private static String deriveWebhdfsBase(URI uri) {
        int httpPort = Integer.parseInt(System.getProperty("system.function.webhdfs.port", "9870"));
        return "http://" + uri.getHost() + ":" + httpPort;
    }

    private JsonNode fileStatus(String base, String hdfsPath) {
        return get(base + "/webhdfs/v1" + encodePath(hdfsPath) + "?op=GETFILESTATUS", hdfsPath);
    }

    /** 递归收集 [hdfsPath, relativeToRoot]（文件）。 */
    private void listRecursive(String base, String hdfsPath, String relPrefix, List<String[]> out) {
        JsonNode node = get(base + "/webhdfs/v1" + encodePath(hdfsPath) + "?op=LISTSTATUS", hdfsPath);
        for (JsonNode fs : node.path("FileStatuses").path("FileStatus")) {
            String name = fs.path("pathSuffix").asText();
            String type = fs.path("type").asText();
            String full = hdfsPath.endsWith("/") ? hdfsPath + name : hdfsPath + "/" + name;
            String rel = relPrefix.isEmpty() ? name : relPrefix + "/" + name;
            if ("DIRECTORY".equals(type)) {
                listRecursive(base, full, rel, out);
            } else {
                out.add(new String[]{full, rel});
            }
        }
    }

    private void download(String base, String hdfsPath, Path dest) {
        String url = base + "/webhdfs/v1" + encodePath(hdfsPath) + "?op=OPEN";
        try {
            Files.createDirectories(dest.getParent());
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .GET()
                    .timeout(Duration.ofMinutes(5))
                    .build();
            HttpResponse<Path> resp = client.send(req, HttpResponse.BodyHandlers.ofFile(dest));
            if (resp.statusCode() >= 400) {
                throw new IllegalArgumentException("WebHDFS 下载失败: HTTP " + resp.statusCode() + " "
                        + url + " —— " + httpStatusMessage(resp.statusCode(), hdfsPath));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("WebHDFS 下载中断: " + url, e);
        } catch (IOException e) {
            if (isNetworkUnreachable(e)) {
                throw networkUnreachable("hdfs://" + hdfsPath, URI.create(url).getHost(), e);
            }
            throw new IllegalStateException("WebHDFS 下载失败: " + url, e);
        }
    }

    private JsonNode get(String url, String hdfsPath) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .GET()
                    .timeout(Duration.ofMinutes(2))
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() >= 400) {
                throw new IllegalArgumentException("WebHDFS 请求失败: HTTP " + resp.statusCode() + " "
                        + url + " —— " + httpStatusMessage(resp.statusCode(), hdfsPath)
                        + "（hdfs:// 需 Namenode 开启 WebHDFS，-Ddojao.webhdfs.base 可覆盖）");
            }
            return mapper.readTree(resp.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("WebHDFS 请求中断: " + url, e);
        } catch (IOException e) {
            if (isNetworkUnreachable(e)) {
                throw networkUnreachable("hdfs://" + hdfsPath, URI.create(url).getHost(), e);
            }
            throw new IllegalStateException("WebHDFS 请求失败: " + url, e);
        }
    }

    /** WebHDFS HTTP 状态码 → 人话。 */
    private static String httpStatusMessage(int code, String hdfsPath) {
        if (code == 401 || code == 403) {
            return "无权限访问 " + hdfsPath + "（WebHDFS 需授权/Kerberos）";
        }
        if (code == 404) {
            return "路径不存在: " + hdfsPath;
        }
        if (code >= 500) {
            return "NameNode 服务端错误";
        }
        return "请求被拒绝";
    }

    /** 逐段 URL 编码，保留 "/"。 */
    private static String encodePath(String p) {
        if (p == null || p.isEmpty()) {
            return "/";
        }
        StringBuilder sb = new StringBuilder();
        for (String seg : p.split("/")) {
            sb.append('/').append(URLEncoder.encode(seg, StandardCharsets.UTF_8).replace("+", "%20"));
        }
        return sb.length() == 0 ? "/" : sb.toString();
    }

    private static String lastSegment(String p, String fallback) {
        String s = p.endsWith("/") ? p.substring(0, p.length() - 1) : p;
        int i = s.lastIndexOf('/');
        String name = i >= 0 ? s.substring(i + 1) : s;
        return name.isBlank() ? fallback : name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
