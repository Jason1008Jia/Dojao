package com.dojao.server.master;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Job 级别资源容器 —— 管理用户函数、临时文件、连接句柄等随 Job 生命周期的资源。
 *
 * <p>与集群资源（{@link ClusterResources}）无关：这是 Job 私有的资源视图，
 * Job 结束时统一 {@link #closeAll()} 释放。后续可按函数 / 连接 / 临时文件分类管理。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class JobResources {

    private final ConcurrentHashMap<String, Object> resources = new ConcurrentHashMap<>();

    /** 放入资源；value 为 null 等价于移除。 */
    public void put(String key, Object value) {
        if (value == null) {
            resources.remove(key);
        } else {
            resources.put(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) resources.get(key);
    }

    public Object remove(String key) {
        return resources.remove(key);
    }

    public boolean contains(String key) {
        return resources.containsKey(key);
    }

    public Map<String, Object> asMap() {
        return Map.copyOf(resources);
    }

    /** Job 结束（无论成败/取消）后统一清理全部资源。 */
    public void closeAll() {
        resources.clear();
    }
}
