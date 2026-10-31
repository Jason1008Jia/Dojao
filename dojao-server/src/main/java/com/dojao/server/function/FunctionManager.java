package com.dojao.server.function;

import com.dojao.core.function.BackendRegistry;
import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.function.table.TableFunctionRegistry;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.storage.MetaStoreStorage;
import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.function.CollectAggregateRegistry;
import com.dojao.spi.function.FunctionBackend;
import com.dojao.spi.function.ScalarFunction;
import com.dojao.spi.function.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 函数管理器 —— CREATE / DROP FUNCTION 的入口。
 *
 * <p>职责：① CREATE FUNCTION 按 {@code type} 解析后端 → {@code init()} + {@code validate()} 冒烟校验
 * （<b>失败 fail-fast，不落库不注册</b>）→ 内存注册 + 持久化（永久函数落 {@link MetaStoreStorage}）；
 * ② DROP FUNCTION 从内存注册表 + 执行注册表 + 存储移除；③ 查询前 {@link #ensureLoaded()} 懒加载，
 * 把已定义函数灌进执行注册表（标量→ScalarFunctionRegistry、聚合→AggregateFunctionRegistry、
 * 表函数→TableFunctionRegistry）；
 * ④ 启动时 {@link #loadPermanentFunctions()} 从存储恢复永久函数。</p>
 *
 * <p>持久化：LOCAL 存储已实现（进程内 Map）；MySQL/PG/SQLServer 需建 {@code dojao_functions} 表后
 * 覆盖 {@link MetaStoreStorage} 函数 CRUD。后端未实现持久化时，CREATE/DROP 仍生效但只限本进程（告警）。
 * {@code temporary} 语义：临时函数只进内存会话表，不落库，DROP 时按 temporary 匹配。</p>
 */
public final class FunctionManager {

    private static final Logger log = LoggerFactory.getLogger(FunctionManager.class);

    /** 永久函数注册表：函数名 → 定义（CREATE FUNCTION 非 temporary）。 */
    private static final Map<String, FunctionDefinition> DEFINITIONS = new ConcurrentHashMap<>();

    /** 临时函数注册表：CREATE TEMPORARY/SESSION FUNCTION（只进内存，不落库）。 */
    private static final Map<String, FunctionDefinition> TEMPORARY = new ConcurrentHashMap<>();

    /** 已加载进执行注册表（ScalarFunctionRegistry / AggregateFunctionRegistry）的函数名。 */
    private static final java.util.Set<String> LOADED = ConcurrentHashMap.newKeySet();

    /** 函数持久化后端（由 MasterServer 启动时注入；null = 仅内存）。 */
    private static volatile MetaStoreStorage storage;

    private FunctionManager() {
    }

    /** 注入持久化后端（MasterServer 启动时调用）。 */
    public static void init(MetaStoreStorage storageBackend) {
        storage = storageBackend;
    }

    /**
     * 创建函数：后端解析 → 冒烟校验（先）→ init 兜底（后）→ 内存注册 + 持久化。
     *
     * <p><b>顺序关键</b>：validate 必须先于 init —— Python 后端在 validate 里把
     * code_path 解析出的目录注入 PYTHONPATH，必须在 sidecar spawn（init）之前完成；
     * 否则进程池启动时缺用户代码目录。validate 内部已自 ensureReady，init 仅为幂等兜底。</p>
     *
     * <p>永久函数（{@code temporary=false}）：校验通过后先落 {@link MetaStoreStorage} 再进内存表；
     * 后端未实现函数持久化（{@link UnsupportedOperationException}）→ 告警并降级为仅内存
     * （函数本进程可用，重启丢失）。临时函数只进内存 {@code TEMPORARY} 表，不落库。</p>
     *
     * @param def        函数定义（name/type/code_path/class_name 已就绪）
     * @param temporary  CREATE TEMPORARY/SESSION FUNCTION
     * @param ifNotExists 已存在时是否静默成功（IF NOT EXISTS）
     * @return 注册后的定义
     * @throws IllegalArgumentException 类型不支持 / 校验失败 / 重名冲突（fail-fast，不落库）
     */
    public static FunctionDefinition createFunction(FunctionDefinition def, boolean temporary, boolean ifNotExists) {
        return createFunction(def, temporary, ifNotExists, null);
    }

    /**
     * 创建函数（带归属创建者）。
     *
     * @param owner 归属创建者（Web 控制台登录用户名）；{@code null} = 全局（CLI/JDBC/未登录）。
     *              仅当 {@code WITH (is_private='true')} 时用于可见性隔离。
     */
    public static FunctionDefinition createFunction(FunctionDefinition def, boolean temporary,
                                                    boolean ifNotExists, String owner) {
        Map<String, FunctionDefinition> target = temporary ? TEMPORARY : DEFINITIONS;
        // 定义已带 owner（如派发恢复）则尊重之，否则注入本次创建者
        String effectiveOwner = def.owner() != null ? def.owner() : owner;
        FunctionDefinition stored = withOwner(def, effectiveOwner);
        if (target.containsKey(stored.name())) {
            if (ifNotExists) {
                return stored; // IF NOT EXISTS：已存在则静默成功
            }
            throw new IllegalArgumentException("函数已存在: " + stored.name());
        }
        FunctionBackend backend = BackendRegistry.resolve(stored.type())
                .orElseThrow(() -> new IllegalArgumentException(
                        "不支持的函数类型: " + stored.type()
                                + "（已注册: " + BackendRegistry.types() + "）"));
        backend.validate(stored); // 冒烟校验失败 → 异常向上抛，不落库
        backend.init();        // 幂等兜底（validate 已确保就绪）
        if (!temporary) {
            persist(stored);   // 永久函数落库；后端不支持持久化时告警降级（函数仍可用）
        }
        target.put(stored.name(), stored);
        return stored;
    }

    /**
     * 删除函数：从目标注册表（临时/永久）+ 执行注册表 + 存储移除。
     *
     * <p>执行注册表清理：标量→{@link ScalarFunctionRegistry#unregisterGlobal}、
     * 聚合→{@link AggregateFunctionRegistry#unregisterGlobal}（Python 后端一个函数可能同时注册两类，
     * 所以两类都清）。同时从 {@link #LOADED} 移除，保证同名重建后会重新加载。</p>
     *
     * @param name     函数名（全限定，与 CREATE 一致）
     * @param temporary 是否按临时函数删除（DROP TEMPORARY/SESSION FUNCTION）
     * @param ifExists DROP FUNCTION IF EXISTS 静默成功
     * @return 被删除的定义；IF EXISTS 且不存在时返回 null
     * @throws IllegalArgumentException 函数不存在且非 IF EXISTS
     */
    public static FunctionDefinition dropFunction(String name, boolean temporary, boolean ifExists) {
        return dropFunction(name, temporary, ifExists, null);
    }

    /**
     * 删除函数（带归属校验）。
     *
     * @param owner 当前视角用户名；{@code null} = 全权（admin / CLI/JDBC）。私有函数仅创建者
     *              （或 {@code owner==null}）可删。
     */
    public static FunctionDefinition dropFunction(String name, boolean temporary, boolean ifExists, String owner) {
        Map<String, FunctionDefinition> target = temporary ? TEMPORARY : DEFINITIONS;
        FunctionDefinition def = target.get(name);
        if (def == null) {
            if (ifExists) {
                log.info("DROP FUNCTION IF EXISTS {}（不存在，静默跳过）", name);
                return null;
            }
            throw new IllegalArgumentException("函数不存在: " + name);
        }
        if (!MetaStore.visibleTo(owner, def.owner(), def.properties())) {
            throw new IllegalArgumentException("函数 '" + name + "' 属于其他用户，无权删除");
        }
        target.remove(name);
        LOADED.remove(name);
        ScalarFunctionRegistry.unregisterGlobal(name);
        AggregateFunctionRegistry.unregisterGlobal(name);
        CollectAggregateRegistry.unregisterGlobal(name);
        TableFunctionRegistry.unregisterGlobal(name);
        if (!temporary) {
            try {
                if (storage != null) {
                    storage.dropFunction(name);
                }
            } catch (UnsupportedOperationException e) {
                log.warn("存储后端未实现函数持久化，DROP {} 仅作用于本进程: {}", name, e.getMessage());
            }
        }
        log.info("DROP FUNCTION {}（temporary={}）", name, temporary);
        return def;
    }

    /**
     * 启动恢复：从存储读出全部永久函数定义 → 内存注册表。
     *
     * <p>只恢复定义，不在此刻加载成可执行函数 —— 首次查询时由 {@link #ensureLoaded()} 懒加载
     * （坏函数只在被使用时才 fail-fast，启动不被坏函数阻塞）。</p>
     */
    public static void loadPermanentFunctions() {
        if (storage == null) {
            log.warn("函数持久化后端未注入，跳过启动恢复（FunctionManager.init 未调用？）");
            return;
        }
        for (FunctionDefinition def : storage.listFunctions()) {
            DEFINITIONS.put(def.name(), def);
            log.info("函数已从存储恢复: name={}, type={}", def.name(), def.type());
        }
    }

    /**
     * 查询前惰性加载：把已定义但尚未实例化的函数（永久 + 临时）加载进执行注册表。
     *
     * <p>CREATE FUNCTION 只做冒烟校验 + 存定义；真正"加载成可执行函数"在查询前做（懒加载），
     * 幂等：同一函数只加载一次。标量函数经 {@code backend.loadScalar} 后按 SQL 名注册进
     * {@link ScalarFunctionRegistry}（SELECT 即自动走通）；聚合函数经 {@code backend.loadAggregate}
     * 由后端自注册进 {@link AggregateFunctionRegistry}（GROUP BY 按名取用）。</p>
     *
     * <p>分类判定：加载按标量→聚合→表函数依次尝试，契约不匹配（{@link IllegalArgumentException}）跳过、
     * 真实加载失败（{@link IllegalStateException}）原样抛出。四类契约都不匹配的
     * Window 函数留后续接线，明确报错。</p>
     */
    public static synchronized void ensureLoaded() {
        ensureLoadedFrom(DEFINITIONS);
        ensureLoadedFrom(TEMPORARY);
    }

    private static void ensureLoadedFrom(Map<String, FunctionDefinition> defs) {
        for (FunctionDefinition def : defs.values()) {
            ensureLoadedFor(def);
        }
    }

    /**
     * 注册一个来自 Master 的函数定义（真分布式 L2：片段派发附带）——放入永久定义表，
     * 供 {@link #ensureLoadedFor} 加载成可执行函数。幂等：同名已存在则不覆盖。
     */
    public static void registerExternal(FunctionDefinition def) {
        DEFINITIONS.putIfAbsent(def.name(), def);
    }

    /**
     * 把单个函数定义加载进执行注册表（幂等，同一函数只加载一次）。
     *
     * <p>真分布式下 Worker 是独立 JVM，静态注册表不与 Master 共享，Master 把片段聚合引用的
     * 函数定义经派发载荷带过来后，Worker 用它重建可执行函数（标量→ScalarFunctionRegistry、
     * 聚合→AggregateFunctionRegistry/CollectAggregateRegistry、表函数→TableFunctionRegistry）。
     * 语义与 {@code ensureLoadedFrom} 的逐函数加载一致。</p>
     */
    public static synchronized void ensureLoadedFor(FunctionDefinition def) {
        String name = def.name();
        if (LOADED.contains(name)) return;
        FunctionBackend backend = BackendRegistry.resolve(def.type())
                .orElseThrow(() -> new IllegalStateException("函数后端未注册: " + def.type()));
        boolean loaded = false;
        try {
            ScalarFunction fn = backend.loadScalar(def);
            ScalarFunctionRegistry.registerGlobal(name, fn);
            loaded = true;
        } catch (IllegalArgumentException e) {
            // 非标量契约 → 继续试聚合
        }
        try {
            backend.loadAggregate(def);
            loaded = true;
        } catch (IllegalArgumentException e) {
            // 非聚合契约 → 忽略（validate 已保证至少满足一类契约）
        }
        try {
            TableFunction tf = backend.loadTable(def);
            TableFunctionRegistry.registerGlobal(name, tf);
            loaded = true;
        } catch (IllegalArgumentException e) {
            // 非表函数契约 → 忽略（validate 已保证至少满足一类契约）
        }
        if (!loaded) {
            throw new IllegalStateException(
                    "函数 [" + name + "] 无法按标量/聚合/表函数契约加载"
                            + "（窗口函数查询期接线暂未支持）");
        }
        LOADED.add(name);
    }

    /** 按名查定义（永久优先，临时兜底）；未注册返回 null。 */
    public static FunctionDefinition get(String name) {
        FunctionDefinition def = DEFINITIONS.get(name);
        return def != null ? def : TEMPORARY.get(name);
    }

    /** 已注册的全部函数定义（不可变视图，永久 + 临时）。 */
    public static Map<String, FunctionDefinition> definitions() {
        Map<String, FunctionDefinition> all = new ConcurrentHashMap<>(DEFINITIONS);
        all.putAll(TEMPORARY);
        return Map.copyOf(all);
    }

    /**
     * 对指定视角可见的函数定义（永久 + 临时，按 is_private 过滤）。
     *
     * @param viewer 登录用户名；{@code null} = 看全部（admin / 无登录全局视角）。
     */
    public static Map<String, FunctionDefinition> definitions(String viewer) {
        if (viewer == null) {
            return definitions();
        }
        Map<String, FunctionDefinition> all = new ConcurrentHashMap<>(DEFINITIONS);
        all.putAll(TEMPORARY);
        Map<String, FunctionDefinition> result = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, FunctionDefinition> e : all.entrySet()) {
            FunctionDefinition def = e.getValue();
            if (MetaStore.visibleTo(viewer, def.owner(), def.properties())) {
                result.put(e.getKey(), def);
            }
        }
        return Map.copyOf(result);
    }

    /** 复制定义并替换 owner（owner 未变则原样返回）。 */
    private static FunctionDefinition withOwner(FunctionDefinition def, String owner) {
        if (java.util.Objects.equals(def.owner(), owner)) {
            return def;
        }
        return new FunctionDefinition(def.name(), def.type(), def.jarPath(), def.className(),
                def.properties(), def.comment(), def.createdAt(), def.updatedAt(), owner);
    }

    /** 落库：后端未实现函数持久化时告警降级（仅内存），其余异常原样抛出。 */
    private static void persist(FunctionDefinition def) {
        if (storage == null) {
            return; // 未注入存储：仅内存注册（测试 / 单机无库场景）
        }
        try {
            storage.registerFunction(def);
        } catch (UnsupportedOperationException e) {
            log.warn("存储后端未实现函数持久化，CREATE {} 仅作用于本进程: {}", def.name(), e.getMessage());
        }
    }
}
