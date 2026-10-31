package com.example.dojao.csvdir;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Split;
import com.dojao.spi.bean.Table;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.ScanReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * <b>示例连接器</b>：把一个本地目录当成数据库读 —— 文件系统布局即元数据。
 *
 * <pre>
 *   &lt;dir&gt;/&lt;schema&gt;/&lt;table&gt;/part-000.csv
 *                              /part-001.csv
 * </pre>
 *
 * <ul>
 *   <li>schema = {@code <dir>} 下的一级子目录；</li>
 *   <li>table = schema 目录下的子目录（含 {@code *.csv} 即为有效表）；</li>
 *   <li>列 = 各 part 文件的首行表头，<b>类型一律 STRING</b>（示例从简）；</li>
 *   <li>一个 part 文件（或一组文件）= 一个切片，所以并行扫描是"按文件"天然切开的。</li>
 * </ul>
 *
 * <p><b>本类演示的四个要点</b>（写自定义连接器必看，详见 {@code docs/custom-connector.md}）：</p>
 * <ol>
 *   <li><b>元数据从哪来</b>：{@link #listSchemas} / {@link #listTables} / {@link #getTableColumns}
 *       直接读文件系统 —— 引擎据此做语义校验、列裁剪与 CTAS 类型推导。</li>
 *   <li><b>切片用载荷表达</b>：{@link #planSplits} 返回「一组文件 = 一个切片」，
 *       文件清单编码进 {@link Split#payload()}（{@link CsvSplitPayload}），
 *       而不是硬塞进 {@code ReadRequest.filterExpression()} 之类的公共字段。</li>
 *   <li><b>游标状态在 {@link ScanReader} 里</b>：连接器实例无状态，故<b>无需</b>覆写
 *       {@link #copy()} —— 多线程各自 {@link #openSplit} 各拿一个 reader，互不干扰。</li>
 *   <li><b>必须兑现 {@code filterExpression}</b>：引擎把可下推谓词交给连接器后<b>会消掉本地 Filter</b>，
 *       不理它就会静默多返回行。这里用 {@link SimpleFilter} 在本地求值。</li>
 * </ol>
 *
 * <p><b>刻意不做的（真实连接器按需补齐）</b>：写 / 建表 / 谓词翻译成 SQL / 列级统计 / 类型推断
 * （全部列 STRING）/ 引号与转义（字段内的分隔符、换行压不住）。</p>
 *
 * @author 张三
 * @version 1.0
 * @date 2026/9/20
 */
public final class CsvDirConnector implements DojaoConnector {

    /** 每页行数（与 Hive/MySQL 内置连接器同量级，便于对比内存占用）。 */
    private static final int BATCH_ROWS = 1024;

    private final String catalogName;
    private final Path root;
    private final char delimiter;
    private final BufferAllocator allocator;

    CsvDirConnector(String catalogName, Path root, char delimiter) {
        this.catalogName = catalogName;
        this.root = root;
        this.delimiter = delimiter;
        // 堆外分配器统一挂到节点共享根（dojao-common 的 NodeMemory）—— 与引擎其它消费方共用一本账、
        // 受节点堆外硬上限约束。插件只需依赖 dojao-spi（传递 dojao-common）即可 import。
        this.allocator = NodeMemory.shared().newChildAllocator("connector/csvdir", 0);
    }

    // ==================== 元数据（Catalog / Schema / Table / Column） ====================

    @Override
    public boolean catalogExists(String catalog) {
        return catalogName.equals(catalog);
    }

    @Override
    public Optional<Catalog> getCatalog(String catalog) {
        if (!catalogExists(catalog)) return Optional.empty();
        return Optional.of(new Catalog(catalogName, "csvdir",
                Map.of("dir", root.toString()), null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    @Override
    public boolean schemaExists(String catalog, String schema) {
        return catalogExists(catalog) && Files.isDirectory(root.resolve(schema));
    }

    @Override
    public Optional<com.dojao.spi.bean.Schema> getSchema(String catalog, String schema) {
        if (!schemaExists(catalog, schema)) return Optional.empty();
        return Optional.of(new com.dojao.spi.bean.Schema(catalogName, schema, Map.of()));
    }

    @Override
    public List<String> listSchemas(String catalog) {
        if (!catalogExists(catalog)) return List.of();
        return listDirectories(root);
    }

    @Override
    public boolean tableExists(String catalog, String schema, String table) {
        return schemaExists(catalog, schema) && !partFiles(schema, table).isEmpty();
    }

    @Override
    public Optional<Table> getTable(String catalog, String schema, String table) {
        List<Column> columns = getTableColumns(catalog, schema, table);
        if (columns.isEmpty()) return Optional.empty();
        return Optional.of(new Table(catalogName, schema, table, columns, Map.of(), null));
    }

    @Override
    public List<Column> getTableColumns(String catalog, String schema, String table) {
        List<String> files = partFiles(schema, table);
        if (files.isEmpty()) return List.of();
        try {
            List<String> header = readHeader(files.get(0));
            List<Column> columns = new ArrayList<>(header.size());
            for (String name : header) {
                columns.add(new Column(name, "string", null, null, true));
            }
            return columns;
        } catch (IOException e) {
            // 元数据读不出来就是要报错：静默返回空表会被上层当成"表不存在"，很难查
            throw new UncheckedIOException("读取 CSV 表头失败: " + files.get(0), e);
        }
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        if (!schemaExists(catalog, schema)) return List.of();
        List<String> tables = new ArrayList<>();
        for (String dir : listDirectories(root.resolve(schema))) {
            if (!partFiles(schema, dir).isEmpty()) {
                tables.add(dir);
            }
        }
        return tables;
    }

    @Override
    public boolean columnExists(String catalog, String schema, String table, String column) {
        return getColumn(catalog, schema, table, column).isPresent();
    }

    @Override
    public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
        for (Column c : getTableColumns(catalog, schema, table)) {
            if (c.name().equals(column)) return Optional.of(c);
        }
        return Optional.empty();
    }

    // ==================== 统计探查（并行度估算用） ====================

    @Override
    public TableStats getTableStats(String catalog, String schema, String table) {
        List<String> files = partFiles(schema, table);
        if (files.isEmpty()) {
            return new TableStats(catalogName, schema, table, -1L, -1L, List.of());
        }
        long rows = 0;
        long bytes = 0;
        for (String file : files) {
            Path path = Path.of(file);
            try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
                long all = lines.count();
                rows += Math.max(0, all - 1);   // 首行是表头
            } catch (IOException e) {
                // 单文件读不了不拖垮整体探查：按未知处理（-1），由框架回退默认并行度
                return new TableStats(catalogName, schema, table, -1L, -1L, List.of());
            }
            try {
                bytes += Files.size(path);
            } catch (IOException ignored) {
                // 体积未知不影响行数结论，按 0 计入（框架只用它估算并行度）
            }
        }
        // 注意：这里是真数了一遍行数，大文件会很慢。真实连接器应当用廉价元数据
        // （Hive 读 ORC footer、MySQL 查 information_schema.TABLES），不要扫数据。
        return new TableStats(catalogName, schema, table, rows, bytes, List.of());
    }

    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        return List.of();   // 未做列级统计 = 未知，框架自行回退
    }

    // ==================== 切片规划（并行扫描） ====================

    /**
     * 把表的 part 文件切成至多 {@code parallelism} 组，每组一个切片。
     *
     * <p>契约要求「互不重叠且合起来恰好覆盖全表」——这里用连续分组实现：第 g 组取
     * {@code [g*n/G, (g+1)*n/G)} 的文件，区间首尾相接、无交叠、并集是全部文件。</p>
     */
    @Override
    public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
        // 分页请求语义上与切片互斥（切了就没法保证 offset/limit 的全局顺序）
        if (parallelism <= 1 || request.limit() > 0 || request.offset() > 0) {
            return List.of();
        }
        List<String> files = partFiles(request.schema(), request.table());
        if (files.size() <= 1) {
            return List.of();   // 单文件切不出并行度 → 返回空，上层退化为单切片整表扫描
        }
        int groups = Math.min(parallelism, files.size());
        List<ScanSlice> slices = new ArrayList<>(groups);
        for (int g = 0; g < groups; g++) {
            int from = (int) ((long) g * files.size() / groups);
            int to = (int) ((long) (g + 1) * files.size() / groups);
            if (from >= to) continue;
            List<String> bucket = files.subList(from, to);
            slices.add(new ScanSlice(request, Split.of(CsvSplitPayload.encode(bucket))));
        }
        return slices;
    }

    /**
     * true —— {@link #openSplit} 会读 {@code request.filterExpression()} 并用 {@link SimpleFilter}
     * 逐行过滤，所以引擎下推之后可以放心不再重复过滤。
     *
     * <p>反例：连接器若忽略 {@code filterExpression}（只按切片信息读文件），必须返回 false ——
     * 否则规划器下推后消掉本地 Filter，查询会<b>静默多返回行</b>。</p>
     */
    @Override
    public boolean supportsPredicatePushdown() {
        return true;
    }

    @Override
    public ScanReader openSplit(ReadRequest request, Split split) {
        List<String> files = (split != null && split.hasPayload())
                ? CsvSplitPayload.decode(split.payload())
                : partFiles(request.schema(), request.table());
        SimpleFilter filter = request.filterExpression()
                .filter(f -> !f.isBlank())
                .map(SimpleFilter::parse)
                .orElseGet(SimpleFilter::none);
        return new CsvScanReader(files, request, filter);
    }

    // ==================== 写：本示例不支持 ====================

    @Override
    public void write(WriteRequest request) {
        throw new UnsupportedOperationException(
                "示例连接器（csvdir）是只读的，不支持写入: " + request.schema() + "." + request.table());
    }

    // ==================== 文件系统工具 ====================

    /** 列出 {@code <root>/<schema>/<table>/} 下的 {@code *.csv}，按文件名排序（保证切片顺序稳定）。 */
    private List<String> partFiles(String schema, String table) {
        Path dir = root.resolve(schema).resolve(table);
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".csv"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .map(p -> p.toAbsolutePath().toString())
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("列举 CSV 表目录失败: " + dir, e);
        }
    }

    /** 列出目录下的一级子目录（排序保证 SHOW 结果稳定）。 */
    private static List<String> listDirectories(Path dir) {
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("列举目录失败: " + dir, e);
        }
    }

    private List<String> readHeader(String file) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Path.of(file), StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            if (line == null) {
                throw new IOException("CSV 文件是空的（没有表头）: " + file);
            }
            return splitLine(line);
        }
    }

    private List<String> splitLine(String line) {
        String[] parts = line.split(java.util.regex.Pattern.quote(String.valueOf(delimiter)), -1);
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            out.add(p.trim());
        }
        return out;
    }

    // ==================== 读取游标 ====================

    /**
     * 一个切片的读取游标：顺序读文件清单，逐页产出 Arrow 数据。
     *
     * <p>状态（读到第几个文件、当前文件读了多少行、已吐了多少行）全在本对象上 —— 这正是
     * {@link ScanReader} 存在的意义，连接器实例因此可以被多线程共享。</p>
     */
    private final class CsvScanReader implements ScanReader {

        private final List<String> files;
        private final List<String> projection;
        private final long limit;
        private final SimpleFilter filter;

        private int fileIndex;
        private BufferedReader current;
        private List<String> currentHeader;
        private long emitted;
        private boolean done;

        CsvScanReader(List<String> files, ReadRequest request, SimpleFilter filter) {
            this.files = files;
            List<String> cols = request.columns().orElse(List.of());
            // 列清单为空 = 读所有列；含哨兵 "*" 同样按「所有列」处理 —— 若把它当列名去表头里找，
            // validateProjection 会直接抛"投影列 '*' 不存在"（详见 docs/custom-connector.md §4.3）
            this.projection = cols.contains("*") ? List.of() : cols;
            this.limit = request.limit();
            this.filter = filter;
        }

        @Override
        public Page next() {
            while (true) {
                if (done) return Page.eof();
                if (limit > 0 && emitted >= limit) {
                    done = true;
                    return Page.eof();
                }
                if (current == null) {
                    if (fileIndex >= files.size()) {
                        done = true;
                        return Page.eof();
                    }
                    String file = files.get(fileIndex);
                    try {
                        current = Files.newBufferedReader(Path.of(file), StandardCharsets.UTF_8);
                        String headerLine = current.readLine();
                        if (headerLine == null) {
                            throw new IOException("CSV 文件是空的（没有表头）: " + file);
                        }
                        currentHeader = splitLine(headerLine);
                        validateProjection(file);
                    } catch (IOException e) {
                        closeCurrent();
                        throw new UncheckedIOException("打开 CSV 文件失败: " + file, e);
                    }
                }
                Page page = readBatch();
                if (page == null) {
                    closeCurrent();
                    fileIndex++;
                    continue;
                }
                return page;
            }
        }

        @Override
        public void close() {
            closeCurrent();
            done = true;
        }

        private void closeCurrent() {
            if (current != null) {
                try {
                    current.close();
                } catch (IOException ignored) {
                    // 关闭失败没有补救动作，不掩盖查询的主错误
                }
                current = null;
                currentHeader = null;
            }
        }

        /** 投影里出现表头没有的列 → 直接抛错（静默少列比报错难查得多）。 */
        private void validateProjection(String file) {
            for (String col : projection) {
                if (!currentHeader.contains(col)) {
                    throw new IllegalStateException(
                            "投影列 '" + col + "' 不存在于 " + file + " 的表头 " + currentHeader);
                }
            }
        }

        /** 读满一页（或读到底）；本文件读完且一行都没产出时返回 null，由调用方翻到下一个文件。 */
        private Page readBatch() {
            List<String> outCols = projection.isEmpty() ? currentHeader : projection;
            List<Integer> sourceIndexes = new ArrayList<>(outCols.size());
            for (String col : outCols) {
                sourceIndexes.add(currentHeader.indexOf(col));
            }

            List<Field> fields = new ArrayList<>(outCols.size());
            for (String col : outCols) {
                fields.add(Field.nullable(col, new ArrowType.Utf8()));
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
            VarCharVector[] vectors = new VarCharVector[outCols.size()];
            for (int i = 0; i < outCols.size(); i++) {
                vectors[i] = (VarCharVector) root.getVector(i);
                vectors[i].allocateNew(BATCH_ROWS);
            }

            int rows = 0;
            Map<String, String> row = new HashMap<>();
            try {
                while (rows < BATCH_ROWS) {
                    if (limit > 0 && emitted + rows >= limit) break;
                    String line = current.readLine();
                    if (line == null) break;
                    if (line.isBlank()) continue;

                    List<String> values = splitLine(line);
                    if (values.size() != currentHeader.size()) {
                        throw new IllegalStateException("CSV 行列数与表头不一致: 期望 "
                                + currentHeader.size() + " 列，实际 " + values.size()
                                + " 列（文件 " + files.get(fileIndex) + "）");
                    }

                    if (!filter.isEmpty()) {
                        row.clear();
                        for (int i = 0; i < currentHeader.size(); i++) {
                            row.put(currentHeader.get(i), values.get(i));
                        }
                        if (!filter.test(row)) continue;
                    }

                    for (int i = 0; i < vectors.length; i++) {
                        vectors[i].setSafe(rows, values.get(sourceIndexes.get(i))
                                .getBytes(StandardCharsets.UTF_8));
                    }
                    rows++;
                }
            } catch (IOException e) {
                root.close();
                throw new UncheckedIOException("读取 CSV 文件失败: " + files.get(fileIndex), e);
            }

            if (rows == 0) {
                root.close();
                return null;
            }
            root.setRowCount(rows);   // 关键：不设行数，Page.getRowCount() 为 0，页面会被当空页丢掉
            emitted += rows;
            return Page.fromRoot(root);
        }
    }
}
