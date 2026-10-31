package com.dojao.connector.excel;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.Split;
import com.dojao.spi.bean.Table;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.ScanReader;
import com.dojao.spi.util.DojaoTypeMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Excel（.xlsx / .xlsm）连接器 —— 把工作簿当数据源参与联邦查询。
 *
 * <p><b>层级映射</b>：</p>
 * <ul>
 *   <li>catalog → 目录（目录下每个工作簿是一个 schema）或单个工作簿文件；</li>
 *   <li>schema → 工作簿（文件名去掉扩展名）；</li>
 *   <li>table → 工作表（Excel 里的 sheet 名）。</li>
 * </ul>
 *
 * <p><b>★读取恒为单分区。</b>{@link #planSplits} 固定返回<b>一个</b>整表切片，
 * {@link #getTableStats} 也把 {@code splitCount} 报成 1。原因不是「还没做并行」，而是
 * <b>切了只有坏处</b>：xlsx 是一份 ZIP、一张表是一条 XML 流，每多切一片，那个 Worker 就要把
 * 同一个 ZIP 再解压一遍、从压缩流开头顺序走到自己的起点 —— 总 I/O 是「片数倍」而不是 1 倍，
 * 并行省下的时间还不够多解压几次。需要并行的是下游的聚合 / join / 排序，那些本来就按
 * 数据行重新分布，与 Excel 侧切几片无关。</p>
 *
 * <p><b>类型推断</b>：读表头附近 {@value #DEFAULT_SAMPLE_ROWS} 行（{@code catalog.excel.sampleRows}
 * 可调）推断列类型 —— 全整数 → BIGINT、含小数 → DOUBLE、布尔 → BOOLEAN、日期 → DATETIME、
 * 其余（含混型）→ STRING。数字单元格若带日期格式（{@code styles.xml} 里的 numberFormat），
 * 按 Excel 日期序列值换算成时间，而不是把 {@code 2026-10-21} 读成 {@code 46305}。</p>
 *
 * <p><b>只读</b>：Excel 工作簿不支持写入 / 建表（{@link #write} 明确抛错），
 * 谓词下推声明为不支持（xlsx 无法执行 SQL 谓词），由引擎在扫描之上保留自己的
 * {@code FilterOperator}，结果仍然正确。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
public class ExcelConnector implements DojaoConnector {

    /** 连接器类型名（{@code CREATE CATALOG ... USING excel}）。 */
    public static final String TYPE = "excel";

    /** 默认类型推断采样行数。 */
    public static final int DEFAULT_SAMPLE_ROWS = 200;

    /** 单页行数：一页一个 Arrow 批次，读侧内存与行数无关。 */
    private static final int PAGE_ROWS = 8192;

    private final String catalogName;
    private final Path root;
    private final boolean header;
    private final int sampleRows;
    private final BufferAllocator allocator;

    /** 工作簿 → 工作表清单缓存（按「文件 + 修改时间 + 大小」失效）。 */
    private final Map<String, CachedSheets> sheetsCache = new ConcurrentHashMap<>();
    /** 工作簿#工作表 → 列元数据缓存（同上失效规则）。 */
    private final Map<String, CachedSheet> sheetCache = new ConcurrentHashMap<>();

    public ExcelConnector(String catalogName, Path root, boolean header) {
        this(catalogName, root, header, DEFAULT_SAMPLE_ROWS,
                NodeMemory.shared().newChildAllocator("connector/excel", 0));
    }

    public ExcelConnector(String catalogName, Path root, boolean header, int sampleRows) {
        this(catalogName, root, header, sampleRows,
                NodeMemory.shared().newChildAllocator("connector/excel", 0));
    }

    public ExcelConnector(String catalogName, Path root, boolean header, int sampleRows,
                          BufferAllocator allocator) {
        this.catalogName = catalogName;
        this.root = root;
        this.header = header;
        this.sampleRows = Math.max(1, sampleRows);
        this.allocator = allocator;
    }

    // ==================== Catalog ====================

    @Override
    public boolean catalogExists(String catalog) {
        return catalogName.equals(catalog);
    }

    @Override
    public Optional<Catalog> getCatalog(String catalog) {
        if (!catalogExists(catalog)) return Optional.empty();
        return Optional.of(new Catalog(catalogName, TYPE,
                Map.of("path", root.toString()), null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    /**
     * 建 catalog 前的真实连接：能打开工作簿、能读出工作表清单才算成功。
     *
     * <p>Excel 是文件数据源，「构造连接器」只存了个路径；路径写错 / 文件被占用 / 不是合法
     * OOXML，都要挡在 catalog 落库<b>之前</b>，而不是等第一次查询才炸。</p>
     */
    @Override
    public void probeConnectivity(String catalog) {
        try {
            if (Files.isRegularFile(root)) {
                try (XlsxWorkbook wb = XlsxWorkbook.open(root)) {
                    wb.sheets();
                }
            } else if (Files.isDirectory(root)) {
                List<Path> files = workbookFiles(root);
                if (files.isEmpty()) {
                    throw new IOException("目录下没有任何 .xlsx / .xlsm 工作簿");
                }
                try (XlsxWorkbook wb = XlsxWorkbook.open(files.get(0))) {
                    wb.sheets();
                }
            } else {
                throw new IOException("路径不存在或不是文件 / 目录");
            }
        } catch (IOException e) {
            throw new RuntimeException("无法读取 Excel 数据源 " + catalog + "（" + root + "）: "
                    + e.getMessage(), e);
        }
    }

    // ==================== Schema（= 工作簿） ====================

    @Override
    public boolean schemaExists(String catalog, String schema) {
        return fileForSchema(catalog, schema) != null;
    }

    @Override
    public Optional<Schema> getSchema(String catalog, String schema) {
        Path file = fileForSchema(catalog, schema);
        if (file == null) return Optional.empty();
        return Optional.of(new Schema(catalogName, schema,
                Map.of("workbook", file.toString())));
    }

    @Override
    public List<String> listSchemas(String catalog) {
        if (!catalogExists(catalog)) return List.of();
        try {
            if (Files.isRegularFile(root)) {
                String name = root.getFileName().toString();
                return XlsxWorkbook.isWorkbookFile(name)
                        ? List.of(XlsxWorkbook.baseName(name)) : List.of();
            }
            if (!Files.isDirectory(root)) return List.of();
            Set<String> names = new HashSet<>();
            for (Path file : workbookFiles(root)) {
                names.add(XlsxWorkbook.baseName(file.getFileName().toString()));
            }
            List<String> out = new ArrayList<>(names);
            out.sort(String.CASE_INSENSITIVE_ORDER);
            return out;
        } catch (IOException e) {
            throw new RuntimeException("列举 Excel 工作簿失败（" + root + "）: " + e.getMessage(), e);
        }
    }

    // ==================== Table（= 工作表） ====================

    @Override
    public boolean tableExists(String catalog, String schema, String table) {
        Path file = fileForSchema(catalog, schema);
        if (file == null) return false;
        try {
            return sheetRef(file, table) != null;
        } catch (IOException e) {
            throw new RuntimeException("读取 Excel 工作簿结构失败（" + file + "）: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public Optional<Table> getTable(String catalog, String schema, String table) {
        Optional<SheetMeta> meta = sheetMeta(catalog, schema, table);
        if (meta.isEmpty()) return Optional.empty();
        Path file = fileForSchema(catalog, schema);
        return Optional.of(new Table(catalogName, schema, table, toColumns(meta.get()),
                Map.of("file", file.toString(), "sheet", table),
                "Excel 工作表（只读）"));
    }

    @Override
    public List<Column> getTableColumns(String catalog, String schema, String table) {
        return sheetMeta(catalog, schema, table).map(this::toColumns).orElse(List.of());
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        Path file = fileForSchema(catalog, schema);
        if (file == null) return List.of();
        try {
            List<String> names = new ArrayList<>();
            for (XlsxWorkbook.SheetRef ref : sheetRefs(file)) names.add(ref.name());
            return names;
        } catch (IOException e) {
            throw new RuntimeException("读取 Excel 工作簿结构失败（" + file + "）: "
                    + e.getMessage(), e);
        }
    }

    // ==================== Column ====================

    @Override
    public boolean columnExists(String catalog, String schema, String table, String column) {
        for (MetaColumn c : columns(catalog, schema, table)) {
            if (c.name().equalsIgnoreCase(column)) return true;
        }
        return false;
    }

    @Override
    public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
        for (Column c : getTableColumns(catalog, schema, table)) {
            if (c.name().equalsIgnoreCase(column)) return Optional.of(c);
        }
        return Optional.empty();
    }

    @Override
    public Map<String, DojaoType> getColumnTypes(String schema, String table) {
        Map<String, DojaoType> out = new LinkedHashMap<>();
        for (MetaColumn c : columns(catalogName, schema, table)) {
            out.put(c.name(), c.type());
        }
        return out;
    }

    /**
     * CTAS 的源列类型：直接用本连接器推断出的 Arrow 类型（与运行期读出的类型逐列一致）。
     * 不做 JDBC 式的「元数据探针」，因为 Excel 没有声明类型 —— 采样推断就是这里最准的答案。
     */
    @Override
    public List<Field> sourceFields(String catalog, String schema, String table,
                                    List<String> columns) {
        List<MetaColumn> all = columns(catalog, schema, table);
        if (all.isEmpty()) return List.of();
        List<MetaColumn> picked;
        if (columns == null || columns.isEmpty()) {
            picked = all;
        } else {
            picked = new ArrayList<>(columns.size());
            for (String name : columns) {
                MetaColumn match = findColumn(all, name);
                if (match != null) picked.add(match);
            }
        }
        List<Field> fields = new ArrayList<>(picked.size());
        for (MetaColumn c : picked) fields.add(DojaoTypeMapper.toField(c.name(), c.type()));
        return fields;
    }

    // ==================== 统计 ====================

    /**
     * 表统计：{@code splitCount} 恒为 1（<b>单分区契约</b>），体积取工作簿文件长度。
     *
     * <p>{@code rowCount} 返回 -1（未知）：精确行数要把整张表流式走一遍，在规划期做这件事
     * 的代价可能比查询本身还大；而并行度已经由 {@code splitCount = 1} 决定，不需要行数。
     * -1 是本 SPI 约定的「未知」，调用方会按集群槽位回退，不会误当空表。</p>
     */
    @Override
    public TableStats getTableStats(String catalog, String schema, String table) {
        Path file = fileForSchema(catalog, schema);
        long size = -1;
        if (file != null) {
            try {
                size = Files.size(file);
            } catch (IOException ignored) {
                // 体积未知 → -1，不影响正确性
            }
        }
        return new TableStats(catalog, schema, table, -1L, size, List.of(), 1L);
    }

    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        return List.of();
    }

    // ==================== 读取 ====================

    /**
     * <b>恒为单分区</b>：无论引擎要多少并行度，都只返回一个整表切片。
     *
     * <p>返回 {@link ScanSlice#whole}（不是空列表）是刻意的：空列表的语义是「计划不出分区，
     * 请调用方自己退化」，而这里的分区计划是明确的 —— 就是「一片，整张表」。同时在
     * {@link #getTableStats} 里把 {@code splitCount} 报成 1，让规划期在派发前就知道并行度上限。</p>
     */
    @Override
    public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
        return List.of(ScanSlice.whole(request));
    }

    /** Excel 无法执行 SQL 谓词（引擎必须保留自己的 Filter）。 */
    @Override
    public boolean supportsPredicatePushdown() {
        return false;
    }

    @Override
    public ScanReader openSplit(ReadRequest request, Split split) {
        if (split != null && split.hasPayload()) {
            throw new UnsupportedOperationException(
                    "Excel 连接器只产出整表切片，不接受带载荷的 Split");
        }
        return new ExcelScanReader(request);
    }

    /** 只读数据源。 */
    @Override
    public void write(WriteRequest request) {
        throw new UnsupportedOperationException(
                "Excel 连接器是只读的，不支持写入: " + catalogName);
    }

    // ==================== 元数据解析 ====================

    /** 解析后的工作表结构：列名 / 物理列下标 / 推断类型 + ZIP 内 XML 路径。 */
    private record SheetMeta(List<MetaColumn> columns, String part) {
    }

    /** 一列：输出名、工作表物理列下标（0 起）、推断出的 Dojao 类型。 */
    record MetaColumn(String name, int index, DojaoType type) {
    }

    private record FileStamp(long modified, long size) {
    }

    private record CachedSheets(FileStamp stamp, List<XlsxWorkbook.SheetRef> sheets) {
    }

    private record CachedSheet(FileStamp stamp, SheetMeta meta) {
    }

    /** catalog + schema → 工作簿文件；不存在返回 null。 */
    private Path fileForSchema(String catalog, String schema) {
        if (schema == null || schema.isBlank() || !catalogExists(catalog)) return null;
        if (Files.isRegularFile(root)) {
            String name = root.getFileName().toString();
            return XlsxWorkbook.isWorkbookFile(name)
                    && XlsxWorkbook.baseName(name).equalsIgnoreCase(schema) ? root : null;
        }
        if (!Files.isDirectory(root)) return null;
        try {
            for (Path file : workbookFiles(root)) {
                if (XlsxWorkbook.baseName(file.getFileName().toString())
                        .equalsIgnoreCase(schema)) {
                    return file;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("列举 Excel 工作簿失败（" + root + "）: " + e.getMessage(), e);
        }
        return null;
    }

    private static List<Path> workbookFiles(Path dir) throws IOException {
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(Files::isRegularFile)
                    .filter(p -> XlsxWorkbook.isWorkbookFile(p.getFileName().toString()))
                    .sorted((a, b) -> a.getFileName().toString()
                            .compareToIgnoreCase(b.getFileName().toString()))
                    .toList();
        }
    }

    private XlsxWorkbook.SheetRef sheetRef(Path file, String table) throws IOException {
        if (table == null) return null;
        for (XlsxWorkbook.SheetRef ref : sheetRefs(file)) {
            if (ref.name().equalsIgnoreCase(table)) return ref;
        }
        return null;
    }

    private List<XlsxWorkbook.SheetRef> sheetRefs(Path file) throws IOException {
        FileStamp stamp = stamp(file);
        CachedSheets cached = sheetsCache.get(file.toString());
        if (cached != null && cached.stamp().equals(stamp)) return cached.sheets();
        List<XlsxWorkbook.SheetRef> refs;
        try (XlsxWorkbook wb = XlsxWorkbook.open(file)) {
            refs = wb.sheets();
        }
        sheetsCache.put(file.toString(), new CachedSheets(stamp, refs));
        return refs;
    }

    private Optional<SheetMeta> sheetMeta(String catalog, String schema, String table) {
        Path file = fileForSchema(catalog, schema);
        if (file == null) return Optional.empty();
        try {
            XlsxWorkbook.SheetRef ref = sheetRef(file, table);
            if (ref == null) return Optional.empty();
            return Optional.of(loadSheetMeta(file, ref));
        } catch (IOException e) {
            throw new RuntimeException("读取 Excel 工作表失败（" + file + "#" + table + "）: "
                    + e.getMessage(), e);
        }
    }

    private List<MetaColumn> columns(String catalog, String schema, String table) {
        return sheetMeta(catalog, schema, table).map(SheetMeta::columns).orElse(List.of());
    }

    private SheetMeta loadSheetMeta(Path file, XlsxWorkbook.SheetRef ref) throws IOException {
        String key = file + "#" + ref.name();
        FileStamp stamp = stamp(file);
        CachedSheet cached = sheetCache.get(key);
        if (cached != null && cached.stamp().equals(stamp)) return cached.meta();
        SheetMeta meta = readSheetMeta(file, ref);
        sheetCache.put(key, new CachedSheet(stamp, meta));
        return meta;
    }

    /**
     * 流式扫描表头 + 采样行，推断列名与类型。只读前 {@code sampleRows} 行（表头不计），
     * 与整表大小无关。
     */
    private SheetMeta readSheetMeta(Path file, XlsxWorkbook.SheetRef ref) throws IOException {
        try (XlsxWorkbook wb = XlsxWorkbook.open(file);
             XlsxSheetStream stream = wb.openSheet(ref.part())) {
            List<String> headerNames = null;
            int headerWidth = -1;
            List<TypeAcc> acc = new ArrayList<>();
            int sampled = 0;
            boolean first = true;
            boolean sawHeader = false;
            while (stream.nextRow()) {
                XlsxSheetStream.Row row = stream.row();
                if (row.isEmpty()) continue;
                if (header && first) {
                    first = false;
                    sawHeader = true;
                    headerWidth = row.maxCol() + 1;
                    headerNames = headerNames(row, headerWidth);
                    continue;
                }
                first = false;
                int width = header ? headerWidth : row.maxCol() + 1;
                while (acc.size() < width) acc.add(new TypeAcc());
                for (int i = 0; i < width; i++) acc.get(i).add(row.cell(i));
                if (++sampled >= sampleRows) break;
            }

            if (header && !sawHeader) {
                // 空工作表：没有表头也就没有列
                return new SheetMeta(List.of(), ref.part());
            }
            int width = header ? headerWidth : acc.size();
            List<MetaColumn> cols = new ArrayList<>(width);
            for (int i = 0; i < width; i++) {
                String name = header ? headerNames.get(i) : "col" + (i + 1);
                DojaoType type = i < acc.size() ? acc.get(i).type() : DojaoType.STRING;
                cols.add(new MetaColumn(name, i, type));
            }
            return new SheetMeta(List.copyOf(cols), ref.part());
        }
    }

    /** 表头行的列名：空表头生成 {@code colN}，重名追加 {@code _2} / {@code _3}… */
    private static List<String> headerNames(XlsxSheetStream.Row row, int width) {
        List<String> names = new ArrayList<>(width);
        Set<String> used = new HashSet<>();
        for (int i = 0; i < width; i++) {
            XlsxSheetStream.Cell cell = row.cell(i);
            String raw = cell == null || cell.text() == null ? "" : cell.text().trim();
            String base = raw.isEmpty() ? "col" + (i + 1) : raw;
            String name = base;
            int suffix = 2;
            while (!used.add(name.toLowerCase(Locale.ROOT))) {
                name = base + "_" + suffix++;
            }
            names.add(name);
        }
        return names;
    }

    private List<Column> toColumns(SheetMeta meta) {
        List<Column> out = new ArrayList<>(meta.columns().size());
        for (MetaColumn c : meta.columns()) {
            out.add(new Column(c.name(), nativeType(c.type()), null, null, true));
        }
        return out;
    }

    private static String nativeType(DojaoType type) {
        return switch (type) {
            case BOOLEAN -> "BOOLEAN";
            case BIGINT -> "BIGINT";
            case DOUBLE -> "DOUBLE";
            case DATETIME -> "DATETIME";
            default -> "STRING";
        };
    }

    private static MetaColumn findColumn(List<MetaColumn> columns, String name) {
        if (name == null) return null;
        for (MetaColumn c : columns) {
            if (c.name().equalsIgnoreCase(name)) return c;
        }
        return null;
    }

    private static FileStamp stamp(Path file) throws IOException {
        FileTime time = Files.getLastModifiedTime(file);
        return new FileStamp(time.toMillis(), Files.size(file));
    }

    /** 采样期的列类型累积器：全整数 → BIGINT，整数+小数 → DOUBLE，混型 → STRING。 */
    private static final class TypeAcc {
        private DojaoType type;

        void add(XlsxSheetStream.Cell cell) {
            if (cell == null || cell.kind() == XlsxSheetStream.CellKind.ERROR) return;
            String text = cell.text();
            if (text == null || text.isEmpty()) return;
            switch (cell.kind()) {
                case NUMBER -> merge(isIntegral(text) ? DojaoType.BIGINT : DojaoType.DOUBLE);
                case BOOLEAN -> merge(DojaoType.BOOLEAN);
                case DATE -> merge(DojaoType.DATETIME);
                case STRING -> merge(DojaoType.STRING);
                case ERROR -> { /* 已在上方过滤 */ }
            }
        }

        private void merge(DojaoType other) {
            if (type == null || type == other) {
                type = other;
                return;
            }
            if ((type == DojaoType.BIGINT && other == DojaoType.DOUBLE)
                    || (type == DojaoType.DOUBLE && other == DojaoType.BIGINT)) {
                type = DojaoType.DOUBLE;
                return;
            }
            // 类型混合（数字 + 文本 / 数字 + 日期…）→ 文本，保证不丢值
            type = DojaoType.STRING;
        }

        DojaoType type() {
            return type == null ? DojaoType.STRING : type;
        }

        private static boolean isIntegral(String text) {
            String t = text.trim();
            if (t.isEmpty()) return false;
            int i = (t.charAt(0) == '+' || t.charAt(0) == '-') ? 1 : 0;
            if (i >= t.length()) return false;
            for (; i < t.length(); i++) {
                char ch = t.charAt(i);
                if (ch < '0' || ch > '9') return false;
            }
            return true;
        }
    }

    // ==================== 流式读取器 ====================

    /**
     * 单分区流式读取：一个工作表 = 一条 XML 流，内存里始终只有一页。
     *
     * <p>表头行、全空行都不算数据行；{@code offset} 统计的是<b>数据行</b>（与 SQL 分页语义一致），
     * {@code limit} 读满即停。</p>
     */
    private final class ExcelScanReader implements ScanReader {

        private XlsxWorkbook workbook;
        private XlsxSheetStream stream;
        private final List<MetaColumn> projection;
        private final String sheetName;
        private final boolean date1904;
        private final long toSkip;
        private final long limit;
        private long skipped;
        private long emitted;
        private long dataRowNo;
        private boolean headerPending;
        private boolean done;
        private boolean released;
        private long batchIndex;
        private XlsxSheetStream.Row currentRow;

        ExcelScanReader(ReadRequest request) {
            this.sheetName = request.table();
            Path file = fileForSchema(request.catalog(), request.schema());
            if (file == null) {
                throw new IllegalArgumentException("Excel catalog 中不存在工作簿: "
                        + request.catalog() + "." + request.schema());
            }
            SheetMeta meta = sheetMeta(request.catalog(), request.schema(), request.table())
                    .orElseThrow(() -> new IllegalArgumentException("Excel 工作簿 " + schemaName(file)
                            + " 中不存在工作表: " + request.table()));
            this.projection = resolveProjection(meta, request.columns().orElse(null));
            this.toSkip = Math.max(0, request.offset());
            this.limit = request.limit() > 0 ? request.limit() : 0;
            this.headerPending = header;
            if (meta.columns().isEmpty() || projection.isEmpty()) {
                this.workbook = null;
                this.stream = null;
                this.date1904 = false;
                this.done = true;
                return;
            }
            try {
                this.workbook = XlsxWorkbook.open(file);
                this.stream = workbook.openSheet(meta.part());
                this.date1904 = workbook.date1904();
            } catch (IOException e) {
                close();
                throw new RuntimeException("打开 Excel 工作表失败（" + file + "#" + sheetName + "）: "
                        + e.getMessage(), e);
            }
        }

        private List<MetaColumn> resolveProjection(SheetMeta meta, List<String> requested) {
            if (requested == null || requested.isEmpty()) return meta.columns();
            List<MetaColumn> out = new ArrayList<>(requested.size());
            for (String name : requested) {
                if ("*".equals(name)) {
                    out.addAll(meta.columns());
                    continue;
                }
                MetaColumn match = findColumn(meta.columns(), name);
                if (match == null) {
                    List<String> available = new ArrayList<>();
                    for (MetaColumn c : meta.columns()) available.add(c.name());
                    throw new IllegalArgumentException("Excel 工作表 '" + sheetName
                            + "' 不存在列 '" + name + "'；可用列: " + available);
                }
                out.add(match);
            }
            return out;
        }

        @Override
        public Page next() {
            if (done) return Page.eof();
            if (limit > 0 && emitted >= limit) {
                finish();
                return Page.eof();
            }
            List<Field> fields = new ArrayList<>(projection.size());
            List<FieldVector> vectors = new ArrayList<>(projection.size());
            try {
                for (MetaColumn c : projection) {
                    Field field = DojaoTypeMapper.toField(c.name(), c.type());
                    FieldVector vector = field.createVector(allocator);
                    vector.setInitialCapacity(PAGE_ROWS);
                    vector.allocateNew();
                    fields.add(field);
                    vectors.add(vector);
                }
                int rows = fillPage(vectors);
                if (rows == 0) {
                    closeVectors(vectors);
                    finish();
                    return Page.eof();
                }
                for (FieldVector vector : vectors) vector.setValueCount(rows);
                VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, rows);
                return Page.fromRoot(root, batchIndex++);
            } catch (RuntimeException e) {
                closeVectors(vectors);
                finish();
                throw e;
            }
        }

        private int fillPage(List<FieldVector> vectors) {
            int rows = 0;
            while (rows < PAGE_ROWS && (limit <= 0 || emitted < limit)) {
                if (!readNextDataRow()) break;
                fillRow(vectors, rows);
                rows++;
                emitted++;
            }
            return rows;
        }

        /** 前进到下一数据行（跳过表头、全空行与 offset 覆盖的行）。 */
        private boolean readNextDataRow() {
            while (stream.nextRow()) {
                XlsxSheetStream.Row row = stream.row();
                if (row.isEmpty()) continue;
                if (headerPending) {
                    headerPending = false;
                    continue;
                }
                if (skipped < toSkip) {
                    skipped++;
                    continue;
                }
                currentRow = row;
                dataRowNo++;
                return true;
            }
            currentRow = null;
            return false;
        }

        private void fillRow(List<FieldVector> vectors, int row) {
            for (int i = 0; i < projection.size(); i++) {
                MetaColumn column = projection.get(i);
                FieldVector vector = vectors.get(i);
                XlsxSheetStream.Cell cell = currentRow.cell(column.index());
                String text = cell == null ? null : cell.text();
                if (cell == null || cell.kind() == XlsxSheetStream.CellKind.ERROR
                        || text == null || text.isEmpty()) {
                    vector.setNull(row);
                    continue;
                }
                try {
                    switch (column.type()) {
                        case BIGINT -> ((BigIntVector) vector).setSafe(row, parseLong(text));
                        case DOUBLE -> ((Float8Vector) vector).setSafe(row,
                                Double.parseDouble(text.trim()));
                        case BOOLEAN -> ((BitVector) vector).setSafe(row,
                                parseBoolean(text) ? 1 : 0);
                        case DATETIME -> ((TimeStampMilliVector) vector).setSafe(row,
                                toEpochMillis(text));
                        default -> ((VarCharVector) vector).setSafe(row,
                                text.getBytes(StandardCharsets.UTF_8));
                    }
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException("解析 Excel 工作表 '" + sheetName
                            + "' 第 " + dataRowNo + " 个数据行、列 '" + column.name()
                            + "' 的值 '" + text + "' 失败（目标类型 " + column.type()
                            + "）: " + e.getMessage(), e);
                }
            }
        }

        private long parseLong(String text) {
            String t = text.trim();
            try {
                return Long.parseLong(t);
            } catch (NumberFormatException e) {
                try {
                    return new BigDecimal(t).longValueExact();
                } catch (ArithmeticException | NumberFormatException e2) {
                    throw new NumberFormatException("不是整数: " + t);
                }
            }
        }

        private boolean parseBoolean(String text) {
            String t = text.trim().toLowerCase(Locale.ROOT);
            if ("1".equals(t) || "true".equals(t) || "yes".equals(t)) return true;
            if ("0".equals(t) || "false".equals(t) || "no".equals(t)) return false;
            throw new NumberFormatException("不是布尔值: " + text);
        }

        private long toEpochMillis(String text) {
            String t = text.trim();
            String iso = t.replace(' ', 'T');
            try {
                return LocalDateTime.parse(iso).toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (DateTimeParseException ignored) {
                // 继续尝试其它形态
            }
            try {
                return OffsetDateTime.parse(iso).toInstant().toEpochMilli();
            } catch (DateTimeParseException ignored) {
                // 继续尝试其它形态
            }
            try {
                return Instant.parse(iso).toEpochMilli();
            } catch (DateTimeParseException ignored) {
                // 继续尝试其它形态
            }
            try {
                return LocalDate.parse(iso).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (DateTimeParseException ignored) {
                // 最后兜底：可能是没带日期样式的纯数字序列值
            }
            String serial = XlsxSheetStream.excelSerialToIso(t, date1904);
            if (serial != null) {
                return LocalDateTime.parse(serial).toInstant(ZoneOffset.UTC).toEpochMilli();
            }
            throw new DateTimeParseException("不是可识别的时间格式", t, 0);
        }

        private void finish() {
            done = true;
            close();
        }

        @Override
        public void close() {
            if (released) return;
            released = true;
            done = true;
            if (stream != null) stream.close();
            if (workbook != null) workbook.close();
        }
    }

    private static void closeVectors(List<FieldVector> vectors) {
        for (FieldVector vector : vectors) {
            try {
                vector.close();
            } catch (RuntimeException ignored) {
                // 关已分配的 Arrow 列失败无补救动作，不能掩盖主错误
            }
        }
    }

    private String schemaName(Path file) {
        return XlsxWorkbook.baseName(file.getFileName().toString());
    }

    /** 测试 / 诊断用：当前连接器的根路径。 */
    public Path root() {
        return root;
    }
}
