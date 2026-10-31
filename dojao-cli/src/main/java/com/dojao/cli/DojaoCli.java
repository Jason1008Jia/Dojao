package com.dojao.cli;

import com.dojao.common.sql.SqlSplitter;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Dojao 命令行客户端：直连 Master（Arrow Flight {@code execute_query}）执行 SQL。
 *
 * <p>用法：</p>
 * <pre>
 *   dojao-cli                                     # 交互式终端（REPL）
 *   dojao-cli "select * from mysql.dga.users limit 5"
 *   dojao-cli -F csv -f query.sql &gt; out.csv       # 文件批量执行，CSV 落盘
 * </pre>
 *
 * <p><b>启动即握手探活</b>：连上 {@code --host:--port}（默认 {@code 127.0.0.1:6530}）后先发一次真实
 * 往返（{@code listActions}）—— <b>Master 没启动直接报错退出</b>（EXIT_ERROR），
 * 不会让用户敲完 SQL 才发现连不上。</p>
 *
 * <p>交互式特性：多行 SQL（以 {@code ;} 结束）、{@code ↑/↓} 翻历史
 * （最近 {@value #HISTORY_SIZE} 条，退出时落盘 {@code ~/.dojao_history}，下次启动可回顾）、
 * 元命令 {@code \h \q \f \timing}、结果格式 table/csv/json、逐行流式输出 + 行数/耗时统计。</p>
 *
 * <p><b>行编辑（JLine）只在真正的终端上启用</b>：stdin/stdout 是管道、重定向或 IDE 输出面板时
 * 没有 console，JLine 只能拿到"哑终端"（{@code TYPE_DUMB}）—— 实测它<b>不回显输入、回车也不换行</b>，
 * 提示符会跟结果黏在同一行，比朴素逐行读取更糟。这种环境自动退化为 {@link #replPlain()}
 * （回显交给宿主终端），并在启动时提示"无法手敲 SQL"。</p>
 *
 * <p>stdin 用尽（管道读完 / Ctrl-D）时会明确打印"输入已结束，退出"再退出，
 * <b>不再打印最后一个提示符后静默消失</b>。</p>
 *
 * <p>输出约定：<b>结果写 stdout，诊断/统计/错误/提示符写 stderr</b> —— 因此
 * {@code -F csv > out.csv}（或 {@code printf '...' | dojao-cli -F csv > out.csv}）
 * 得到的文件是干净的数据。</p>
 *
 * @author jiaxin
 * @date 2026/9/17
 */
public final class DojaoCli {

    private static final String VERSION = "0.1.0";
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 6530;
    private static final String PRIMARY_PROMPT = "dojao> ";
    private static final String CONTINUE_PROMPT = "   ...> ";
    private static final String HISTORY_FILE = ".dojao_history";
    /** 历史保留条数：↑ 最多回溯这么多条，落盘文件同样按这个上限裁剪。 */
    private static final int HISTORY_SIZE = 20;

    private static final int EXIT_OK = 0;
    private static final int EXIT_ERROR = 1;
    private static final int EXIT_USAGE = 2;

    private final Options opt = new Options();
    private final PrintStream out = System.out;
    private final PrintStream err = System.err;

    private FlightQueryClient client;
    private ResultFormatter.Format format = ResultFormatter.Format.TABLE;
    private boolean timing = true;

    public static void main(String[] args) {
        System.exit(new DojaoCli().run(args));
    }

    private int run(String[] args) {
        try {
            if (opt.parse(args)) {
                out.println(helpText());
                return EXIT_OK;
            }
        } catch (IllegalArgumentException e) {
            err.println("参数错误: " + e.getMessage());
            err.println();
            err.println(helpText());
            return EXIT_USAGE;
        }
        format = opt.format;

        // ===== 连接 Master 并握手探活：连不上直接报错退出，不让用户敲完 SQL 才发现 =====
        try (FlightQueryClient c = new FlightQueryClient(opt.host, opt.port)) {
            client = c;
            c.connect();
            if (!opt.quiet) {
                err.println("Dojao CLI " + VERSION + "  →  已连接 Master " + opt.host + ":" + opt.port);
            }
            if (opt.file != null) {
                return runBatch(opt.file);
            }
            if (!opt.sql().isEmpty()) {
                return runBatchSql(opt.sql());
            }
            return repl();
        } catch (FlightQueryClient.QueryException | IOException e) {
            err.println("错误: " + e.getMessage());
            return EXIT_ERROR;
        }
    }

    // ==================== 批处理（-f 文件 / 位置参数 SQL） ====================

    private int runBatch(String file) throws IOException {
        Path path = Path.of(file);
        if (!Files.isReadable(path)) {
            err.println("错误: 无法读取文件 " + file);
            return EXIT_ERROR;
        }
        return runBatchSql(Files.readString(path, StandardCharsets.UTF_8));
    }

    /** 批量执行：语句之间按顶层分号切分；任一条失败即停止（脚本 fail-fast）。 */
    private int runBatchSql(String text) {
        SqlSplitter.Parts parts = SqlSplitter.split(text);
        List<String> statements = new ArrayList<>(parts.statements);
        // 末尾无分号的最后一句照样执行（`dojao-cli "select 1"` 这种用法不必写分号）；
        // 纯注释 / 空白的残留（restHasCode=false）丢弃
        if (parts.restHasCode) {
            statements.add(parts.rest.trim());
        }
        if (statements.isEmpty()) {
            err.println("没有可执行的语句");
            return EXIT_ERROR;
        }
        for (String sql : statements) {
            if (!exec(sql)) {
                return EXIT_ERROR;
            }
        }
        return EXIT_OK;
    }

    // ==================== 交互式 REPL ====================

    private int repl() throws IOException {
        if (!opt.quiet) {
            err.println("输入 SQL 以分号结尾执行；\\h 查看帮助，\\q 退出。");
        }
        Terminal terminal = null;
        try {
            // dumb(true) 只是"允许退化成哑终端"的兜底提示，并不会阻止 JLine 在真有 console 时
            // 用 jansi/jna 拿到真实终端（行编辑、历史、输入回显都依赖真实终端）。
            terminal = TerminalBuilder.builder().dumb(true).build();
        } catch (Throwable t) {
            // JLine 初始化失败 → 退化为纯行读取
            err.println("（终端增强不可用，退化为简单模式：" + t.getClass().getSimpleName() + "）");
        }
        if (terminal == null || Terminal.TYPE_DUMB.equals(terminal.getType())) {
            // 哑终端 = 没有 console（stdin/stdout 是管道 / 重定向 / IDE 输出面板）。实测这种终端下
            // JLine 不回显输入、回车不换行 —— 提示符会和结果黏在一行，比朴素的逐行读取更糟；
            // 改用 replPlain()（回显由宿主终端负责）。
            closeQuietly(terminal);
            if (!opt.quiet && System.console() == null) {
                // 注意措辞：这类环境"输入回显"由宿主决定（mintty 会回显、纯管道不会），
                // 不能武断地说敲不了字 —— 只提示能力受限 + 怎么拿到完整体验
                err.println("提示: 未检测到交互终端（stdin/stdout 是管道、重定向或 IDE 输出面板）："
                        + "行编辑与历史不可用，输入回显由宿主决定；若敲字没有反应，"
                        + "请改用真实控制台运行（IDEA 可勾选 \"Emulate terminal in output console\"），"
                        + "批量执行可用 -f <file> 或把 SQL 直接当参数。");
            }
            return replPlain();
        }
        return replJline(terminal);
    }

    /** 关掉探测用的终端（哑终端上 close 不影响 System.in，已实测）。 */
    private static void closeQuietly(Terminal terminal) {
        if (terminal == null) {
            return;
        }
        try {
            terminal.close();
        } catch (Exception ignored) {
            // 关闭失败无需打扰用户
        }
    }

    private int replJline(Terminal terminal) throws IOException {
        DefaultHistory history = new DefaultHistory();
        // 每行原样返回（续行判断由 SqlSplitter 负责），避免 JLine 自己因未闭合引号而拼接多行；
        // escapeChars 清空 = 关闭反斜杠转义处理，SQL 里的 \ 原样交给引擎（元命令 \q 也靠它原样到达）
        DefaultParser parser = new DefaultParser()
                .eofOnUnclosedQuote(false)
                .eofOnUnclosedBracket()
                .escapeChars(new char[0]);
        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .appName("dojao")
                .parser(parser)
                .history(history)
                .build();
        Path historyPath = Path.of(System.getProperty("user.home", "."), HISTORY_FILE);
        reader.setVariable(LineReader.HISTORY_FILE, historyPath);
        // 只留最近 HISTORY_SIZE 条：HISTORY_SIZE 管内存里 ↑ 的回溯范围，HISTORY_FILE_SIZE 管落盘
        // 文件的行数（JLine 没有 setHistorySize API，只能用这两个变量设）
        reader.setVariable(LineReader.HISTORY_SIZE, HISTORY_SIZE);
        reader.setVariable(LineReader.HISTORY_FILE_SIZE, HISTORY_SIZE);
        // ★ 必须在 load() 之前手动 attach：LineReaderImpl 是在 readLine() 开头才 history.attach(this)，
        // 而我们在第一次 readLine 之前就 load() —— 不 attach 的话 DefaultHistory 内部 reader 为 null，
        // load() 静默读到 0 条（跨会话回忆失效），save() 也不会按 HISTORY_FILE_SIZE 裁剪文件。
        history.attach(reader);
        try {
            history.load();
        } catch (IOException e) {
            err.println("（历史文件读取失败，忽略：" + e.getMessage() + "）");
        }

        StringBuilder buf = new StringBuilder();
        int last = EXIT_OK;
        try {
            while (true) {
                String line;
                try {
                    line = reader.readLine(buf.length() == 0 ? PRIMARY_PROMPT : CONTINUE_PROMPT);
                } catch (UserInterruptException e) {
                    buf.setLength(0);            // Ctrl-C：丢弃当前输入，回到主提示符
                    continue;
                } catch (EndOfFileException e) {
                    // Ctrl-D（真实终端）或 stdin 用尽（管道）—— 明确说一声，避免"提示符出现后
                    // 再无反应"的困惑
                    if (!opt.quiet) {
                        err.println("（输入已结束，退出）");
                    }
                    break;
                }
                if (buf.length() == 0) {
                    MetaResult meta = handleMeta(line);
                    if (meta == MetaResult.QUIT) {
                        break;
                    }
                    if (meta == MetaResult.HANDLED) {
                        continue;
                    }
                }
                buf.append(line).append('\n');
                SqlSplitter.Parts parts = SqlSplitter.split(buf.toString());
                for (String sql : parts.statements) {
                    if (!exec(sql)) {
                        last = EXIT_ERROR;
                    }
                }
                // 残留：半句（含未闭合字符串）继续累积 → 下轮用续行提示符；纯注释/空白丢弃 → 回到主提示符
                buf.setLength(0);
                if (parts.restHasCode) {
                    buf.append(parts.rest);
                }
            }
        } finally {
            try {
                history.save();
            } catch (IOException ignored) {
                // 历史落盘失败不影响退出
            }
            terminal.close();
        }
        return last;
    }

    /**
     * 无行编辑的退路：直接按行读 stdin，回显交给宿主终端。
     *
     * <p>提示符写 <b>stderr</b>（与统计/错误同类，属诊断信息）—— 既保证
     * {@code printf '...' | dojao-cli -F csv > out.csv} 的 stdout 干净，也让"提示符跟结果黏一行"
     * 的单行显示问题不再出现在数据里。</p>
     */
    private int replPlain() {
        int last = EXIT_OK;
        StringBuilder buf = new StringBuilder();
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            while (true) {
                err.print(buf.length() == 0 ? PRIMARY_PROMPT : CONTINUE_PROMPT);
                err.flush();
                String line = in.readLine();
                if (line == null) {
                    if (!opt.quiet) {
                        err.println("（输入已结束，退出）");
                    }
                    break;
                }
                if (buf.length() == 0) {
                    MetaResult meta = handleMeta(line);
                    if (meta == MetaResult.QUIT) {
                        break;
                    }
                    if (meta == MetaResult.HANDLED) {
                        continue;
                    }
                }
                buf.append(line).append('\n');
                SqlSplitter.Parts parts = SqlSplitter.split(buf.toString());
                for (String sql : parts.statements) {
                    if (!exec(sql)) {
                        last = EXIT_ERROR;
                    }
                }
                buf.setLength(0);
                if (parts.restHasCode) {
                    buf.append(parts.rest);
                }
            }
        } catch (IOException e) {
            err.println("错误: 读取输入失败：" + e.getMessage());
            return EXIT_ERROR;
        }
        return last;
    }

    // ==================== 元命令 ====================

    private enum MetaResult { NOT_META, HANDLED, QUIT }

    private MetaResult handleMeta(String line) {
        String s = line.trim();
        if (s.isEmpty()) {
            return MetaResult.HANDLED;
        }
        if (!s.startsWith("\\")) {
            if (s.equalsIgnoreCase("exit") || s.equalsIgnoreCase("quit")) {
                return MetaResult.QUIT;
            }
            return MetaResult.NOT_META;
        }
        String[] tokens = s.split("\\s+", 2);
        String cmd = tokens[0];
        String arg = tokens.length > 1 ? tokens[1].trim() : "";
        switch (cmd) {
            case "\\q":
            case "\\quit":
                return MetaResult.QUIT;
            case "\\h":
            case "\\help":
            case "\\?":
                out.println(helpText());
                return MetaResult.HANDLED;
            case "\\f":
            case "\\format":
                if (arg.isEmpty()) {
                    err.println("当前输出格式: " + format + "（可选 table / csv / json）");
                } else {
                    try {
                        format = ResultFormatter.Format.parse(arg);
                        err.println("输出格式: " + format);
                    } catch (IllegalArgumentException e) {
                        err.println("错误: " + e.getMessage());
                    }
                }
                return MetaResult.HANDLED;
            case "\\timing":
                timing = !timing;
                err.println("耗时统计: " + (timing ? "开" : "关"));
                return MetaResult.HANDLED;
            default:
                err.println("未知命令: " + cmd + "（\\h 查看帮助）");
                return MetaResult.HANDLED;
        }
    }

    // ==================== 单条语句执行 ====================

    /**
     * 去掉语句结尾的分号：Dojao 的 SQL parser 不认语句结束符
     * （带分号会报 {@code token recognition error at: ';'}），分号只作客户端切分用。
     */
    private static String stripTerminator(String sql) {
        String s = sql.trim();
        if (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1).trim();
        }
        return s;
    }

    /** 执行一条 SQL，流式打印结果；失败返回 {@code false}（错误已打印）。 */
    private boolean exec(String sql) {
        String statement = stripTerminator(sql);
        if (statement.isEmpty()) {
            return true;
        }
        ResultFormatter fmt = new ResultFormatter(format, out);
        boolean[] began = {false};
        FlightQueryClient.RowSink sink = new FlightQueryClient.RowSink() {
            @Override
            public void begin(List<String> columns) {
                began[0] = !columns.isEmpty();
                fmt.begin(columns);
            }

            @Override
            public void row(List<String> values) {
                fmt.row(values);
            }
        };
        try {
            FlightQueryClient.Stats stats = client.execute(statement, sink);
            fmt.end();
            if (!began[0]) {
                out.flush();
                err.println("（服务端未返回列信息）");
            }
            if (timing) {
                StringBuilder sb = new StringBuilder("(").append(stats.rows).append(" 行, ")
                        .append(stats.elapsedMs).append(" ms");
                if (!stats.queryId.isEmpty()) {
                    sb.append(", queryId=").append(stats.queryId);
                }
                err.println(sb.append(')').toString());
            }
            return true;
        } catch (FlightQueryClient.QueryException e) {
            fmt.end();
            err.println("错误: " + e.getMessage());
            return false;
        }
    }

    // ==================== 命令行参数 ====================

    private static final class Options {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        ResultFormatter.Format format = ResultFormatter.Format.TABLE;
        String file;
        boolean quiet;
        private final List<String> positional = new ArrayList<>();

        String sql() {
            return String.join(" ", positional);
        }

        /** @return true = 请求帮助（调用方打印帮助并以 0 退出） */
        boolean parse(String[] args) {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                switch (a) {
                    case "--help":
                    case "-?":
                        return true;
                    case "--host":
                        host = requireValue(args, ++i, a);
                        break;
                    case "-p":
                    case "--port":
                        port = parsePort(requireValue(args, ++i, a));
                        break;
                    case "-f":
                    case "--file":
                        file = requireValue(args, ++i, a);
                        break;
                    case "-F":
                    case "--format":
                        format = ResultFormatter.Format.parse(requireValue(args, ++i, a));
                        break;
                    case "-q":
                    case "--quiet":
                        quiet = true;
                        break;
                    default:
                        if (a.startsWith("-") && !a.equals("-")) {
                            throw new IllegalArgumentException("未知选项: " + a);
                        }
                        positional.add(a);
                }
            }
            return false;
        }

        private static String requireValue(String[] args, int idx, String flag) {
            if (idx >= args.length) {
                throw new IllegalArgumentException(flag + " 缺少参数值");
            }
            return args[idx];
        }

        private static int parsePort(String s) {
            try {
                int p = Integer.parseInt(s);
                if (p <= 0 || p > 65535) {
                    throw new IllegalArgumentException("端口超出范围: " + s);
                }
                return p;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("端口不是数字: " + s);
            }
        }
    }

    private String helpText() {
        return "Dojao CLI " + VERSION + " —— 分布式 SQL 引擎命令行客户端\n"
                + "\n"
                + "用法:\n"
                + "  dojao-cli [选项] [SQL]        # 带 SQL 参数 = 一次性执行；否则进入交互式终端\n"
                + "\n"
                + "选项:\n"
                + "  --host <host>         Master 主机（默认 " + DEFAULT_HOST + "）\n"
                + "  -p, --port <port>     Master 端口（默认 " + DEFAULT_PORT + "）\n"
                + "  -f, --file <file>     执行 SQL 文件（按顶层分号切分，逐条执行，失败即停）\n"
                + "  -F, --format <fmt>    输出格式：table（默认）/ csv / json\n"
                + "  -q, --quiet           不打印启动横幅\n"
                + "  -?, --help            显示本帮助\n"
                + "\n"
                + "交互式元命令:\n"
                + "  \\h, \\?                帮助\n"
                + "  \\q, \\quit             退出（也可用 exit / quit）\n"
                + "  \\f [fmt]              查看/切换输出格式（table / csv / json）\n"
                + "  \\timing               开关行数+耗时统计\n"
                + "\n"
                + "说明:\n"
                + "  - 启动即对 Master 握手探活（listActions 往返），连不上直接报错退出（EXIT=1）\n"
                + "  - 多行 SQL 以分号结尾后执行；\\q 退出\n"
                + "  - 结果写 stdout，统计与错误写 stderr，便于重定向（如 -F csv > out.csv）\n"
                + "  - ↑ / ↓ 翻历史（最近 " + HISTORY_SIZE + " 条，退出时存 ~/.dojao_history，下次启动可回顾）\n"
                + "  - 交互需要真实终端（有 console）：管道 / 重定向 / IDE 输出面板下只能逐行读输入\n"
                + "  - 表名形如 mysql.<schema>.<table>（联邦查询）\n"
                + "  - 中文乱码时加 JVM 参数：-Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8";
    }
}
