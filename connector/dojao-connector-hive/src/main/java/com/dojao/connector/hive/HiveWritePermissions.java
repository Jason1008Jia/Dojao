package com.dojao.connector.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Map;

/**
 * Dojao 写入 Hive 时新建目录 / 文件的 HDFS 权限。
 *
 * <p><b>为什么需要</b>：不显式设置时，目录 / 文件取进程 umask（常见 007 → 目录 770、文件 660），
 * 属组是写入身份（{@code hadoop.user.name}）的组。HiveServer2 开启 impersonation 后按查询用户做
 * HDFS 授权检查，非 owner、不在组内的用户读不了表目录 —— 表现为
 * {@code SemanticException Unable to fetch table ... AccessControlException: Permission denied}。</p>
 *
 * <p>默认值对齐 Hive 自身的 {@code hive.files.umask.value=0002}（目录 775 / 文件 664），
 * 可用 catalog 属性覆盖：{@code catalog.hive.write.dir.permission} /
 * {@code catalog.hive.write.file.permission}（八进制），{@code catalog.hive.write.group}（可选属组）。</p>
 *
 * <p>只处理<b>本次创建</b>的目录 / 文件：已存在的目录可能属于其他写入方，不去改其权限。</p>
 */
final class HiveWritePermissions {

    static final String DIR_KEY = "hive.write.dir.permission";
    static final String FILE_KEY = "hive.write.file.permission";
    static final String GROUP_KEY = "hive.write.group";

    private static final FsPermission DEFAULT_DIR = new FsPermission((short) 0775);
    private static final FsPermission DEFAULT_FILE = new FsPermission((short) 0664);

    static final HiveWritePermissions DEFAULTS =
            new HiveWritePermissions(DEFAULT_DIR, DEFAULT_FILE, null);

    private final FsPermission dirPermission;
    private final FsPermission filePermission;
    /** 目标属组；null = 不 chgrp（保留写入身份的默认组）。 */
    private final String group;

    private HiveWritePermissions(FsPermission dirPermission, FsPermission filePermission, String group) {
        this.dirPermission = dirPermission;
        this.filePermission = filePermission;
        this.group = group;
    }

    static HiveWritePermissions from(Map<String, String> props) {
        String group = props.get(GROUP_KEY);
        return new HiveWritePermissions(
                parse(props.get(DIR_KEY), DEFAULT_DIR, DIR_KEY),
                parse(props.get(FILE_KEY), DEFAULT_FILE, FILE_KEY),
                group == null || group.isBlank() ? null : group.trim());
    }

    /** 建目录并落权限；目录已存在则什么都不做（不覆盖别人的权限）。 */
    void mkdirs(FileSystem fs, Path dir) throws IOException {
        if (!fs.mkdirs(dir, dirPermission)) return;
        if (!hasPosixPerms(fs)) return;
        applyMode(fs, dir, dirPermission);
        chgrp(fs, dir);
    }

    /**
     * 给本会话写出的文件补权限。<b>必须在 rename 进表目录之前调用</b>：
     * HDFS rename 不改变权限，文件会带着 umask 结果（常见 660）落表，Hive 读侧同样访问不了。
     */
    void applyFilePerms(FileSystem fs, Path root) throws IOException {
        if (!hasPosixPerms(fs) || !fs.exists(root)) return;
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
        while (files.hasNext()) {
            Path path = files.next().getPath();
            applyMode(fs, path, filePermission);
            chgrp(fs, path);
        }
    }

    /**
     * 权限位只在 HDFS 系（hdfs / viewfs / ofs）上有意义。本地文件系统与对象存储跳过：
     * Windows 本地 FS 缺 winutils 时连列举都会抛 {@link UnsatisfiedLinkError}，
     * setPermission 也只会抛 {@link UnsupportedOperationException}。
     */
    private static boolean hasPosixPerms(FileSystem fs) {
        // 不用 fs.getScheme()：Hadoop 新版 LocalFs 直接抛 UnsupportedOperationException
        String scheme = fs.getUri() == null ? null : fs.getUri().getScheme();
        return "hdfs".equalsIgnoreCase(scheme)
                || "viewfs".equalsIgnoreCase(scheme)
                || "ofs".equalsIgnoreCase(scheme);
    }

    private static void applyMode(FileSystem fs, Path path, FsPermission permission) throws IOException {
        try {
            fs.setPermission(path, permission);
        } catch (UnsupportedOperationException ignored) {
            // 非 POSIX 文件系统（Windows 本地 / 对象存储）：没有权限位可设
        }
    }

    private void chgrp(FileSystem fs, Path path) throws IOException {
        if (group == null) return;
        try {
            fs.setOwner(path, null, group);
        } catch (UnsupportedOperationException ignored) {
            return;
        } catch (IOException e) {
            throw new IOException("设置 Hive 写入路径属组失败: " + path + " → " + group
                    + "（catalog.hive." + GROUP_KEY + "；写入身份需在目标组内或具备超级用户权限）", e);
        }
    }

    private static FsPermission parse(String raw, FsPermission dflt, String key) {
        if (raw == null || raw.isBlank()) return dflt;
        try {
            int value = Integer.parseInt(raw.trim(), 8);
            if (value < 0 || value > 0777) throw new NumberFormatException("超出 0000-0777");
            return new FsPermission((short) value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("catalog.hive." + key
                    + " 必须是八进制权限（如 775 / 664）: " + raw);
        }
    }
}
