#!/usr/bin/env bash
# ============================================================================
# Dojao 三节点部署脚本 —— 在 aliyun003（Master 机）上执行
#
# 做的事（严格按顺序）：
#   1. 删除 /opt/module/dojao，把 dojao-1.0.0.tar.gz 解压到 /opt/module/
#   2. 改 /opt/module/dojao/bin/dojao：在 `set -e` 上一行插入
#         JAVA_HOME=/opt/module/jdk-21.0.10
#   3. 改本机（aliyun003）的 /opt/module/dojao/conf/dojao.conf：
#         system.master.host = aliyun003
#         system.worker.host = aliyun003
#      并按 WORKER_NODES 生成 conf/workers.conf（dojao-cluster.sh 的启动清单）
#   4. 清掉 aliyun001/002 上的旧目录（防止旧版 jar 残留），再 xsync 分发整个 dojao 目录
#   5. ssh aliyun001 / aliyun002，各改自己的 conf：
#         system.worker.host = aliyun001 | aliyun002
#         system.role        = worker
#   6. 回显三台机器的最终关键配置（只读）
#
# 用法：
#   ./deploy-cluster.sh                                  # 自动找 tar 包
#   ./deploy-cluster.sh /root/dojao-1.0.0.tar.gz  # 显式指定
#   TARBALL=/path/xxx.tar.gz ./deploy-cluster.sh
#
# 前提：
#   * 有写 /opt/module 的权限 —— 直接以 root 执行，**别用 sudo**
#     （sudo 会换掉 SSH 密钥上下文，第 5 步就 ssh 不出去了）
#   * 已配好到 aliyun001 / aliyun002 的免密 ssh（含 known_hosts）
#   * xsync 可用
# ============================================================================
set -euo pipefail

# ---------------------------- 可调参数 ----------------------------
DOJAO_HOME=/opt/module/dojao
JAVA_HOME_VALUE=/opt/module/jdk-21.0.10
MASTER_NODE=aliyun003
WORKER_NODES=(aliyun001 aliyun002)
TARBALL_NAME=dojao-1.0.0.tar.gz
SSH_OPTS="${SSH_OPTS:--o ConnectTimeout=10}"

CONF="$DOJAO_HOME/conf/dojao.conf"
BIN="$DOJAO_HOME/bin/dojao"

# ---------------------------- 小工具 ----------------------------
log()  { printf '\n\033[1;34m=== %s ===\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m[ERROR] %s\033[0m\n' "$*" >&2; exit 1; }

# 把 conf 里的 key 设成 val：已存在 → 原地改值（含注释掉的不动），不存在 → 追加到末尾。幂等，可重复跑。
set_conf() {
    local file="$1" key="$2" val="$3"
    if grep -qE "^[[:space:]]*${key}[[:space:]]*=" "$file"; then
        sed -i -E "s|^[[:space:]]*${key}[[:space:]]*=.*$|${key} = ${val}|" "$file"
    else
        printf '%s = %s\n' "$key" "$val" >> "$file"
    fi
}

# 打印某个 conf 的「角色 / Master 地址 / Worker 地址」三行
show_conf() {
    grep -hE '^[[:space:]]*system\.(role|master\.host|worker\.host)[[:space:]]*=' "$1" \
        | sed -E 's/^[[:space:]]*/    /' || true
}

# ============================ 1/6 解压 ============================
log "1/6 解压 $TARBALL_NAME 到 /opt/module/"

TARBALL="${1:-${TARBALL:-}}"
if [ -z "$TARBALL" ]; then
    for cand in "/opt/software/$TARBALL_NAME" "$PWD/$TARBALL_NAME" "$HOME/$TARBALL_NAME"; do
        if [ -f "$cand" ]; then
            TARBALL="$cand"
            break
        fi
    done
fi
[ -n "$TARBALL" ] || die "找不到 $TARBALL_NAME（用法：$0 /path/$TARBALL_NAME 或 TARBALL=... $0）"
[ -f "$TARBALL" ] || die "tar 包不存在：$TARBALL"
[ -d /opt/module ] || die "/opt/module 不存在"
[ -w /opt/module ] || die "/opt/module 不可写 —— 请用 root 直接执行（别用 sudo，会丢失 ssh 密钥上下文）"

# 只允许删这一个约定路径，防手滑（DOJAO_HOME 被改坏时宁可停）
[ "$DOJAO_HOME" = "/opt/module/dojao" ] || die "内部错误：DOJAO_HOME=$DOJAO_HOME 不是约定路径，拒绝删除"
if [ -e "$DOJAO_HOME" ]; then
    echo "  删除旧目录 $DOJAO_HOME"
    rm -rf "$DOJAO_HOME"
fi
tar -xzf "$TARBALL" -C /opt/module/
[ -x "$BIN" ] || die "解压后找不到可执行文件 $BIN（tar 包是否含顶层 dojao/ 目录？）"
ok "已解压：$DOJAO_HOME"

# ==================== 2/6 bin/dojao 注入 JAVA_HOME ====================
log "2/6 修改 bin/dojao：在 set -e 上一行插入 JAVA_HOME"

if grep -qE '^JAVA_HOME=' "$BIN"; then
    warn "已存在 JAVA_HOME 行，跳过（幂等）"
else
    grep -qE '^set -e$' "$BIN" || die "$BIN 里找不到独立的 'set -e' 行，拒绝盲改"
    sed -i "s|^set -e\$|JAVA_HOME=${JAVA_HOME_VALUE}\nset -e|" "$BIN"
    ok "已插入 JAVA_HOME=${JAVA_HOME_VALUE}"
fi
[ -x "$JAVA_HOME_VALUE/bin/java" ] || warn "$JAVA_HOME_VALUE/bin/java 不存在 —— 确认该机 JDK 路径"
grep -nE '^(JAVA_HOME=|set -e$)' "$BIN" | sed 's/^/    /' || true

# ==================== 3/6 本机（Master 机）conf ====================
log "3/6 修改本机 $MASTER_NODE 的 conf + 生成 conf/workers.conf"

[ -f "$CONF" ] || die "找不到 $CONF"
set_conf "$CONF" system.master.host "$MASTER_NODE"
set_conf "$CONF" system.worker.host "$MASTER_NODE"
ok "已设置 system.master.host / system.worker.host = $MASTER_NODE"
show_conf "$CONF"

# dojao-cluster.sh 只读 conf/workers.conf 决定 ssh 到哪几台机器启动；这里按部署清单生成。
# 必须在第 4 步 xsync 之前写，这样 Worker 机也会同步到同一份清单（路径/顺序一致）。
WORKERS_CONF="$DOJAO_HOME/conf/workers.conf"
{
    echo "# 集群 Worker 机清单（由 scripts/deploy-cluster.sh 生成；一行一个，本机不写）"
    for node in "${WORKER_NODES[@]}"; do
        echo "$node"
    done
} > "$WORKERS_CONF"
ok "已生成 conf/workers.conf：${WORKER_NODES[*]}"

# ==================== 4/6 清理旧目录 + xsync 分发 ====================
log "4/6 清理各 Worker 旧目录并分发 $DOJAO_HOME"

command -v xsync >/dev/null 2>&1 || die "找不到 xsync（集群同步脚本），请先安装并保证在 PATH 上"

# 先删远端旧目录：多数 xsync 用 `rsync -av`（无 --delete），不删的话新版里**删掉的 jar 会残留在
# Worker 上**，出现「本机跑新代码、Worker 跑旧代码」的诡异不一致。
for node in "${WORKER_NODES[@]}"; do
    echo "  清理 $node:$DOJAO_HOME"
    ssh $SSH_OPTS "$node" bash -s -- "$DOJAO_HOME" <<'REMOTE'
set -euo pipefail
DOJAO_HOME="$1"
[ "$DOJAO_HOME" = "/opt/module/dojao" ] || { echo "拒绝删除非常规路径: $DOJAO_HOME" >&2; exit 1; }
rm -rf "$DOJAO_HOME"
REMOTE
done
ok "旧目录已清空"

xsync "$DOJAO_HOME"
ok "分发完成"

# ==================== 5/6 Worker 机 conf ====================
log "5/6 ssh 修改 ${WORKER_NODES[*]} 的 conf"

for node in "${WORKER_NODES[@]}"; do
    echo "  --> $node"
    ssh $SSH_OPTS "$node" bash -s -- "$node" <<'REMOTE'
set -euo pipefail
NODE="$1"
CONF=/opt/module/dojao/conf/dojao.conf
[ -f "$CONF" ] || { echo "找不到 $CONF" >&2; exit 1; }

set_conf() {
    local file="$1" key="$2" val="$3"
    if grep -qE "^[[:space:]]*${key}[[:space:]]*=" "$file"; then
        sed -i -E "s|^[[:space:]]*${key}[[:space:]]*=.*$|${key} = ${val}|" "$file"
    else
        printf '%s = %s\n' "$key" "$val" >> "$file"
    fi
}

set_conf "$CONF" system.worker.host "$NODE"
set_conf "$CONF" system.role worker
grep -hE '^[[:space:]]*system\.(role|master\.host|worker\.host)[[:space:]]*=' "$CONF" \
    | sed -E 's/^[[:space:]]*/      /' || true
REMOTE
done
ok "Worker 机 conf 修改完成"

# ==================== 6/6 回显最终配置 ====================
log "6/6 三台机器最终关键配置"

printf '  %-12s\n' "$MASTER_NODE"
show_conf "$CONF"

for node in "${WORKER_NODES[@]}"; do
    printf '  %-12s\n' "$node"
    ssh $SSH_OPTS "$node" bash -s <<'REMOTE'
grep -hE '^[[:space:]]*system\.(role|master\.host|worker\.host)[[:space:]]*=' \
    /opt/module/dojao/conf/dojao.conf | sed -E 's/^[[:space:]]*/      /' || true
REMOTE
done

# ---------------------------- 收尾提示 ----------------------------
ROLE_NOW=$(grep -hE '^[[:space:]]*system\.role[[:space:]]*=' "$CONF" | head -1 | sed -E 's/.*=[[:space:]]*//' || true)
cat <<EOF

部署完成。
  $MASTER_NODE 的 system.role 当前为 [${ROLE_NOW:-未设置}] —— 只想起 Master 就改成 master；
  想连它一起当第 3 个 Worker（共 3 Worker）就保持 both。

启动（在 Master 机上）：
  /opt/module/dojao/bin/dojao-cluster.sh   # 本机 + conf/workers.conf 里的全部 Worker
  或逐台 /opt/module/dojao/bin/dojao       # 各机按自己 conf 的 system.role 启动

注意：conf 在进程启动时读一次，改完必须重启进程。
EOF
