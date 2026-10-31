#!/usr/bin/env bash
# ============================================================================
# dojao 电商场景造数 —— 一键跑（在集群节点 aliyun003 上执行，全程内网）
#
#   ./run.sh              # 造 MySQL → 造 Hive
#   ./run.sh mysql        # 只造业务数据（MySQL dojao 库）
#   ./run.sh hive         # 只造行为日志（Hive dojao_test 库）
#   ./run.sh check        # 只检查依赖
#   ./run.sh install      # 装依赖（系统 pip 被 PEP668 挡住时自动建 .venv）
#
# 透传参数用 `--`：./run.sh mysql -- --reset
# 规模 / 分布全部在 config.py 里改，不必动代码。
#
# 不用本脚本、直接 `python3 gen_hive.py` 跑也完全可以 —— 依赖齐全就行。
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"

# 挑一个**真能跑**的 python：优先 .venv（install 失败时建的那个），否则系统 python3。
# 只 `command -v` 不够 —— Windows 上 python3 是 Microsoft Store 的占位程序，
# 存在但一跑就打印"Python was not found"，所以这里用 `-c ""` 实跑一次来验。
pick_py() {
    for c in .venv/bin/python python3 python; do
        if [ -x "$c" ]; then
            "$c" -c "" >/dev/null 2>&1 && { echo "$c"; return 0; }
        elif command -v "$c" >/dev/null 2>&1; then
            "$c" -c "" >/dev/null 2>&1 && { echo "$c"; return 0; }
        fi
    done
    return 1
}
PY=$(pick_py) || { echo "找不到可用的 python3（试过 .venv/bin/python, python3, python）" >&2; exit 1; }

check() {
    "$PY" - <<'PY'
import importlib.util, sys
miss = [m for m in ("numpy", "pyarrow", "pymysql") if importlib.util.find_spec(m) is None]
if not miss:
    sys.exit(0)
print("缺少 Python 依赖：" + "、".join(miss), file=sys.stderr)
print("Debian/Ubuntu 的系统 pip 会被 PEP668（externally-managed）挡住，任选一种：", file=sys.stderr)
print("    sudo apt install -y python3-numpy python3-pyarrow python3-pymysql", file=sys.stderr)
print("    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt", file=sys.stderr)
print("    python3 -m pip install --break-system-packages -r requirements.txt", file=sys.stderr)
print("（也可以直接用本机已有的 python3 环境跑，本脚本不强制装依赖）", file=sys.stderr)
sys.exit(1)
PY
}

install() {
    if python3 -m pip install -q -r requirements.txt 2>/dev/null; then
        echo "依赖已就绪（系统环境）。"
        return
    fi
    echo "系统 pip 不可用（PEP668 externally-managed），改用虚拟环境 .venv ..."
    python3 -m venv .venv
    .venv/bin/pip install -q -r requirements.txt
    echo "依赖已就绪（.venv，之后本脚本会自动用它）。"
}

STEP="${1:-all}"
shift || true
if [ "${1:-}" = "--" ]; then shift; fi

case "$STEP" in
    install) install; exit 0 ;;
    check)   check && echo "依赖就绪：$PY" || exit 1 ;;
    mysql|hive|all) check ;;
    *) echo "用法：$0 [mysql|hive|all|check|install] [-- <额外参数>]" >&2; exit 2 ;;
esac

case "$STEP" in
    mysql) "$PY" gen_mysql.py "$@" ;;
    hive)  "$PY" gen_hive.py "$@" ;;
    all)   "$PY" gen_mysql.py "$@" && "$PY" gen_hive.py "$@" ;;
esac
