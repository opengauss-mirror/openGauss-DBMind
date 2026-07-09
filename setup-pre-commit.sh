#!/usr/bin/env bash
# =============================================================================
# setup-pre-commit.sh  —— pre-commit 一键安装脚本
#
# 作用：在当前仓库为开发者装好提交前检查（pre-commit）。
#
# 设计原则（重要）：
#   * 不污染系统/全局环境：所有工具装进本仓库下的独立虚拟环境
#     .pre-commit-venv/（已建议加入 .gitignore），
#     不动系统 python、不执行 `pip config set`、不写 ~/.config/pip/pip.conf。
#     删除该目录即可完全卸载，环境干净如初。
#   * pip 镜像源仅本脚本临时使用（-i / 环境变量），不影响其它项目。
#   * 幂等：可重复运行。
#
# 用法：
#   bash setup-pre-commit.sh
#   PIP_MIRROR=https://pypi.tuna.tsinghua.edu.cn/simple bash setup-pre-commit.sh
#
# 前提：能访问 gitcode.com；本机有 python>=3.10。
# =============================================================================
set -euo pipefail

PIP_MIRROR="${PIP_MIRROR:-https://mirrors.aliyun.com/pypi/simple/}"
PIP_HOST="$(printf '%s' "$PIP_MIRROR" | sed -E 's#^https?://([^/]+)/.*#\1#')"
VENV_DIR=".pre-commit-venv"     # 工具隔离环境，位于仓库根目录

log()  { printf '\033[1;32m[setup]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn ]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# ---- 0. 基本环境检查 ----
command -v git >/dev/null 2>&1 || die "未找到 git"
git rev-parse --show-toplevel >/dev/null 2>&1 || die "当前不在 git 仓库内，请在仓库根目录运行"
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
[ -f .pre-commit-config.yaml ] || die "当前仓库没有 .pre-commit-config.yaml，无法安装"

# 选一个 python>=3.10 用来【建 venv】（不会往它里面装包，故不受 PEP668 限制）
PY=""
for c in python3.13 python3.12 python3.11 python3.10 python3; do
  if command -v "$c" >/dev/null 2>&1; then
    ver="$("$c" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo 0.0)"
    major="${ver%%.*}"; minor="${ver##*.}"
    if [ "${major:-0}" -eq 3 ] && [ "${minor:-0}" -ge 10 ]; then PY="$c"; break; fi
  fi
done
[ -n "$PY" ] || die "需要 python>=3.10（pre-commit 要求）。请先安装。"
log "使用 python: $PY ($("$PY" --version 2>&1))"
log "使用 pip 镜像(临时): $PIP_MIRROR"

# ---- 1. 创建独立虚拟环境（隔离，不污染系统）----
if [ ! -d "$VENV_DIR" ]; then
  log "创建隔离环境: $VENV_DIR/"
  "$PY" -m venv "$VENV_DIR" || die "创建 venv 失败（可能缺 python venv 模块）"
else
  log "复用已存在的隔离环境: $VENV_DIR/"
fi
VENV_PY="$VENV_DIR/bin/python"

# ---- 2. 在 venv 内安装 pre-commit 及本机工具（源仅临时指定）----
# mypy 走 local 需本机存在；DBMind 仓 lint 用 flake8 也需存在。
PKGS=(pre-commit mypy)
if grep -q "id: flake8" .pre-commit-config.yaml 2>/dev/null; then
  PKGS+=(flake8)
fi
log "安装工具到隔离环境: ${PKGS[*]}"
"$VENV_PY" -m pip install --disable-pip-version-check -q \
  -i "$PIP_MIRROR" --trusted-host "$PIP_HOST" --upgrade pip
"$VENV_PY" -m pip install --disable-pip-version-check \
  -i "$PIP_MIRROR" --trusted-host "$PIP_HOST" \
  "${PKGS[@]}"

PRE_COMMIT="$VENV_DIR/bin/pre-commit"

# ---- 3. 安装 git 钩子 ----
# pre-commit install 会把 venv 里的 pre-commit 路径写进 .git/hooks/pre-commit，
# 因此 commit 时无需手动激活 venv，钩子会自动用这个隔离环境里的工具。
log "安装 git 钩子"
"$PRE_COMMIT" install

# ---- 4. 预热：拉取并构建各钩子环境（源用环境变量临时指定）----
log "预热钩子环境，首次会联网从 gitcode 拉取，请稍候…"
PIP_INDEX_URL="$PIP_MIRROR" PIP_TRUSTED_HOST="$PIP_HOST" \
  "$PRE_COMMIT" install-hooks

# ---- 5. 建议把 venv 加入本地忽略（不提交隔离环境）----
if ! grep -qxF "$VENV_DIR/" .gitignore 2>/dev/null && ! grep -qxF "$VENV_DIR" .gitignore 2>/dev/null; then
  warn "建议将 $VENV_DIR/ 加入 .gitignore（避免误提交隔离环境）"
fi

log "完成！之后每次 git commit 会自动检查本次改动（无需手动激活 venv）。"
log "手动试跑： $PRE_COMMIT run --files <某个文件>"
log "卸载：     $PRE_COMMIT uninstall 并删除 $VENV_DIR/"