#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

platform="$(uname -p)"
case "${platform}" in
  x86_64)
    node_arch="x64"
    requirements_file="requirements-x86.txt"
    ;;
  aarch64)
    node_arch="arm64"
    requirements_file="requirements-aarch64.txt"
    ;;
  *)
    echo "当前平台(${platform})暂不支持，请手动安装 Python 与 Node.js 环境。"
    exit 1
    ;;

esac

MINICONDA_INSTALLER="Miniconda3-py39_24.4.0-0-Linux-${platform}.sh"
NODE_ARCHIVE="node-v16.9.0-linux-${node_arch}.tar.xz"
PYTHON_DIR="${SCRIPT_DIR}/python"
NODE_DIR="${SCRIPT_DIR}/node-v16.9.0-linux-${node_arch}"

if [ ! -d "${PYTHON_DIR}" ]; then
  if [ ! -f "${MINICONDA_INSTALLER}" ]; then
    echo "下载 Miniconda 安装包..."
    wget "https://repo.anaconda.com/miniconda/${MINICONDA_INSTALLER}" -O "${MINICONDA_INSTALLER}" --no-check-certificate
  else
    echo "检测到已存在 ${MINICONDA_INSTALLER}，跳过下载。"
  fi
  echo "安装 Miniconda 到 ${PYTHON_DIR}..."
  bash "${MINICONDA_INSTALLER}" -b -p "${PYTHON_DIR}"
  rm -f "${MINICONDA_INSTALLER}"
else
  echo "检测到已有 Python 环境 ${PYTHON_DIR}，跳过安装。"
fi

if [ ! -d "${NODE_DIR}" ]; then
  if [ ! -f "${NODE_ARCHIVE}" ]; then
    echo "下载 Node.js 安装包..."
    wget "https://nodejs.org/dist/v16.9.0/${NODE_ARCHIVE}" --no-check-certificate
  else
    echo "检测到已存在 ${NODE_ARCHIVE}，跳过下载。"
  fi
  echo "解压 Node.js 到 ${NODE_DIR}..."
  tar -xJf "${NODE_ARCHIVE}"
  rm -f "${NODE_ARCHIVE}"
else
  echo "检测到已有 Node.js 目录 ${NODE_DIR}，跳过解压。"
fi

export CI="${CI:-False}"
export PATH="${NODE_DIR}/bin:${PYTHON_DIR}/bin:${PATH}"

if [ ! -d "${PYTHON_DIR}" ]; then
  echo "未找到 Python 环境，脚本终止。"
  exit 1
fi

echo "升级 pip 并安装 Python 依赖 (${requirements_file})..."
"${PYTHON_DIR}/bin/python" -m pip install --upgrade pip
"${PYTHON_DIR}/bin/python" -m pip install -r "${SCRIPT_DIR}/${requirements_file}"

if [ ! -d "${SCRIPT_DIR}/ui" ]; then
  echo "未找到 ui 目录，脚本终止。"
  exit 1
fi

echo "安装前端依赖并构建 UI..."
pushd "${SCRIPT_DIR}/ui" > /dev/null
npm install
npm run build
popd > /dev/null

echo "环境准备完成，可执行 ./gs_dbmind service restart -c dbmindconf 重启服务。"
