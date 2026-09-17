#!/bin/bash

platform=`uname -p`
if [ $platform == 'x86_64' ]
then
	version='x64'
elif [ $platform == 'aarch64' ]
then
	version='arm64'
else
	echo 'Not supported platform, please download the compiled nodejs and Python runtime manually.'
	exit 1
fi

# 下载并安装Miniconda和Node.js
wget https://repo.anaconda.com/miniconda/Miniconda3-py39_24.4.0-0-Linux-${platform}.sh -O miniconda.sh --no-check-certificate && sh miniconda.sh -b -p python
wget https://nodejs.org/dist/v18.20.0/node-v18.20.0-linux-${version}.tar.xz --no-check-certificate && tar -xJvf node-v18.20.0-linux-${version}.tar.xz

export CI=False
export PATH=$(pwd)/node-v18.20.0-linux-${version}/bin:$(pwd)/python/bin:$PATH

# 清理旧的依赖
rm -rf ui/node_modules

# 进入ui目录并使用--legacy-peer-deps安装依赖
cd ui

# 安装特定版本的依赖
npm install --save-dev ajv@8.11.0 ajv-keywords@3.5.2 @babel/core@7.16.0 --legacy-peer-deps

# 安装其他依赖
npm install --legacy-peer-deps

# 构建UI
npm run build

# 回到根目录
cd ..

# 手动执行打包步骤
# 清理
find . -type d -name '__pycache__' -exec rm -rf {} +
rm -rf dbmind-installer*.sh
rm -rf payload.tar*
rm -rf build *.spec
rm -rf 3rd

## 下载第三方依赖
PYTHON=$(pwd)/python/bin/python3
#if [ $(uname -m | grep x86 | wc -m) -eq 0 ]; then
#    $PYTHON -m pip install -r requirements-aarch64.txt --target=3rd --prefer-binary -i https://mirrors.aliyun.com/pypi/simple/
#else
#    $PYTHON -m pip install -r requirements-x86.txt --target=3rd --prefer-binary -i https://mirrors.aliyun.com/pypi/simple/
#fi

# 打包
PYTHON_VERSION=$($PYTHON -c 'import sys; print("%d.%d"% sys.version_info[0:2])')
installer_name="dbmind-installer-${platform}-python${PYTHON_VERSION}.sh"
tar --exclude='ui' --exclude='tests' --exclude='Makefile' --exclude='decompress' --exclude='tox.ini' --exclude='node-*' --exclude='miniconda.sh' -cf payload.tar *
tar --append --file=payload.tar ui/build
gzip payload.tar
cat decompress payload.tar.gz > $installer_name
chmod +x $installer_name

# dbmind打包
mkdir dbmind-installer-${platform}
cp -r dbmind-installer-${platform}-python3.*.sh dbmind-installer-${platform}/
cp -r requirements-aarch64.txt dbmind-installer-${platform}/
cp -r requirements-optional.txt dbmind-installer-${platform}/
cp -r requirements-x86.txt dbmind-installer-${platform}/
tar -zcf dbmind-installer-${platform}.tar.gz dbmind-installer-${platform}
echo "Successfully generated DBMind installation package $installer_name."

# 运行测试
# $PYTHON -m pip install pytest
# $PYTHON -m pytest tests
