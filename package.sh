#!/bin/bash
# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

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
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${platform}.sh -O miniconda.sh --no-check-certificate && sh miniconda.sh -b -p python
wget https://nodejs.org/dist/v16.9.0/node-v16.9.0-linux-${version}.tar.xz --no-check-certificate && tar -xJvf node-v16.9.0-linux-${version}.tar.xz
export CI=False
export PATH=`pwd`/node-v16.9.0-linux-${version}/bin:`pwd`/python/bin:$PATH
make package
make test
