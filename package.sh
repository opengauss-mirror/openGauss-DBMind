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
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${platform}.sh -O miniconda.sh && sh miniconda.sh -b -p python
wget https://nodejs.org/dist/v16.9.0/node-v16.9.0-linux-${version}.tar.xz && tar -xJvf node-v16.9.0-linux-${version}.tar.xz 
export CI=False
export PATH=`pwd`/node-v16.9.0-linux-${version}/bin:`pwd`/python/bin:$PATH
make package
make test
