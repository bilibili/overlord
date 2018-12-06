#!/bin/sh

MESOS_VERSION=1.7.0

wget -c http://mirrors.tuna.tsinghua.edu.cn/apache/mesos/${MESOS_VERSION}/mesos-${MESOS_VERSION}.tar.gz -O mesos-${MESOS_VERSION}.tar.gz

mkdir -p /data/
tar -zxvf mesos-${MESOS_VERSION}.tar.gz -C /data/

cd /data/mesos-${MESOS_VERSION}
apt update
apt-get install -y tar wget git
apt-get install -y openjdk-7-jdk
apt-get install -y autoconf libtool
apt-get -y install build-essential python-dev python-six python-virtualenv libcurl4-nss-dev libsasl2-dev libsasl2-modules maven libapr1-dev libsvn-dev zlib1g-dev iputils-ping

./bootstrap
mkdir -p build
cd build
../configure
make -j 40
make install
