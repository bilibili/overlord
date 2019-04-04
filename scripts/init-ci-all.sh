#!/bin/bash

set -e

export HostIP="127.0.0.1"

docker run -d -v /usr/share/ca-certificates/:/etc/ssl/certs -p 4001:4001 -p 2380:2380 -p 2379:2379 \
       --name etcd quay.io/coreos/etcd:v2.3.8 \
       -name etcd0 \
       -advertise-client-urls http://${HostIP}:2379,http://${HostIP}:4001 \
       -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 \
       -initial-advertise-peer-urls http://${HostIP}:2380 \
       -listen-peer-urls http://0.0.0.0:2380 \
       -initial-cluster-token etcd-cluster-1 \
       -initial-cluster etcd0=http://${HostIP}:2380 \
       -initial-cluster-state new

docker pull grokzen/redis-cluster:4.0.9
docker run -e "IP=0.0.0.0" -d -p 7000-7007:7000-7007 grokzen/redis-cluster:4.0.9
docker run -e "IP=0.0.0.0" -d -p 8000-8007:8000-8007 grokzen/redis-cluster:4.0.9

cd /tmp/

which memcached
which redis-server

redis-server --port 9001 --daemonize yes
redis-server --port 9002 --daemonize yes

memcached -h
memcached -p 9101 -l 0.0.0.0 -d
memcached -p 9102 -l 0.0.0.0 -d
cd -

