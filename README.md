# Overlord 
[![Build Status](https://travis-ci.org/felixhao/overlord.svg?branch=master)](https://travis-ci.org/felixhao/overlord) 
[![GoDoc](http://godoc.org/github.com/felixhao/overlord/proxy?status.svg)](http://godoc.org/github.com/felixhao/overlord/proxy) 
[![codecov](https://codecov.io/gh/felixhao/overlord/branch/master/graph/badge.svg)](https://codecov.io/gh/felixhao/overlord)
[![Go Report Card](https://goreportcard.com/badge/github.com/felixhao/overlord)](https://goreportcard.com/report/github.com/felixhao/overlord)


Overlord is a proxy based high performance Memcached and Redis solution written in Go.  
It is fast and lightweight and used primarily to horizontally scale your distributed caching architecture.  
It is inspired by Twemproxy,Codis.  

## Quick Start

#### Download

```go
cd $GOPATH/src
git clone https://github.com/felixhao/overlord.git
```

#### Build

```shell
cd $GOPATH/src/overlord/cmd/proxy
go build
```

#### Run

```shell
./proxy -cluster=proxy-cluster-example.toml
```
###### Please first run a memcache or redis server, which bind 11211 or 6379 port.

#### Test

```shell
# test memcache
echo -e "set a_11 0 0 5\r\nhello\r\n" | nc 127.0.0.1 21211
# STORED
echo -e "get a_11\r\n" | nc 127.0.0.1 21211
# VALUE a_11 0 5
# hello
# END

# test redis
python ./scripts/validate_redis_features.py # require fakeredis==0.11.0 redis==2.10.6 gevent==1.3.5
```

Congratulations! You've just run the overlord proxy.

## Features

- [x] support memcache protocol
- [x] support redis&cluster protocol
- [x] connection pool for reduce number to backend caching servers
- [x] keepalive & failover
- [x] hash tag: specify the part of the key used for hashing
- [x] promethues stat metrics support
- [ ] cache backup
- [ ] hot reload: add/remove cache node
- [ ] QoS: limit/breaker
- [ ] L1&L2 cache
- [ ] hot|cold cache
- [ ] broadcast
- [ ] cache node scheduler

## Architecture

![architecture](doc/images/overlord_arch.png)


## Cache-Platform

we have made new automatic cache management platform based on mesos/etcd.

### cache-platform Architecture

![cache-platform Architecture](doc/images/cache-platform-arch.png)

### quick start

#### 1. prepare

1. etcd cluster
2. mesos

#### 2. build

```bash
make build
```

#### 3. deployment

##### cache binary deployment

The platform need init the mesos-agent node with pre-build cache binary to the dir as this:

```
/data/lib/memcache
└── 1.5.12
    └── bin
        └── memcached
/data/lib/redis
└── 4.0.11
    └── bin
        └── redis-server
```

#### file server deployment

mesos need an http server to fetch executor, we suggest use nginx:

```
# cat /etc/nginx/conf.d/fs.conf
server {
    client_max_body_size 4G;
    listen       20001;
    proxy_pass_request_body on;

    #charset koi8-r;
    access_log  /data/log/nginx/host.cache-platform-fs.access.log  main;

    location / {
        root /data/overlord/statics/;
        try_files $uri $uri/ /index.html;
    }
}
```

and then ,you need to setup the addr into cache platform's etcd:

```
mkdir -p /data/overlord/statics/
cp cmd/executor/executor /data/overlord/statics/ 
etcdctl set /overlord/fs 'http://172.22.33.198:20001'
```

#### apiserver deployment

apiserver config file and you need to change the etcd and grafana addr:

```
listen = "0.0.0.0:8880"
etcd = "http://127.0.0.1:2379"
log_vl = 10
log = "info"
debug = true
stdout = true

[monitor]
  url = "http://127.0.0.1:1234"
  panel = "overlord"
  name_var = "cluster"
  org_id = 1

[[versions]]
  cache_type = "redis"
  versions = ["4.0.11", "3.2.8"]

[[versions]]
  cache_type = "redis_cluster"
  versions = ["4.0.11", "3.2.8", "5.0"]


[[versions]]
  cache_type = "memcache"
  versions = ["1.5.0"]

[cluster]
dial_timeout = 1000
read_timeout = 1000
write_timeout = 1000
node_connections = 2
ping_fail_limit = 3
ping_auto_eject = true
```

run:
```
cmd/apiserver/apiserver -conf cmd/apiserver/conf.toml
```

#### scheduler deployment

scheduler config file content

```
user = "root"
name = "overlord"
checkpoint = true
master = "127.0.0.1:5050"
executor_url = "http://127.0.0.1:20001/executor"
db_end_point = "http://127.0.0.1:2379"
fail_voer = "72h"
role=["sh001"]
```

run:

```
cmd/scheduler/scheduler -conf cmd/scheduler/scheduler.toml
```

### Chunk & Dist

Chunk is the algorithm name for placement the cache node designed to balance the pressure and avoid cache crash avalanche of the whole cluster.

proof is there: [chunk proof](doc/chunk.text)

Dist is the part of chunk, make each cache host has less than half the whole cluster, be used to apply for redis/mesos singeton node.

# Contributing

Overlord is probably not perfect, but you are welcome to use it in your dev/test/product and so please let me know if anything feels wrong or incomplete.  
I'm trying very hard to keep it simple and fast. And I always happy to receive pull requests. It follow the gitflow, please send you pull request to `develop` branch.  
