# Overlord [![Build Status](https://travis-ci.org/felixhao/overlord.svg?branch=master)](https://travis-ci.org/felixhao/overlord) [![GoDoc](http://godoc.org/github.com/felixhao/overlord/proxy?status.svg)](http://godoc.org/github.com/felixhao/overlord/proxy)


Overlord is a proxy based high performance Memcached and Redis solution written in Go.  
It is fast and lightweight and used primarily to horizontally scale your distributed caching architecture.  
It is inspired by Twemproxy,Codis.  

## Quick Start

#### Download

```go
go get github.com/felixhao/overlord
```

#### Build

```shell
cd $GOPATH/github.com/felixhao/overlord/cmd/proxy
go build
```

#### Run

```shell
./proxy -cluster=proxy-cluster-example.toml
```
###### Please first run a memcache server, which bind 11211 port.

#### Test

```shell
echo -e "set a_11 0 0 5\r\nhello\r\n" | nc 127.0.0.1 21211
# STORED
echo -e "get a_11\r\n" | nc 127.0.0.1 21211
# VALUE a_11 0 5
# hello
# END
```

Congratulations! You've just run the overlord proxy.

## Architecture

![arch](doc/images/overlord_arch.png)

## Features

- [x] support memcache protocol
- [ ] support redis protocol
- [x] connection pool for reduce number to backend caching servers
- [x] keepalive & failover
- [x] hash tag: specify the part of the key used for hashing
- [x] promethues stat metrics support
- [ ] cache backup
- [ ] hot reload: add/remove cluster/node...
- [ ] QoS: limit/breaker...
- [ ] L1&L2 cache
- [ ] hot|cold cache???
- [ ] broadcast???
- [ ] doube hashing???

# Contributing

Overlord is probably not perfect, but you are welcome to use it in your dev/test/product and so please let me know if anything feels wrong or incomplete.  
I'm trying very hard to keep it simple and fast. And I always happy to receive pull requests. It follow the gitflow, please send you pull request to `develop` branch.  
