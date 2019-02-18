# Anzi

anzi 是源自 bilibili 的轻量级 Redis 数据同步工具。在过去，我们采用 vipshop 开源的 [redis-migrate-tool](https://github.com/vipshop/redis-migrate-tool) 工具进行迁移，然而在使用的时候，我们发现了这个工具的很多不足之处。首先，这个工具不再支持 RDB 7 (redis-3.x)以上的版本，也就意味着它不能再将 redis-4.x 及以上版本的 redis 当做数据源来导入，这是我们要替换掉它的最主要原因。另外就是，原版本工具使用C编写，在维护性上稍差；原版本工具对磁盘磁盘性能高，主要是需要将RDB导入到磁盘中再读出来。

anzi 采用 Go 语言编写，同时借助 [overlord/proxy](https://github.com/bilibili/overlord/blob/master/doc/wiki-cn/proxy.md) 启动一个代理将命令分发。

## 功能
anzi 支持的功能列表如下：

* redis 高版本支持(^redis 5.0, RDB v9)
* 多数据源支持: 多数据源中的 key 覆盖规则为随机覆盖
* 多后端协议支持：目前支持后端为 `redis`(twemproxy模式)和 `redis_cluster`(redis_cluster) 模式。
* hash method 支持列表: fnv1a64
* hash distribution 列表：ketama
* 后端多连接支持

将来可能会做的功能：

* 服务化：接收请求并开始同步
* 上游断线重连: 按照主从协议的要求进行断线重连

## 使用

```
cd cmd/anzi && go build && ./anzi -std
```
