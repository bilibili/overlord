# overlord proxy 使用指南

## 编译安装

overlord 大部分组件都是采用 Go 语言编写，因此你需要一个 go 版本 > 1.11 的编译器，在项目根目录:

```
make build

cmd/proxy/proxy -cluster cmd/proxy/proxy-cluster-example.conf -std -log-vl 5
```

安装：仅仅需要拷贝一个二进制文件即可。

## 配置指南

```toml

[[clusters]]
# 每一个集群都应该拥有自己的姓名
name = "test-mc"

# overlord 现在仅仅支持默认的 fnv1a_64 的 hash 算法。写了其他的也没用。
hash_method = "fnv1a_64"

# overlord 在代理模式下仅仅支持 twemproxy 实现的 ketama hash 模式。
hash_distribution = "ketama"

# hash tag 应该是两个字符。如果key中出现这两个字符，那么 overlord 仅仅会使用这两个字符之间的子串来进行 hash 计算。也就是说，
# 当 hash tag 为 "{}" 的时候:  "test{123}name" 与 "{123}age" 将一定会出现在同一个缓存节点上。
hash_tag = ""

# 目前 overlord proxy 支持四种协议：
# 代理模式：memcache | memcache_binary | redis
# redis cluster模式：redis_cluster
cache_type = "memcache"

# overlord支持你改变协议族，但是强烈不建议更改协议族，这里保持默认即可。
listen_proto = "tcp"

# 与协议族相对应的，这里是监听地址。
# 如果协议族是 unix ，则此地址应该为某个 sock 文件地址
# 如果(通常)协议族是 tcp，则此地址应该为 "0.0.0.0:端口号"
listen_addr = "0.0.0.0:21211"


# 暂不支持的选项，后期可能会考虑支持。
redis_auth = ""

# 建立连接超时,毫秒，一般应该大于客户端超时
dial_timeout = 1000
# 读超时,毫秒，一般应该大于客户端超时。
read_timeout = 1000

# 写超时，毫秒，一般应该大于客户端超时。
write_timeout = 1000

# 与每个缓存后端的连接数。
# 由于 overlord 是预先建立连接的，因此，连接数也就意味着 overlord 与后端保持的长连接的数量。
# 经过我们的一轮一轮压测，我们强烈建议将overlord到后端的连接设置为2。在这个时候，overlord可以发挥出极限性能。
# 但同时，因为有多个连接，那么来自同一个客户端的请求可能会被打乱顺序执行。
node_connections = 2

# 自动剔除节点次数。overlord-proxy 会每隔 300ms 对所有后端节点发送测试的 ping 指令。
# 一旦 ping 指令失败（任何失败都算），则计数累加1，直到达到次上限，则提出对应的后端节点。
ping_fail_limit = 3

# 是否启用自动剔除、加回节点。
ping_auto_eject = true

# 服务器端所有配置
# 代理模式下,每一项的格式应该为:
#   "{ip}:{port}:{weight} {alias}"
# 介绍一下上面的字段:
#  ip:port: 缓存节点的地址
#  weight: 缓存节点在 ketama 一致性 hash 里的权重，理论上，权重越大的节点，能承载越多的流量。
#  alias: 缓存节点的别名，有别名的时候，overlord 将以别名计算本节点在 hash 环上的位置，一般情况下我们保证 alias 不变的情况下，将新节点加入集群的时候替换掉前面的 ip:port 即可。
#
# 集群模式下：
#  "{ip}:{port}"
# 应当配置redis-cluster 一系列种子节点，为了避免缓存服务器宕机影响，配置的种子节点应该都不在一台机器上。
# 注意：虽然不需要配齐全部的 redis-cluster 节点，但是为了避免大规模失效，我们还是建议配置十个以上的节点在这里
servers = [
    "127.0.0.1:11211:1 mc1",
]
```

## 最佳实践

