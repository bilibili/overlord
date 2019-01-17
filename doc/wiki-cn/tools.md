# 工具列表

## TODO 集群管理工具 - enri

鉴于[ruskit](https://github.com/eleme/ruskit) 已经不再维护，我们决定重写这个管理工具。并添加一些诸如监控、报告、分析等更加自动化的功能。

## TODO redis数据导入导出工具 - Anzi

anzi 是一款使用 Go 编写的、快速的、同时支持多种目标的 redis-cluster 数据同步工具。原先，我们使用 [redis-migrate-tools](https://github.com/vipshop/redis-migrate-tool)。但是，这个工具本身不能快速简单的做服务化，也没有进度报告之类的可以反馈的，并且同一时间只支持同步一个集群的数据。更重要的是，它已经好久不维护了，甚至不支持最新的 redis rdb v9 协议。因此，我们决定使用 Go 去重写这个迁移工具。

### 功能要点

1. 必须同时支持多个集群迁移
2. 支持缓存事件(开始、RDB解析、REPL解析开始、追平数据、实时增量进度)订阅，如有可能，给出同步进度UI
3. 支持 twemproxy/single/redis cluster 作为 target
4. 支持 ketama hash consistent 算法
5. hash 算法支持 twemproxy 多种算法

### 解析流程

```
[redis server] -> psync -> full sync -> ANZI -> as redis command -> consistent hash -> [redis server]
                        -> repl sync ->
```
