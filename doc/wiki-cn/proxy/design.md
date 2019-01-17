# 设计

## 背景

B站内部一直使用[twemproxy](https://github.com/twitter/twemproxy)作为缓存代理中间件，但随着业务发展，集群规模越来越大，出现了很多不可避免的痛点：
1. 单进程单线程模型，和redis类似，在处理大key时会出现io瓶颈
2. 二次开发难度高成本大（我司有基于twemproxy二次开发的多进程版本[bilitw](https://github.com/anewhuahua/bilitw)
3. 不支持自动伸缩，不支持autorebalance，增删节点需要重启，难与缓存管理平台互动
4. 运维不友好，无控制面板，无其他细致监控手段

参考业界其他开源代理工具：
[codis](https://github.com/CodisLabs/codis) 只支持redis协议，且需要使用patch版本的redis
[mcrouter](https://github.com/facebook/mcrouter) 只支持memcache协议，C开发，会有tw相同问题

基于上述一些原因，以及团队都是Go技术栈，所以决定自研`overlord`作为公司统一的缓存代理中间件！

## 设计目标

* 支持memcache和redis协议，且能伪装redis-cluster
* 支持动态扩缩容，且自动迁移rebalance
* 固定数量的与缓存节点连接
* 多种一致性hash规则
* 统一端口提供监控信息（包括代理本身状态）
* 自动failover及recover
* 配置动态下发（与缓存管理平台集成）

#### 性能目标
前提：40c128g机器
* 延迟损耗小于：5ms
* 平均每核QPS：2W

## 架构图

![architecture](../../images/overlord_arch.png)

## 代码详解

#### 代码结构

目录`proxy`内：

```bash
proxy
├── proto                         # 协议相关目录
│   ├── memcache                  # memcache text协议具体实现目录
│   │   ├── binary                # memcache binary协议具体实现目录
│   │   │   ├── node_conn.go      # proxy到memcache binary节点的连接实现，负责写读数据
│   │   │   ├── pinger.go         # memcache节点的ping实现，forwarder会利用进行健康监测，如果ping失败会踢出故障节点
│   │   │   ├── proxy_conn.go     # memcache binary client到proxy的连接处理，负责解析及回写message和request。
│   │   │   ├── request.go        # memcache binary消息数据对应的`Request`接口实现，包含24byte的header及扩展data的[]byte结构
│   │   ├── node_conn.go          # proxy到memcache节点的连接实现，负责写读数据
│   │   ├── pinger.go             # memcache节点的ping实现，forwarder会利用进行健康监测，如果ping失败会踢出故障节点
│   │   ├── proxy_conn.go         # memcache client到proxy的连接处理，负责解析及回写message和request。
│   │   ├── request.go            # memcache消息数据对应的`Request`接口实现，包含key和data的[]byte结构
│   ├── redis                     # redis的相关处理
│   │   ├── cluster               # redis-cluster的相关逻辑
│   │   │   ├── cluster.go        # redis-cluster针对接口Forwarder的实现，使用cluster规定的crc16及slot逻辑对key进行hash
│   │   │   ├── fetch.go          # redis-cluster的slot一致性检查和更新逻辑
│   │   │   ├── node_conn.go      # proxy到redis-cluster的节点连接，相对非cluster模式，多了对ASK和MOVED处理
│   │   │   ├── proxy_conn.go     # redis-cluster的client到proxy的连接处理，相对非cluster模式，多了对cluster相关命令的处理
│   │   │   ├── slot.go           # redis-cluster cluster-nodes命令解析
│   │   ├── node_conn.go          # proxy到redis节点的连接实现，负责写读数据
│   │   ├── pinger.go             # redis节点的ping实现，forwarder会利用进行健康监测，如果ping失败会踢出故障节点
│   │   ├── proxy_conn.go         # redis client到proxy的连接处理，负责解析及回写message和request。需要注意一些批量命令的特殊处理逻辑。
│   │   ├── request.go            # redis消息数据对应的`Request`接口实现，包含resp
│   │   ├── resp.go               # redis RESP协议解析实现
│   ├── message.go                # message对象及分配逻辑
│   ├── pipe.go                   # 聚合message批量发送给缓存node节点的抽象工具类，核心思想是减少系统write调用，将多个命令聚合为一次writev
│   └── types.go                  # 抽象`mc&redis命令`为`message`对象，命令携带的数据为`Request`对象。抽象client到proxy的连接为`ProxyConn`，抽象proxy到缓存node的连接为`NodeConn`。抽象节点健康监测`Pinger`。
├── config.go                     # 代理相关配置类
├── forwarder.go                  # 是`proto/types.go`内Forwarder的默认实现，负责对key进行一致性hash后发送给缓存node节点的中间层
├── handler.go                    # client端的处理方法，负责分配message和处理message的生命周期，内部有waitgroup管理proxy->node中间的异步行为
├── listen.go                     # listen相关工具方法
├── proxy.go                      # 代理主入口，负责监听端口和创建client连接对应的handler，还可以进行最大连接数限制
```

#### 一些QA

Q：memcache的text和binary协议可以共存？  
A：memcache本身可以同一连接即可以text又可以binary，但overlord-proxy对齐进行了限制，保证客户端只能同时使用一种协议。

Q：redis和cluster目录为什么是分开的？  
A：overlord-proxy即兼容一致性哈希使用redis，也兼容redis-cluster，所以对齐进行了抽象和简化。cluster目录内的代码实现也是基于redis目录内基本协议处理的封装。
