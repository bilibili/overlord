# overlord

overlord 是 [bilibili](www.bilibili.com) 提出的一套完整的缓存解决方案。旨在为初创型（startup）公司提供一套完整迅速的缓存解决方案。

它由一系列的组件组成，其中包括：

1. 一个轻量、快速、多功能的缓存代理组件：overlord-proxy。
2. 一个迅速、自愈、管理，负责部署缓存的平台：overlord-platform。
3. 一个简单的的命令行工具：enri。
4. 一个快速简单的redis数据导入导出工具。

## overlord-proxy 介绍

overlord-proxy 的灵感来源自著名的缓存代理组件 twemproxy 和 corvus，在一致性 hash 的基础上，overlord-prxoy 同时支持了 redis-cluster 模式。同时支持了 memcache text/memcache binary/redis cluster/ redis单节点。你可以自由的选择 memcache、redis 甚至是 redis cluster 客户端连接到对应的端口即可。

我们为所有的 overlord-proxy 用户提供了一套默认配置，你可以直接在 'cmd/proxy' 目录里找到他。这个代理组件是如此的轻量，甚至于你可以脱离整个overlord体系，单独使用proxy做自己的缓存代理。

同时，proxy 目前正在计划进行下一步计划，我们将在 overlord-proxy 1.0 的基础上加上一些更为高级的功能：暖机、多级缓存、多写、先序路由、动态重载等。我们将在保证缓存代理基本功能的情况下，尽快的支持一些高级的使用方式。

## overlord-platform

在缓存运维的工作中，我们总结了过去的运维的经验，开发出了一套部署缓存的平台。它着重解决两个核心目标：

1. 如何快速的利用所有的池化的机器资源。
2. 如何保证集群在节点故障的时候的稳定性。

为此，我们使用了 [chunk算法](https://github.com/eleme/ruskit/pull/46)/dist 两大算法作为保证 redis-cluster 故障稳定性的基础，同时配合着 mesos 强大的 scale 能力。这样一来，overlord-platform 要做的事情其实就已经极少了。

我们的 overlord-platform 平台，支持以单例模式部署 memcache/redis ,同时也支持以 redis-cluster 模式部署 redis。其中，由于单例模式本身是无状态的，所以部署起来极其的简单。

但是，现行的 redis-cluster 创建方案却有着非常显著的缺陷：慢。

通常，我们为了创建一个 redis-cluster，要在各个机器上渲染配置文件，以 cluster 模式配置启动，最后还要拿 redis-trib 来创建整个完整的集群。其中， redis-trib 将会依次的给各个节点发送 `CLUSTER MEET` 请求，并进行握手，握手成功之后通过 gossip 互相更新协议。然而节点一多，O(n^2) 的复杂度,让节点数量达到一定数量级的集群,更难吃创建出来，尤其在600个主节点情况下，最差甚至能达到30分钟以上。

为此，我们使用了一种，模拟集群恢复的方法进行创建集群。同样是创建600个主节点，使用新方法仅仅需要10s的时间，速度提升了百倍不止。

## enri

enri 是我们自己开发拓展的 redis cluster 管理工具，它将在命令行为运维人员提供一种简单快速的检查和管理集群的方式。它受 [ruskit](https://github.com/eleme/ruskit) 的启发，并且加入了许多其没有的功能，同时在性能上进行了改进。可以让治理集群的工作变得简单轻松。

TODO:剩下的得工具写完才能写

## 数据迁移工具

受 [redis-migrate-tool](https://github.com/vipshop/redis-migrate-tool) 的启发，我们开发了自己的迁移工具。与原工具相比，新的迁移工具采用Go语言编写，同时将支持更加服务化的场景，可以同时创建和传输多个集群的数据。同时可以集成 platform,也就意味着用户可以更加方便的将集群数据在平台之间迁移。
