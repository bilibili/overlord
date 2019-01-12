## 简介
overlord 是一套完整的缓存服务解决方案，致力于提供自动化，高可用的缓存服务解决方案。overlord主题包括三个部分:  
* [proxy](#proxy) 缓存代理模块，轻量高可用的代理模块，支持memcache，redis 协议，通过代理实现缓存分布式，高可用，动态扩展。  
* [framework](#framework) 缓存调度部署平台，基于mesos实现的framework调度框架，支持cluster以及singleton的自动化部署。
* [apiserver](#apiserver) 缓存管理运维后台，通过dashboard可视化方便日常的运维管理

## proxy
轻量高可用的代理模块，支持memcache 及redis协议，支持从twemproxy到overlord的无缝迁移。支持redis-cluster，屏蔽了cluster的实现细节，允许客户端像使用单例的redis一样使用redis-cluster，同时通过伪装自身为cluster，通过mock cluster nodes,cluster slots 等命令，也支持各种语言redis-cluster sdk的直接接入。

## framework 
framework是基于mesos实现的二次调度框架,包括[scheduler](#scheduler)和[executor](#executor)两个模块，基于对mesos资源的二次调度，实现了缓存资源的自动调度部署以及故障检测恢复。

### scheduler
framework的核心调度模块，基于[chunk](./doc/chunk.txt)算法实现redis-cluster节点的调度分配，保证了redis-cluster的节点分布满足：  
1. 主从不在同一个物理节点  
2. 任意一个物理节点分配的节点数少于总数的一半  
3. 尽可能部署在资源最充足的物理节点  
对于memcache和redis singleton的节点分布，则只需要满足2 3两个条件。

### executor
缓存节点启动部署模块，由mesos-agent收到调度任务后拉起，executor会通过下发的taskid从etcd获取详细的节点部署信息，包括缓存类型（mc|redis...）启动配置等。通过fork command的方式启动缓存节点。节点启动完成后，更新task状态为running，当集群的所有状态全部更新为running时，则表明当前集群创建完成。节点启动完成后，executor会同时启动一个routine进行节点的health check，保证节点的可用性，同时更新节点任务状态。当节点启动失败或发生故障时，会通过事件通知scheduler进行故障恢复处理

## apiserver
缓存资源运维管理后台，通过后台dashboard对缓存集群进行快捷的创建集群，增删节点，以及绑定业务appid等操作。