
## etcd 存储设计

### 目录结构

```
overlord
├── clusters
│   ├── cluster1
│   │   └── instances
│   │       └── instance($ip:$port)
│   │           ├── info
│   │           ├── nodes.conf (redis-cluster)
│   │           └── server.conf
│   └── cluster2
│       └── instances
│           └── instance
│               └── info
├── config
│   └── cluster1
└── task
    └── task1
```

#### 目录说明

* /overlord 为项目根目录
* /clusters 为集群目录
* /cluster$i 为具体的集群信息
* /instances 为集群节点目录
* /instance 为具体的节点名字 ($ip:$port)
* /info 为节点的信息详情，按json格式存储
* /server.conf 为服务启动配置项，value为服务的配置内容
* /nodes.conf 为redis-cluster的nodes信息，用于mock模式启动redis-cluster

* /config 为配置目录
* /cluster$i 为集群的具体配置

* /task 为mesos的任务目录
* /task$i 为任务id，value是任务详情 
			
## sheduler 设计

#### scheduler 主体流程

1. 连接到etcd/zk 获取 `/overlord/task` 目录下未完成的task并监听目录实时获取api-server下发的任务信息并保存到本地任务队列
2. 注册到mesos master 并订阅mesos event。
3. 监听event_offer事件，并判断当前任务队列是否有未完成任务
4. oferr resources 是否符合task任务资源描述需求 （判断依据 mem cpu balance）
5. 接受offer，并下发executor任务给agent
6. 监听agent回复的event_update事件
7. event为running时，则表示任务启动成功并且正常运行
8. event为failed时，scheduler 需要重新调度重新分配task
9. 当running数为task需要部署的instance数时，表示所有任务部署成功，scheduler更新task状态为完成并写入将状态写入ectd

## Executor 约定

### 注意

无论是服务，还是缓存配置还是目录，始终用 memcache 指代 memcache 服务。仅在表示 memcached 这个 binary 本身的时候使用 memcached

### 目录约定
一些约定：

1. redis server binary 的存放路径为 `/data/lib/redis/4.0.8/bin/redis-server`
2. memcache 的存放路径为 `/data/lib/memcache/1.5.10/bin/memcached`
3. 多版本共存请修改 4.0.8(1.5.10) 到对应版本
4. redis 的 working dir 位置 /data/redis[_cluster]/${port}
5. redis 节点的配置文件约定为 /data/redis/${port}/redis.conf
6. redis_cluster 节点的nodes.conf文件约定为 /data/redis_cluster/${port}/nodes.conf
7. redis 的 pid 文件为 redis.pid 放在 working dir 目录下。
8. memcache 类似。
9. 所有缓存服务都需要在 working dir 下维持一个 meta.toml 文件，用来存储类似 "cluster name"等节点信息。严格禁止并发修改这个文件。
10. mesos 的 executor 的 tmp dir 目录为 /data/var/overlord/tmp/{task_id}/

所有文件（目录）的权限均为 `0755`，用户恒为 `root.root`


一个典型的 redis 目录为：

```
7010
├── console.log # 启动日志
├── meta.tmol   # 元信息，里面保存了启动关闭命令
├── nodes.conf  # 仅存在于 redis cluster 的 nodes.conf 文件
├── redis.conf  # redis 配置文件
└── redis.pid   # redis 的 pid 
```
