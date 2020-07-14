# 缓存平台部署
本章，我们来介绍如何部署 overlord-platform 缓存平台。

## 路径约定

overlord-platform是一套完整的部署平台，它要求接管整个缓存机器。相应的，也就有了部署时候的路径要求。

1. overlord 的主要工作目录在 `/data` 下，因此，应该保证 overlord 对此目录有完全控制权限
2. 如果有可能， `/data` 应该挂载为单独的磁盘
3. 缓存程序的二进制文件应该提前放置在 `/data/lib/{cache_type}/{version}/bin/{bin_name}` 下。例子：`/data/lib/redis/4.0.11/bin/redis-server`
4. overlord应该有对临时目录有操作的权限。例如 `/tmp` 。
5. mesos-agent 也应该部署在 `/data` 下
6. 提前创建 /data/log/{apiserver,scheduler,executor,mesos}/ 目录，并将日志输出至此。

## 前期需求

1. etcd cluster
2. mesos master/mesos agent

## 安装文档

编译：

```bash
root: make build
root: fd . './cmd' -t x
cmd/apicli/apicli
cmd/apiserver/apiserver
cmd/executor/executor
cmd/proxy/proxy
cmd/scheduler/scheduler
```

### apiserver

apiserver 需要用户在配置文件里写明  etcd 的地址，以及 目前支持的版本如下:

```toml
listen = "0.0.0.0:8880"
etcd = "http://127.0.0.1:2379"
log_vl = 10
log = "info"
debug = true
stdout = true

[monitor]                       #overlord集成普罗米修斯与grafana的参数
  url = "http://127.0.0.1:1234"
  panel = "overlord"
  name_var = "cluster"
  org_id = 1

[[versions]]                    # 版本信息
  cache_type = "redis"
  versions = ["4.0.11", "3.2.8"]
  image = "redis"

[[versions]]
  cache_type = "redis_cluster"
  versions = ["4.0.11", "3.2.8", "5.0"]
  image = "redis"

[[versions]]
  cache_type = "memcache"
  versions = ["1.5.0"]

[cluster]                       #集群默认配置:overlord-proxy专用配置
dial_timeout = 1000
read_timeout = 1000
write_timeout = 1000
node_connections = 2
ping_fail_limit = 3
ping_auto_eject = true
```

```bash
# 启动
cd cmd/apiserver && ./apiserver
```

### scheduler

scheduler 即 mesos-framework 的调度器，因此除了上述配置之外，还需要写上mesos master的地址。

```toml
master = "172.22.33.167:5050"
```

```bash
# 启动
cd cmd/scheduler && ./scheduler
```

### executor

executor 需要放到一个 mesos 支持的存储服务器（http）里。我们通常使用 nginx 作为文件服务器。

### front-end

#### 编译打包
```bash
cd web
npm install
yarn run build
tar zcf dist.tar.gz dist
```

1. nodejs version 8.x。
2. 前端项目使用了路由的 history 模式，请在服务器中配置 [vue router history-mode](https://router.vuejs.org/zh/guide/essentials/history-mode.html)。
3. 打包好了之后上传到与 apiserver 同域名下的 nginx 即可。

## 错误处理

在运维过程中经常遇到各种各样的错误，下面列举几种常见的恢复方法：

1. 创建集群的时候因为资源不足而失败：重新创建，不需要任何改动。
2. 创建redis-cluster但是一直未能正确加载所有集群:重启 apiserver 重新发送balance任务。
3. 创建失败，导致有节点未启动：在 clusters 页面发送重启任务即可。
