# enri

## Install
```
go get github.com/bilibili/overlord/cmd/enri
```

## useage

### 创建集群

```shell
// 创建redis cluster并指定slave数为1 
enri create -n 127.0.0.1:7001 -n 127.0.0.1:7002 -n 127.0.0.1:7003 -n 127.0.0.1:7004 -n 127.0.0.1:7005 -n 127.0.0.1:7006 -s 1
```

### 添加节点

```shell
// 把 7007 节点添加进集群 7000
enri add -c 127.0.0.1:7000 -n 127.0.0.1:7007 

// 把 7007,7008 节点添加进集群 7000,并且7007为master 7008为slave
enri add -c 127.0.0.1:7000 -n 127.0.0.1:7007,127.0.0.1:7008
```

### 删除节点

```shell
// 从集群中删除7007 节点
enri del -c 127.0.0.1:7000 -n 127.0.0.1:7007
```

### 修复集群

```shell
// 修复集群信息
enri fix -n 127.0.0.1:7001	
```

### 重新分布slot

```shell
enri reshard -n 127.0.0.1:7001
```

### 迁移slot

```shell
// 从7001 迁移10个slot到7002
enri migrate -o 127.0.0.1:7001 -d 127.0.0.1:7002 -c 10
// 迁移7001全部slot到7002
enri migrate -o 127.0.0.1:7001 -d 127.0.0.1:7002 
// 把slot 10 从7001迁移到7002 
enri migrate -o 127.0.0.1:7001 -d 127.0.0.1:7002 -s 10
// 从集群其他节点迁移10个slot到7001
enri migrate -d 127.0.0.1:7001  -c 10
// 从7001迁移10个slot到集群其他节点
enri migrate -o 127.0.0.1:7001  -c 10
```

### 设置replicate
```shell
// 设置7006 为7007的从节点
enri replicate -m 127.0.0.1:7007 -s 127.0.0.1:7006
```

### 集群信息info
```shell
enri info -c 127.0.0.1:7001
```