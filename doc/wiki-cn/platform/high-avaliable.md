# 高可用设计

## framework 高可用
mesos framework 的高可用参考了[Designing Highly Available Mesos Frameworks](http://mesos.apache.org/documentation/latest/high-availability-framework-guide/#designing-highly-available-mesos-frameworks)
包括:
1. [mesos-master高可用](#mesos-master高可用)
2. [framework高可用](#framework高可用)
3. [mesos-agent高可用](#mesos-agent高可用)
4. [元数据存储高可用](#etcd高可用)

### mesos-master高可用
mesos原生支持[高可用模式部署](http://mesos.apache.org/documentation/latest/high-availability/)通过zk进行master的选举，保证单个master挂掉的情况下请他standby master升级为leader提供服务。通过启动参数 --zk=zk://host1:port1,host2:port2,.../path 连接zk启用高可用mesos集群。

### framework高可用
在framewrok运行的过程中，可能出现服务的故障，即使没有故障，也会出现服务的升级迭代等，那么如何在framework重新启动的时候获取已经运行的任务。  
framework初次启动的时候，mesos master会给framework分给一个framework id。通过将这个framework id保存在etcd中，并framework故障重启的时候只需要冲etcd获取这个framework id并重新注册到mesos即可重新获取到当前framwork正在运行的task信息。需要注意的是，在线上环境中，一定要确保把 fail_over 设置的足够大，fail_over 是mesos master允许的framwork最大故障时间，如果超过这个时间framework没有恢复，那么mesos将剔除这个framework并删除所有已运行的task并回收资源。只有当framework在这个时间内恢复的时候，才会根据framework id从mesos获取运行task信息，并进行后续的task管理。线上建议把fail_over设置为一周。 

### mesos-agent高可用
mesos-agent 发生故障的时候，mesos默认会结束这个agent下所有运行的task，对于缓存服务来说，这明显是不可接受的。因此要如何保证mesos agent重启的时候不影响运行的缓存实例呢。答案是**设置checkpoint**。  
通过在启动scheduler的时候设置checkpoint为true，当agent故障的时候，当前agent运行的task检测到checkpoint为true就不会立即自动退出。而是等待agent恢复进行重连。
### etcd高可用 
使用etcd存储集群元信息，使用集群的方式部署etcd，保证存储的高可用