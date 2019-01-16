# 节点恢复策略
framework强烈建议开启checkpoint，节点恢复策略只基于开启checkpoint的情况进行讨论。节点发生故障通常分为两种情况:
1. 服务本身实例故障  
服务实例启动的后，executor会启动一个单独线程对服务进行health check，当health check连续多次出现失败的时候，executor会认为当前服务不可用，并向shceduler发送task fail消息。scheduler收到task fail消息后及进入failover流程。
2. 服务实例所在机器故障或网络故障导致机器失联。    
如果是机器故障（如机器宕机）导致机器上所有的服务节点都退出，framewrok会收到agent fail事件，进入failover流程。如果只是mesos-ageng故障，由于开启了checkpoint，只要及时回复对服务无影响
如果是网络分区导致的失联，mesos-master会在agent_ping_timeout时间后把agent以及agent所在机器上的所有task设置为lost，并进入fail_over

对于故障的恢复，采取了以下恢复策略:
1. [原地重启](#原地重启)
2. [寻找新机器恢复](#寻找新机器恢复)
3. [原有集群机器里恢复](#原有集群机器里恢复)

三种恢复策略的优先级为1>2>3,每一种恢复策略都会累计故障重试次数，当故障重试次数超过6次后，则会认为该故障无法自动failover，scheduler会停止自动重试，需要由人工介入
## 原地重启
scheduler收到task失败的消息后，首先会尝试从task原先所在机器恢复task，并将task id加1，用于表示task历史失败的次数。如果从原机器恢复task成功并成功重启服务节点，则failover流程结束。
## 寻找新机器恢复
如果无法从原机器恢复节点，则scheduler会尝试寻找当前节点所在集群没有使用过的机器，并将故障节点部署在该机器上。如果从mesos获取到的offer所在机器都已经部署有该集群的节点，那么则进入下一步。
## 原有集群机器里恢复
当无法找到该集群未部署任何节点的机器的时候，scheduler会尝试从现在集群所在机器里寻找最合适的机器部署新节点，新机器的寻找必须依然遵循[chunk/dist算法](./chunk.md).

