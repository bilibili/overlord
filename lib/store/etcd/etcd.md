## etcd 存储设计

### 目录结构

overlord
├── clusters
│   ├── cluster1
│   │   └── instances
│   │       └── instance
│   │           └── info
│   └── cluster2
│       └── instances
│           └── instance
│               └── info
├── config
│   └── cluster1
└── task
	└── task1
	
#### 目录说明
* /overlord 为项目根目录
* /clusters 为集群目录
* /cluster$i 为具体的集群信息
* /instances 为集群节点目录
* /instance 为具体的节点名字
* /info 为节点的信息详情，按json格式存储

* /config 为配置目录
* /cluster$i 为集群的具体配置

* /task 为mesos的任务目录
* /task$i 为任务id，value是任务详情 
			
