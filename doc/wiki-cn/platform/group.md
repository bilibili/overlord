# 资源分组
资源分组通过mesos的[roles](http://mesos.apache.org/documentation/latest/roles/)实现,mesos-agent启动时，通过设置roles来指定资源所属分组。framework同样也是通过配置role来获取对应role提供的offers。

一般情况下，推荐所有的机器加入公用的资源池，无需对资源进行分组，可以高效利用机器资源，减少机器资源碎片以及合理部署不同类型资源提搞机器利用率。

如果必须对资源进行分组，则根据上述所述
1. 设置agent资源role
2. 设置framework role

mesos会自动帮你你实现资源分组。具体逻辑参见[mesos](http://mesos.apache.org/documentation/latest/roles/),这里不再赘述。