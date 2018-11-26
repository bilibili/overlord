## 部署说明

### 安装jdk 
mesos及zookeeper依赖java,必须使用jdk8以上  
[安装jdk8](../scripts/install_java.sh)

### 安装mesos 
建议版本 v1.7.0,建议部署三master以保证高可用  
[安装步骤](http://mesos.apache.org/documentation/latest/building/) 参考官方说明  
**mesos构建较慢，建议构建一次后打包大其他机器减少其他机器构建时间** 

### 安装etcd
用于存储部署信息及集群原信息，推荐版本v3.3.10

### 安装zookeeper
可选，如果mesos-master需要保证高可用，则为必选，推荐版本v3.4.12  
[安装zk集群](../scripts/install_zk.sh)
** 需要把ip_array替换为部署的机器列表地址**

### 安装ngnix 
可选，用于作为文件服务器，提供redis memcache mesos-executor的二进制下载