#!/bin/bash
if [ ! -n "$1" ];then
echo "zookeeper myid can not be null,pass myid by arg.
eg:./install_zk.sh 2"
exit 1
else
myid=$1
fi
echo $myid
zk_ver=3.4.12
ip_array=("host1" "host2" "host3")
ip_num=${#ip_array[@]}
jdk=/usr/local/jdk8
zk_path=/usr/local/zookeeper-${zk_ver}
work_dir=/data/server/zookeeper
log_dir=/data/log/zookeeper
curl -L http://apache.stu.edu.tw/zookeeper/zookeeper-{zk_ver}/zookeeper-3.4.12.tar.gz -o zookeeper-${zk_ver}.tar.gz
tar zxf zookeeper-${zk_ver}.tar.gz
mkdir -p $zk_path
mkdir -p $log_dir
mkdir -p $work_dir
echo $myid>$work_dir/myid
mv zookeeper-${zk_ver}/* $zk_path
echo "tickTime=2000" > $zk_path/conf/zoo.cfg
echo "initLimit=10">> $zk_path/conf/zoo.cfg 
echo "syncLimit=5" >>$zk_path/conf/zoo.cfg
echo "dataDir=/data/server/zookeeper" >>$zk_path/conf/zoo.cfg
echo "clientPort=2181" >>$zk_path/conf/zoo.cfg
echo "autopurge.snapRetainCount=5" >>$zk_path/conf/zoo.cfg
echo "autopurge.purgeInterval=24" >>$zk_path/conf/zoo.cfg
for ((index=0;index<$ip_num;index++))
do
    tmp=$[$index+1]
    echo "server.$tmp=${ip_array[index]}:2888:3888" >>$zk_path/conf/zoo.cfg 
done

### start by supervisor
apt-get install -y supervisor
supervisor_path=/etc/supervisor/conf.d/zookeeper.conf
echo "[program:zookeeper]">$supervisor_path
echo "command=/usr/local/zookeeper-${zk_ver}/bin/zkServer.sh start-foreground">>$supervisor_path
echo "directory=/usr/local/zookeeper-${zk_ver}">>$supervisor_path
echo 'user=root
autostart=true
autorestart=true
stopsignal=KILL
startsecs=10
startretries=3
stdout_logfile = /data/log/zookeeper/stdout.log
stdout_logfile_backups = 3
stderr_logfile = /data/log/zookeeper/stderr.log
stderr_logfile_backups = 3
logfile_maxbytes=20MB'>>$supervisor_path
echo "environment=JAVA_HOME=\"${jdk}\",JRE_HOME=\"${jdk}/jre\"">>$supervisor_path
supervisorctl update