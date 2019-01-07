#!/bin/bash
#mesos agent 使用supvisor启动配置脚本
apt-get install -y supervisor
# 写入mesos启动配置
zk_addr=127.0.0.1:2181
echo -e "[program:mesos]">/etc/supervisor/conf.d/mesos-agent.conf
echo -e "command=/data/mesos-1.7.0/build/bin/mesos-agent.sh --master=zk://${zk_addr}/mesos --work_dir=/data/lib/mesos --ip=\c">>/etc/supervisor/conf.d/mesos-agent.conf
ifconfig |grep inet|grep -v '127.0.0.1'|awk '{print $2}' |awk -F ':' '{print $2}'>>/etc/supervisor/conf.d/mesos-agent.conf
echo 'directoy=/data/mesos-1.7.0/build
user=root
autostart=true
autorestart=true
stopsignal=KILL
startsecs=10
startretries=3
stdout_logfile = /data/log/mesos/stdout.log
stdout_logfile_backups = 3
stderr_logfile = /data/log/mesos/stderr.log
stderr_logfile_backups = 3
logfile_maxbytes=20MB'>>/etc/supervisor/conf.d/mesos-agent.conf

mkdir -p /data/log/mesos
supervisorctl update
