#!/bin/sh

/data/etcd/etcdctl mkdir /overlord/clusters
/data/etcd/etcdctl mkdir /ovelord/instances
/data/etcd/etcdctl mkdir /overlord/heartbeat
/data/etcd/etcdctl mkdir /overlord/config
/data/etcd/etcdctl mkdir /overlord/jobs
/data/etcd/etcdctl mkdir /overlord/job_detail
/data/etcd/etcdctl mkdir /overlord/framework
/data/etcd/etcdctl mkdir /overlord/appids
/data/etcd/etcdctl mkdir /overlord/specs
/data/etcd/etcdctl set /overlord/fs "http://172.22.20.48:20080"
