package mesos

import "time"

type taskType uint8

const (
	taskTypeRedis taskType = iota
	taskTypeRedisCluster
	taskTypeMemcache
)

// Task detail
type Task struct {
	Name string
	Type taskType
	Num  int // num of instances ,if redis-cluster,mean master number
	I    *Instance
}

// Instance  detail.
type Instance struct {
	Name   string
	Memory float64 // capacity of memory of the instance in MB
	CPU    float64 // num of cpu cors if the instance
}

// Config scheduler config.
type Config struct {
	User     string // Supply a username
	Name     string // Supply a frameworkname
	Master   string //MesosMaster's endpoint zk://mesos.master/2181 or 10.11.12.13:5050
	FailVoer time.Duration
	Role     string

	DBType     string //Type of the database etcd/zk
	DBEndPoint string //Endpoint of the database

	ExecutorURL string
}
