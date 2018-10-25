package mesos

type taskType uint8

const (
	taskTypeRedis taskType = iota
	taskTypeRedisCluster
	taskTypeMemcache
)

type Task struct {
	Name string
	Type taskType
	Num  int // num of instances
	I    *Instance
}

type Instance struct {
	Name   string
	Memory int     // capacity of memory of the instance in MB
	CPU    float32 // num of cpu cors if the instance
}
