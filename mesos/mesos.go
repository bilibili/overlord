package mesos

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
	Num  int // num of instances
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
	User        string // Supply a username
	Name        string // Supply a frameworkname
	Master      string //MesosMaster's endpoint zk://mesos.master/2181 or 10.11.12.13:5050
	DBType      string //Type of the database etcd/zk
	DBEndPoint  string //Endpoint of the database
	URL         string // mesos master scheduler url.
	ExecutorURL string
}
