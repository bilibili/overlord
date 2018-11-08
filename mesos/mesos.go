package mesos

import "time"

// Config scheduler config.
type Config struct {
	User       string // Supply a username
	Name       string // Supply a frameworkname
	Checkpoint bool
	Master     string //MesosMaster's endpoint zk://mesos.master/2181 or 10.11.12.13:5050
	FailVoer   time.Duration
	Role       string

	DBType     string //Type of the database etcd/zk
	DBEndPoint string //Endpoint of the database

	ExecutorURL string
}

// TaskData encdoing to byte and send by task.
type TaskData struct {
	IP         string
	Port       int
	DBEndPoint string
}
