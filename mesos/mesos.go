package mesos

import "time"

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
