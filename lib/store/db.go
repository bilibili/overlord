package store

import (
	"context"
)

// etcd dir.
const (
	BASEDIR    = "/overlord"
	CLUSTERDIR = "/overlord/clusters"
	CONFIGDIR  = "/overlord/config"
	TASKDIR    = "/overlord/task"
)

// DB store interface.
type DB interface {
	//Perform the initial setup of the database/KV store by creating DB/Namespace etc that are important for running MrRedis
	Setup(ctx context.Context, config string) error
	//Check if the database is setup already or not for Redis Framework
	IsSetup() bool
	//Optionally used if the db provides any auth mechanism perform that will handle DB apis like Connect/Login/Authorize etc.,
	Login() error
	//Set the value for the Key , if the key does not exist create one (Will be an Insert if we RDBMS is introduced)
	Set(ctx context.Context, k, v string) error
	// Get the vaule by key
	Get(ctx context.Context, k string) (v string, err error)
	Watch(ctx context.Context, k string) (ch chan string, err error)
}
