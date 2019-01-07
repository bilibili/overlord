package mesos

import (
	"overlord/lib/log"
	"time"
)

// Config scheduler config.
type Config struct {
	*log.Config
	User       string   `toml:"user"` // Supply a username
	Name       string   `toml:"name"` // Supply a frameworkname
	Checkpoint bool     `toml:"checkpoint"`
	Master     string   `toml:"master"` //MesosMaster's endpoint zk://mesos.master/2181 or 10.11.12.13:5050
	FailOver   Duration `toml:"fail_voer"`
	Roles      []string `toml:"role"`
	Hostname   string   `toml:"hostname"`
	Principal  string   `toml:"principal"`
	DBType     string   `toml:"db_type"`      //Type of the database etcd/zk
	DBEndPoint string   `toml:"db_end_point"` //Endpoint of the database

	ExecutorURL string `toml:"executor_url"`
}

// TaskData encdoing to byte and send by task.
type TaskData struct {
	IP         string
	Port       int
	DBEndPoint string
}

// Duration parse toml time duration
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	tmp, err := time.ParseDuration(string(text))
	if err == nil {
		*d = Duration(tmp)
	}
	return err
}
