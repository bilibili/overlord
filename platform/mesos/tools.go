package mesos

import (
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ms "github.com/mesos/mesos-go/api/v1/lib"
)

func splitJobID(key string) string {
	_, file := filepath.Split(key)
	return file
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

// taskid should be ip:port-cluster-id
// if id not equal zero mean task had fail before and been recover.
func parseTaskID(t ms.TaskID) (cluster, ip, port string, id int64, err error) {
	v := t.GetValue()
	tids := strings.Split(v, ",")
	ss := strings.Split(tids[0], "-")
	if len(ss) != 3 {
		err = errTaskID
		return
	}
	host := ss[0]
	cluster = ss[1]
	ids := ss[2]
	id, _ = strconv.ParseInt(ids, 10, 64)
	idx := strings.IndexByte(host, ':')
	ip = host[:idx]
	port = host[idx+1:]
	return
}
