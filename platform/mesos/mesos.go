package mesos

import (
	"context"
	"overlord/pkg/log"
	"strconv"

	ms "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/resources"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
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

func makeResources(cpu, mem float64, ports ...uint64) (r ms.Resources) {
	r.Add(
		resources.NewCPUs(cpu).Resource,
		resources.NewMemory(mem).Resource,
	)
	portRange := resources.BuildRanges()
	for _, port := range ports {
		portRange.Span(port, port)
	}
	r.Add(resources.Build().Name(resources.NamePorts).Ranges(portRange.Ranges).Resource)
	return
}

func checkOffer(offer ms.Offer, cpu, mem float64, port uint64) bool {
	for _, res := range offer.GetResources() {
		switch {
		case res.GetName() == "cpus":
			return res.GetScalar().Value > cpu
		case res.GetName() == "mem":
			return res.GetScalar().Value > mem
		case res.GetName() == "ports":
			for _, rg := range res.GetRanges().GetRange() {
				if port > rg.GetBegin() && port < rg.GetEnd() {
					return true
				}
			}
			return false
		}
	}
	return false
}

func failure(_ context.Context, e *scheduler.Event) error {
	var (
		f              = e.GetFailure()
		eid, aid, stat = f.ExecutorID, f.AgentID, f.Status
	)
	if eid != nil {
		// executor failed..
		msg := "executor '" + eid.Value + "' terminated"
		if aid != nil {
			msg += " on agent '" + aid.Value + "'"
		}
		if stat != nil {
			msg += " with status=" + strconv.Itoa(int(*stat))
		}
		log.Infof(msg)
	} else if aid != nil {
		// agent failed..
		log.Infof("agent '" + aid.Value + "' terminated")
	}
	return nil
}

func eventLog(l *scheduler.Event) {
	log.Infof("[Event] %v", l)
}
