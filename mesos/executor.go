package mesos

import (
	"bytes"
	"context"
	"net/url"
	"strconv"

	"time"

	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpexec"

	"github.com/mesos/mesos-go/api/v1/lib/httpcli"

	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/task/create"

	ms "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
	"github.com/mesos/mesos-go/api/v1/lib/executor"
	"github.com/mesos/mesos-go/api/v1/lib/executor/calls"
	"github.com/mesos/mesos-go/api/v1/lib/executor/config"
	"github.com/pborman/uuid"
)

// Executor define overlord mesos executor.
type Executor struct {
	cli            calls.Sender
	cfg            config.Config
	framework      ms.FrameworkInfo
	executor       ms.ExecutorInfo
	agent          ms.AgentInfo
	db             *etcd.Etcd
	subscriber     calls.SenderFunc
	unackedTasks   map[ms.TaskID]ms.TaskInfo
	unackedUpdates map[string]executor.Call_Update
	failedTasks    map[ms.TaskID]ms.TaskStatus // send updates for these as we can
	shouldQuit     bool
}

const (
	apiPath = "/api/v1/executor"
)

// New Executor instance.
func New() *Executor {
	cfg, err := config.FromEnv()
	if err != nil {
		panic(err)
	}
	var (
		apiURL = url.URL{
			Scheme: "http", // TODO(jdef) make this configurable
			Host:   cfg.AgentEndpoint,
			Path:   apiPath,
		}
		http = httpcli.New(
			httpcli.Endpoint(apiURL.String()),
			httpcli.Codec(codecs.ByMediaType[codecs.MediaTypeProtobuf]),
			httpcli.Do(httpcli.With(httpcli.Timeout(time.Second))),
		)
		callOptions = executor.CallOptions{
			calls.Framework(cfg.FrameworkID),
			calls.Executor(cfg.ExecutorID),
		}
		subscriber = calls.SenderWith(
			httpexec.NewSender(http.Send, httpcli.Close(true)),
			callOptions...,
		)
	)
	ec := &Executor{
		subscriber:     subscriber,
		unackedTasks:   make(map[ms.TaskID]ms.TaskInfo),
		unackedUpdates: make(map[string]executor.Call_Update),
		cli: calls.SenderWith(
			httpexec.NewSender(http.Send),
			callOptions...,
		),
	}
	return ec
}

// HandleEvent handle mesos executor event.
func (ec *Executor) handleEvent(e *executor.Event) {
	switch e.GetType() {
	case executor.Event_SUBSCRIBED:
		ec.subcribe(e)
	case executor.Event_LAUNCH:
		ec.lanch(e)
	case executor.Event_SHUTDOWN:

	}
}

func (ec *Executor) subcribe(e *executor.Event) {
	ec.framework = e.Subscribed.FrameworkInfo
	ec.executor = e.Subscribed.ExecutorInfo
	ec.agent = e.Subscribed.AgentInfo
}

func (ec *Executor) lanch(e *executor.Event) {
	data := e.GetLaunch().Task.GetData()
	idx := bytes.IndexByte(data, ':')
	if idx == -1 {
		log.Error("err data")
		return
	}
	port, err := strconv.ParseInt(string(data[idx:]), 10, 64)
	if err != nil {
		log.Error("err data")
		return
	}
	dpinfo, err := create.GenDeployInfo(ec.db, string(data[:idx]), int(port))
	if err != nil {
		return
	}
	err = create.SetupCacheService(dpinfo)
	if err != nil {
		log.Errorf("start cache service err %v", err)
		return
	}
	task := e.Launch.Task
	status := ec.newStatus(task.TaskID)
	status.State = ms.TASK_RUNNING.Enum()
	err = ec.update(status)
	if err != nil {
		log.Errorf("update lanch status fail %v ", err)
	}
}

// Subscribe start executor subcribe.
func (ec *Executor) Subscribe(c context.Context) {
	sub := calls.Subscribe(ec.unacknowledgedTasks(), ec.unacknowledgedUpdates())
	resp, err := ec.subscriber.Send(c, calls.NonStreaming(sub))
	if resp != nil {
		defer resp.Close()
	}
	if err == nil {
		ec.eventLoop(resp)
	}
	return
}

func (ec *Executor) eventLoop(resp encoding.Decoder) {
	var err error
	for err == nil && !ec.shouldQuit {
		var e executor.Event
		if err = resp.Decode(&e); err == nil {
			ec.handleEvent(&e)
		}
	}

}

func (ec *Executor) unacknowledgedTasks() (result []ms.TaskInfo) {
	if n := len(ec.unackedTasks); n > 0 {
		result = make([]ms.TaskInfo, 0, n)
		for k := range ec.unackedTasks {
			result = append(result, ec.unackedTasks[k])
		}
	}
	return
}

func (ec *Executor) unacknowledgedUpdates() (result []executor.Call_Update) {
	if n := len(ec.unackedUpdates); n > 0 {
		result = make([]executor.Call_Update, 0, n)
		for k := range ec.unackedUpdates {
			result = append(result, ec.unackedUpdates[k])
		}
	}
	return
}

func (ec *Executor) newStatus(id ms.TaskID) ms.TaskStatus {
	return ms.TaskStatus{
		TaskID:     id,
		Source:     ms.SOURCE_EXECUTOR.Enum(),
		ExecutorID: &ec.executor.ExecutorID,
		// TODO:rand id
		UUID: []byte(uuid.NewRandom()),
	}
}

func (ec *Executor) update(status ms.TaskStatus) error {
	upd := calls.Update(status)
	resp, err := ec.cli.Send(context.TODO(), calls.NonStreaming(upd))
	if resp != nil {
		resp.Close()
	}
	if err != nil {
		log.Infof("failed to send update: %+v", err)

	} else {
		ec.unackedUpdates[string(status.UUID)] = *upd.Update
	}
	return err
}
