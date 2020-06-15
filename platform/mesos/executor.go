package mesos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	"overlord/pkg/container"
	"overlord/pkg/etcd"
	"overlord/pkg/log"
	"overlord/pkg/memcache"
	"overlord/pkg/myredis"
	"overlord/pkg/proc"
	"overlord/pkg/types"
	"overlord/platform/job/create"

	ms "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
	"github.com/mesos/mesos-go/api/v1/lib/executor"
	"github.com/mesos/mesos-go/api/v1/lib/executor/calls"
	"github.com/mesos/mesos-go/api/v1/lib/executor/config"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpexec"
	"github.com/pborman/uuid"
)

const (
	nodeTTL = time.Second * 5
	maxErr  = 10
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
	p              *proc.Proc
	c              *container.Container
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
		cfg:            cfg,
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
	log.Infof("executor get event %+v", *e)
	switch e.GetType() {
	case executor.Event_SUBSCRIBED:
		ec.subcribe(e)
	case executor.Event_LAUNCH:
		err := ec.launch(e)
		task := e.Launch.Task
		status := ec.newStatus(task.TaskID)
		if err != nil {
			status.State = ms.TASK_FAILED.Enum()
			ec.shouldQuit = true
		} else {
			status.State = ms.TASK_STARTING.Enum()
		}
		err = ec.update(status)
		if err != nil {
			log.Errorf("update lanch status fail %v ", err)
		}
	case executor.Event_KILL:
		ec.kill(e.Kill.TaskID)
		ec.shouldQuit = true
	case executor.Event_SHUTDOWN:
	case executor.Event_ACKNOWLEDGED:
		delete(ec.unackedTasks, e.Acknowledged.TaskID)
		delete(ec.unackedUpdates, string(e.Acknowledged.UUID))
	}
}

func (ec *Executor) subcribe(e *executor.Event) {
	ec.framework = e.Subscribed.FrameworkInfo
	ec.executor = e.Subscribed.ExecutorInfo
	ec.agent = e.Subscribed.AgentInfo
}

func (ec *Executor) launch(e *executor.Event) (err error) {
	task := e.GetLaunch().Task
	ec.unackedTasks[task.TaskID] = task
	data := task.GetData()
	tdata := new(TaskData)
	if err = json.Unmarshal(data, tdata); err != nil {
		log.Errorf("err task data %v", err)
		return
	}
	ec.db, err = etcd.New(tdata.DBEndPoint)
	if err != nil {
		log.Errorf("new db endpoint fail err %v", err)
		return
	}
	dpinfo, err := create.GenDeployInfo(ec.db, tdata.IP, tdata.Port)
	if err != nil {
		log.Errorf("get deploy info err %v", err)
		return
	}
	if dpinfo.Image != "" {
		ec.c, err = create.SetupCacheContainer(dpinfo)
	} else {
		ec.p, err = create.SetupCacheService(dpinfo)
	}
	if err != nil {
		log.Errorf("start cache service err %v", err)
		return
	}

	host := fmt.Sprintf("%s:%d", tdata.IP, tdata.Port)
	err = ec.db.Set(context.Background(), fmt.Sprintf("%s/%s", etcd.HeartBeatDir, host), task.TaskID.String())
	if err != nil {
		log.Errorf("set heartbeat key err %v", err)
		return
	}
	ec.monitor(e, dpinfo.CacheType, host)
	return
}

func (ec *Executor) monitor(e *executor.Event, tp types.CacheType, host string) {
	var cli Pinger
	switch tp {
	case types.CacheTypeRedis, types.CacheTypeRedisCluster:
		cli = myredis.NewConn(host)
	case types.CacheTypeMemcache:
		cli = memcache.New(host, time.Millisecond*100, time.Millisecond*100, time.Millisecond*100)
	}

	go func() {
		var errCount int
		var running = false
		for {
			// close monitor when continuous fail over maxErr
			if errCount > maxErr {
				cli.Close()
				ec.shouldQuit = true
				return
			}
			err := cli.Ping()
			// refresh ttl no sucess.
			if err == nil {
				if !running {
					status := ec.newStatus(e.Launch.Task.TaskID)
					status.State = ms.TASK_RUNNING.Enum()
					_ = ec.update(status)
					running = true
				}
				errCount = 0
				_ = ec.db.Refresh(context.Background(), host, nodeTTL)
			} else {
				errCount++
				log.Errorf("%v health check err %v", tp, err)
			}
			time.Sleep(time.Second)
		}
	}()
}

// Pinger service.
type Pinger interface {
	Ping() error
	Close() error
}

// Run start executor.
func (ec *Executor) Run(c context.Context) {
	defer func() {
		if ec.c != nil {
			ec.c.Stop()
		} else if ec.p != nil {
			ec.p.Stop()
		}
	}()
	var (
		shouldReconnect = maybeReconnect(ec.cfg)
		disconnected    = time.Now()
	)
	go ec.quitCheck()
	for {
		sub := calls.Subscribe(ec.unacknowledgedTasks(), ec.unacknowledgedUpdates())
		resp, err := ec.subscriber.Send(c, calls.NonStreaming(sub))
		if resp != nil {
			defer resp.Close()
		}
		if err == nil {
			ec.eventLoop(resp)
			disconnected = time.Now()
		}
		if ec.shouldQuit {
			log.Error("executor quit")
			return
		}
		if !ec.cfg.Checkpoint {
			log.Infof("gracefully exiting because framework checkpointing is NOT enabled")
			return
		}
		if time.Now().Sub(disconnected) > ec.cfg.RecoveryTimeout {
			log.Infof("failed to re-establish subscription with agent within %v, aborting", ec.cfg.RecoveryTimeout)
			return
		}
		<-shouldReconnect // wait for some amount of time before retrying subscription
	}
}

func (ec *Executor) quitCheck() {
	for {
		if ec.c != nil {
			log.Infof("executor exit with err %v", ec.c.Wait())
			os.Exit(0)
		} else if ec.p != nil {
			log.Infof("executor exit with err %v", ec.p.Wait())
			os.Exit(0)
		}

		time.Sleep(time.Second * 5)
	}
}

func maybeReconnect(cfg config.Config) <-chan struct{} {
	if cfg.Checkpoint {
		return backoff.Notifier(1*time.Second, cfg.SubscriptionBackoffMax*3/4, nil)
	}
	return nil
}

func (ec *Executor) eventLoop(resp encoding.Decoder) {
	var err error
	for err == nil && !ec.shouldQuit {
		ec.sendFailedTasks()
		var e executor.Event
		if err = resp.Decode(&e); err == nil {
			ec.handleEvent(&e)
		}
	}

}

func (ec *Executor) sendFailedTasks() {
	for taskID, status := range ec.failedTasks {
		updateErr := ec.update(status)
		if updateErr != nil {
			log.Errorf("failed to send status update for task %s: %+v", taskID.Value, updateErr)
		} else {
			delete(ec.failedTasks, taskID)
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
		Timestamp:  protoFloat64(float64(time.Now().Unix())),
		UUID:       []byte(uuid.NewRandom()),
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
		status.State = ms.TASK_FAILED.Enum()
		status.Message = protoString(err.Error())
		ec.failedTasks[status.TaskID] = status
	} else {
		ec.unackedUpdates[string(status.UUID)] = *upd.Update
	}
	return err
}

func protoString(s string) *string { return &s }

func protoFloat64(f float64) *float64 { return &f }

func (ec *Executor) kill(id ms.TaskID) {
	status := ec.newStatus(id)
	status.State = ms.TASK_KILLED.Enum()
	err := ec.update(status)
	if err != nil {
		log.Errorf("kill task err %v", err)
	}
}
