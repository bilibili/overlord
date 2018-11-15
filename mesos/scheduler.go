package mesos

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/proto"
	"overlord/task"
	"overlord/task/create"

	pb "github.com/golang/protobuf/proto"
	ms "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/callrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/eventrules"
	mstore "github.com/mesos/mesos-go/api/v1/lib/extras/store"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/resources"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

// Scheduler mesos scheduler.
type Scheduler struct {
	c         *Config
	Tchan     chan string
	task      *list.List // use task list.
	db        *etcd.Etcd
	cli       calls.Caller
	rwlock    sync.RWMutex
	taskInfos map[string]*ms.TaskInfo // mesos tasks
	failTask  chan ms.TaskID
}

// NewScheduler new scheduler instance.
func NewScheduler(c *Config, db *etcd.Etcd) *Scheduler {
	return &Scheduler{
		db:        db,
		c:         c,
		task:      list.New(),
		taskInfos: make(map[string]*ms.TaskInfo),
		failTask:  make(chan ms.TaskID, 100),
	}
}

// Get get framework id from etcd if set.
func (s *Scheduler) Get() (fid string, err error) {
	fid, _ = s.db.Get(context.TODO(), etcd.FRAMEWORK)
	return
}

// Set framework id into etcd.
func (s *Scheduler) Set(fid string) (err error) {
	return s.db.Set(context.TODO(), etcd.FRAMEWORK, fid)
}

// Run scheduler and watch at task dir for task info.
// call run to start scheduler.
func (s *Scheduler) Run() (err error) {
	// watch task dir to get new task.
	ch, err := s.db.Watch(context.Background(), etcd.TaskDir)
	if err != nil {
		log.Errorf("start watch task fail err %v", err)
		return
	}
	s.Tchan = ch
	go s.taskEvent()
	s.cli = buildHTTPSched(s.c)
	s.cli = callrules.New(callrules.WithFrameworkID(mstore.GetIgnoreErrors(s))).Caller(s.cli)

	err = controller.Run(context.TODO(),
		s.buildFrameworkInfo(),
		s.cli,
		controller.WithEventHandler(s.buildEventHandle()),
		controller.WithFrameworkID(mstore.GetIgnoreErrors(s)),
	)
	return
}

func (s *Scheduler) taskEvent() {
	for {
		v := <-s.Tchan
		var t task.Task
		if err := json.Unmarshal([]byte(v), &t); err != nil {
			log.Errorf("err task info err %v", err)
			continue
		}
		s.task.PushBack(t)
		// revive offer when task comming.
		revice := calls.Revive()
		if err := calls.CallNoData(context.Background(), s.cli, revice); err != nil {
			log.Errorf("err call to revie %v", err)
			continue
		}
	}
}

func buildHTTPSched(cfg *Config) calls.Caller {
	cli := httpcli.New(
		httpcli.Endpoint(fmt.Sprintf("http://%s/api/v1/scheduler", cfg.Master)),
		httpcli.Codec(codecs.ByMediaType[codecs.MediaTypeProtobuf]),
		httpcli.Do(httpcli.With(httpcli.Timeout(time.Second))),
	)
	return httpsched.NewCaller(cli, httpsched.Listener(func(n httpsched.Notification) {
	}))
}

func (s *Scheduler) buildFrameworkInfo() *ms.FrameworkInfo {
	frameworkInfo := &ms.FrameworkInfo{
		User:       s.c.User,
		Name:       s.c.Name,
		ID:         &ms.FrameworkID{Value: mstore.GetIgnoreErrors(s)()},
		Checkpoint: &s.c.Checkpoint,
	}
	if s.c.FailOver > 0 {
		failOverTimeout := time.Duration(s.c.FailOver).Seconds()
		frameworkInfo.FailoverTimeout = &failOverTimeout
	}
	if s.c.Role != "" {
		frameworkInfo.Role = &s.c.Role
	}
	return frameworkInfo
}

func (s *Scheduler) buildEventHandle() events.Handler {
	logger := controller.LogEvents(nil)
	return eventrules.New(
		logAllEvents(),
	).Handle(events.Handlers{
		scheduler.Event_FAILURE: logger.HandleF(failure),
		scheduler.Event_OFFERS:  s.trackOffersReceived().HandleF(s.resourceOffers()),
		scheduler.Event_UPDATE:  controller.AckStatusUpdates(s.cli).AndThen().HandleF(s.statusUpdate()),
		scheduler.Event_SUBSCRIBED: eventrules.New(
			logger,
			controller.TrackSubscription(s, time.Second*30),
		),
	}.Otherwise(logger.HandleEvent))
}

// logAllEvents logs every observed event; this is somewhat expensive to do
func logAllEvents() eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, ch eventrules.Chain) (context.Context, *scheduler.Event, error) {
		log.Infof("%+v\n", *e)
		return ch(ctx, e, err)
	}
}

func (s *Scheduler) trackOffersReceived() eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
		if err == nil {
			log.Infof("get offer %v ", e.GetOffers().GetOffers())
		}
		return chain(ctx, e, err)
	}
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

func (s *Scheduler) resourceOffers() events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		var (
			offers = e.GetOffers().GetOffers()
			// callOption             = calls.RefuseSecondsWithJitter(rand.new, state.config.maxRefuseSeconds)
		)
		select {
		case taskid := <-s.failTask:
			s.tryRecovery(taskid, offers)
		default:
		}
		for taskEle := s.task.Front(); taskEle != nil; {
			t := taskEle.Value.(task.Task)
			imem := t.MaxMem
			icpu := t.CPU
			inum := t.Num
			switch t.CacheType {
			case proto.CacheTypeRedisCluster:
				chunks, err := chunk.Chunks(inum, imem, icpu, offers...)
				if err != nil {
					log.Errorf("task(%v) can not get offer by chunk, err %v", t, err)
					taskEle = taskEle.Next()
					continue
				}
				log.Infof("get chunks(%v) by offers (%v)", chunks, offers)
				var (
					ofm     = make(map[string]ms.Offer)
					tasks   = make(map[string][]ms.TaskInfo)
					offerid = make(map[ms.OfferID]struct{})
				)
				rtask := create.NewRedisClusterTask(s.db)
				err = rtask.Create(&create.RedisClusterInfo{
					Chunks:    chunks,
					MaxMemory: t.MaxMem,
					Name:      t.Name,
					TaskID:    t.ID,
					Version:   t.Version,
					MasterNum: t.Num,
				})
				if err != nil {
					log.Errorf("create cluster err %v", err)
					continue
				}
				for _, offer := range offers {
					ofm[offer.GetHostname()] = offer
				}
				for _, ck := range chunks {
					for _, node := range ck.Nodes {
						task := ms.TaskInfo{
							Name:     node.Name,
							TaskID:   ms.TaskID{Value: node.Addr() + "-" + strconv.FormatInt(int64(time.Now().Second()), 10)},
							AgentID:  ofm[node.Name].AgentID,
							Executor: s.buildExcutor(node.Addr(), []ms.Resource{}),
							//  plus the port obtained by adding 10000 to the data port for redis cluster.
							Resources: makeResources(icpu, imem, uint64(node.Port)),
						}
						data := &TaskData{
							IP:         node.Name,
							Port:       node.Port,
							DBEndPoint: s.c.DBEndPoint,
						}
						task.Data, _ = json.Marshal(data)
						s.rwlock.Lock()
						s.taskInfos[task.TaskID.Value] = &task
						s.rwlock.Unlock()
						offerid[ofm[node.Name].ID] = struct{}{}
						tasks[node.Name] = append(tasks[node.Name], task)
					}
				}
				for _, offer := range offers {
					accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(tasks[offer.Hostname]...)}.WithOffers(offer.ID))
					err = calls.CallNoData(ctx, s.cli, accept)
					if err != nil {
						log.Errorf("fail to lanch task err %v", err)
						taskEle = taskEle.Next()
						continue
					}
				}
				ttask := taskEle
				s.task.Remove(ttask)
				return nil
			case proto.CacheTypeRedis, proto.CacheTypeMemcache:
				dist, err := chunk.DistIt(t.Num, t.MaxMem, t.CPU, offers...)
				if err != nil {
					log.Errorf("task(%v) can not get offer by chunk, err %v", t, err)
					taskEle = taskEle.Next()
					continue
				}
				ci := &create.CacheInfo{
					TaskID:    t.ID,
					Name:      t.Name,
					CacheType: t.CacheType,
					Number:    t.Num,
					Thread:    int(t.CPU),
					Version:   t.Version,
					Dist:      dist,
				}

				ctask := create.NewCacheTask(s.db, ci)
				err = ctask.Create()
				if err != nil {
					log.Errorf("create cluster err %v", err)
					return nil
				}

				var (
					ofm     = make(map[string]ms.Offer)
					tasks   = make(map[string][]ms.TaskInfo)
					offerid = make(map[ms.OfferID]struct{})
				)

				for _, offer := range offers {
					ofm[offer.GetHostname()] = offer
				}
				for _, addr := range dist.Addrs {
					task := ms.TaskInfo{
						Name:     addr.IP,
						TaskID:   ms.TaskID{Value: addr.String() + "-" + strconv.FormatInt(int64(time.Now().Second()), 10)},
						AgentID:  ofm[addr.IP].AgentID,
						Executor: s.buildExcutor(addr.String(), []ms.Resource{}),
						//  plus the port obtained by adding 10000 to the data port for redis cluster.
						Resources: makeResources(t.CPU, t.MaxMem, uint64(addr.Port)),
					}
					data := &TaskData{
						IP:         addr.IP,
						Port:       addr.Port,
						DBEndPoint: s.c.DBEndPoint,
					}
					task.Data, _ = json.Marshal(data)
					offerid[ofm[addr.IP].ID] = struct{}{}
					tasks[addr.IP] = append(tasks[addr.IP], task)
				}

				for _, offer := range offers {
					accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(tasks[offer.Hostname]...)}.WithOffers(offer.ID))
					err = calls.CallNoData(ctx, s.cli, accept)
					if err != nil {
						log.Errorf("fail to lanch task err %v", err)
						taskEle = taskEle.Next()
						continue
					}
				}
				ttask := taskEle
				s.task.Remove(ttask)
				return nil
			default:
				ttask := taskEle
				taskEle = taskEle.Next()
				s.task.Remove(ttask)
				log.Errorf("undefine job type,delete undefine task %v", ttask)
			}
		}
		// if there don't have any task,decline all offers and suppress offer envent.
		// until task comming and call revive to schedule new workloads by taskevent.
		ofid := make([]ms.OfferID, len(offers))
		for _, offer := range offers {
			ofid = append(ofid, offer.ID)
		}
		decli := calls.Decline(ofid...)
		// TODO: deal with error
		_ = calls.CallNoData(ctx, s.cli, decli)
		suppress := calls.Suppress()
		// TODO: deal with error
		_ = calls.CallNoData(ctx, s.cli, suppress)
		return nil
	}
}

func (s *Scheduler) tryRecovery(t ms.TaskID, offers []ms.Offer) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	task, ok := s.taskInfos[t.Value]
	log.Infof("try recover task(%v)", task)
	if !ok {
		return
	}
	id := task.AgentID
	// TODO:check mem cpu info
	for _, offer := range offers {
		if offer.GetAgentID() == id {
			accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(*task)}.WithOffers(offer.ID))
			err := calls.CallNoData(context.Background(), s.cli, accept)
			if err != nil {
				log.Errorf("try recover task from pre agent fail %v", err)
			}
		}
	}
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

func (s *Scheduler) buildExcutor(name string, rs []ms.Resource) *ms.ExecutorInfo {
	executorUris := []ms.CommandInfo_URI{}
	executorUris = append(executorUris, ms.CommandInfo_URI{Value: s.c.ExecutorURL, Executable: pb.Bool(true)})
	exec := &ms.ExecutorInfo{
		Type:       ms.ExecutorInfo_CUSTOM,
		ExecutorID: ms.ExecutorID{Value: name},
		Name:       &name,
		Command: &ms.CommandInfo{
			Value: pb.String("./executor -debug -log-vl 5 -std"),
			URIs:  executorUris,
		},
		Resources: rs,
	}
	return exec
}

func (s *Scheduler) statusUpdate() events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		status := e.GetUpdate().GetStatus()
		msg := "Task " + status.TaskID.Value + " is in state " + status.GetState().String()
		if m := status.GetMessage(); m != "" {
			msg += " with message '" + m + "'"
		}
		log.Info(msg)
		switch st := status.GetState(); st {
		case ms.TASK_FINISHED:
			log.Info("state finish")
		case ms.TASK_LOST, ms.TASK_FAILED, ms.TASK_ERROR:
			taskid := e.GetUpdate().GetStatus().TaskID
			select {
			case s.failTask <- taskid:
				log.Infof("add fail task into failtask chan,task id %v", taskid)
			default:
				log.Error("failtask chan full")
			}
			// revive offer for retry fail task.
			revice := calls.Revive()
			if err := calls.CallNoData(context.Background(), s.cli, revice); err != nil {
				log.Errorf("err call to revie %v", err)
			}
		case ms.TASK_KILLED:
		case ms.TASK_RUNNING:
			taskid := status.TaskID.String()
			idx := strings.IndexByte(taskid, '-')
			addr := taskid[:idx]
			s.db.Set(ctx, etcd.InstanceDirPrefix+addr, task.StateRunning)
		}

		return nil
	}
}
