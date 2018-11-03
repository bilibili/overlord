package mesos

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"

	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/task/create"

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
	c     *Config
	Tchan chan string
	task  *list.List
	db    *etcd.Etcd
	cli   calls.Caller
}

// NewScheduler new scheduler instance.
func NewScheduler(c *Config, db *etcd.Etcd) *Scheduler {
	return &Scheduler{
		db:   db,
		c:    c,
		task: list.New(),
	}
}

// Get get framework id from etcd if set.
func (s *Scheduler) Get() (fid string, err error) {
	return s.db.Get(context.TODO(), etcd.FRAMEWORK)
}

// Set framework id into etcd.
func (s *Scheduler) Set(fid string) (err error) {
	return s.db.Set(context.TODO(), etcd.FRAMEWORK, fid)
}

// Run scheduler and watch at task dir for task info.
func (s *Scheduler) Run() (err error) {
	// watch task dir to get new task.
	ch, err := s.db.Watch(context.Background(), etcd.TASKDIR)
	if err != nil {
		log.Errorf("start watch task fail err %v", err)
		return
	}
	s.Tchan = ch
	go s.taskEvent()
	s.cli = buildHTTPSched(s.c)
	s.cli = callrules.New(callrules.WithFrameworkID(mstore.GetIgnoreErrors(s))).Caller(s.cli)

	controller.Run(context.TODO(),
		buildFrameworkInfo(s.c),
		s.cli,
		controller.WithEventHandler(s.buildEventHandle()),
		controller.WithFrameworkID(mstore.GetIgnoreErrors(s)),
	)
	return
}

func (s *Scheduler) taskEvent() {
	for {
		v := <-s.Tchan
		var t Task
		if err := json.Unmarshal([]byte(v), &t); err != nil {
			log.Errorf("err task info err %v", err)
			continue
		}
		for i := 0; i < t.Num; i++ {
			s.task.PushBack(t)
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

func buildFrameworkInfo(cfg *Config) *ms.FrameworkInfo {
	frameworkInfo := &ms.FrameworkInfo{
		User: cfg.User,
		Name: cfg.Name,
	}
	if cfg.FailVoer > 0 {
		failOverTimeout := cfg.FailVoer.Seconds()
		frameworkInfo.FailoverTimeout = &failOverTimeout
	}
	if cfg.Role != "" {
		frameworkInfo.Role = &cfg.Role
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
			controller.TrackSubscription(s, time.Second),
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
			log.Infof("offer %v ", e.GetOffers().GetOffers())
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
		for taskEle := s.task.Front(); taskEle != nil; {
			task := taskEle.Value.(Task)
			imem := task.I.Memory
			icpu := task.I.CPU
			inum := task.Num
			if task.Type == taskTypeRedisCluster {
				chunks, err := chunk.Chunks(inum, imem, icpu, offers...)
				if err != nil {
					taskEle = taskEle.Next()
					continue
				}
				var (
					ofm     = make(map[string]ms.Offer)
					tasks   []ms.TaskInfo
					offerid = make(map[ms.OfferID]struct{})
				)
				rtask := create.NewRedisClusterTask(s.db)
				// TODO:set other redis cluster info
				rtask.ServerCreateCluster(&create.RedisClusterInfo{
					Chunks: chunks,
				})
				for _, offer := range offers {
					ofm[offer.GetHostname()] = offer
				}
				for _, ck := range chunks {
					for _, node := range ck.Nodes {
						task := ms.TaskInfo{
							Name:      node.Name,
							TaskID:    ms.TaskID{Value: node.RunID},
							AgentID:   ofm[node.Name].AgentID,
							Executor:  s.buildExcutor(node.Name, []ms.Resource{}),
							Resources: makeResources(icpu, imem, uint64(node.Port)),
							Data:      []byte(fmt.Sprintf("%s:%d", node.Name, node.Port)),
						}
						offerid[ofm[node.Name].ID] = struct{}{}
						tasks = append(tasks, task)
					}
				}
				offerids := make([]ms.OfferID, len(offerid))
				for id := range offerid {
					offerids = append(offerids, id)
				}
				accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(tasks...)}.WithOffers(offerids...))
				err = calls.CallNoData(ctx, s.cli, accept)
				if err != nil {
					log.Errorf("fail to lanch task err %v", err)
					taskEle = taskEle.Next()
					continue
				}
				ttask := taskEle
				taskEle = taskEle.Next()
				s.task.Remove(ttask)
				return nil
			}
		}
		return nil
	}
}

// func (s *Scheduler) resourceOffers() events.HandlerFunc {
// 	return func(ctx context.Context, e *scheduler.Event) error {
// 		var (
// 			offers = e.GetOffers().GetOffers()
// 			// callOption             = calls.RefuseSecondsWithJitter(rand.new, state.config.maxRefuseSeconds)
// 		)
// 		for i, offer := range offers {
// 			memRes := filterResource(offer.Resources, "mem")
// 			mems := 0.0
// 			for _, mem := range memRes {
// 				mems += mem.GetScalar().GetValue()
// 			}
// 			log.Infof("Recived offer with MEM=%v", mems)
// 			var tasks []ms.TaskInfo
// 			for taskEle := s.task.Front(); taskEle != nil; {
// 				task := taskEle.Value.(Task)
// 				tkMem := float64(task.I.Memory)
// 				if tkMem <= mems {
// 					tskID := ms.TaskID{Value: strconv.FormatInt(rand.Int63(), 10)}
// 					memsosTsk := ms.TaskInfo{
// 						Name:     task.Name,
// 						TaskID:   tskID,
// 						AgentID:  offer.AgentID,
// 						Executor: s.buildExcutor(task.Name, []ms.Resource{}),
// 						// TODO:find suitable resource.
// 						Resources: buildWantsExecutorResources(task.I.CPU, task.I.Memory),
// 					}
// 					cTask := taskEle
// 					taskEle = taskEle.Next()
// 					s.task.Remove(cTask)
// 					tasks = append(tasks, memsosTsk)
// 				} else {
// 					taskEle = taskEle.Next()
// 				}
// 			}
// 			if len(tasks) > 0 {
// 				accept := calls.Accept(
// 					calls.OfferOperations{calls.OpLaunch(tasks...)}.WithOffers(offers[i].ID),
// 				) //.With(callOption)
// 				err := calls.CallNoData(ctx, s.cli, accept)
// 				if err != nil {
// 					log.Errorf("failed to lanch task :%v", err)
// 				}
// 			} else {
// 				decli := calls.Decline(offers[i].ID)
// 				calls.CallNoData(ctx, s.cli, decli)
// 			}
// 		}
// 		return nil
// 	}
// }

func filterResource(rs []ms.Resource, filter string) (fs []ms.Resource) {
	for _, r := range rs {
		if r.GetName() == filter {
			fs = append(fs, r)
		}
	}
	return
}

func makeResources(cpu, mem float64, port uint64) (r ms.Resources) {
	r.Add(
		resources.NewCPUs(cpu).Resource,
		resources.NewMemory(mem).Resource,
		resources.Build().Ranges(ms.NewRanges(port)).Resource,
	)
	return
}

func (s *Scheduler) buildExcutor(name string, rs []ms.Resource) *ms.ExecutorInfo {
	executorUris := []ms.CommandInfo_URI{}
	executorUris = append(executorUris, ms.CommandInfo_URI{Value: s.c.ExecutorURL, Executable: proto.Bool(true)})
	exec := &ms.ExecutorInfo{
		Type:       ms.ExecutorInfo_CUSTOM,
		ExecutorID: ms.ExecutorID{Value: "default"},
		Name:       &name,
		Command: &ms.CommandInfo{
			Value: proto.String("./executor"),
			URIs:  executorUris,
		},
		Resources: rs,
	}
	return exec
}

func (s *Scheduler) statusUpdate() events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		s := e.GetUpdate().GetStatus()
		msg := "Task " + s.TaskID.Value + " is in state " + s.GetState().String()
		if m := s.GetMessage(); m != "" {
			msg += " with message '" + m + "'"
		}
		fmt.Println(msg)

		switch st := s.GetState(); st {
		case ms.TASK_FINISHED:
			fmt.Println("state finish")
		case ms.TASK_LOST, ms.TASK_KILLED, ms.TASK_FAILED, ms.TASK_ERROR:
			fmt.Println("Exiting because task " + s.GetTaskID().Value +
				" is in an unexpected state " + st.String() +
				" with reason " + s.GetReason().String() +
				" from source " + s.GetSource().String() +
				" with message '" + s.GetMessage() + "'")
		}
		return nil
	}
}
