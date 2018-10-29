package mesos

import (
	"container/list"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"

	"overlord/lib/log"
	"overlord/lib/store"

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
	db    store.DB
	cli   calls.Caller
}

// NewScheduler new scheduler instance.
func NewScheduler(c *Config, db store.DB) *Scheduler {
	return &Scheduler{
		db:   db,
		c:    c,
		task: list.New(),
	}
}

// Run scheduler and watch at task dir for task info.
func (s *Scheduler) Run() (err error) {
	// watch task dir to get new task.
	ch, err := s.db.Watch(context.Background(), store.TASKDIR)
	if err != nil {
		log.Errorf("start watch task fail err %v", err)
		return
	}
	s.Tchan = ch
	go s.taskEvent()
	fidStore := mstore.DecorateSingleton(
		mstore.NewInMemorySingleton(),
		mstore.DoSet().AndThen(func(_ mstore.Setter, v string, _ error) error {
			log.Info("FrameworkID", v)
			return nil
		}))
	s.cli = buildHTTPSched(s.c)
	s.cli = callrules.New(callrules.WithFrameworkID(mstore.GetIgnoreErrors(fidStore))).Caller(s.cli)

	controller.Run(context.TODO(),
		buildFrameworkInfo(s.c),
		s.cli,
		controller.WithEventHandler(s.buildEventHandle(fidStore)),
		controller.WithFrameworkID(mstore.GetIgnoreErrors(fidStore)),
	)
	return
}

func (s *Scheduler) taskEvent() {
	for {
		v := <-s.Tchan
		_ = v
		var t Task
		// if err := json.Unmarshal([]byte(v), &t); err != nil {
		// 	log.Errorf("err task info err %v", err)
		// 	continue
		// }
		t = Task{
			Name: "11",
			Type: taskTypeRedis,
			Num:  3,
			I: &Instance{
				Name:   "redis",
				Memory: 100,
			},
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

func (s *Scheduler) buildEventHandle(fid mstore.Singleton) events.Handler {
	logger := controller.LogEvents(nil)
	return eventrules.New(
		logAllEvents(),
	).Handle(events.Handlers{
		scheduler.Event_FAILURE: logger.HandleF(failure),
		scheduler.Event_OFFERS:  s.trackOffersReceived().HandleF(s.resourceOffers()),
		scheduler.Event_UPDATE:  controller.AckStatusUpdates(s.cli).AndThen().HandleF(s.statusUpdate()),
		scheduler.Event_SUBSCRIBED: eventrules.New(
			logger,
			controller.TrackSubscription(fid, time.Second),
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
		for i, offer := range offers {
			memRes := filterResource(offer.Resources, "mem")
			mems := 0.0
			for _, mem := range memRes {
				mems += mem.GetScalar().GetValue()
			}
			log.Infof("Recived offer with MEM=%v", mems)
			var tasks []ms.TaskInfo
			for taskEle := s.task.Front(); taskEle != nil; {
				task := taskEle.Value.(Task)
				tkMem := float64(task.I.Memory)
				if tkMem <= mems {
					tskID := ms.TaskID{Value: strconv.FormatInt(rand.Int63(), 10)}
					memsosTsk := ms.TaskInfo{
						Name:     task.Name,
						TaskID:   tskID,
						AgentID:  offer.AgentID,
						Executor: s.buildExcutor(task.Name, buildWantsExecutorResources(task.I.CPU, task.I.Memory)),
						// TODO:find suitable resource.
						Resources: buildWantsExecutorResources(task.I.CPU, task.I.Memory),
					}
					cTask := taskEle
					taskEle = taskEle.Next()
					s.task.Remove(cTask)
					tasks = append(tasks, memsosTsk)
				} else {
					taskEle = taskEle.Next()
				}
			}
			if len(tasks) > 0 {
				accept := calls.Accept(
					calls.OfferOperations{calls.OpLaunch(tasks...)}.WithOffers(offers[i].ID),
				) //.With(callOption)
				err := calls.CallNoData(ctx, s.cli, accept)
				if err != nil {
					log.Errorf("failed to lanch task :%v", err)
				}
			} else {
				decli := calls.Decline(offers[i].ID)
				calls.CallNoData(ctx, s.cli, decli)
			}
		}
		return nil
	}
}

func filterResource(rs []ms.Resource, filter string) (fs []ms.Resource) {
	for _, r := range rs {
		if r.GetName() == filter {
			fs = append(fs, r)
		}
	}
	return
}

func buildWantsExecutorResources(cpu, mem float64) (r ms.Resources) {
	r.Add(
		resources.NewCPUs(cpu).Resource,
		resources.NewMemory(mem).Resource,
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
