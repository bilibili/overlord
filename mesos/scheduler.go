package mesos

import (
	"context"
	"strconv"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"

	"overlord/lib/log"
	"overlord/lib/store"

	ms "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/callrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/eventrules"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

// Scheduler mesos scheduler.
type Scheduler struct {
	c     *Config
	Tchan chan string
	db    store.DB
	cli   calls.Caller
}

// NewScheduler new scheduler instance.
func NewScheduler(c *Config, db store.DB) *Scheduler {
	return &Scheduler{
		db: db,
		c:  c,
	}
}

// Run scheduler and watch at task dir for task info.
func (s *Scheduler) Run() (err error) {
	// ch, err := s.db.Watch(context.Background(), store.TASKDIR)
	// if err != nil {
	// 	log.Errorf("start watch task fail err %v", err)
	// 	return
	// }
	// s.Tchan = ch
	s.cli = buildHTTPSched(s.c)
	s.cli = callrules.New().Caller(s.cli)
	controller.Run(context.TODO(),
		buildFrameworkInfo(s.c),
		s.cli,
		controller.WithEventHandler(s.buildEventHandle()),
	)
	return
}

func buildHTTPSched(cfg *Config) calls.Caller {
	cli := httpcli.New(
		httpcli.Endpoint("http://:5050/api/v1/scheduler"),
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
	return frameworkInfo
}

func (s *Scheduler) buildEventHandle() events.Handler {
	logger := controller.LogEvents(nil)
	return eventrules.New(
		logAllEvents(),
	).Handle(events.Handlers{
		scheduler.Event_FAILURE: logger.HandleF(failure),
		scheduler.Event_OFFERS:  s.trackOffersReceived(),
		//	scheduler.Event_UPDATE:  controller.AckStatusUpdates(state.cli).AndThen().HandleF(statusUpdate(state)),
		scheduler.Event_SUBSCRIBED: eventrules.New(
			logger,
			//controller.TrackSubscription(fidStore, state.config.failoverTimeout),
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
