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

	"overlord/job"
	"overlord/job/create"
	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/proto"

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
	ch, err := s.db.Watch(context.Background(), etcd.JobsDir)
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
		var t job.Job
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
			t := taskEle.Value.(job.Job)
			imem := t.MaxMem
			icpu := t.CPU
			inum := t.Num
			switch t.CacheType {
			case proto.CacheTypeRedisCluster:

				err := s.dispatchCluster(t, inum, imem, icpu, offers)
				if err != nil {
					taskEle = taskEle.Next()
					continue
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
					JobID:     t.ID,
					Name:      t.Name,
					MaxMemory: t.MaxMem,
					CPU:       t.CPU,
					CacheType: t.CacheType,
					Number:    t.Num,
					Thread:    int(t.CPU) + 1,
					Version:   t.Version,
					Dist:      dist,
				}
				ctask := create.NewCacheJob(s.db, ci)
				err = ctask.Create()
				if err != nil {
					log.Errorf("create cluster err %v", err)
					return nil
				}
				if err = s.acceptOffer(ci, dist, offers); err != nil {
					taskEle = taskEle.Next()
					continue
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

	var (
		err           error
		cluster, port string
		info          *create.CacheInfo
		ip            string
	)

	cluster, ip, port = parseTaskID(t)
	log.Infof("try recover task(%v)", t)
	info, err = s.getInfoFromEtcd(context.Background(), cluster)
	if err != nil {
		return
	}
	uport, _ := strconv.ParseUint(port, 10, 64)
	task := &ms.TaskInfo{
		Name:      ip + port,
		TaskID:    ms.TaskID{Value: t.GetValue()},
		Executor:  s.buildExcutor(ip+port, []ms.Resource{}),
		Resources: makeResources(info.CPU, info.MaxMemory, uport),
	}

	data := &TaskData{
		IP:         ip,
		Port:       int(uport),
		DBEndPoint: s.c.DBEndPoint,
	}
	task.Data, _ = json.Marshal(data)
	for _, offer := range offers {
		// try to recover from origin agent with the same info.
		if offer.Hostname == ip {
			task.AgentID = offer.GetAgentID()
			accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(*task)}.WithOffers(offer.ID))
			err := calls.CallNoData(context.Background(), s.cli, accept)
			if err == nil {
				log.Info("recover task successfully")
				return
			}
			log.Errorf("try recover task from pre agent fail %v", err)
		}
	}
	// can not recovery from origin host,try to scale by append.
	switch info.CacheType {
	case proto.CacheTypeMemcache, proto.CacheTypeRedis:
		// delete old host
		for i, addr := range info.Dist.Addrs {
			if addr.IP == ip {
				info.Dist.Addrs = append(info.Dist.Addrs[:i], info.Dist.Addrs[i+1:]...)
			}
		}
		newDist, err := chunk.DistAppendIt(info.Dist, 1, info.MaxMemory, info.CPU, offers...)
		if err != nil {
			log.Errorf("chunk,DistAppend err %v", err)
			return
		}
		info.Dist.Addrs = append(info.Dist.Addrs, newDist.Addrs...)
		s.acceptOffer(info, newDist, offers)
		ctask := create.NewCacheJob(s.db, info)
		err = ctask.Create()
		if err != nil {
			log.Errorf("recovery task create cache job err %v", err)
			return
		}

	case proto.CacheTypeRedisCluster:
		// if cluster cann't recovery from origin agent.
		// set status as WaitApprove,and then should be recover by op with chunkrecover
	default:
		log.Errorf("recover from err cache type %v", err)
	}
}

func (s *Scheduler) acceptOffer(info *create.CacheInfo, dist *chunk.Dist, offers []ms.Offer) (err error) {
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
			Name:     addr.String(),
			TaskID:   ms.TaskID{Value: addr.String() + "-" + info.Name},
			AgentID:  ofm[addr.IP].AgentID,
			Executor: s.buildExcutor(addr.String(), []ms.Resource{}),
			//  plus the port obtained by adding 10000 to the data port for redis cluster.
			Resources: makeResources(info.CPU, info.MaxMemory, uint64(addr.Port)),
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
		err = calls.CallNoData(context.Background(), s.cli, accept)
		if err != nil {
			log.Errorf("fail to lanch task err %v", err)
			continue
		}
	}
	return err
}

func (s *Scheduler) getInfoFromEtcd(ctx context.Context, cluster string) (t *create.CacheInfo, err error) {
	i, err := s.db.ClusterInfo(ctx, cluster)
	if err != nil {
		return
	}
	t = new(create.CacheInfo)
	err = json.Unmarshal([]byte(i), t)
	return
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
				log.Error("add fail task to chan fail, chan full")
				time.Sleep(time.Second * 5)
				return nil
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
			s.db.Set(ctx, etcd.InstanceDirPrefix+addr, job.StateRunning)
		}
		return nil
	}
}

func parseTaskID(t ms.TaskID) (cluster, ip, port string) {
	v := t.GetValue()
	idx := strings.IndexByte(v, '-')
	if idx == -1 {
		return
	}
	cluster = v[idx+1:]
	host := v[:idx]
	idx = strings.IndexByte(host, ':')
	ip = host[:idx]
	port = host[idx+1:]
	return
}

func (s *Scheduler) dispatchCluster(t job.Job, num int, mem, cpu float64, offers []ms.Offer) (err error) {
	var chunks []*chunk.Chunk
	var jobChunks []*chunk.Chunk
	switch t.OpType {
	case job.OpCreate:
		chunks, err = chunk.Chunks(num, mem, cpu, offers...)
		if err != nil {
			log.Errorf("task(%v) can not get offer by chunk, err %v", t, err)
			return
		}
		jobChunks = chunks
	case job.OpScale:
		var newChunk []*chunk.Chunk
		newChunk, err = chunk.ChunksAppend(chunks, num, mem, cpu, offers...)
		if err != nil {
			log.Errorf("chunk.ChunksAppend with job (%v) err %v", t, err)
			return
		}
		jobChunks = newChunk
		chunks = append(chunks, newChunk...)
	}

	log.Infof("get chunks(%v) by offers (%v)", chunks, offers)
	var (
		ofm     = make(map[string]ms.Offer)
		tasks   = make(map[string][]ms.TaskInfo)
		offerid = make(map[ms.OfferID]struct{})
	)
	rtask := create.NewRedisClusterJob(s.db, &create.CacheInfo{
		Chunks:    chunks,
		CacheType: t.CacheType,
		MaxMemory: t.MaxMem,
		Name:      t.Name,
		JobID:     t.ID,
		Version:   t.Version,
		Number:    t.Num,
	})
	err = rtask.Create()
	if err != nil {
		log.Errorf("create cluster err %v", err)
		return
	}
	for _, offer := range offers {
		ofm[offer.GetHostname()] = offer
	}
	for _, ck := range jobChunks {
		for _, node := range ck.Nodes {
			task := ms.TaskInfo{
				Name:     node.Name,
				TaskID:   ms.TaskID{Value: node.Addr() + "-" + t.Name},
				AgentID:  ofm[node.Name].AgentID,
				Executor: s.buildExcutor(node.Addr(), []ms.Resource{}),
				//  plus the port obtained by adding 10000 to the data port for redis cluster.
				Resources: makeResources(cpu, mem, uint64(node.Port)),
			}
			data := &TaskData{
				IP:         node.Name,
				Port:       node.Port,
				DBEndPoint: s.c.DBEndPoint,
			}
			task.Data, _ = json.Marshal(data)
			offerid[ofm[node.Name].ID] = struct{}{}
			tasks[node.Name] = append(tasks[node.Name], task)
		}
	}
	for _, offer := range offers {
		accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(tasks[offer.Hostname]...)}.WithOffers(offer.ID))
		err = calls.CallNoData(context.Background(), s.cli, accept)
		if err != nil {
			log.Errorf("fail to lanch task err %v", err)
			return
		}
	}
	return
}
