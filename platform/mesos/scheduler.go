package mesos

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"overlord/pkg/etcd"
	"overlord/pkg/log"
	"overlord/pkg/types"
	"overlord/platform/chunk"
	"overlord/platform/job"
	"overlord/platform/job/create"

	pb "github.com/golang/protobuf/proto"
	ms "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/callrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/eventrules"
	mstore "github.com/mesos/mesos-go/api/v1/lib/extras/store"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
	"github.com/pkg/errors"
	cli "go.etcd.io/etcd/client"
)

const (
	maxRetry = 5
)

var (
	errTaskID = errors.New("error taskid format")
	errOffer  = errors.New("invalid offer")
)

// Scheduler mesos scheduler.
type Scheduler struct {
	c         *Config
	Tchan     chan string
	task      *list.List // use task list.
	db        *etcd.Etcd
	cli       calls.Caller
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
	log.Infof("start scheduler with conf %v", s.c)
	// watch task dir to get new task.
	for _, role := range s.c.Roles {
		ch, err := s.db.WatchOn(context.Background(), etcd.JobsDir+"/"+role)
		if err != nil {
			log.Errorf("start watch task fail err %v", err)
			return err
		}
		go s.taskEvent(ch)
	}
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

func (s *Scheduler) taskEvent(ch chan *cli.Node) {
	for {
		n := <-ch
		var t job.Job
		if err := json.Unmarshal([]byte(n.Value), &t); err != nil {
			log.Errorf("get task info err %v", err)
			continue
		}
		t.ID = splitJobID(n.Key)
		s.task.PushBack(t)
		log.Infof("get task (%+v)", t)
		// revive offer when task comming.
		revice := calls.Revive()
		if err := calls.CallNoData(context.Background(), s.cli, revice); err != nil {
			log.Errorf("revie offer fail  err(%v)", err)
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
		User:         s.c.User,
		Name:         s.c.Name,
		ID:           &ms.FrameworkID{Value: mstore.GetIgnoreErrors(s)()},
		Checkpoint:   &s.c.Checkpoint,
		Capabilities: []ms.FrameworkInfo_Capability{ms.FrameworkInfo_Capability{Type: ms.FrameworkInfo_Capability_MULTI_ROLE}},
	}
	if s.c.FailOver > 0 {
		failOverTimeout := time.Duration(s.c.FailOver).Seconds()
		frameworkInfo.FailoverTimeout = &failOverTimeout
	} else {
		// default one week to failover.
		failOverTimeout := time.Duration(time.Hour * 24 * 7).Seconds()
		frameworkInfo.FailoverTimeout = &failOverTimeout
	}
	if len(s.c.Roles) != 0 {
		frameworkInfo.Roles = s.c.Roles
	}
	if s.c.Hostname != "" {
		frameworkInfo.Hostname = &s.c.Hostname
	}
	fmt.Println(s.c.Checkpoint)
	if s.c.Principal != "" {
		frameworkInfo.Principal = &s.c.Principal
	}
	return frameworkInfo
}

func (s *Scheduler) buildEventHandle() events.Handler {
	logger := controller.LogEvents(eventLog)
	return eventrules.New().Handle(events.Handlers{
		scheduler.Event_FAILURE: logger.HandleF(failure),
		scheduler.Event_OFFERS:  s.trackOffersReceived().HandleF(s.resourceOffers()),
		scheduler.Event_UPDATE:  controller.AckStatusUpdates(s.cli).AndThen().HandleF(s.statusUpdate()),
		scheduler.Event_SUBSCRIBED: eventrules.New(
			logger,
			controller.TrackSubscription(s, time.Second*30),
		),
	})
}

func (s *Scheduler) trackOffersReceived() eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
		if err == nil {
			offers := e.GetOffers().GetOffers()
			log.Infof("get offer num %v ", len(offers))
			for _, offer := range offers {
				log.Infof("[offer detail] %v ", offer.Resources)
			}
		}
		return chain(ctx, e, err)
	}
}

func (s *Scheduler) resourceOffers() events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		var (
			offers = e.GetOffers().GetOffers()
			// callOption             = calls.RefuseSecondsWithJitter(rand.new, state.config.maxRefuseSeconds)
		)

		select {
		case taskid := <-s.failTask:
			s.tryRecovery(taskid, offers, false)
			return nil
		default:
		}
		for taskEle := s.task.Front(); taskEle != nil; {
			t := taskEle.Value.(job.Job)
			imem := t.MaxMem
			icpu := t.CPU
			inum := t.Num
			switch t.CacheType {
			case types.CacheTypeRedisCluster:
				err := s.dispatchCluster(t, inum, imem, icpu, offers)
				if err != nil {
					taskEle = taskEle.Next()
					continue
				}
				ttask := taskEle
				s.task.Remove(ttask)
				return nil
			case types.CacheTypeRedis, types.CacheTypeMemcache:
				err := s.dispatchSingleton(t, offers)
				if err != nil {
					log.Errorf("dispatchSingleton err %v", err)
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
		s.declineAndSuppress(offers, ctx)
		return nil
	}
}

func (s *Scheduler) declineAndSuppress(offers []ms.Offer, ctx context.Context) {
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
}

func (s *Scheduler) tryRecovery(t ms.TaskID, offers []ms.Offer, force bool) (err error) {

	var (
		cluster, port string
		info          *create.CacheInfo
		ip            string
		id            int64
	)
	defer func() {
		if err != nil {
			for _, offer := range offers {
				decline := calls.Decline(offer.ID)
				calls.CallNoData(context.Background(), s.cli, decline)
			}
		}
	}()
	cluster, ip, port, id, err = parseTaskID(t)
	if err != nil {
		log.Errorf("cannot reovery task(%s) with err taskid ", t.String())
		return
	}
	log.Infof("try recover task(%v)", t)
	if id > maxRetry && !force {
		log.Errorf("drop recovery because of retry over than %s", t.GetValue())
		err = errors.Errorf("retry over max")
		return
	}
	info, err = s.getInfoFromEtcd(context.Background(), cluster)
	if err != nil {
		return
	}
	uport, _ := strconv.ParseUint(port, 10, 64)
	task := &ms.TaskInfo{
		Name:      ip + ":" + port,
		TaskID:    ms.TaskID{Value: fmt.Sprintf("%s:%s-%s-%d", ip, port, cluster, id+1)},
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
		agentIP := chunk.ValidateIPAddress(offer.Hostname)
		// try to recover from origin agent with the same info.
		if agentIP == ip {
			if !checkOffer(offer, info.CPU, info.MaxMemory, uport) {
				err = errOffer
				return
			}
			task.AgentID = offer.GetAgentID()
			s.db.SetTaskID(context.Background(), task.Name, task.TaskID.GetValue()+","+task.AgentID.GetValue())
			accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(*task)}.WithOffers(offer.ID))
			err = calls.CallNoData(context.Background(), s.cli, accept)
			if err == nil {
				log.Info("recover task successfully")
				// decline other offer
				for _, offer := range offers {
					if agentIP != ip {
						decline := calls.Decline(offer.ID)
						calls.CallNoData(context.Background(), s.cli, decline)
					}
				}
				return
			}
			log.Errorf("try recover task from origin agent fail %v", err)
		}
	}
	// can not recovery from origin host,try to scale by append.
	switch info.CacheType {
	case types.CacheTypeMemcache, types.CacheTypeRedis:
		// delete old host
		var (
			alias   string
			newDist *chunk.Dist
		)
		for i, addr := range info.Dist.Addrs {
			if addr.String() == ip+port {
				info.Dist.Addrs = append(info.Dist.Addrs[:i], info.Dist.Addrs[i+1:]...)
				alias = addr.ID
			}
		}
		newDist, err = chunk.DistAppendIt(info.Dist, 1, info.MaxMemory, info.CPU, offers...)
		if err != nil {
			log.Errorf("chunk,DistAppend err %v", err)
			return
		}
		newDist.Addrs[0].ID = alias
		info.Dist.Addrs = append(info.Dist.Addrs, newDist.Addrs...)
		err = s.acceptOffer(info, newDist, offers)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		ctask := create.NewCacheJob(s.db, info)
		err = ctask.Create()
		if err != nil {
			log.Errorf("recovery task create cache job err %v", err)
			return
		}

	case types.CacheTypeRedisCluster:
		err = fmt.Errorf("restart redis cluster err")
		log.Error("can not revoer redis-cluster from origin agent,need to be rescale by op")
		// if cluster cann't recovery from origin agent.
		// set status as WaitApprove,and then should be recover by op with chunkrecover
	default:
		log.Errorf("recover from err cache type")
	}
	return
}

func (s *Scheduler) acceptOffer(info *create.CacheInfo, dist *chunk.Dist, offers []ms.Offer) (err error) {
	var (
		ofm     = make(map[string]ms.Offer)
		tasks   = make(map[string][]ms.TaskInfo)
		offerid = make(map[ms.OfferID]struct{})
	)

	for _, offer := range offers {
		ofm[chunk.ValidateIPAddress(offer.GetHostname())] = offer
	}
	for _, addr := range dist.Addrs {
		task := ms.TaskInfo{
			Name:     addr.String(),
			TaskID:   ms.TaskID{Value: addr.String() + "-" + info.Name + "-" + "0"},
			AgentID:  ofm[addr.IP].AgentID,
			Executor: s.buildExcutor(addr.String(), []ms.Resource{}),
			//  plus the port obtained by adding 10000 to the data port for redis cluster.
			Resources: makeResources(info.CPU, info.MaxMemory, uint64(addr.Port)),
		}
		s.db.SetTaskID(context.Background(), addr.String(), task.TaskID.GetValue()+","+task.AgentID.GetValue())
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
		accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(tasks[chunk.ValidateIPAddress(offer.Hostname)]...)}.WithOffers(offer.ID))
		err = calls.CallNoData(context.Background(), s.cli, accept)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	return nil
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

func (s *Scheduler) buildExcutor(name string, rs []ms.Resource) *ms.ExecutorInfo {
	executorUris := []ms.CommandInfo_URI{}
	executorUris = append(executorUris, ms.CommandInfo_URI{Value: s.c.ExecutorURL, Executable: pb.Bool(true)})
	exec := &ms.ExecutorInfo{
		Type:       ms.ExecutorInfo_CUSTOM,
		ExecutorID: ms.ExecutorID{Value: name},
		Name:       &name,
		Command: &ms.CommandInfo{
			Value: pb.String("./executor"),
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
		taskid := status.TaskID.GetValue()
		idx := strings.IndexByte(taskid, '-')
		addr := taskid[:idx]
		addr += "/state"
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
			s.db.Set(ctx, etcd.InstanceDirPrefix+"/"+addr, job.StateFail)
		case ms.TASK_KILLED:
			log.Infof("task(%v) killed", status.TaskID)
		case ms.TASK_RUNNING:
			s.db.Set(ctx, etcd.InstanceDirPrefix+"/"+addr, job.StateRunning)
		}
		return nil
	}
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
		var (
			newChunk []*chunk.Chunk
			ci       *create.CacheInfo
		)
		ci, err = s.getInfoFromEtcd(context.Background(), t.Name)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		chunks = ci.Chunks
		newChunk, err = chunk.ChunksAppend(chunks, num, mem, cpu, offers...)
		if err != nil {
			log.Errorf("chunk.ChunksAppend with job (%v) err %v", t, err)
			return
		}
		jobChunks = newChunk
		chunks = append(chunks, newChunk...)
	case job.OpDestroy:
		s.destroyCluster(t, offers)
		return
	case job.OpRestart:
		s.restartNode(t, offers)
		return
	}

	log.Infof("get chunks(%v) by offers (%v)", chunks, offers)
	var (
		ofm   = make(map[string]ms.Offer)
		tasks = make(map[string][]ms.TaskInfo)
	)
	rtask := create.NewRedisClusterJob(s.db, &create.CacheInfo{
		Chunks:    chunks,
		CacheType: t.CacheType,
		MaxMemory: t.MaxMem,
		Name:      t.Name,
		JobID:     t.ID,
		Version:   t.Version,
		Image:     t.Image,
		Number:    t.Num,
		Group:     t.Group,
	})
	err = rtask.Create()
	if err != nil {
		log.Errorf("create cluster err %v", err)
		return
	}
	for _, offer := range offers {
		ofm[chunk.ValidateIPAddress(offer.GetHostname())] = offer
	}
	for _, ck := range jobChunks {
		for _, node := range ck.Nodes {
			task := ms.TaskInfo{
				Name:     node.Addr(),
				TaskID:   ms.TaskID{Value: node.Addr() + "-" + t.Name + "-" + "0"},
				AgentID:  ofm[node.Name].AgentID,
				Executor: s.buildExcutor(node.Addr(), []ms.Resource{}),
				//  plus the port obtained by adding 10000 to the data port for redis cluster.
				Resources: makeResources(cpu, mem, uint64(node.Port)),
			}
			taskid := task.TaskID.GetValue() + "," + task.AgentID.GetValue()
			err = s.db.SetTaskID(context.Background(), node.Addr(), taskid)
			if err != nil {
				log.Errorf("node %s set taskid(%s) err(%v)", node.Addr(), taskid, err)
			}
			//	s.db.Set(ctx context.Context, k string, v string)
			data := &TaskData{
				IP:         node.Name,
				Port:       node.Port,
				DBEndPoint: s.c.DBEndPoint,
			}
			task.Data, _ = json.Marshal(data)
			tasks[node.Name] = append(tasks[node.Name], task)
		}
	}
	for _, offer := range offers {
		accept := calls.Accept(calls.OfferOperations{calls.OpLaunch(tasks[chunk.ValidateIPAddress(offer.Hostname)]...)}.WithOffers(offer.ID))
		err = calls.CallNoData(context.Background(), s.cli, accept)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	return
}

func (s *Scheduler) dispatchSingleton(t job.Job, offers []ms.Offer) (err error) {
	var (
		dist    *chunk.Dist
		jobDist *chunk.Dist
		ctx     = context.Background()
		ci      *create.CacheInfo
	)
	switch t.OpType {
	case job.OpCreate:
		dist, err = chunk.DistIt(t.Num, t.MaxMem, t.CPU, offers...)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		jobDist = dist
	case job.OpDestroy:
		s.destroyCluster(t, offers)
		return
	case job.OpScale:
		var (
			newDist *chunk.Dist
		)
		ci, err = s.getInfoFromEtcd(context.Background(), t.Name)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		dist = ci.Dist
		delta := t.Num - len(dist.Addrs)
		if delta >= 0 {
			newDist, err = chunk.DistAppendIt(dist, t.Num, t.MaxMem, t.CPU, offers...)
			if err != nil {
				err = errors.WithStack(err)
				return
			}
			jobDist = newDist
			dist.Addrs = append(dist.Addrs, newDist.Addrs...)
		} else {
			// TODO:scale down
			return
		}
	case job.OpRestart:
		s.restartNode(t, offers)
		return
	case job.OpMigrate:
		var (
			newDist *chunk.Dist
		)
		ci, err = s.getInfoFromEtcd(context.Background(), t.Name)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		var alias = make([]string, 0)

		for _, node := range t.Nodes {
			var (
				al, id string
			)
			id, err = s.db.TaskID(context.Background(), node)
			if err == nil {
				ids := strings.Split(id, ",")
				if len(ids) != 2 {
					return
				}
				kill := calls.Kill(ids[0], ids[1])
				calls.CallNoData(context.Background(), s.cli, kill)
			}
			al, err = s.db.Alias(ctx, node)
			if err != nil {
				return
			}
			alias = append(alias, al)
		}

		num := len(alias)
		newDist, err = chunk.DistAppendIt(ci.Dist, num, ci.MaxMemory, ci.CPU, offers...)
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		for i, dis := range newDist.Addrs {
			dis.ID = alias[i]
		}
		jobDist = newDist
		dist.Addrs = append(dist.Addrs, jobDist.Addrs...)
	}

	ci = &create.CacheInfo{
		JobID:     t.ID,
		Name:      t.Name,
		MaxMemory: t.MaxMem,
		CPU:       t.CPU,
		CacheType: t.CacheType,
		Number:    t.Num,
		Thread:    int(t.CPU) + 1,
		Version:   t.Version,
		Image:     t.Image,
		Dist:      dist,
		Group:     t.Group,
	}
	ctask := create.NewCacheJob(s.db, ci)
	err = ctask.Create()
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if err = s.acceptOffer(ci, jobDist, offers); err != nil {
		err = errors.WithStack(err)
	}
	return
}

func (s *Scheduler) destroyCluster(t job.Job, offers []ms.Offer) {
	var (
		ctx = context.Background()
	)
	// destroy didn't need to offers，should decline it immediately
	s.declineAndSuppress(offers, ctx)

	ci, err := s.getInfoFromEtcd(ctx, t.Name)
	if err != nil {
		log.Errorf("get info fail err %v", err)
	}
	if ci != nil {
		switch t.CacheType {
		case types.CacheTypeRedisCluster:
			for _, ck := range ci.Chunks {
				for _, n := range ck.Nodes {
					var id string
					id, err = s.db.TaskID(ctx, n.Addr())
					if err != nil {
						log.Errorf("get task(%s) err %v", n.Addr(), err)
						continue
					}
					s.kill(id)
				}
			}
		default:
			for _, d := range ci.Dist.Addrs {
				var id string
				id, err = s.db.TaskID(ctx, d.String())
				if err != nil {
					log.Errorf("get task(%s) err %v", d.String(), err)
					continue
				}
				s.kill(id)
			}
		}
	}
	// clear etcd info
	var nodes []*etcd.Node
	nodes, err = s.db.LS(ctx, fmt.Sprintf(etcd.ClusterInstancesDir, ci.Name))
	if err != nil {
		log.Errorf("get cluster(%s) nodes info err %v", ci.Name, err)
	}
	s.db.RMDir(ctx, fmt.Sprintf("%s/%s", etcd.ClusterDir, ci.Name))
	for _, node := range nodes {
		err = s.db.RMDir(ctx, etcd.InstanceDirPrefix+"/"+node.Value)
		log.Errorf("rm instance dir (%s) fail err %v", node.Value, err)
	}
}

func (s *Scheduler) restartNode(job job.Job, offers []ms.Offer) (err error) {
	ctx := context.Background()
	// restart one node each time.
	node := job.Nodes[0]
	var id string
	id, err = s.db.Get(ctx, fmt.Sprintf("%s/%s/%s", etcd.InstanceDirPrefix, node, "taskid"))
	if err != nil {
		log.Errorf("get taskid err %v", err)
		return
	}
	taskid := ms.TaskID{
		Value: id,
	}
	if err = s.tryRecovery(taskid, offers, true); err == nil {
		return
	}
	log.Errorf("try restart node from origin agent fail")
	return
}

func (s *Scheduler) kill(id string) {
	ids := strings.Split(id, ",")
	if len(ids) != 2 {
		return
	}
	kill := calls.Kill(ids[0], ids[1])
	calls.CallNoData(context.Background(), s.cli, kill)

}
