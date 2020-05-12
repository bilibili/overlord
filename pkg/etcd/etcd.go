// Package etcd provider etcd store.
package etcd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"overlord/pkg/log"

	cli "go.etcd.io/etcd/client"
)

// etcd base dir
const (
	FRAMEWORK           = "/overlord/framework"
	ClusterDir          = "/overlord/clusters"
	ClusterInstancesDir = "/overlord/clusters/%s/instances/"
	InstanceDir         = "/overlord/instances/%s:%d"
	InstanceDirPrefix   = "/overlord/instances"
	HeartBeatDir        = "/overlord/heartbeat"
	ConfigDir           = "/overlord/config"
	JobsDir             = "/overlord/jobs"
	JobDetailDir        = "/overlord/job_detail"
	FrameWork           = "/overlord/framework"
	AppidsDir           = "/overlord/appids"
	SpecsDir            = "/overlord/specs"
	FileServer          = "/overlord/fs"
	PortSequence        = "/overlord/port_sequence"
)

// define watch event
// get, set, delete, update, create, compareAndSwap,
// compareAndDelete and expire
const (
	// ActionGet              = "get"
	ActionSet              = "set"
	ActionDelete           = "delete"
	ActionUpdate           = "update"
	ActionCreate           = "create"
	ActionCompareAndSwap   = "compareAndSwap"
	ActionCompareAndDelete = "compareAndDelete"
	ActionExpire           = "expire"
)

// Node etcd kv info.
type Node struct {
	Key   string
	Value string
}

// Etcd etcd implement.
type Etcd struct {
	cli  cli.Client  //The client context
	kapi cli.KeysAPI //The api context for Get/Set/Delete/Update/Watcher etc.,
	cfg  cli.Config  //Configuration details of the connection should be loaded from a configuration file
}

//New Function to create an etce object
func New(endpoint string) (e *Etcd, err error) {
	e = &Etcd{}
	e.cfg = cli.Config{
		Endpoints: []string{endpoint},
		Transport: cli.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second * 2,
	}
	e.cli, err = cli.New(e.cfg)
	if err != nil {
		return
	}
	e.kapi = cli.NewKeysAPI(e.cli)
	return
}

// Mkdir will create a directory in Etcd store
func (e *Etcd) Mkdir(ctx context.Context, k string) (err error) {
	_, err = e.kapi.Set(ctx, k, "", &cli.SetOptions{Dir: true, PrevExist: cli.PrevNoExist})
	return err
}

// Set value into key.
func (e *Etcd) Set(ctx context.Context, k, v string) (err error) {
	_, err = e.kapi.Set(ctx, k, v, nil)
	return err
}

// Refresh refresh key ttl.
func (e *Etcd) Refresh(ctx context.Context, k string, ttl time.Duration) (err error) {
	_, err = e.kapi.Set(ctx, k, "", &cli.SetOptions{
		TTL:     ttl,
		Refresh: true,
	})
	return
}

// Get value by key.
func (e *Etcd) Get(ctx context.Context, k string) (v string, err error) {
	resp, err := e.kapi.Get(ctx, k, nil)
	if err != nil {
		return
	}
	v = resp.Node.Value
	return
}

// LS list kv in this dir.
func (e *Etcd) LS(ctx context.Context, dir string) (nodes []*Node, err error) {
	resp, err := e.kapi.Get(ctx, dir, &cli.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		return
	}
	nodes = make([]*Node, 0)
	for _, node := range resp.Node.Nodes {
		nodes = append(nodes, &Node{Key: node.Key, Value: node.Value})
	}
	return
}

// Watch on given key.
func (e *Etcd) Watch(ctx context.Context, k string) (ch chan string, err error) {
	watcher := e.kapi.Watcher(k, &cli.WatcherOptions{Recursive: true})
	ch = make(chan string)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			resp, err := watcher.Next(ctx)
			fmt.Println(resp, err)
			if err != nil {
				log.Errorf("watch etcd node %s err %v", k, err)
				continue
			}
			ch <- resp.Node.Value
		}
	}()
	return
}

// Delete will delete the given key
func (e *Etcd) Delete(ctx context.Context, path string) error {
	_, err := e.kapi.Delete(ctx, path, &cli.DeleteOptions{})
	return err
}

// RMDir will delete the given path with recursive
func (e *Etcd) RMDir(ctx context.Context, path string) error {
	_, err := e.kapi.Delete(ctx, path, &cli.DeleteOptions{Dir: true, Recursive: true})
	return err
}

// GenID will generate new id str with cas operation.
func (e *Etcd) GenID(ctx context.Context, path string, value string) (string, error) {
	resp, err := e.kapi.CreateInOrder(ctx, path, value, nil)
	if err != nil {
		return "", err
	}
	idx := strings.LastIndexByte(resp.Node.Key, '/')
	if idx == -1 {
		return resp.Node.Key, nil
	}
	return resp.Node.Key[idx+1:], nil
}

// SetJobState will change job state.
func (e *Etcd) SetJobState(ctx context.Context, group, jobID string, state string) error {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err := e.kapi.Set(subctx, fmt.Sprintf("%s/%s/%s/state", JobDetailDir, group, jobID), state, &cli.SetOptions{})
	return err
}

// WatchOnExpire watch expire action in this dir.
func (e *Etcd) WatchOnExpire(ctx context.Context, dir string) (key chan string, err error) {
	watcher := e.kapi.Watcher(dir, &cli.WatcherOptions{Recursive: true})
	key = make(chan string)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			resp, err := watcher.Next(ctx)
			if err != nil {
				log.Errorf("watch etcd node %s err %v", dir, err)
				continue
			}
			if resp.Action == "expire" {
				key <- resp.Node.Key
			}
		}
	}()
	return
}

// WatchOn will watch the given path forever
func (e *Etcd) WatchOn(ctx context.Context, path string, interestings ...string) (key chan *cli.Node, err error) {
	log.Infof("watch on %v", path)
	evtMap := make(map[string]struct{})
	for _, interest := range interestings {
		switch interest {
		case ActionSet, ActionDelete, ActionUpdate, ActionCreate, ActionCompareAndSwap, ActionCompareAndDelete, ActionExpire:
			evtMap[interest] = struct{}{}
		default:
			log.Infof("bad interest with %s", interest)
		}
	}

	var (
		resp *cli.Response
	)

	watcher := e.kapi.Watcher(path, &cli.WatcherOptions{Recursive: true})
	key = make(chan *cli.Node)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			resp, err = watcher.Next(ctx)
			if err != nil {
				log.Errorf("watch etcd node %s err %v", path, err)
				continue
			}
			if len(evtMap) == 0 {
				key <- resp.Node
			}

			if _, ok := evtMap[resp.Action]; ok {
				key <- resp.Node
			}
		}
	}()
	return
}

// WatchOneshot will watch the key until the context was reached Done.
func (e *Etcd) WatchOneshot(ctx context.Context, path string, interestings ...string) (key string, val string, err error) {
	evtMap := make(map[string]struct{})
	for _, interest := range interestings {
		evtMap[interest] = struct{}{}
	}

	var (
		resp *cli.Response
	)

	watcher := e.kapi.Watcher(path, &cli.WatcherOptions{Recursive: true})
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		resp, err = watcher.Next(ctx)
		if err != nil {
			log.Errorf("watch etcd node %s err %v", path, err)
			continue
		}

		if _, ok := evtMap[resp.Action]; ok {
			key = resp.Node.Key
			val = resp.Node.Value
			return
		}
	}
}

// ClusterInfo get cluster info .
func (e *Etcd) ClusterInfo(ctx context.Context, cluster string) (info string, err error) {
	return e.Get(ctx, fmt.Sprintf("%s/%s/info", ClusterDir, cluster))
}

// TaskID get cluster info .
func (e *Etcd) TaskID(ctx context.Context, node string) (id string, err error) {
	return e.Get(ctx, fmt.Sprintf("%s/%s/taskid", InstanceDirPrefix, node))
}

// Alias get node alias.
func (e *Etcd) Alias(ctx context.Context, node string) (id string, err error) {
	return e.Get(ctx, fmt.Sprintf("%s/%s/alias", InstanceDirPrefix, node))
}

// SetTaskID set node taskid
func (e *Etcd) SetTaskID(ctx context.Context, node string, id string) (err error) {
	return e.Set(ctx, fmt.Sprintf("%s/%s/taskid", InstanceDirPrefix, node), id)
}

// GetAllSpecs get all the specifications
func (e *Etcd) GetAllSpecs(ctx context.Context) ([]string, error) {
	nodes, err := e.LS(ctx, SpecsDir)
	if err != nil {
		return nil, err
	}

	specs := make([]string, len(nodes))
	for i, node := range nodes {
		specs[i] = node.Value
	}
	return specs, nil
}

// Cas will compareAndSwap with the given value
func (e *Etcd) Cas(ctx context.Context, key, old, newer string) error {
	_, err := e.kapi.Set(ctx, key, newer, &cli.SetOptions{PrevValue: old})
	return err
}

// Sequence get the auto increment value of the special key
func (e *Etcd) Sequence(ctx context.Context, key string) (int64, error) {
	for {
		val, err := e.Get(ctx, key)
		if cli.IsKeyNotFound(err) {
			err = e.Set(ctx, key, "20000")
			return 20000, err
		} else if err != nil {
			return -1, err
		}

		ival, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return -1, err
		}

		err = e.Cas(ctx, key, val, fmt.Sprintf("%d", ival+1))
		if err != nil {
			continue
		}

		return ival + 1, nil
	}
}
