// Package etcd provider etcd store.
package etcd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"overlord/lib/log"
	"overlord/task"

	cli "go.etcd.io/etcd/client"
)

// etcd base dir
const (
	FRAMEWORK           = "/overlord/framework"
	ClusterInstancesDir = "/overlord/clusters/%s/instances/"
	InstanceDir         = "/overlord/instances/%s:%d"
	HeartBeatDir        = "/overlord/heartbeat"
	ClusterDir          = "/overlord/clusters"
	ConfigDir           = "/overlord/config"
	TaskDir             = "/overlord/task"
	TaskDetialDir       = "overlord/task_detial"
	FrameWork           = "/overlord/framework"
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
		HeaderTimeoutPerRequest: time.Second,
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
	resp, err := e.kapi.Get(ctx, dir, &cli.GetOptions{Recursive: true})
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
			resp, err := watcher.Next(ctx)
			fmt.Println(resp, err)
			// TODO:rewatch if err.
			if err != nil {
				log.Errorf("watch etcd node %s err %v", k, err)
			}
			ch <- resp.Node.Value
		}
	}()
	return
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

// SetTaskState will change task state.
func (e *Etcd) SetTaskState(ctx context.Context, taskID string, state task.StateType) error {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err := e.kapi.Set(subctx, fmt.Sprintf("%s/%s/state", TaskDetialDir, taskID), state, &cli.SetOptions{})
	return err
}

// WatchOnExpire watch expire action in this dir.
func (e *Etcd) WatchOnExpire(ctx context.Context, dir string) (key chan string, err error) {
	watcher := e.kapi.Watcher(dir, &cli.WatcherOptions{Recursive: true})
	key = make(chan string)
	go func() {
		for {
			resp, err := watcher.Next(ctx)
			if err != nil {
				log.Errorf("watch etcd node %s err %v", dir, err)
			}
			if resp.Action == "expire" {
				key <- resp.Node.Key
			}
		}
	}()
	return
}