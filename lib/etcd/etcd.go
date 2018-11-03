// Package etcd provider etcd store.
package etcd

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"overlord/lib/log"

	cli "go.etcd.io/etcd/client"
)

// etcd base dir
const (
	BASEDIR    = "/overlord"
	CLUSTERDIR = "/overlord/clusters"
	CONFIGDIR  = "/overlord/config"
	TASKDIR    = "/overlord/task"
	FRAMEWORK  = "/overlord/framework"
)

// Node etcd kv info.
type Node struct {
	Key   string
	Value string
}

// Etcd etcd implement.
type Etcd struct {
	cli     cli.Client  //The client context
	kapi    cli.KeysAPI //The api context for Get/Set/Delete/Update/Watcher etc.,
	cfg     cli.Config  //Configuration details of the connection should be loaded from a configuration file
	isSetup bool        //Has this been setup
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

// GenID will generate new id with cas operation.
func (e *Etcd) GenID(ctx context.Context, path string, value string) (int, error) {
	resp, err := e.kapi.CreateInOrder(ctx, path, value, nil)
	if err != nil {
		return -1, err
	}
	order, err := strconv.ParseInt(resp.Node.Key, 10, 64)
	if err != nil {
		return -1, err
	}
	return int(order), nil
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
