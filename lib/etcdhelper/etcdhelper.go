package etcdhelper

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.etcd.io/etcd/client"
)

// New create new EtcdHelper with given etcd config
func New(endpoints []string) *EtcdHelper {
	c, err := client.New(client.Config{
		Endpoints: endpoints,
		Transport: client.DefaultTransport,
	})
	if err != nil {
		panic(fmt.Sprintf("etcd connected fail due to err %v", err))
	}

	e := &EtcdHelper{
		client: c,
		api:    client.NewKeysAPI(c),
	}

	return e
}

// EtcdHelper is the client helper for etcd.
type EtcdHelper struct {
	client client.Client
	api    client.KeysAPI
}

// GenID will generate new id with cas operation.
func (e *EtcdHelper) GenID(ctx context.Context, path string, value string) (int, error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()

	resp, err := e.api.CreateInOrder(sub, path, value,
		&client.CreateInOrderOptions{TTL: time.Duration(0)})
	if err != nil {
		return -1, err
	}

	order, err := strconv.ParseInt(resp.Node.Key, 10, 64)
	if err != nil {
		return -1, err
	}

	return int(order), nil
}

// Set will create new key-val pair
func (e *EtcdHelper) Set(ctx context.Context, path, value string) error {
	_, err := e.api.Set(ctx, path, "", &client.SetOptions{})
	return err
}

// Mkdir will create new dir
func (e *EtcdHelper) Mkdir(ctx context.Context, path string) error {
	_, err := e.api.Set(ctx, path, "", &client.SetOptions{Dir: true})
	return err
}

// Get the given value of path
func (e *EtcdHelper) Get(ctx context.Context, path string) (value string, err error) {
	var resp *client.Response
	resp, err = e.api.Get(ctx, path, &client.GetOptions{})
	if err != nil {
		return
	}
	value = resp.Node.Value
	return
}
