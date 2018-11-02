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
	}

	return e
}

// EtcdHelper is the client helper for etcd.
type EtcdHelper struct {
	client client.Client
}

// GenID will generate new id with cas operation.
func (e *EtcdHelper) GenID(ctx context.Context, path string, value string) (int, error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()

	keysapi := client.NewKeysAPI(e.client)
	resp, err := keysapi.CreateInOrder(sub, path, value,
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
