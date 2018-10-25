// Package etcd provider etcd store.
package etcd

import (
	"context"
	"overlord/lib/log"
	"strings"
	"time"

	cli "go.etcd.io/etcd/client"
)

const (
	baseDir     = "/overlord"
	instanceDir = baseDir + "/%s" + "/instances" // %s is cluster name
	configDir   = baseDir + "/%s" + "/config"
)

type etcd struct {
	C       cli.Client  //The client context
	Kapi    cli.KeysAPI //The api context for Get/Set/Delete/Update/Watcher etc.,
	Cfg     cli.Config  //Configuration details of the connection should be loaded from a configuration file
	isSetup bool        //Has this been setup
}

//New Function to create an etcDB object
func New() *etcd {
	return &etcd{isSetup: false}
}

//Login This implements connecting to the ETCD instance
func (db *etcd) Login() error {

	var err error
	db.C, err = cli.New(db.Cfg)
	if err != nil {

		return err
	}
	db.Kapi = cli.NewKeysAPI(db.C)
	return nil
}

// Setup will create/establish connection with the etcd store and also setup
// the nessary environment if etcd is running for the first time
func (db *etcd) Setup(ctx context.Context, config string) (err error) {
	db.Cfg = cli.Config{
		Endpoints: []string{config},
		Transport: cli.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	err = db.Login()
	if err != nil {
		return
	}
	err = db.CreateSection(ctx, baseDir)
	if err != nil && strings.Contains(err.Error(), "Key already exists") != true {
		return
	}
	err = db.CreateSection(ctx, instanceDir)
	if err != nil && strings.Contains(err.Error(), "Key already exists") != true {
		return
	}
	err = db.CreateSection(ctx, configDir)
	if err != nil && strings.Contains(err.Error(), "Key already exists") != true {
		return
	}
	db.isSetup = true
	return
}

func (db *etcd) IsSetup() bool {
	return db.isSetup
}

//CreateSection will create a directory in etcd store
func (db *etcd) CreateSection(ctx context.Context, k string) (err error) {
	_, err = db.Kapi.Set(ctx, k, "", &cli.SetOptions{Dir: true, PrevExist: cli.PrevNoExist})
	return err
}

func (db *etcd) Set(ctx context.Context, k, v string) (err error) {
	_, err = db.Kapi.Set(ctx, k, v, nil)
	return err
}

func (db *etcd) Get(ctx context.Context, k string) (v string, err error) {
	resp, err := db.Kapi.Get(ctx, k, nil)
	if err != nil {
		return
	}
	v = resp.Node.Value
	return
}

func (db *etcd) Watch(ctx context.Context, k string) (ch chan string, err error) {
	watcher := db.Kapi.Watcher(k, nil)
	ch = make(chan string)
	go func() {
		for {
			resp, err := watcher.Next(ctx)
			if err != nil {
				log.Errorf("watch etcd node %s err %v", k, err)
			}
			ch <- resp.Node.Value
		}
	}()
	return
}
