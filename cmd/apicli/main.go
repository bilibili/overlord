package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"overlord/lib/log"
	"strings"

	"github.com/pkg/errors"

	"overlord/api/model"
)

func main() {
	flag.StringVar(&server, "server", "", "api server addr")
	flag.StringVar(&cmd, "cmd", "", "cli cmd")
	flag.StringVar(&name, "name", "", "cluster name")
	flag.Parse()
	log.Init(nil)
	var err error
	switch {
	case cmd == "create":
		if name != "" {
			defCreate.Name = name
		}
		createCluster(defCreate)
	case cmd == "getcluster":
		if name == "" {
			clusters()
		} else {
			cluster(name)
		}
	case cmd == "delete":
		if name == "" {
			panic("delete cluster name can not be nil")
		}
		err = deleteCluster(name)

	}
	if err != nil {
		fmt.Printf("err %v", err)
	}
}

var (
	name      string
	cmd       string
	server    string
	defCreate = &model.ParamCluster{
		Name:        "default",
		Appids:      []string{"appid.test.appid"},
		Spec:        "0.25c10m",
		Version:     "4.0.11",
		TotalMemory: 200,
		CacheType:   "redis",
		Group:       "sh001",
	}
	client = &http.Client{}
)

const (
	base = "/api/v1/clusters/"
)

func createCluster(arg *model.ParamCluster) (err error) {
	bs, err := json.Marshal(arg)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	newReq(http.MethodPost, server+base, string(bs))
	return
}

func clusters() (err error) {
	return newReq(http.MethodGet, server+base+name, "")
}
func cluster(name string) (err error) {
	return newReq(http.MethodGet, server+base+"?"+fmt.Sprintf("name=%s", name), "")
}
func deleteCluster(name string) (err error) {
	return newReq(http.MethodDelete, server+base+name, "")
}
func addAppID(cluster string, appid string) (err error) {
	return
}
func deleteAppID(cluster string, appid string) (err error) {
	return
}
func getJobs() (err error) {
	return
}
func getJob(job string) (err error) {
	return
}
func changeWeight(cluster, instance string, weigth int) (err error) {
	return
}

func newReq(method, url, body string) (err error) {
	var req *http.Request
	req, err = http.NewRequest(method, url, strings.NewReader(body))
	resp, err := client.Do(req)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	defer resp.Body.Close()
	bs, err := ioutil.ReadAll(resp.Body)
	log.Infof("url %s resp %v err %v\n", url, string(bs), err)
	return
}
