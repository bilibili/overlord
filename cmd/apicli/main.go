package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/pkg/errors"

	"overlord/pkg/log"
	"overlord/platform/api/model"
	"overlord/version"
)

func main() {
	flag.StringVar(&server, "server", "", "api server addr")
	flag.StringVar(&cmd, "cmd", "", "cli cmd")
	flag.StringVar(&name, "name", "", "cluster name")
	flag.StringVar(&appid, "appid", "", "appid name")
	flag.StringVar(&addr, "addr", "", "addr of node to restart")
	flag.Parse()
	if version.ShowVersion() {
		return
	}

	log.Init(nil)
	var err error
	switch {
	case cmd == "create":
		if name != "" {
			defCreate.Name = name
		}
		err = createCluster(defCreate)
	case cmd == "getcluster":
		if name == "" {
			err = clusters()
		} else {
			err = cluster(name)
		}
	case cmd == "delete":
		if name == "" {
			panic("delete cluster name can not be nil")
		}
		err = deleteCluster(name)
	case cmd == "assign":
		err = addAppID(name, appid)
	case cmd == "unassign":
		err = deleteAppID(name, appid)
	case cmd == "restart":
		err = restartNode(name, addr)
	}
	if err != nil {
		fmt.Printf("err %v", err)
	}
}

var (
	name      string
	cmd       string
	server    string
	appid     string
	addr      string
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
	err = newReq(http.MethodPost, server+base, string(bs))
	return
}

func clusters() (err error) {
	return newReq(http.MethodGet, server+base+name, "")
}
func cluster(name string) (err error) {
	return newReq(http.MethodGet, server+base+name, "")
}
func deleteCluster(name string) (err error) {
	return newReq(http.MethodDelete, server+base+name, "")
}
func addAppID(cluster string, appid string) (err error) {
	arg := &model.ParamAssign{
		Appid: appid,
	}
	bs, _ := json.Marshal(arg)
	return newReq(http.MethodPost, fmt.Sprintf("%s%s%s/appid", server, base, cluster), string(bs))
}
func deleteAppID(cluster string, appid string) (err error) {
	arg := &model.ParamAssign{
		Appid: appid,
	}
	bs, _ := json.Marshal(arg)
	return newReq(http.MethodDelete, fmt.Sprintf("%s%s%s/appid", server, base, cluster), string(bs))
}
func restartNode(cluster string, addr string) (err error) {
	return newReq(http.MethodPost, fmt.Sprintf("%s%s%s/instance/%s/restart", server, base, cluster, addr), "")
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
