package main

import (
	"flag"
	"overlord/config"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/task/balance"

	"strings"
)

var (
	cluster string
	db      string
)

func main() {
	config.SetRunMode(config.RunModeProd)

	log.Init(log.NewStdHandler())
	flag.StringVar(&cluster, "cluster", "", "cluster name")
	flag.StringVar(&db, "db", "", "etcd dsn")
	flag.Parse()

	var etcdURL string
	if strings.HasPrefix(db, "http://") {
		etcdURL = db
	} else {
		etcdURL = "http://" + db
	}

	e, err := etcd.New(etcdURL)
	if err != nil {
		log.Errorf("balance fail to connect to etcd due %v", err)
		return
	}

	t, err := balance.GenTryBalanceTask(cluster, e)
	if err != nil {
		log.Errorf("fail to init balance %s task due %v", cluster, err)
		return
	}

	err = t.Balance()
	if err != nil {
		log.Errorf("balance cluster %s err due %v", cluster, err)
	} else {
		log.Infof("succeed balance %s", cluster)
	}
}
