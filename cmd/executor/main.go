package main

import (
	"context"
	"flag"

	"overlord/pkg/log"
	"overlord/platform/mesos"
	"overlord/version"
)

func main() {
	flag.Parse()
	if version.ShowVersion() {
		return
	}

	ec := mesos.New()
	log.InitHandle(log.NewStdHandler())
	ec.Run(context.Background())
}
