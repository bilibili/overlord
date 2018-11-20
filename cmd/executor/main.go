package main

import (
	"context"
	"overlord/lib/log"
	"overlord/mesos"
)

func main() {
	ec := mesos.New()
	log.Init(log.NewStdHandler())
	ec.Run(context.Background())

}
