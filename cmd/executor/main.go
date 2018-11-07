package main

import (
	"context"
	"overlord/mesos"
)

func main() {
	ec := mesos.New()
	ec.Subscribe(context.Background())
}
