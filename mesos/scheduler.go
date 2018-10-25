package mesos

import (
	"overlord/lib/store"
)

// Scheduler mesos scheduler.
type Scheduler struct {
	Tchan chan *Task
	db    store.DB
}

// NewScheduler new scheduler instance.
func NewScheduler(db store.DB) *Scheduler {
	return &Scheduler{
		db: db,
	}
}

