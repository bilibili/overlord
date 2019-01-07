package service

import (
	"overlord/api/dao"
	"overlord/config"
	"overlord/lib/myredis"
)

// New create new service of overlord
func New(cfg *config.ServerConfig) *Service {
	s := &Service{
		cfg:    cfg,
		client: myredis.New(),
		d:      dao.New(cfg),
	}
	go s.jobManager()
	return s
}

// Service is the struct for api server
type Service struct {
	d      *dao.Dao
	client *myredis.Client
	cfg    *config.ServerConfig
}
