package service

import (
	"overlord/pkg/myredis"
	"overlord/platform/api/dao"
	"overlord/platform/api/model"
)

// New create new service of overlord
func New(cfg *model.ServerConfig) *Service {
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
	cfg    *model.ServerConfig
}
