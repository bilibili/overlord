package service

import (
	"context"
	"overlord/api/dao"
	"overlord/api/model"
	"overlord/config"
)

// New create new service of overlord
func New(cfg *config.ServerConfig) *Service {
	s := &Service{
		cfg: cfg,
		d:   dao.New(cfg),
	}

	return s
}

// Service is the struct for api server
type Service struct {
	d   *dao.Dao
	cfg *config.ServerConfig
}

// CreateCluster will create new cluster
func (s *Service) CreateCluster(p *model.ParamCluster) (string, error) {
	return s.d.CreateCluster(context.Background(), p)
}
