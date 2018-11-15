package service

import (
	"overlord/api/dao"
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
