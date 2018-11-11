package service

import (
	"context"
	"overlord/api/dao"
	"overlord/api/model"
)

// Service is the struct for api server
type Service struct {
	d *dao.Dao
}

// CreateCluster will create new cluster
func (s *Service) CreateCluster(p *model.ParamCluster) (int64, error) {
	return s.d.CreateCluster(context.Background(), p)
}
