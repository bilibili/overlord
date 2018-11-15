package service

import (
	"context"
	"overlord/api/model"
)

// CreateCluster will create new cluster
func (s *Service) CreateCluster(p *model.ParamCluster) (string, error) {
	return s.d.CreateCluster(context.Background(), p)
}

// GetCluster by given cluster name
func (s *Service) GetCluster(cname string) (*model.Cluster, error) {
	// TODO: make it cached
	return nil, nil
}
