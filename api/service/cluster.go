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
	return s.d.GetCluster(context.Background(), cname)
}

// GetClusters will get all clusters
func (s *Service) GetClusters() ([]*model.Cluster, error) {
	return s.d.GetClusters(context.Background())
}


// RemoveCluster will remove the cluster if the cluster is not associated with other appids
func (s *Service) RemoveCluster(cname string) (*model.Cluster, error) {
	return nil, nil
}
