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
func (s *Service) RemoveCluster(cname string) (string, error) {
	return s.d.RemoveCluster(context.Background(), cname)
}

// ScaleCluster will scale the given cluster with params.
// for redis cluster, number means scale chunk.
// for redis/memcache, number means scale numbers.
func (s *Service) ScaleCluster(p *model.ParamScale) (jobID string,err error) {
	jobID, err = s.d.ScaleCluster(context.Background(), p)
	return
}
