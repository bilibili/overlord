package service

import (
	"context"
	"overlord/platform/api/model"
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
func (s *Service) GetClusters(name string) ([]*model.Cluster, error) {
	return s.d.GetClusters(context.Background(), name)
}

// RemoveCluster will remove the cluster if the cluster is not associated with other appids
func (s *Service) RemoveCluster(cname string) (string, error) {
	return s.d.RemoveCluster(context.Background(), cname)
}

// ScaleCluster will scale the given cluster with params.
// for redis cluster, number means scale chunk.
// for redis/memcache, number means scale numbers.
func (s *Service) ScaleCluster(p *model.ParamScale) (jobID string, err error) {
	jobID, err = s.d.ScaleCluster(context.Background(), p)
	return
}

// AssignAppid will asign appid and cluster
func (s *Service) AssignAppid(cname, appid string) error {
	sub, cancel := context.WithCancel(context.Background())
	defer cancel()
	return s.d.AssignAppid(sub, cname, appid)
}

// UnassignAppid will unasign appid and cluster
func (s *Service) UnassignAppid(cname, appid string) error {
	sub, cancel := context.WithCancel(context.Background())
	defer cancel()
	return s.d.UnassignAppid(sub, cname, appid)
}
