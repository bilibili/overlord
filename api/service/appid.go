package service

import (
	"context"
	"overlord/api/model"
)

// GetTreeAppid get the grouped all result
func (s *Service) GetTreeAppid() ([]*model.TreeAppid, error) {
	return s.d.GetTreeAppid(context.Background())
}

// GetGroupedAppid will query the grouped cluster by appid
func (s *Service) GetGroupedAppid(appid string) (*model.GroupedAppid, error) {
	return s.d.GetGroupedAppid(context.Background(), appid)
}

// RemoveAppid the given appid
func (s *Service) RemoveAppid(appid string) error {
	return s.d.RemoveAppid(context.Background(), appid)
}
