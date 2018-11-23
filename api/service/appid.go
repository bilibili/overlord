package service

import (
	"context"
	"overlord/api/model"
)

// SearchAppids will search the given name
func (s *Service) SearchAppids(name string, page *model.QueryPage) ([]*model.Appid, error) {
	return s.d.SearchAppids(context.Background(), name, page)
}


// RemoveAppid the given appid
func (s *Service) RemoveAppid(appid string) error {
	return s.d.RemoveAppid(context.Background(), appid)
}
