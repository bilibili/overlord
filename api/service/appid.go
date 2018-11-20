package service

import (
	"context"
	"overlord/api/model"
)

// SearchAppids will search the given name
func (s *Service) SearchAppids(name string, page *model.QueryPage) ([]*model.Appid, error) {
	return s.d.SearchAppids(context.Background(), name, page)
}
