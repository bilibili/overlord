package service

import "overlord/platform/api/model"

// GetAllGroups will load groups from file path
func (s *Service) GetAllGroups() []*model.Group {
	groups := make([]*model.Group, 0, len(s.cfg.Groups))
	for slug, nameCN := range s.cfg.Groups {
		groups = append(groups, &model.Group{
			Name:   slug,
			NameCN: nameCN,
		})
	}
	return groups
}
