package service

import "overlord/api/model"

// GetAllVersions will load version from file path
func (s *Service)GetAllVersions() ([]*model.Version, error) {
	versions := make([]*model.Version, len(s.cfg.Versions))
	for i, version := range  s.cfg.Versions {
		versions[i] = &model.Version{CacheType: version.CacheType, Versions: version.Versions}
	}
	return versions, nil
}
