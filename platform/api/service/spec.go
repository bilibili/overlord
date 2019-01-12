package service

import "context"

// GetAllSpecs will get all specifications.
func (s *Service) GetAllSpecs() ([]string, error) {
	return s.d.GetAllSpecs(context.Background())
}

// RemoveSpec will remove the given specification
func (s *Service) RemoveSpec(spec string) error {
	return s.d.RemoveSpec(context.Background(), spec)
}
