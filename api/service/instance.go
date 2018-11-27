package service

import (
	"context"
)

// SetInstanceWeight will search the given name
func (s *Service) SetInstanceWeight(addr string, weight int) error {
	return s.d.SetInstanceWeight(context.Background(), addr, weight)
}
