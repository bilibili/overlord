package service

import (
	"context"
)

// SetInstanceWeight will search the given name
func (s *Service) SetInstanceWeight(addr string, weight int) error {
	return s.d.SetInstanceWeight(context.Background(), addr, weight)
}

// RestartInstance will try send restart job to etcd
func (s *Service) RestartInstance(cname string, addr string) (string, error) {
	return s.d.RestartInstance(context.Background(), cname, addr)
}
