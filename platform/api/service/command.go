package service

import "overlord/pkg/myredis"

// Execute command into the given addr
func (s *Service) Execute(addr, cmd string) (*myredis.Command, error) {
	return s.client.Execute(addr, cmd)
}
