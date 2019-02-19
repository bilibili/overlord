package service

import "overlord/pkg/myredis"

// Execute command into the given addr
func (s *Service) Execute(addr string, cmd string, arg ...string) (*myredis.Command, error) {
	return s.client.Execute(addr, myredis.NewCmd(cmd).Arg(arg...))
}
