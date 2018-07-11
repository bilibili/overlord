package redis

import libnet "overlord/lib/net"

type pinger struct {
}

func newPinger(conn *libnet.Conn) *pinger {
	return &pinger{}
}

func (p *pinger) ping() (err error) {
	return
}
