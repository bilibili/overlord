package chunk

import (
	"fmt"

	ms "github.com/mesos/mesos-go/api/v1/lib"
)

// Addr is a cache instance endpoint
type Addr struct {
	IP   string
	Port int
}

// Dist is the distribution of mesos
type Dist struct {
	Addrs []*Addr
}

func (a *Addr) String() string {
	return fmt.Sprintf("%s:%d", a.IP, a.Port)
}

func mapHostResIntoDist(hrs []*hostRes, portsMap map[string][]int) *Dist {
	addrs := make([]*Addr, 0)
	portsCountMap := make(map[string]int)
	for name := range portsMap {
		portsCountMap[name] = 0
	}

	for _, hr := range hrs {
		count := portsCountMap[hr.name]
		addrs = append(addrs, &Addr{IP: hr.name, Port: portsMap[hr.name][count]})
		portsCountMap[hr.name] = count + 1
	}

	return &Dist{Addrs: addrs}
}

// DistIt will cacluate Dist by the given offer.
func DistIt(num int, mem, cpu float64, offers ...ms.Offer) (dist *Dist, err error) {
	hrs := dpFillHostRes(mapIntoHostRes(offers, mem, cpu), num, 1)
	if !checkDist(hrs, num) {
		err = ErrBadDist
		return
	}
	portsMap := mapIntoPortsMap(offers)
	dist = mapHostResIntoDist(hrs, portsMap)
	return
}

// DistAppendIt will re-dist it by append new nodes.
func DistAppendIt(dist *Dist, num int, memory, cpu float64, offers ...ms.Offer) (err error) {
	return
}
