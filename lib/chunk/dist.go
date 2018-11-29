package chunk

import (
	"fmt"
	"sort"

	ms "github.com/mesos/mesos-go/api/v1/lib"
)

// Addr is a cache instance endpoint
type Addr struct {
	ID   string // genid ,also be used as alias in singleton.
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

func mapHostResIntoDist(name string, idx int, hrs []*hostRes, portsMap map[string][]int) *Dist {
	addrs := make([]*Addr, 0)
	portsCountMap := make(map[string]int)
	for name := range portsMap {
		portsCountMap[name] = 0
	}

	for _, hr := range hrs {
		for i := 0; i < hr.count; i++ {
			count := portsCountMap[hr.name]
			idx++
			addrs = append(addrs, &Addr{IP: hr.name, Port: portsMap[hr.name][count]})
			portsCountMap[hr.name] = count + 1
		}
	}

	return &Dist{Addrs: addrs}
}

// DistIt will cacluate Dist by the given offer.
func DistIt(name string, num int, mem, cpu float64, offers ...ms.Offer) (dist *Dist, err error) {
	hrs := dpFillHostRes(nil, nil, mapIntoHostRes(offers, mem, cpu), num, 1)
	if !checkDist(hrs, num) {
		err = ErrBadDist
		return
	}
	portsMap := mapIntoPortsMap(offers)
	dist = mapHostResIntoDist(name, 0, hrs, portsMap)
	return
}

// DistAppendIt will re-dist it by append new nodes.
func DistAppendIt(name string, dist *Dist, num int, memory, cpu float64, offers ...ms.Offer) (newDist *Dist, err error) {
	if num <= 0 {
		return
	}
	hrs := mapIntoHostRes(offers, memory, cpu)
	hrm := make(map[string]*hostRes, len(hrs))
	sort.Sort(byCountDesc(hrs))
	for _, hr := range hrs {
		hrm[hr.name] = hr
	}
	addrm := make(map[string]int)
	for _, addr := range dist.Addrs {
		addrm[addr.IP] = addrm[addr.IP] + 1
	}
	// try get resource never dist before
	dhr := make(map[string]int)
	for num > 0 {
		allused := true
		for _, hr := range hrs {
			if _, ok := addrm[hr.name]; !ok && num > 0 {
				allused = false
				dhr[hr.name] = dhr[hr.name] + 1
				addrm[hr.name] = addrm[hr.name] + 1
				num = num - 1
			}
			if num <= 0 {
				break
			}
		}
		if allused {
			break
		}
	}
	oldDist := make([]*hostRes, 0, len(addrm))
	for addr, count := range addrm {
		oldDist = append(oldDist, &hostRes{name: addr, count: count})

	}
	sort.Sort(byCountAsc(oldDist))
	// can distribution  with new host,try to use min deployed host.
	for num > 0 {
		for _, addr := range oldDist {
			if _, ok := hrm[addr.name]; ok && num > 0 {
				dhr[addr.name] = dhr[addr.name] + 1
				num = num - 1
			}
		}
	}
	newHrs := make([]*hostRes, 0)
	for addr, count := range dhr {
		newHrs = append(newHrs, &hostRes{name: addr, count: count})
	}
	portsMap := mapIntoPortsMap(offers)
	newDist = mapHostResIntoDist(name, len(dist.Addrs), newHrs, portsMap)
	return
}
