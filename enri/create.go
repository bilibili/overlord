package enri

import "errors"

var (
	errMasterCount = errors.New("err num of master")
)

// Create create cluster.
func Create(addrs []string, slave int) (c *Cluster, err error) {
	master := len(addrs) / (slave + 1)
	if master < 3 {
		err = errMasterCount
		return
	}
	var nodes = make([]*Node, 0, len(addrs))
	for _, addr := range addrs {
		var node *Node
		node, err = NewNode(addr)
		if err != nil {
			return
		}
		node.Init()
		nodes = append(nodes, node)
	}

	c = &Cluster{
		nodes:       nodes,
		masterCount: master,
		slaveCount:  slave,
	}
	c.initSlot()
	return
}

func spread(hosts map[string][]*Node, num int) (nodes []*Node) {
	var total int
	for _, host := range hosts {
		total += len(host)
	}
	if total < num {
		return
	}
	for {
		for host, n := range hosts {
			if len(nodes) >= num {
				return
			}
			nodes = append(nodes, n[0])
			hosts[host] = n[1:]
		}
	}
}

func splitSlot(n, m int) (slots [][2]int) {
	chunk := divide(n, m)
	var total int
	for _, c := range chunk {
		slots = append(slots, [2]int{total, total + c})
		total += c
	}
	return
}

func distributeSlave(masters, slaves []*Node) {
	inuse := make(map[string]*Node)
	var i int
	for {
		for _, master := range masters {
			for _, slave := range slaves {
				key := slave.ip + slave.port
				if _, ok := inuse[key]; ok {
					continue
				}
				inuse[key] = slave
				i++
				slave.slaveof = master.name
				break
			}
		}
		if i >= len(slaves) {
			return
		}
	}
}
