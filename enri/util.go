package enri

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
)

var (
	errAddr = errors.New("Err Addr")
)

func divide(n, m int) (res []int) {
	avg := n / m
	remain := n % m
	for i := 0; i < m; i++ {
		if i < remain {
			res = append(res, avg+1)
		} else {
			res = append(res, avg)
		}
	}
	return
}

func parseAddr(addr string) (ip, port string, err error) {
	strs := strings.Split(addr, ":")
	if len(strs) != 2 {
		err = errAddr
		return
	}
	return strs[0], strs[1], nil
}

func joinInts(is []int64, sep byte) string {
	if len(is) == 0 {
		return ""
	}
	if len(is) == 1 {
		return strconv.FormatInt(is[0], 10)
	}
	buf := bytes.NewBuffer([]byte{})
	for _, i := range is {
		buf.WriteString(strconv.FormatInt(i, 10))
		buf.WriteByte(sep)
	}
	if buf.Len() > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	s := buf.String()
	return s
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

func otherMaster(nodes []*Node, node *Node) (om []*Node) {
	for _, n := range nodes {
		if n.isMaster() && n.name != node.name {
			om = append(om, n)
		}
	}
	return
}
