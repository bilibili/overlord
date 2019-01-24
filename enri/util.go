package enri

import (
	"errors"
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
