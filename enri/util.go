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
