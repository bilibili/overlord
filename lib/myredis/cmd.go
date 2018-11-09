package myredis

import (
	"bufio"
	"io"
	"strconv"
)

// resp type define
const (
	respUnknown respType = '0'
	respString  respType = '+'
	respError   respType = '-'
	respInt     respType = ':'
	respBulk    respType = '$'
	respArray   respType = '*'
)

// respType is the type of redis resp
type respType = byte

type resp struct {
	rtype respType

	data  []byte
	array []*resp
}

func (r *resp) decode(br *bufio.ReadWriter) error {
	line, err := br.ReadBytes(byte('\n'))
	if err != nil && err != io.EOF {
		return err
	}

	r.rtype = line[0]
	switch line[0] {
	case respString, respError, respInt:
		r.decodePlain(line[1 : len(line)-2])
		return err
	case respBulk:
		return r.decodeBulk(br, line[1:len(line)-2])
	case respArray:
		return r.decodeArray(br, line[1:len(line)-2])
	default:
		panic("not support resp")
	}
}

func (r *resp) decodePlain(line []byte) {
	r.data = line
}

func (r *resp) decodeBulk(br *bufio.ReadWriter, line []byte) error {
	count, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return err
	}
	if count == -1 {
		return nil
	}
	buf := make([]byte, count+2)
	_, err = io.ReadFull(br, buf)
	return err
}

func (r *resp) decodeArray(br *bufio.ReadWriter, line []byte) error {
	count, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return err
	}

	if count == -1 {
		return nil
	}

	for i := 0; i < int(count); i++ {
		err = r.decode(br)
		if err != nil {
			return err
		}
	}

	return nil
}

func newCmd(command string) *cmd {
	return &cmd{command: command}
}

type cmd struct {
	command string
	reply   *resp
}

func (c *cmd) execute(n *node) error {
	if c.reply == nil {
		c.reply = &resp{}
	}
	n.wr.WriteString(c.command)
	_, err := n.wr.WriteString("\r\n")
	if err != nil {
		return err
	}

	err = n.wr.Flush()
	if err != nil {
		return err
	}

	return c.reply.decode(n.wr)
}
