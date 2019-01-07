package myredis

import (
	"bufio"
	"io"
	"strconv"
)

// Resp type define
const (
	RespUnknown respType = '0'
	RespString  respType = '+'
	RespError   respType = '-'
	RespInt     respType = ':'
	RespBulk    respType = '$'
	RespArray   respType = '*'
)

// respType is the type of redis Resp
type respType = byte

type Resp struct {
	RType respType

	Data  []byte
	Array []*Resp
}

func (r *Resp) decode(br *bufio.ReadWriter) error {
	line, err := br.ReadBytes(byte('\n'))
	if err != nil && err != io.EOF {
		return err
	}

	r.RType = line[0]
	switch line[0] {
	case RespString, RespError, RespInt:
		r.decodePlain(line[1 : len(line)-2])
		return err
	case RespBulk:
		return r.decodeBulk(br, line[1:len(line)-2])
	case RespArray:
		return r.decodeArray(br, line[1:len(line)-2])
	default:
		panic("not support Resp")
	}
}

func (r *Resp) decodePlain(line []byte) {
	r.Data = line
}

func (r *Resp) decodeBulk(br *bufio.ReadWriter, line []byte) error {
	count, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return err
	}
	if count == -1 {
		return nil
	}
	buf := make([]byte, count+2)
	_, err = io.ReadFull(br, buf)
	r.Data = buf
	return err
}

func (r *Resp) decodeArray(br *bufio.ReadWriter, line []byte) error {
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

// NewCmd parse command
func NewCmd(command string) *Command {
	return &Command{command: command}
}

// Command is the exported command
type Command struct {
	command string
	Reply   *Resp
}

func (c *Command) execute(n *node) error {
	if c.Reply == nil {
		c.Reply = &Resp{}
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

	return c.Reply.decode(n.wr)
}
