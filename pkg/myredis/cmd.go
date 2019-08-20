package myredis

import (
	"bufio"
	"fmt"
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
	line, err := br.ReadBytes('\n')
	if err != nil {
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
	r.Data = line
	count, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return err
	}
	if count == -1 {
		return nil
	}
	for i := 0; i < int(count); i++ {
		ar := new(Resp)
		err = ar.decode(br)
		if err != nil {
			return err
		}
		r.Array = append(r.Array, ar)
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
	args    []string
	Reply   *Resp
}

func (c *Command) Arg(arg ...string) *Command {
	c.args = append(c.args, arg...)
	return c
}

func (c *Command) String() string {
	content := fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n", len(c.args)+1, len(c.command), c.command)
	for _, a := range c.args {
		content = fmt.Sprintf("%s$%d\r\n%s\r\n", content, len(a), a)
	}
	return content
}

func (c *Command) execute(n *node) error {
	if c.Reply == nil {
		c.Reply = &Resp{}
	}
	_, err := n.wr.WriteString(c.String())
	if err != nil {
		return err
	}

	err = n.wr.Flush()
	if err != nil {
		return err
	}

	return c.Reply.decode(n.wr)
}
