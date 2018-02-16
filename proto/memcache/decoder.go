package memcache

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// memcached protocol: https://github.com/memcached/memcached/blob/master/doc/protocol.txt

type decoder struct {
	br *bufio.Reader

	err error
}

func (d *decoder) Decode() (req *Request, err error) {
	data, err := d.br.ReadString('\n')
	start := time.Now()

	if err != nil {
		if err == io.EOF {
			log.Println("Connection closed")
		} else {
			log.Printf("Error while reading text command line: %s\n", err.Error())
		}
		err = errors.Wrap(err, "Error while reading text command line from decoder")
		return
	}

	clParts := strings.Split(strings.TrimSpace(data), " ")

	switch clParts[0] {
	case "set":
		return setRequest(d.br, clParts, RequestSet)
	case "add":
		return setRequest(d.br, clParts, RequestAdd)
	case "replace":
		return setRequest(d.br, clParts, RequestReplace)
	case "append":
		return setRequest(d.br, clParts, RequestAppend)
	case "prepend":
		return setRequest(d.br, clParts, RequestPrepend)
	case "get":
		if len(clParts) < 2 {
			return nil, ErrBadRequest
		}

		var keys [][]byte
		for _, key := range clParts[1:] {
			keys = append(keys, []byte(key))
		}

		opaques := make([]uint32, len(keys))
		quiet := make([]bool, len(keys))

		return &Request{
			Keys:    keys,
			Opaques: opaques,
			Quiet:   quiet,
			NoopEnd: false,
		}, common.RequestGet, start, nil

	case "delete":
		if len(clParts) != 2 {
			return nil, ErrBadRequest
		}

		return &Request{
			Key:    []byte(clParts[1]),
			Opaque: uint32(0),
		}, common.RequestDelete, start, nil

	// TODO: Error handling for invalid cmd line
	case "touch":
		if len(clParts) != 3 {
			return nil, ErrBadRequest
		}

		key := []byte(clParts[1])

		exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
		if err != nil {
			log.Printf("Error parsing ttl for touch command: %s\n", err.Error())
			return nil, ErrBadRequest
		}

		return &Request{
			Key:     key,
			Exptime: uint32(exptime),
			Opaque:  uint32(0),
		}, common.RequestTouch, start, nil
	case "noop":
		if len(clParts) != 1 {
			return nil, ErrBadRequest
		}
		return &Request{
			Opaque: 0,
		}, common.RequestNoop, start, nil

	case "quit":
		if len(clParts) != 1 {
			return nil, common.RequestQuit, start, common.ErrBadRequest
		}
		return &Request{
			Opaque: 0,
			Quiet:  false,
		}, common.RequestQuit, start, nil

	case "version":
		if len(clParts) != 1 {
			return nil, common.RequestQuit, start, common.ErrBadRequest
		}
		return &Request{
			Opaque: 0,
		}, common.RequestVersion, start, nil

	default:
		return nil, common.RequestUnknown, start, nil
	}
}

func setRequest(r *bufio.Reader, clParts []string, reqType RequestType) (req *Request, err error) {
	// sanity check
	if len(clParts) != 5 {
		return nil, ErrBadRequest
	}

	key := []byte(clParts[1])
	buf := bytes.NewBuffer()

	flags, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
	if err != nil {
		log.Printf("Error parsing flags for set/add/replace command: %s\n", err.Error())
		return nil, ErrBadFlags
	}
	buf.WriteString(clParts[2])
	buf.WriteByte(' ')

	exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[3]), 10, 32)
	if err != nil {
		log.Printf("Error parsing ttl for set/add/replace command: %s\n", err.Error())
		return nil, ErrBadExptime
	}
	buf.WriteString(clParts[3])
	buf.WriteByte(' ')

	length, err := strconv.ParseUint(strings.TrimSpace(clParts[4]), 10, 32)
	if err != nil {
		log.Printf("Error parsing length for set/add/replace command: %s\n", err.Error())
		return nil, ErrBadLength
	}
	buf.WriteString(clParts[4])
	buf.WriteByte(' ')

	// Read in data
	bs := make([]byte, length) // TODO: reuse bytes
	n, err := io.ReadAtLeast(r, bs, int(length))
	if err != nil {
		return nil, ErrInternal
	}
	buf.Write(bs)
	buf.WriteString("\r\n")

	// Consume the last two bytes "\r\n"
	r.ReadString(byte('\n'))

	return &Request{
		Type:  reqType,
		Key:   key,
		Value: buf.Bytes(),
	}, nil
}
