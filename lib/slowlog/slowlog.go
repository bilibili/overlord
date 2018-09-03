package slowlog

import (
	"fmt"
	"os"
	"overlord/lib/log"
	"overlord/proto"
	"strings"
	"sync/atomic"
)

var (
	// mapping [cluster name] -> [slowlog_max_count]**proto.Message
	slowlogBucket map[string]*bucket
)

type bucket struct {
	cursor int64
	count  int64
	msgs   []atomic.Value
	ch     chan *proto.Message
}

func (b *bucket) push(req *proto.Message) {
	for {
		if atomic.CompareAndSwapInt64(&b.cursor, b.cursor, b.cursor+1) {
			idx := b.cursor % b.count
			b.msgs[idx].Store(req)

			// skip send when busy
			select {
			case b.ch <- req:
			default:
			}

			break
		}
	}
}

func (b *bucket) getSlowlog() []*proto.Message {
	msgs := []*proto.Message{}
	for _, v := range b.msgs {
		p := v.Load()
		if p == nil {
			break
		}
		msgs = append(msgs, p.(*proto.Message))
	}
	return msgs
}

// Init slowlog collector
func Init(name string, fpath string, slowlogMaxCount int) {
	if slowlogBucket == nil {
		slowlogBucket = make(map[string]*bucket)
	}

	b := &bucket{
		cursor: 0,
		count:  int64(slowlogMaxCount),
		msgs:   make([]atomic.Value, slowlogMaxCount),
		ch:     make(chan *proto.Message, 1024),
	}
	fd, err := os.Create(fpath)
	if err != nil {
		log.Errorf("slowlog path %s not exists", fpath)
		return
	}

	// spwan async func
	go func(mc chan *proto.Message) {
		defer fd.Close()
		defer close(mc)

		for {
			m := <-mc
			str := fmt.Sprintf("%s %s\n", name, m.AsSlowlog())
			_, err := fd.WriteString(str)

			if err != nil {
				log.Errorf("fail to write string due to %s", err)
				return
			}
		}
	}(b.ch)
	slowlogBucket[name] = b
}

// SendHandler will do and send back the *proto.Message into backend.
type SendHandler func(*proto.Message)

// GetSendHandler will get the collect chan by cluster name.
func GetSendHandler(name string) SendHandler {
	if _, ok := slowlogBucket[name]; ok {
		hd := sendHandlerFactory(name)
		return hd
	}

	log.Warnf("slowlog not enabled for %s", name)
	return func(_arg2 *proto.Message) {}
}

func sendHandlerFactory(name string) SendHandler {
	var hd SendHandler = func(p *proto.Message) {
		b := slowlogBucket[name]
		b.push(p)
	}
	return hd
}

// Get returns the Request object.
func Get(name string) []*proto.Message {
	b := slowlogBucket[name]
	return b.getSlowlog()
}

// GetString will get slowlog tesxt content, all fields was quoted.
// fields in each line:
//   start-time write-time total-duration remote-duration cmd-quoted-string
func GetString(name string) string {
	b := slowlogBucket[name]
	ms := b.getSlowlog()
	strs := make([]string, len(ms))
	for i, m := range ms {
		strs[i] = fmt.Sprintf("%s %d %s", name, i, m.AsSlowlog())
	}
	return strings.Join(strs, "\n")
}
