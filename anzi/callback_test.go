package anzi

import (
	"bufio"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCallback(t *testing.T) {
	for _, rname := range allRdbs {
		t.Run(rname, func(tt *testing.T) {
			buf, err := _loadRDB(rname + ".rdb")
			assert.NoError(tt, err, "should load db ok")
			cb := NewProtocolCallbacker("127.0.0.1:6379")
			rdb := NewRDB(bufio.NewReader(buf), cb)
			err = rdb.bgSyncProc()
			assert.NoError(tt, err)
		})
	}
}
