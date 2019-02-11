package anzi

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"bufio"

	"github.com/stretchr/testify/assert"
)

type MockMeta struct {
	Calls map[string]int
}

func _buildCB() *mockRDBCallback {
	m := MockMeta{
		Calls: make(map[string]int),
	}
	r := &mockRDBCallback{
		MockMeta: m,
	}
	return r
}

func (m *MockMeta) getFuncName(deep int) string {
	pc, _, _, ok := runtime.Caller(deep)
	// fmt.Println(reflect.TypeOf(pc), reflect.ValueOf(pc))
	if ok {
		funcname := runtime.FuncForPC(pc).Name()     // main.(*MyStruct).foo
		funcname = filepath.Ext(funcname)            // .foo
		funcname = strings.TrimPrefix(funcname, ".") // foo
		return funcname
	}
	return "anonymousFunc"
}

func (m *MockMeta) Record(args ...interface{}) {
	fname := m.getFuncName(2)
	if v, ok := m.Calls[fname]; ok {
		m.Calls[fname] = v + 1
	} else {
		m.Calls[fname] = 1
	}
	// fmt.Printf("call %s with args", fname)
	// for _, arg := range args {
	// 	fmt.Printf("\t@%s#", arg)
	// }
	// fmt.Println()
}

type mockRDBCallback struct {
	MockMeta
}

func (r *mockRDBCallback) SelectDB(dbnum uint64) {
	r.Record(fmt.Sprintf("%d", dbnum))
}
func (r *mockRDBCallback) AuxField(key, data []byte) {
	r.Record(key, data)
}
func (r *mockRDBCallback) ResizeDB(size, esize uint64) {
	r.Record(fmt.Sprintf("%d", size), fmt.Sprintf("%d", size))
}
func (r *mockRDBCallback) EndOfRDB() {
	r.Record()
}
func (r *mockRDBCallback) CmdSet(key, val []byte, expire uint64) {
	r.Record(key, val, fmt.Sprintf("%d", expire))
}

// List Command
func (r *mockRDBCallback) CmdRPush(key, val []byte) {
	r.Record(key, val)
}

// Set
func (r *mockRDBCallback) CmdSAdd(key, val []byte) {
	r.Record(key, val)
}

// ZSet
func (r *mockRDBCallback) CmdZAdd(key []byte, score float64, val []byte) {
	r.Record(key, fmt.Sprintf("%f", score), val)
}

// Hash
func (r *mockRDBCallback) CmdHSet(key, field, value []byte) {
	r.Record(key, field, value)
}

func (r *mockRDBCallback) CmdHSetInt(key, field []byte, value int64) {
	r.Record(key, field, fmt.Sprintf("%d", value))
}

// Expire
func (r *mockRDBCallback) ExpireAt(key []byte, expiry uint64) {
	r.Record(key, fmt.Sprintf("%d", expiry))
}

func _loadRDB(name string) (*bytes.Buffer, error) {
	_, filename, _, _ := runtime.Caller(4)
	basePath, err := filepath.Abs(filepath.Dir(filename))
	if err != nil {
		return nil, err
	}

	fname := fmt.Sprintf("%s/dumps/%s", basePath, name)

	buf, err := ioutil.ReadFile(fname)
	b := bytes.NewBuffer(buf)
	return b, err
}

func TestParseEmptyRDBOk(t *testing.T) {
	buf, err := _loadRDB("empty_database.rdb")
	assert.NoError(t, err, "should load db ok")

	cb := _buildCB()
	rdb := NewRDB(bufio.NewReader(buf), cb)

	err = rdb.bgSyncProc()
	assert.NoError(t, err)
	assert.Contains(t, cb.MockMeta.Calls, "EndOfRDB")
	assert.Len(t, cb.MockMeta.Calls, 1)
}

func TestMultipleDatabase(t *testing.T) {
	buf, err := _loadRDB("multiple_databases.rdb")
	assert.NoError(t, err, "should load db ok")

	cb := _buildCB()
	rdb := NewRDB(bufio.NewReader(buf), cb)

	err = rdb.bgSyncProc()
	assert.NoError(t, err)
	assert.Contains(t, cb.MockMeta.Calls, "EndOfRDB")
	assert.Contains(t, cb.MockMeta.Calls, "SelectDB")
}

func TestKeysWithExpire(t *testing.T) {
	buf, err := _loadRDB("keys_with_expiry.rdb")
	assert.NoError(t, err, "should load db ok")
	cb := _buildCB()
	rdb := NewRDB(bufio.NewReader(buf), cb)
	err = rdb.bgSyncProc()

	assert.NoError(t, err)
	assert.Len(t, cb.MockMeta.Calls, 3)
	assert.Equal(t, uint64(0), rdb.expiry)
}

func TestInterKeys(t *testing.T) {
	buf, err := _loadRDB("integer_keys.rdb")
	assert.NoError(t, err, "should load db ok")
	cb := _buildCB()
	rdb := NewRDB(bufio.NewReader(buf), cb)
	err = rdb.bgSyncProc()
	assert.NoError(t, err)
	assert.Len(t, cb.MockMeta.Calls, 3)
}

func TestStringKeysWithCompression(t *testing.T) {
	buf, err := _loadRDB("integer_keys.rdb")
	assert.NoError(t, err, "should load db ok")
	cb := _buildCB()
	rdb := NewRDB(bufio.NewReader(buf), cb)
	err = rdb.bgSyncProc()
	assert.NoError(t, err)
}

var allRdbs = []string{
	"dictionary", "easily_compressible_string_key", "empty_database",
	"hash_as_ziplist", "integer_keys", "intset_16", "intset_32", "intset_64",
	"keys_with_expiry", "linkedlist", "multiple_databases", "non_ascii_values",
	"parser_filters", "rdb_version_5_with_checksum", "rdb_version_8_with_64b_length_and_scores",
	"redis_40_with_module", "redis_50_with_streams", "regular_set", "regular_sorted_set",
	"sorted_set_as_ziplist", "uncompressible_string_keys", "ziplist_that_compresses_easily",
	"ziplist_that_doesnt_compress", "ziplist_with_integers", "zipmap_that_compresses_easily",
	"zipmap_that_doesnt_compress", "zipmap_with_big_values",
}

func TestAllParsingRDB(t *testing.T) {

	for _, rname := range allRdbs {
		t.Run(rname, func(tt *testing.T) {
			buf, err := _loadRDB(rname + ".rdb")
			assert.NoError(tt, err, "should load db ok")
			cb := _buildCB()
			rdb := NewRDB(bufio.NewReader(buf), cb)
			err = rdb.bgSyncProc()
			assert.NoError(tt, err)
		})
	}
}
