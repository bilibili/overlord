package anzi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"overlord/pkg/log"
	"strconv"
	"time"
)

// RBD CONSTANTS
const (
	RDB6BitLen  = 0
	RDB14BitLen = 1
	RDB32BitLen = 0x80
	RDB64BitLen = 0x81
	RDBEncVal   = 3

	RDBOpcodeModuleAux    = 247
	RDBOpcodeIdle         = 248
	RDBOpcodeFreq         = 249
	RDBOpcodeAux          = 250
	RDBOpcodeResizeDB     = 251
	RDBOpcodeExpireTimeMS = 252
	RDBOpcodeExpireTime   = 253
	RDBOpcodeSelectDB     = 254
	RDBOpcodeEOF          = 255

	RDBTypeString          = 0
	RDBTypeList            = 1
	RDBTypeSet             = 2
	RDBTypeZSet            = 3
	RDBTypeHash            = 4
	RDBTypeZset2           = 5 // ZSET version 2 with doubles stored in binary.
	RDBTypeModule          = 6
	RDBTypeModule2         = 7
	RDBTypeHashZipMap      = 9
	RDBTypeListZipList     = 10
	RDBTypeSetIntSet       = 11
	RDBTypeZsetZipList     = 12
	RDBTypeHashZipList     = 13
	RDBTypeListQuickList   = 14
	RDBTypeStreamListPacks = 15

	RDBEncodeInt8  = 0
	RDBEncodeInt16 = 1
	RDBEncodeInt32 = 2
	RDBEncodeLZF   = 3

	RDBModuleOpcodeEOF    = 0 // End of module value.
	RDBModuleOpcodeSInt   = 1
	RDBModuleOpcodeUInt   = 2
	RDBModuleOpcodeFloat  = 3
	RDBModuleOpcodeDouble = 4
	RDBModuleOpcodeString = 5
)

// RDBCallback is the callback interface defined to call
type RDBCallback interface {
	SelectDB(dbnum uint64)
	AuxField(key, data []byte)
	ResizeDB(size uint64)
	EndOfRDB()

	CmdSet(key, val []byte, expire uint64)
	// List Command
	CmdRPush(key, val []byte)

	// ZSet
	CmdZAdd(key []byte, score float64, val []byte)

	// Hash
	CmdHSet(key, field, value []byte)
	CmdHSetInt(key, field []byte, value int64)

	// Expire
	ExpireAt(key []byte, expiry uint64)
}

// CmdInfo is the struct for idle and freq info
// type CmdInfo struct {
// 	Idle uint64
// 	Freq byte
// }

// NewRDB build new rdb from reader
func NewRDB(rd *bufio.Reader, cb RDBCallback) *RDB {
	r := &RDB{
		rd: rd,
		cb: cb,
	}
	return r
}

// RDB is the struct for stream syncing rdb file from upstream
type RDB struct {
	rd *bufio.Reader

	version int

	expiry uint64
	idle   uint64
	freq   byte

	key []byte

	cb RDBCallback
}

// Start create new process in parse data from rdb
func (r *RDB) Start() {
	go func() {
		err := r.bgSyncProc()
		if err != nil {
			log.Errorf("fail to sync rdb in background due %s", err)
		}
	}()
}

func (r *RDB) bgSyncProc() (err error) {
	var (
		dbnum uint64
		dtype byte
	)

	headbuf := make([]byte, 9)
	_, err = io.ReadFull(r.rd, headbuf)
	if err != nil {
		return
	}
	// 1. verify REDIS SPECIAL string
	// 2. verify VERSION
	log.Infof("start syncing rdb with head %s", string(headbuf))
	for {
		r.expiry = 0
		r.idle = 0
		r.freq = 0

		dtype, err = r.rd.ReadByte()
		if err != nil {
			return err
		}

		dtype, err = r.readExpire(dtype)
		if err != nil {
			return
		}

		dtype, err = r.readIdleAndFreq(dtype)
		if err != nil {
			return
		}

		if dtype == RDBOpcodeSelectDB {
			dbnum, err = r.readLength()
			if err != nil {
				return err
			}
			r.cb.SelectDB(dbnum)
			continue
		}

		if dtype == RDBOpcodeAux {
			var auxKey []byte
			var auxVal []byte
			auxKey, err = r.readString()
			if err != nil {
				return
			}
			auxVal, err = r.readString()
			if err != nil {
				return
			}
			r.cb.AuxField(auxKey, auxVal)
			continue
		}

		if dtype == RDBOpcodeResizeDB {
			var dbsize uint64
			dbsize, err = r.readLength()
			if err != nil {
				return
			}
			r.cb.ResizeDB(dbsize)
			continue
		}

		if dtype == RDBOpcodeModuleAux {
			err = r.readModule()
			if err != nil {
				return
			}
			continue
		}

		if dtype == RDBOpcodeEOF {
			r.cb.EndOfRDB()
			if r.version >= 5 {
				_, err = io.ReadFull(r.rd, headbuf[:8])
				if err != nil {
					return
				}
			}
			return
		}

		r.key, err = r.readString()
		if err != nil {
			return
		}
		err = r.readObject(dtype)
		if err != nil {
			return
		}
	}
}

func (r *RDB) readIdleAndFreq(dtype byte) (ndtype byte, err error) {
	ndtype = dtype
	if ndtype == RDBOpcodeIdle {
		r.idle, err = r.readLength()
		if err != nil {
			return
		}
		ndtype, err = r.rd.ReadByte()
		if err != nil {
			return
		}
	}

	if dtype == RDBOpcodeFreq {
		r.freq, err = r.rd.ReadByte()
		if err != nil {
			return
		}
		ndtype, err = r.rd.ReadByte()
		if err != nil {
			return
		}
	}
	return
}

func (r *RDB) readModule() (err error) {
	// TODO: impl it
	return
}

//======================== tools func =================

func (r *RDB) readString() (data []byte, err error) {
	var (
		length uint64
	)

	length, err = r.readLength()
	if err == ErrEncVal {
		err = nil
		return r.readStringEnc(byte(length))
	} else if err != nil {
		return
	}

	data = make([]byte, length)
	_, err = io.ReadFull(r.rd, data)
	return
}

func (r *RDB) readStringEnc(flag byte) (data []byte, err error) {
	if flag == RDBEncodeInt8 {
		var b byte
		b, err = r.rd.ReadByte()
		if err != nil {
			return
		}
		data = []byte{b}
	} else if flag == RDBEncodeInt16 {
		data = make([]byte, 2)
		_, err = io.ReadFull(r.rd, data)
	} else if flag == RDBEncodeInt32 {
		data = make([]byte, 4)
		_, err = io.ReadFull(r.rd, data)
	} else if flag == RDBEncodeLZF {
		var (
			clen, ulen uint64
		)
		clen, err = r.readLength()
		if err != nil {
			return
		}
		ulen, err = r.readLength()
		if err != nil {
			return
		}
		compressed := make([]byte, clen)
		_, err = io.ReadFull(r.rd, compressed)
		data = lzfDecompress(compressed, int64(ulen))
	}
	return
}

// read with encval way but not error
var (
	ErrEncVal = errors.New("length is encoding value, may not error")
)

func (r *RDB) readLength() (length uint64, err error) {
	var (
		data byte
	)
	data, err = r.rd.ReadByte()
	if err != nil {
		return
	}

	flag := (data & 0xc0) >> 6
	if flag == RDBEncVal {
		length = uint64(data & 0x3f)
		err = ErrEncVal
		return
	}

	if flag == RDB6BitLen {
		length = uint64(data & 0x3f)
	} else if flag == RDB14BitLen {
		length = uint64(data&0x3f) << 8
		data, err = r.rd.ReadByte()
		if err != nil {
			return
		}
		length |= uint64(data)
	} else if data == RDB32BitLen {
		var l uint32
		err = binary.Read(r.rd, binary.BigEndian, &l)
		if err != nil {
			return
		}
		length = uint64(l)
	} else if data == RDB64BitLen {
		err = binary.Read(r.rd, binary.BigEndian, &length)
	}

	return
}

func (r *RDB) deferMakeExpireAt() {
	if r.expiry == 0 {
		return
	}
	now := uint64(time.Now().Unix()) * 1000
	r.expiry += now
}

func (r *RDB) readExpire(dtype byte) (ndtype byte, err error) {
	defer r.deferMakeExpireAt()

	ndtype = dtype
	if ndtype == RDBOpcodeExpireTimeMS {
		err = binary.Read(r.rd, binary.LittleEndian, &r.expiry)
		if err != nil {
			return
		}
		ndtype, err = r.rd.ReadByte()
		if err != nil {
			return
		}
	} else if ndtype == RDBOpcodeExpireTime {
		var expireSecond uint32
		err = binary.Read(r.rd, binary.LittleEndian, &expireSecond)
		if err != nil {
			return
		}
		r.expiry = uint64(expireSecond * 1000)
		ndtype, err = r.rd.ReadByte()
		if err != nil {
			return
		}
	}
	return
}

func (r *RDB) readBinaryDouble() (double float64, err error) {
	// TODO impl it
	return
}
func (r *RDB) readFloat() (double float64, err error) {
	// TODO impl it
	return
}

func (r *RDB) readObject(dtype byte) (err error) {
	var (
		data   []byte
		length uint64
	)
	if dtype == RDBTypeString {
		data, err = r.readString()
		if err != nil {
			return
		}
		r.cb.CmdSet(r.key, data, r.expiry)
	} else if dtype == RDBTypeList {
		length, err = r.readLength()
		if err != nil {
			return
		}
		for i := uint64(0); i < length; i++ {
			data, err = r.readString()
			if err != nil {
				return
			}
			r.cb.CmdRPush(r.key, data)
		}
		r.cb.ExpireAt(r.key, r.expiry)
	} else if dtype == RDBTypeZSet || dtype == RDBTypeZset2 {
		length, err = r.readLength()
		if err != nil {
			return
		}

		for i := uint64(0); i < length; i++ {
			data, err = r.readString()
			if err != nil {
				return
			}
			var score float64

			if dtype == RDBTypeZset2 {
				score, err = r.readBinaryDouble()
			} else {
				score, err = r.readFloat()
			}

			if err != nil {
				return
			}

			r.cb.CmdZAdd(r.key, score, data)
		}
		r.cb.ExpireAt(r.key, r.expiry)
	} else if dtype == RDBTypeHash {
		length, err = r.readLength()
		if err != nil {
			return
		}

		var (
			field []byte
			value []byte
		)

		for i := uint64(0); i < length; i++ {
			field, err = r.readString()
			if err != nil {
				return
			}
			value, err = r.readString()
			if err != nil {
				return
			}
			r.cb.CmdHSet(r.key, field, value)
		}

		r.cb.ExpireAt(r.key, r.expiry)
	} else if dtype == RDBTypeHashZipMap {
		err = r.readZipMap()
	} else if dtype == RDBTypeListZipList {
		err = r.readZipList()
	} else if dtype == RDBTypeSetIntSet {
		err = r.readIntSet()
	} else if dtype == RDBTypeZsetZipList {
		err = r.readZSetFromZiplist()
	} else if dtype == RDBTypeHashZipList {
		err = r.readHashFromZipList()
	} else if dtype == RDBTypeListQuickList {
		err = r.readListFromQuickList()
	} else if dtype == RDBTypeModule {
		err = fmt.Errorf("unable to parse module object from value of key %s", strconv.Quote(string(r.key)))
	} else if dtype == RDBTypeModule2 {
		err = r.readModule()
	} else if dtype == RDBTypeStreamListPacks {
		err = r.readStream()
	} else {
		panic("unreachable")
	}

	return
}

// TODO: done to all this
func (r *RDB) readZipMap() (err error) {
	var (
		data []byte
		num  byte
		free byte
	)
	data, err = r.readString()
	if err != nil {
		return
	}
	srd := bytes.NewReader(data)
	num, err = srd.ReadByte()

	var (
		nlen uint64
	)

	for {
		nlen, err = r.readZipMapNextLength(srd)
		if err != nil {
			return
		}
		if nlen == 0 {
			break
		}
		key := make([]byte, nlen)
		_, err = io.ReadFull(srd, key)
		if err != nil {
			return
		}

		nlen, err = r.readZipMapNextLength(srd)
		if err != nil {
			return
		}
		free, err = srd.ReadByte()
		if err != nil {
			return
		}
		val := make([]byte, nlen)
		_, err = io.ReadFull(srd, val)
		if err != nil {
			return
		}
		_, err = srd.Seek(int64(free), io.SeekCurrent)
		if err != nil {
			return
		}

		var ival int64
		ival, err = strconv.ParseInt(string(val), 10, 64)
		if err == nil {
			r.cb.CmdHSetInt(r.key, key, ival)
		} else {
			err = nil
			r.cb.CmdHSet(r.key, key, val)
		}

	}

	r.cb.ExpireAt(r.key, r.expiry)
	return
}

func (r *RDB) readZipMapNextLength(srd *bytes.Reader) (nlen uint64, err error) {
	var (
		data byte
	)
	data, err = srd.ReadByte()
	if err != nil {
		return
	}
	if data < 254 {
		nlen = uint64(data)
	} else if data == 254 {
		var value uint32
		err = binary.Read(srd, binary.LittleEndian, &value)
		nlen = uint64(value)
	}
	return
}

func (r *RDB) readZipList() (err error) {
	return
}
func (r *RDB) readIntSet() (err error) {
	return
}
func (r *RDB) readZSetFromZiplist() (err error) {
	return
}
func (r *RDB) readHashFromZipList() (err error) {
	return
}
func (r *RDB) readListFromQuickList() (err error) {
	return
}

func (r *RDB) readStream() (err error) {
	return
}
