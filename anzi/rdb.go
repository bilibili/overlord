package anzi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"

	"overlord/pkg/log"
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

// Sync create new process in parse data from rdb
func (r *RDB) Sync() (conn net.Conn, err error) {
	err = r.bgSyncProc()
	conn = r.cb.GetConn()
	return
}

func (r *RDB) verifyHeader(hd []byte) error {
	if !bytes.HasPrefix(hd, []byte("REDIS")) {
		return fmt.Errorf("bad RESP HEADER %s", strconv.Quote(string(hd)))
	}
	version, err := strconv.ParseInt(string(hd[5:]), 10, 64)
	if err != nil {
		return err
	}
	r.version = int(version)
	return nil
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
	err = r.verifyHeader(headbuf)
	if err != nil {
		return err
	}

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
			var (
				dbsize uint64
				esize  uint64
			)
			dbsize, err = r.readLength()
			if err != nil {
				return
			}

			esize, err = r.readLength()
			if err != nil {
				return
			}
			r.cb.ResizeDB(dbsize, esize)
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
			log.Infof("finish rdb rading of upstream")
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
	_, err = r.readLength()
	if err != nil && err != ErrEncVal {
		return
	}
	err = r.skipModule()
	return
}

func (r *RDB) skipModule() (err error) {
	var (
		opcode uint64
	)
	opcode, err = r.readLength()
	for opcode != RDBModuleOpcodeEOF {
		switch opcode {
		case RDBModuleOpcodeSInt, RDBModuleOpcodeUInt:
			_, err = r.readLength()
			if err != nil {
				return
			}
		case RDBModuleOpcodeFloat:
			_, err = r.readBinaryFloat()
			if err != nil {
				return
			}
		case RDBModuleOpcodeDouble:
			_, err = r.readBinaryDouble()
			if err != nil {
				return
			}
		case RDBModuleOpcodeString:
			_, err = r.readString()
			if err != nil {
				return
			}
		default:
			err = fmt.Errorf("unknown module opcode %d", opcode)
			return
		}

		opcode, err = r.readLength()
		if err != nil {
			return
		}
	}

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
		data = []byte(fmt.Sprintf("%d", b))
	} else if flag == RDBEncodeInt16 {
		var ival int16
		err = binary.Read(r.rd, binary.LittleEndian, &ival)
		if err != nil {
			return
		}
		data = []byte(fmt.Sprintf("%d", ival))
	} else if flag == RDBEncodeInt32 {
		var ival int32
		err = binary.Read(r.rd, binary.LittleEndian, &ival)
		if err != nil {
			return
		}
		data = []byte(fmt.Sprintf("%d", ival))
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

func (r *RDB) readExpire(dtype byte) (ndtype byte, err error) {
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

func (r *RDB) readBinaryDouble() (f float64, err error) {
	err = binary.Read(r.rd, binary.LittleEndian, &f)
	return
}

func (r *RDB) readBinaryFloat() (f float64, err error) {
	var f32 float32
	err = binary.Read(r.rd, binary.LittleEndian, &f32)
	f = float64(f32)
	return
}

func (r *RDB) readFloat() (double float64, err error) {
	var dbl byte
	dbl, err = r.rd.ReadByte()
	if err != nil {
		return
	}
	if dbl == 253 {
		double = math.NaN()
	} else if dbl == 254 {
		double = math.Inf(1)
	} else if dbl == 255 {
		double = math.Inf(-1)
	}
	buf := make([]byte, dbl)
	_, err = io.ReadFull(r.rd, buf)
	if err != nil {
		return
	}
	double, err = strconv.ParseFloat(string(buf), 64)
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
	} else if dtype == RDBTypeSet {
		length, err = r.readLength()
		if err != nil {
			return
		}
		for i := uint64(0); i < length; i++ {
			data, err = r.readString()
			if err != nil {
				return
			}
			r.cb.CmdSAdd(r.key, data)
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
		err = fmt.Errorf("unreacheable dtype(%d) for key %s", dtype, r.key)
	}

	return
}

// TODO: done to all this
func (r *RDB) readZipMap() (err error) {
	var (
		data []byte
		free byte
	)
	data, err = r.readString()
	if err != nil {
		return
	}
	srd := bytes.NewReader(data)
	_, err = srd.ReadByte()
	if err != nil {
		return
	}

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
	var (
		// zl, tailOffset         uint32
		numEntries uint16
		srd        *bytes.Reader
		zlentry    []byte
	)
	srd, _, _, numEntries, err = r.readZipListHeader()
	if err != nil {
		return
	}

	for i := uint16(0); i < numEntries; i++ {
		zlentry, err = r.readZipListEntry(srd)
		if err != nil {
			return
		}
		r.cb.CmdRPush(r.key, zlentry)
	}
	err = r.readZipListEnd(srd)
	if err != nil {
		return
	}
	r.cb.ExpireAt(r.key, r.expiry)
	return
}

func (r *RDB) readZipListEntry(srd *bytes.Reader) (zlentry []byte, err error) {
	var (
		length     uint32
		preLenByte byte
		preLen     uint32
		header     byte
	)
	preLenByte, err = srd.ReadByte()
	if err != nil {
		return
	}

	if preLenByte == 254 {
		err = binary.Read(srd, binary.LittleEndian, &preLen)
		if err != nil {
			return
		}
	} else {
		preLen = uint32(preLenByte)
	}

	header, err = srd.ReadByte()
	if err != nil {
		return
	}

	if (header >> 6) == 0 {
		zlentry = make([]byte, header&0x3f)
		_, err = io.ReadFull(srd, zlentry)
		if err != nil {
			return
		}
	} else if (header >> 6) == 1 {
		var nextByte byte
		nextByte, err = srd.ReadByte()
		if err != nil {
			return
		}
		length = (uint32(header&0x3f) << 8) | uint32(nextByte)
		zlentry = make([]byte, length)
		_, err = io.ReadFull(srd, zlentry)
		if err != nil {
			return
		}
	} else if (header >> 6) == 2 {
		err = binary.Read(srd, binary.BigEndian, &length)
		if err != nil {
			return
		}
		zlentry = make([]byte, length)
		_, err = io.ReadFull(srd, zlentry)
		if err != nil {
			return
		}
	} else if (header >> 4) == 12 {
		var vint int16
		err = binary.Read(srd, binary.LittleEndian, &vint)
		if err != nil {
			return
		}
		buf := bytes.NewBuffer(nil)
		_, err = buf.WriteString(fmt.Sprintf("%d", vint))
		if err != nil {
			return
		}
		zlentry = buf.Bytes()
	} else if (header >> 4) == 13 {
		var vint int32
		err = binary.Read(srd, binary.LittleEndian, &vint)
		if err != nil {
			return
		}
		buf := bytes.NewBuffer(nil)
		_, err = buf.WriteString(fmt.Sprintf("%d", vint))
		if err != nil {
			return
		}
		zlentry = buf.Bytes()
	} else if (header >> 4) == 14 {
		var vint int64
		err = binary.Read(srd, binary.LittleEndian, &vint)
		if err != nil {
			return
		}
		buf := bytes.NewBuffer(nil)
		_, err = buf.WriteString(fmt.Sprintf("%d", vint))
		if err != nil {
			return
		}
		zlentry = buf.Bytes()
	} else if header == 240 {
		var ival int32
		ival, err = read24ByteInt(srd)
		if err != nil {
			return
		}
		buf := bytes.NewBuffer(nil)
		_, err = buf.WriteString(fmt.Sprintf("%d", ival))
		if err != nil {
			return
		}
		zlentry = buf.Bytes()
	} else if header == 254 {
		var bv byte
		bv, err = srd.ReadByte()
		if err != nil {
			return
		}
		buf := bytes.NewBuffer(nil)
		_, err = buf.WriteString(fmt.Sprintf("%d", int8(bv)))
		if err != nil {
			return
		}
		zlentry = buf.Bytes()
	} else if header >= 241 && header <= 253 {
		vint := header - 241
		buf := bytes.NewBuffer(nil)
		_, err = buf.WriteString(fmt.Sprintf("%d", vint))
		if err != nil {
			return
		}
		zlentry = buf.Bytes()
	} else {
		panic("read ziplist entry fail due to get invalid entry header")
	}
	return
}

func (r *RDB) readIntSet() (err error) {
	var (
		data     []byte
		encoding uint32
		num      uint32
	)
	data, err = r.readString()
	if err != nil {
		return
	}
	srd := bytes.NewReader(data)
	err = binary.Read(srd, binary.LittleEndian, &encoding)
	if err != nil {
		return
	}

	err = binary.Read(srd, binary.LittleEndian, &num)
	if err != nil {
		return
	}
	buf := bytes.NewBuffer(nil)
	for i := uint32(0); i < num; i++ {
		buf.Reset()
		if encoding == 8 {
			var entry int64
			err = binary.Read(srd, binary.LittleEndian, &entry)
			if err != nil {
				return
			}
			_, err = buf.WriteString(fmt.Sprintf("%d", entry))
			if err != nil {
				return
			}
		} else if encoding == 4 {
			var entry int32
			err = binary.Read(srd, binary.LittleEndian, &entry)
			if err != nil {
				return
			}
			_, err = buf.WriteString(fmt.Sprintf("%d", entry))
			if err != nil {
				return
			}
		} else if encoding == 2 {
			var entry int16
			err = binary.Read(srd, binary.LittleEndian, &entry)
			if err != nil {
				return
			}
			_, err = buf.WriteString(fmt.Sprintf("%d", entry))
			if err != nil {
				return
			}
		} else {
			panic("read int set invalid encoding")
		}
		r.cb.CmdSAdd(r.key, buf.Bytes())
	}
	r.cb.ExpireAt(r.key, r.expiry)
	return
}

func (r *RDB) readZipListHeader() (srd *bytes.Reader, zl uint32, tailOffset uint32, numEntries uint16, err error) {
	var (
		data []byte
	)
	data, err = r.readString()
	if err != nil {
		return
	}
	srd = bytes.NewReader(data)
	err = binary.Read(srd, binary.LittleEndian, &zl)
	if err != nil {
		return
	}
	err = binary.Read(srd, binary.LittleEndian, &tailOffset)
	if err != nil {
		return
	}
	err = binary.Read(srd, binary.LittleEndian, &numEntries)
	if err != nil {
		return
	}
	return
}

func (r *RDB) readZipListEnd(srd *bytes.Reader) (err error) {
	var zlEnd byte
	zlEnd, err = srd.ReadByte()
	if err != nil {
		return
	}
	if zlEnd != 255 {
		panic("read ziplist but get invalid zip end")
	}
	return
}

func (r *RDB) readZSetFromZiplist() (err error) {
	var (
		numEntries uint16
		srd        *bytes.Reader
	)
	srd, _, _, numEntries, err = r.readZipListHeader()
	if err != nil {
		return
	}

	if numEntries%2 != 0 {
		panic("read zset from ziplist but get odd number of element")
	}
	numEntries = numEntries / 2

	var (
		member, score []byte
	)
	for i := uint16(0); i < numEntries; i++ {
		member, err = r.readZipListEntry(srd)
		if err != nil {
			return
		}
		score, err = r.readZipListEntry(srd)
		if err != nil {
			return
		}
		var sf float64
		sf, err = strconv.ParseFloat(string(score), 64)
		if err != nil {
			return
		}
		r.cb.CmdZAdd(r.key, sf, member)
	}
	err = r.readZipListEnd(srd)
	if err != nil {
		return
	}
	r.cb.ExpireAt(r.key, r.expiry)
	return
}
func (r *RDB) readHashFromZipList() (err error) {
	var (
		numEntries uint16
		srd        *bytes.Reader
	)

	srd, _, _, numEntries, err = r.readZipListHeader()
	if err != nil {
		return
	}

	if numEntries%2 != 0 {
		panic("read zset from ziplist but get odd number of element")
	}
	numEntries = numEntries / 2
	var (
		field, value []byte
	)

	for i := uint16(0); i < numEntries; i++ {
		field, err = r.readZipListEntry(srd)
		if err != nil {
			return
		}
		value, err = r.readZipListEntry(srd)
		if err != nil {
			return
		}
		r.cb.CmdHSet(r.key, field, value)
	}
	err = r.readZipListEnd(srd)
	if err != nil {
		return
	}

	r.cb.ExpireAt(r.key, r.expiry)
	return
}

func (r *RDB) readListFromQuickList() (err error) {
	var (
		count     uint64
		totalSize int
	)

	count, err = r.readLength()
	if err != nil {
		return
	}

	for i := uint64(0); i < count; i++ {
		var (
			data       []byte
			zl         uint32
			tailOffset uint32
			num        uint16
		)

		data, err = r.readString()
		if err != nil {
			return
		}
		totalSize += len(data)

		srd := bytes.NewReader(data)
		err = binary.Read(srd, binary.LittleEndian, &zl)
		if err != nil {
			return
		}
		err = binary.Read(srd, binary.LittleEndian, &tailOffset)
		if err != nil {
			return
		}
		err = binary.Read(srd, binary.LittleEndian, &num)
		if err != nil {
			return
		}

		for i := uint16(0); i < num; i++ {
			var entry []byte
			entry, err = r.readZipListEntry(srd)
			if err != nil {
				return
			}
			r.cb.CmdRPush(r.key, entry)
		}

		err = r.readZipListEnd(srd)
		if err != nil {
			return err
		}
	}

	r.cb.ExpireAt(r.key, r.expiry)
	return
}

func (r *RDB) readStream() (err error) {
	var (
		entry1, entry2 uint64
		cgroups        uint64
		listpacks      uint64
	)
	listpacks, err = r.readLength()
	if err != nil {
		return
	}

	for i := uint64(0); i < listpacks; i++ {
		_, err = r.readString()
		if err != nil {
			return
		}
		_, err = r.readString()
		if err != nil {
			return
		}
	}

	// items
	_, err = r.readLength()
	if err != nil {
		return
	}

	entry1, err = r.readLength()
	if err != nil {
		return
	}

	entry2, err = r.readLength()
	if err != nil {
		return
	}
	_ = fmt.Sprintf("%d-%d", entry1, entry2)

	cgroups, err = r.readLength()
	if err != nil {
		return
	}

	consumerGroups := make([]*consumerGroup, cgroups)
	for i := uint64(0); i < cgroups; i++ {
		var (
			cgname    []byte
			consumers uint64
			pending   uint64
		)

		cgname, err = r.readString()
		if err != nil {
			return
		}

		entry1, err = r.readLength()
		if err != nil {
			return
		}

		entry2, err = r.readLength()
		if err != nil {
			return
		}

		lasteCGEntryID := fmt.Sprintf("%d-%d", entry1, entry2)
		pending, err = r.readLength()
		if err != nil {
			return
		}

		gpEntries := make([]*streamGroupPendingEntry, pending)
		//  group_pending_entries
		for j := uint64(0); j < pending; j++ {
			eid := make([]byte, 16)
			_, err = io.ReadFull(r.rd, eid)
			if err != nil {
				return
			}
			var (
				dtime  uint64
				dcount uint64
			)

			err = binary.Read(r.rd, binary.LittleEndian, &dtime)
			if err != nil {
				return
			}

			dcount, err = r.readLength()
			if err != nil {
				return
			}
			gpEntries[j] = &streamGroupPendingEntry{
				ID:     eid,
				DTime:  dtime,
				DCount: dcount,
			}
		}

		consumers, err = r.readLength()
		if err != nil {
			return
		}

		cEntries := make([]*streamConsumer, consumers)
		for k := uint64(0); k < consumers; k++ {
			var (
				cname    []byte
				stime    uint64
				cpending uint64
			)

			cname, err = r.readString()
			if err != nil {
				return
			}

			err = binary.Read(r.rd, binary.LittleEndian, &stime)
			if err != nil {
				return
			}

			cpending, err = r.readLength()
			if err != nil {
				return
			}

			pendingIDs := make([][]byte, cpending)
			for w := uint64(0); w < cpending; w++ {
				eid := make([]byte, 16)
				_, err = io.ReadFull(r.rd, eid)
				if err != nil {
					return
				}
				pendingIDs[w] = eid
			}

			cEntries[k] = &streamConsumer{
				Name:     cname,
				SeenTime: stime,
				Pending:  pendingIDs,
			}
		}

		consumerGroups[i] = &consumerGroup{
			Name:        cgname,
			Pending:     gpEntries,
			Consumers:   cEntries,
			LastEntryID: lasteCGEntryID,
		}
	}

	return
}

type consumerGroup struct {
	Name        []byte
	LastEntryID string
	Pending     []*streamGroupPendingEntry
	Consumers   []*streamConsumer
}

type streamConsumer struct {
	Name     []byte
	SeenTime uint64
	Pending  [][]byte
}

type streamGroupPendingEntry struct {
	ID     []byte
	DTime  uint64
	DCount uint64
}

func read24ByteInt(srd *bytes.Reader) (ival int32, err error) {
	buf := make([]byte, 3)
	_, err = io.ReadFull(srd, buf)
	ival |= int32(buf[2])
	ival = ival << 8
	ival |= int32(buf[1])
	ival = ival << 8
	ival |= int32(buf[0])
	return
}
