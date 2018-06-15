package memcache

import (
	"bytes"
	"io"

	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

// memcached protocol: https://github.com/memcached/memcached/blob/master/doc/protocol.txt

type decoder struct {
}

// NewDecoder new a memcache decoder.
func NewDecoder(r io.Reader) proto.Decoder {
	d := &decoder{}
	return d
}

func getNextField(data []byte) (field []byte, end int, err error) {
	begin := noSpaceIdx(data)
	offset := bytes.IndexByte(data[begin:], spaceByte)
	if offset == -1 {
		err = proto.ErrMoreData
		return
	}
	end = begin + offset
	field = data[begin:end]
	return
}

func (d *decoder) Decode(req *proto.Msg, data []byte) error {
	req.Type = proto.CacheTypeMemcache

	cmd, idx, err := getNextField(data)
	if err != nil {
		return err
	}
	lower := conv.ToLower(cmd)
	copy(data[idx-len(cmd):idx], lower)

	switch string(lower) {
	// Storage commands:
	case "set":
		return decodeStorageMsg(req, data, idx, MsgTypeSet, false)
	case "add":
		return decodeStorageMsg(req, data, idx, MsgTypeAdd, false)
	case "replace":
		return decodeStorageMsg(req, data, idx, MsgTypeReplace, false)
	case "append":
		return decodeStorageMsg(req, data, idx, MsgTypeAppend, false)
	case "prepend":
		return decodeStorageMsg(req, data, idx, MsgTypePrepend, false)
	case "cas":
		return decodeStorageMsg(req, data, idx, MsgTypeCas, true)
		// Retrieval commands:

	case "get":
		return decodeRetrievalMsg(req, data, idx, MsgTypeGet)
	case "gets":
		return decodeRetrievalMsg(req, data, idx, MsgTypeGets)
		// // Deletion
	case "delete":
		return decodeDeleteMsg(req, data, idx, MsgTypeDelete)
	// // Increment/Decrement:
	case "incr":
		return decodeIncrDecrMsg(req, data, idx, MsgTypeIncr)
	case "decr":
		return decodeIncrDecrMsg(req, data, idx, MsgTypeDecr)
	// // Touch:
	case "touch":
		return decodeTouchMsg(req, data, idx, MsgTypeTouch)
	// // Get And Touch:
	case "gat":
		return decodeGetAndTouchMsg(req, data, idx, MsgTypeGat)
	case "gats":
		return decodeGetAndTouchMsg(req, data, idx, MsgTypeGats)
	}

	return nil
}

func findLength(line []byte, cas bool) (int, error) {
	pos := len(line)
	if cas {
		// skip cas filed
		high := revNoSpacIdx(line)
		low := revSpacIdx(line[:high])
		pos = low
	}
	up := revNoSpacIdx(line[:pos]) + 1
	low := revSpacIdx(line[:up]) + 1
	lengthBs := line[low:up]
	length, err := conv.Btoi(lengthBs)
	if err != nil {
		return -1, errors.Wrapf(ErrBadLength, "MC Decoder storage Msg parse bytes length(%s)", lengthBs)
	}
	return int(length), nil
}

func findKey(line []byte) (begin int, end int, err error) {
	begin = noSpaceIdx(line)

	offset := bytes.IndexByte(line[begin:], spaceByte)
	if offset == -1 {
		err = ErrBadKey
		return
	}
	end = begin + offset
	return
}

// baka function with five arguments ....
func decodeStorageMsg(req *proto.Msg, data []byte, idx int, mtype MsgType, cas bool) error {
	cmdEndPos := bytes.Index(data[idx:], crlfBytes)
	if cmdEndPos == -1 {
		return proto.ErrMoreData
	}

	line := data[idx:cmdEndPos]

	length, err := findLength(line, cas)
	if err != nil {
		return err
	}
	total := idx + cmdEndPos + 2 + length + 2 + 1
	if len(data) < total {
		return proto.ErrMoreData
	}

	keyLow, keyHigh, err := findKey(line)
	if err != nil {
		return ErrBadKey
	}
	req.SetRefData(data[:total])
	req.WithProto(&MCMsg{
		rTp:  mtype,
		key:  line[keyLow:keyHigh],
		data: data[idx+keyHigh : total],
	})
	return nil
}

func decodeRetrievalMsg(req *proto.Msg, data []byte, idx int, reqType MsgType) (err error) {
	cmdEndPos := bytes.Index(data[idx:], crlfBytes)
	if cmdEndPos == -1 {
		return proto.ErrMoreData
	}

	bs := data[idx : idx+cmdEndPos+2]
	// sanity check
	if len(bs) <= 3 {
		err = errors.Wrapf(ErrBadMsg, "MC Decoder retrieval Msg sanity check bsLen(%d)", len(bs))
		return
	}
	index := noSpaceIdx(bs)
	key := bs[index : len(bs)-2]
	if !legalKey(key, true) {
		err = errors.Wrap(ErrBadKey, "MC Decoder retrieval Msg legal key")
		return
	}
	batch := bytes.Index(key, spaceBytes) > 0
	req.WithProto(&MCMsg{
		rTp:   reqType,
		key:   key,
		data:  bs[len(bs)-2:],
		batch: batch,
	})
	req.SetRefData(data[:idx+len(bs)])
	return
}

func decodeDeleteMsg(req *proto.Msg, data []byte, idx int, reqType MsgType) (err error) {
	cmdEndPos := bytes.Index(data[idx:], crlfBytes)
	if cmdEndPos == -1 {
		return proto.ErrMoreData
	}

	bs := data[idx : idx+cmdEndPos+2]
	// sanity check
	if len(bs) <= 3 {
		err = errors.Wrapf(ErrBadMsg, "MC Decoder delete Msg sanity check bsLen(%d)", len(bs))
		return
	}
	index := noSpaceIdx(bs)
	key := bs[index : len(bs)-2]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder delete Msg legal key")
		return
	}
	req.WithProto(&MCMsg{
		rTp:  reqType,
		key:  key,
		data: bs[len(bs)-2:],
	})
	req.SetRefData(data[:idx+len(bs)])
	return
}

func decodeIncrDecrMsg(req *proto.Msg, data []byte, idx int, reqType MsgType) (err error) {
	cmdEndPos := bytes.Index(data[idx:], crlfBytes)
	if cmdEndPos == -1 {
		return proto.ErrMoreData
	}
	bs := data[idx : idx+cmdEndPos+2]

	index := noSpaceIdx(bs)
	// key
	ki := bytes.IndexByte(bs[index:], spaceByte)
	if ki <= 0 {
		err = errors.Wrap(ErrBadMsg, "MC Decoder incrDecr Msg get key index")
		return
	}
	key := bs[index : index+ki]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder incrDecr Msg legal key")
		return
	}
	index += ki + noSpaceIdx(bs[index+ki:])
	// value
	if index >= len(bs)-2 {
		err = errors.Wrap(ErrBadMsg, "MC Decoder incrDecr Msg get value check bs length")
		return
	}
	valueBs := bs[index : len(bs)-2]
	if !bytes.Equal(valueBs, oneBytes) {
		if _, err = conv.Btoi(valueBs); err != nil {
			err = errors.Wrapf(ErrBadMsg, "MC Decoder incrDecr Msg parse value(%s)", valueBs)
			return
		}
	}
	req.WithProto(&MCMsg{
		rTp:  reqType,
		key:  key,
		data: bs[ki+1:],
	})
	req.SetRefData(data[:idx+len(bs)])
	return
}

func decodeTouchMsg(req *proto.Msg, data []byte, idx int, reqType MsgType) (err error) {
	cmdEndPos := bytes.Index(data[idx:], crlfBytes)
	if cmdEndPos == -1 {
		return proto.ErrMoreData
	}
	bs := data[idx : idx+cmdEndPos+2]

	// sanity check
	if c := bytes.Count(bs, spaceBytes); c != 2 {
		err = errors.Wrapf(ErrBadMsg, "MC Decoder touch Msg sanity check spaceCount(%d)", c)
		return
	}
	index := noSpaceIdx(bs)
	// key
	ki := bytes.IndexByte(bs[index:], spaceByte)
	if ki <= 0 {
		err = errors.Wrap(ErrBadMsg, "MC Decoder touch Msg get key index")
		return
	}
	key := bs[index : index+ki]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder touch Msg legal key")
		return
	}
	index += ki + noSpaceIdx(bs[index+ki:])
	// exptime
	if index >= len(bs)-2 {
		err = errors.Wrap(ErrBadMsg, "MC Decoder touch Msg get exptime check bs length")
		return
	}
	expBs := bs[index : len(bs)-2]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadMsg, "MC Decoder touch Msg parse exptime(%s)", expBs)
			return
		}
	}
	req.WithProto(&MCMsg{
		rTp:  reqType,
		key:  key,
		data: bs[ki+1:],
	})
	req.SetRefData(data[:idx+len(bs)])
	return
}

func decodeGetAndTouchMsg(req *proto.Msg, data []byte, idx int, reqType MsgType) (err error) {
	cmdEndPos := bytes.Index(data[idx:], crlfBytes)
	if cmdEndPos == -1 {
		return proto.ErrMoreData
	}
	bs := data[idx : idx+cmdEndPos+2]

	index := noSpaceIdx(bs)
	// exptime
	ei := bytes.IndexByte(bs[index:], spaceByte)
	if ei <= 0 {
		err = errors.Wrap(ErrBadMsg, "MC Decoder getAndTouch Msg get exptime index")
		return
	}
	expBs := bs[index : index+ei]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadMsg, "MC Decoder getAndTouch Msg parse exptime(%s)", expBs)
			return
		}
	}
	index += ei + noSpaceIdx(bs[index+ei:])
	// key
	if index >= len(bs)-2 {
		err = errors.Wrap(ErrBadMsg, "MC Decoder getAndTouch Msg get exptime check bs length")
		return
	}
	key := bs[index : len(bs)-2]
	if !legalKey(key, true) {
		err = errors.Wrap(ErrBadKey, "MC Decoder getAndTouch Msg legal key")
		return
	}
	batch := bytes.IndexByte(key, spaceByte) > 0
	req.WithProto(&MCMsg{
		rTp:   reqType,
		key:   key,
		data:  expBs, // NOTE: no contains '\r\n'!!!
		batch: batch,
	})
	req.SetRefData(data[:idx+len(bs)])
	return
}

// Currently the length limit of a key is set at 250 characters.
// the key must not include control characters or whitespace.
func legalKey(key []byte, isMulti bool) bool {
	if !isMulti && len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if isMulti && key[i] < ' ' {
			return false
		}
		if !isMulti && key[i] <= ' ' {
			return false
		}
		if key[i] == 0x7f {
			return false
		}
	}
	return true
}

func noSpaceIdx(bs []byte) int {
	for i, b := range bs {
		if b != ' ' {
			return i
		}
	}
	return len(bs)
}

func revNoSpacIdx(bs []byte) int {
	for i := len(bs) - 1; i > -1; i-- {
		if bs[i] != spaceByte {
			return i
		}
	}
	return -1
}

func revSpacIdx(bs []byte) int {
	for i := len(bs) - 1; i > -1; i-- {
		if bs[i] == spaceByte {
			return i
		}
	}
	return -1
}
