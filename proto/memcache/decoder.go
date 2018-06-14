package memcache

import (
	"bytes"
	"io"

	"github.com/felixhao/overlord/lib/bufio"
	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

// memcached protocol: https://github.com/memcached/memcached/blob/master/doc/protocol.txt

const (
	decoderBufferSize = 128 * 1024 // NOTE: keep reading data from client, so relatively large
)

type decoder struct {
	br *bufio.Reader
}

// NewDecoder new a memcache decoder.
func NewDecoder(r io.Reader) proto.Decoder {
	d := &decoder{
		br: bufio.NewReaderSize(r, decoderBufferSize),
	}
	return d
}

// Decode decode bytes from reader.
func (d *decoder) Decode(req *proto.Msg) (err error) {
	bs, err := d.br.ReadUntil(delim)
	if err != nil {
		err = errors.Wrapf(err, "MC decoder while reading text command line from decoder")
		return
	}
	i := noSpaceIdx(bs)
	bs = bs[i:]
	i = bytes.IndexByte(bs, spaceByte)
	if i <= 0 {
		err = errors.Wrap(ErrBadMsg, "MC decoder Decode get cmd index")
		return
	}
	cmd := string(conv.ToLower(bs[:i]))
	ds := bs[i:] // NOTE: consume the begin ' '
	switch cmd {
	// Storage commands:
	case "set":
		return storageMsg(d.br, req, MsgTypeSet, ds, true)
	case "add":
		return storageMsg(d.br, req, MsgTypeAdd, ds, true)
	case "replace":
		return storageMsg(d.br, req, MsgTypeReplace, ds, true)
	case "append":
		return storageMsg(d.br, req, MsgTypeAppend, ds, true)
	case "prepend":
		return storageMsg(d.br, req, MsgTypePrepend, ds, true)
	case "cas":
		return storageMsg(d.br, req, MsgTypeCas, ds, false)
	// Retrieval commands:
	case "get":
		return retrievalMsg(d.br, req, MsgTypeGet, ds)
	case "gets":
		return retrievalMsg(d.br, req, MsgTypeGets, ds)
	// Deletion
	case "delete":
		return deleteMsg(d.br, req, MsgTypeDelete, ds)
	// Increment/Decrement:
	case "incr":
		return incrDecrMsg(d.br, req, MsgTypeIncr, ds)
	case "decr":
		return incrDecrMsg(d.br, req, MsgTypeDecr, ds)
	// Touch:
	case "touch":
		return touchMsg(d.br, req, MsgTypeTouch, ds)
	// Get And Touch:
	case "gat":
		return getAndTouchMsg(d.br, req, MsgTypeGat, ds)
	case "gats":
		return getAndTouchMsg(d.br, req, MsgTypeGats, ds)
	}
	return errors.Wrap(ErrError, "MC Decoder Decode command no exist")
}

func storageMsg(r *bufio.Reader, req *proto.Msg, reqType MsgType, bs []byte, noCas bool) (err error) {
	// sanity check
	index := noSpaceIdx(bs)
	// key
	ki := bytes.IndexByte(bs[index:], spaceByte)
	if ki <= 0 {
		err = errors.Wrap(ErrBadMsg, "MC Decoder storage Msg get key index")
		return
	}
	key := bs[index : index+ki]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder storage Msg legal key")
		return
	}
	index += ki + noSpaceIdx(bs[index+ki:]) // NOTE: +1 consume the begin ' '
	// flags
	fi := bytes.IndexByte(bs[index:], spaceByte)
	if fi <= 0 {
		err = errors.Wrap(ErrBadFlags, "MC Decoder storage Msg get flags index")
		return
	}
	flagBs := bs[index : index+fi]
	if !bytes.Equal(flagBs, zeroBytes) { // NOTE: if equal to zero, there is no need to parse.
		var flags int64
		if flags, err = conv.Btoi(flagBs); err != nil || flags > maxUint32 {
			err = errors.Wrapf(ErrBadFlags, "MC Decoder storage Msg parse flags(%s)", flagBs)
			return
		}
	}
	index += fi + noSpaceIdx(bs[index+fi:])
	// exptime
	ei := bytes.IndexByte(bs[index:], spaceByte)
	if ei <= 0 {
		err = errors.Wrap(ErrBadExptime, "MC Decoder storage Msg get exptime index")
		return
	}
	expBs := bs[index : index+ei]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadExptime, "MC Decoder storage Msg parse exptime(%s)", expBs)
			return
		}
	}
	index += ei + noSpaceIdx(bs[index+ei:])
	// bytes length
	var bsBs []byte
	if noCas {
		if index >= len(bs)-2 {
			err = errors.Wrap(ErrBadMsg, "MC Decoder storage Msg get bytes length index check bs length")
			return
		}
		bsBs = bs[index : len(bs)-2] // NOTE: len(bs)-2 consume the last two bytes '\r\n'
	} else {
		bi := bytes.IndexByte(bs[index:], spaceByte)
		if bi <= 0 {
			err = errors.Wrap(ErrBadLength, "MC Decoder storage Msg get bytes length index")
			return
		}
		bsBs = bs[index : index+bi]
		index += bi + 1
	}
	length, err := conv.Btoi(bsBs)
	if err != nil {
		err = errors.Wrapf(ErrBadLength, "MC Decoder storage Msg parse bytes length(%s)", bsBs)
		return
	}
	if !noCas {
		if index >= len(bs)-2 {
			err = errors.Wrap(ErrBadMsg, "MC Decoder storage Msg get cas index check bs length")
			return
		}
		casBs := bs[index : len(bs)-2]
		if !bytes.Equal(casBs, zeroBytes) {
			if _, err = conv.Btoi(casBs); err != nil {
				err = errors.Wrapf(ErrBadCas, "MC Decoder storage Msg parse cas(%s)", casBs)
				return
			}
		}
	}
	// read storage data
	ds, err := r.ReadFull(int(length + 2)) // NOTE: +2 means until '\r\n'
	if err != nil {
		err = errors.Wrapf(err, "MC decoder storage Msg while reading data line")
		return
	}
	if !bytes.HasSuffix(ds, crlfBytes) {
		err = errors.Wrapf(ErrBadLength, "MC Decoder storage Msg data not end with CRLF length(%d)", length)
		return
	}
	req = &proto.Msg{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCMsg{
		rTp:  reqType,
		key:  key,
		data: append(bs[ki+1:], ds...), // TODO(felix): reuse buffer
	})
	return
}

func retrievalMsg(r *bufio.Reader, req *proto.Msg, reqType MsgType, bs []byte) (err error) {
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
	req = &proto.Msg{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCMsg{
		rTp:   reqType,
		key:   key,
		data:  bs[len(bs)-2:],
		batch: batch,
	})
	return
}

func deleteMsg(r *bufio.Reader, req *proto.Msg, reqType MsgType, bs []byte) (err error) {
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
	req = &proto.Msg{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCMsg{
		rTp:  reqType,
		key:  key,
		data: bs[len(bs)-2:],
	})
	return
}

func incrDecrMsg(r *bufio.Reader, req *proto.Msg, reqType MsgType, bs []byte) (err error) {
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
	req = &proto.Msg{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCMsg{
		rTp:  reqType,
		key:  key,
		data: bs[ki+1:],
	})
	return
}

func touchMsg(r *bufio.Reader, req *proto.Msg, reqType MsgType, bs []byte) (err error) {
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
	req = &proto.Msg{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCMsg{
		rTp:  reqType,
		key:  key,
		data: bs[ki+1:],
	})
	return
}

func getAndTouchMsg(r *bufio.Reader, req *proto.Msg, reqType MsgType, bs []byte) (err error) {
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
	req = &proto.Msg{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCMsg{
		rTp:   reqType,
		key:   key,
		data:  expBs, // NOTE: no contains '\r\n'!!!
		batch: batch,
	})
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
