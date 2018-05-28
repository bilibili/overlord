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
func (d *decoder) Decode() (req *proto.Request, err error) {
	bs, err := d.br.ReadBytes(delim)
	if err != nil {
		err = errors.Wrapf(err, "MC decoder while reading text command line from decoder")
		return
	}
	i := noSpaceIdx(bs)
	bs = bs[i:]
	i = bytes.IndexByte(bs, spaceByte)
	if i <= 0 {
		err = errors.Wrap(ErrBadRequest, "MC decoder Decode get cmd index")
		return
	}
	cmd := string(conv.ToLower(bs[:i]))
	ds := bs[i:] // NOTE: consume the begin ' '
	switch cmd {
	// Storage commands:
	case "set":
		return storageRequest(d.br, RequestTypeSet, ds, true)
	case "add":
		return storageRequest(d.br, RequestTypeAdd, ds, true)
	case "replace":
		return storageRequest(d.br, RequestTypeReplace, ds, true)
	case "append":
		return storageRequest(d.br, RequestTypeAppend, ds, true)
	case "prepend":
		return storageRequest(d.br, RequestTypePrepend, ds, true)
	case "cas":
		return storageRequest(d.br, RequestTypeCas, ds, false)
	// Retrieval commands:
	case "get":
		return retrievalRequest(d.br, RequestTypeGet, ds)
	case "gets":
		return retrievalRequest(d.br, RequestTypeGets, ds)
	// Deletion
	case "delete":
		return deleteRequest(d.br, RequestTypeDelete, ds)
	// Increment/Decrement:
	case "incr":
		return incrDecrRequest(d.br, RequestTypeIncr, ds)
	case "decr":
		return incrDecrRequest(d.br, RequestTypeDecr, ds)
	// Touch:
	case "touch":
		return touchRequest(d.br, RequestTypeTouch, ds)
	// Get And Touch:
	case "gat":
		return getAndTouchRequest(d.br, RequestTypeGat, ds)
	case "gats":
		return getAndTouchRequest(d.br, RequestTypeGats, ds)
	}
	return nil, errors.Wrap(ErrError, "MC Decoder Decode command no exist")
}

func storageRequest(r *bufio.Reader, reqType RequestType, bs []byte, noCas bool) (req *proto.Request, err error) {
	// sanity check
	index := noSpaceIdx(bs)
	// key
	ki := bytes.IndexByte(bs[index:], spaceByte)
	if ki <= 0 {
		err = errors.Wrap(ErrBadRequest, "MC Decoder storage request get key index")
		return
	}
	key := bs[index : index+ki]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder storage request legal key")
		return
	}
	index += ki + noSpaceIdx(bs[index+ki:]) // NOTE: +1 consume the begin ' '
	// flags
	fi := bytes.IndexByte(bs[index:], spaceByte)
	if fi <= 0 {
		err = errors.Wrap(ErrBadFlags, "MC Decoder storage request get flags index")
		return
	}
	flagBs := bs[index : index+fi]
	if !bytes.Equal(flagBs, zeroBytes) { // NOTE: if equal to zero, there is no need to parse.
		var flags int64
		if flags, err = conv.Btoi(flagBs); err != nil || flags > maxUint32 {
			err = errors.Wrapf(ErrBadFlags, "MC Decoder storage request parse flags(%s)", flagBs)
			return
		}
	}
	index += fi + noSpaceIdx(bs[index+fi:])
	// exptime
	ei := bytes.IndexByte(bs[index:], spaceByte)
	if ei <= 0 {
		err = errors.Wrap(ErrBadExptime, "MC Decoder storage request get exptime index")
		return
	}
	expBs := bs[index : index+ei]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadExptime, "MC Decoder storage request parse exptime(%s)", expBs)
			return
		}
	}
	index += ei + noSpaceIdx(bs[index+ei:])
	// bytes length
	var bsBs []byte
	if noCas {
		if index >= len(bs)-2 {
			err = errors.Wrap(ErrBadRequest, "MC Decoder storage request get bytes length index check bs length")
			return
		}
		bsBs = bs[index : len(bs)-2] // NOTE: len(bs)-2 consume the last two bytes '\r\n'
	} else {
		bi := bytes.IndexByte(bs[index:], spaceByte)
		if bi <= 0 {
			err = errors.Wrap(ErrBadLength, "MC Decoder storage request get bytes length index")
			return
		}
		bsBs = bs[index : index+bi]
		index += bi + 1
	}
	length, err := conv.Btoi(bsBs)
	if err != nil {
		err = errors.Wrapf(ErrBadLength, "MC Decoder storage request parse bytes length(%s)", bsBs)
		return
	}
	if !noCas {
		if index >= len(bs)-2 {
			err = errors.Wrap(ErrBadRequest, "MC Decoder storage request get cas index check bs length")
			return
		}
		casBs := bs[index : len(bs)-2]
		if !bytes.Equal(casBs, zeroBytes) {
			if _, err = conv.Btoi(casBs); err != nil {
				err = errors.Wrapf(ErrBadCas, "MC Decoder storage request parse cas(%s)", casBs)
				return
			}
		}
	}
	// read storage data
	ds, err := r.ReadFull(int(length + 2)) // NOTE: +2 means until '\r\n'
	if err != nil {
		err = errors.Wrapf(err, "MC decoder storage request while reading data line")
		return
	}
	if !bytes.HasSuffix(ds, crlfBytes) {
		err = errors.Wrapf(ErrBadLength, "MC Decoder storage request data not end with CRLF length(%d)", length)
		return
	}
	req = &proto.Request{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCRequest{
		rTp:  reqType,
		key:  key,
		data: append(bs[ki+1:], ds...), // TODO(felix): reuse buffer
	})
	return
}

func retrievalRequest(r *bufio.Reader, reqType RequestType, bs []byte) (req *proto.Request, err error) {
	// sanity check
	if len(bs) <= 3 {
		err = errors.Wrapf(ErrBadRequest, "MC Decoder retrieval request sanity check bsLen(%d)", len(bs))
		return
	}
	index := noSpaceIdx(bs)
	key := bs[index : len(bs)-2]
	if !legalKey(key, true) {
		err = errors.Wrap(ErrBadKey, "MC Decoder retrieval request legal key")
		return
	}
	batch := bytes.Index(key, spaceBytes) > 0
	req = &proto.Request{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCRequest{
		rTp:   reqType,
		key:   key,
		data:  bs[len(bs)-2:],
		batch: batch,
	})
	return
}

func deleteRequest(r *bufio.Reader, reqType RequestType, bs []byte) (req *proto.Request, err error) {
	// sanity check
	if len(bs) <= 3 {
		err = errors.Wrapf(ErrBadRequest, "MC Decoder delete request sanity check bsLen(%d)", len(bs))
		return
	}
	index := noSpaceIdx(bs)
	key := bs[index : len(bs)-2]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder delete request legal key")
		return
	}
	req = &proto.Request{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCRequest{
		rTp:  reqType,
		key:  key,
		data: bs[len(bs)-2:],
	})
	return
}

func incrDecrRequest(r *bufio.Reader, reqType RequestType, bs []byte) (req *proto.Request, err error) {
	index := noSpaceIdx(bs)
	// key
	ki := bytes.IndexByte(bs[index:], spaceByte)
	if ki <= 0 {
		err = errors.Wrap(ErrBadRequest, "MC Decoder incrDecr request get key index")
		return
	}
	key := bs[index : index+ki]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder incrDecr request legal key")
		return
	}
	index += ki + noSpaceIdx(bs[index+ki:])
	// value
	if index >= len(bs)-2 {
		err = errors.Wrap(ErrBadRequest, "MC Decoder incrDecr request get value check bs length")
		return
	}
	valueBs := bs[index : len(bs)-2]
	if !bytes.Equal(valueBs, oneBytes) {
		if _, err = conv.Btoi(valueBs); err != nil {
			err = errors.Wrapf(ErrBadRequest, "MC Decoder incrDecr request parse value(%s)", valueBs)
			return
		}
	}
	req = &proto.Request{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCRequest{
		rTp:  reqType,
		key:  key,
		data: bs[ki+1:],
	})
	return
}

func touchRequest(r *bufio.Reader, reqType RequestType, bs []byte) (req *proto.Request, err error) {
	// sanity check
	if c := bytes.Count(bs, spaceBytes); c != 2 {
		err = errors.Wrapf(ErrBadRequest, "MC Decoder touch request sanity check spaceCount(%d)", c)
		return
	}
	index := noSpaceIdx(bs)
	// key
	ki := bytes.IndexByte(bs[index:], spaceByte)
	if ki <= 0 {
		err = errors.Wrap(ErrBadRequest, "MC Decoder touch request get key index")
		return
	}
	key := bs[index : index+ki]
	if !legalKey(key, false) {
		err = errors.Wrap(ErrBadKey, "MC Decoder touch request legal key")
		return
	}
	index += ki + noSpaceIdx(bs[index+ki:])
	// exptime
	if index >= len(bs)-2 {
		err = errors.Wrap(ErrBadRequest, "MC Decoder touch request get exptime check bs length")
		return
	}
	expBs := bs[index : len(bs)-2]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadRequest, "MC Decoder touch request parse exptime(%s)", expBs)
			return
		}
	}
	req = &proto.Request{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCRequest{
		rTp:  reqType,
		key:  key,
		data: bs[ki+1:],
	})
	return
}

func getAndTouchRequest(r *bufio.Reader, reqType RequestType, bs []byte) (req *proto.Request, err error) {
	index := noSpaceIdx(bs)
	// exptime
	ei := bytes.IndexByte(bs[index:], spaceByte)
	if ei <= 0 {
		err = errors.Wrap(ErrBadRequest, "MC Decoder getAndTouch request get exptime index")
		return
	}
	expBs := bs[index : index+ei]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadRequest, "MC Decoder getAndTouch request parse exptime(%s)", expBs)
			return
		}
	}
	index += ei + noSpaceIdx(bs[index+ei:])
	// key
	if index >= len(bs)-2 {
		err = errors.Wrap(ErrBadRequest, "MC Decoder getAndTouch request get exptime check bs length")
		return
	}
	key := bs[index : len(bs)-2]
	if !legalKey(key, true) {
		err = errors.Wrap(ErrBadKey, "MC Decoder getAndTouch request legal key")
		return
	}
	batch := bytes.IndexByte(key, spaceByte) > 0
	req = &proto.Request{Type: proto.CacheTypeMemcache}
	req.WithProto(&MCRequest{
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
