package redis

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
)

var (
	askStr     = "ASK"
	askBytes   = []byte("ASK")
	movedBytes = []byte("MOVED")
)

// errors for only using with Redirect
var (
	ErrRedirect          = errors.New("redirect error occur")
	ErrRedirectBadFormat = errors.New("bad redirect format")
)

// RedirectInfo set and pass with redirect info
type RedirectInfo struct {
	Addr  string
	Slot  int
	IsAsk bool
}

func parseRedirectInfo(data []byte) (*RedirectInfo, error) {
	fields := strings.Fields(string(data))
	if len(fields) != 3 {
		return nil, ErrRedirectBadFormat
	}

	r := &RedirectInfo{
		IsAsk: fields[0] == askStr,
		Addr:  fields[2],
	}
	ival, err := strconv.Atoi(fields[1])
	r.Slot = ival
	return r, err
}

// isRedirect checks if response type is Redis Error
// and payload was prefix with "ASK" && "MOVED"
func isRedirect(r *resp) bool {
	if r.rTp != respError {
		return false
	}

	if r.data == nil {
		return false
	}

	return bytes.HasPrefix(r.data, askBytes) || bytes.HasPrefix(r.data, movedBytes)
}
