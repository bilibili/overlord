package log

import (
	"github.com/pkg/errors"
)

// Handler is used to handle log events, outputting them to
// stdio or sending them to remote services. See the "handlers"
// directory for implementations.
//
// It is left up to Handlers to implement thread-safety.
type Handler interface {
	Log(lv Level, msg string)
	Close() error
}

// Handlers .
type Handlers []Handler

// Log handlers logging.
func (hs Handlers) Log(lv Level, msg string) {
	for _, h := range hs {
		h.Log(lv, msg)
	}
}

// Close close resource.
func (hs Handlers) Close() (err error) {
	for _, h := range hs {
		if e := h.Close(); e != nil {
			err = errors.WithStack(e)
		}
	}
	return
}
