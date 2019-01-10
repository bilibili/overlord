package log

import (
	"fmt"
	stdlog "log"
	"os"
)

// stdoutHandler stdout log handler
type stdoutHandler struct {
	out *stdlog.Logger
}

// NewStdHandler create a stdout log handler
func NewStdHandler() Handler {
	return &stdoutHandler{out: stdlog.New(os.Stdout, "", stdlog.LstdFlags|stdlog.Lshortfile)}
}

// Log stdout loging
func (h *stdoutHandler) Log(lv Level, msg string) {
	_ = h.out.Output(6, fmt.Sprintf("[%s] %s", lv, msg))
}

// Close stdout loging
func (h *stdoutHandler) Close() (err error) {
	return
}
