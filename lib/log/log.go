package log

import (
	"fmt"
)

// Level of severity.
type Level int

const (
	_infoLevel Level = iota
	_warnLevel
	_errorLevel
)

var levelNames = [...]string{
	_infoLevel:  "INFO",
	_warnLevel:  "WARN",
	_errorLevel: "ERROR",
}

// String implementation.
func (l Level) String() string {
	return levelNames[l]
}

// Config log config.
type Config struct {
	Stdout bool
	Family string
	Host   string
	Dir    string
	// VLevel Enable V-leveled logging at the specified level.
	VLevel int32
}

var (
	h Handler
	c *Config
)

// Init create logger with context.
func Init(hs ...Handler) {
	if len(hs) == 0 {
		return
	}
	h = Handlers(hs)
}

// Infof logs a message at the info log level.
func Infof(format string, args ...interface{}) {
	logf(_infoLevel, format, args...)
}

// Warnf logs a message at the warning log level.
func Warnf(format string, args ...interface{}) {
	logf(_warnLevel, format, args...)
}

// Errorf logs a message at the error log level.
func Errorf(format string, args ...interface{}) {
	logf(_errorLevel, format, args...)
}

// Info logs a message at the info log level.
func Info(args ...interface{}) {
	logs(_infoLevel, args...)
}

// Warn logs a message at the warning log level.
func Warn(args ...interface{}) {
	logs(_warnLevel, args...)
}

// Error logs a message at the error log level.
func Error(args ...interface{}) {
	logs(_errorLevel, args...)
}

func logf(lv Level, format string, args ...interface{}) {
	if h == nil {
		return
	}
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	h.Log(lv, msg)
}

func logs(lv Level, args ...interface{}) {
	if h == nil {
		return
	}
	h.Log(lv, fmt.Sprint(args...))
}

// Close close resource.
func Close() (err error) {
	if h == nil {
		return
	}
	return h.Close()
}
