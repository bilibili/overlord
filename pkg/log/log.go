package log

import (
	"flag"
	"fmt"
)

var (
	logStd  bool
	logFile string
	logVl   int
	debug   bool
)

func init() {
	flag.BoolVar(&logStd, "std", false, "log will printing into stdout.")
	flag.BoolVar(&debug, "debug", false, "debug model, will open stdout log. high priority than conf.debug.")
	flag.StringVar(&logFile, "log", "", "log will printing file {log}. high priority than conf.log.")
	flag.IntVar(&logVl, "log-vl", 0, "log verbose level. high priority than conf.log_vl.")
}

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
	Debug  bool
	Log    string
	LogVL  int `toml:"log_vl"`
	Family string
	Host   string
	// VLevel Enable V-leveled logging at the specified level.
}

var (
	h Handler
)

// Init log.
func Init(c *Config) (b bool) {
	var hs []Handler

	if c == nil {
		c = &Config{}
	}
	if logFile != "" {
		c.Log = logFile
	}
	if logVl != 0 {
		c.LogVL = logVl
	}
	c.Stdout = logStd
	c.Debug = debug
	if c.Debug || c.Stdout {
		hs = append(hs, NewStdHandler())
	}
	if c.Log != "" {
		hs = append(hs, NewFileHandler(c.Log))
	}
	if c.LogVL != 0 {
		DefaultVerboseLevel = c.LogVL
	}
	if len(hs) > 0 {
		h = Handlers(hs)
		b = true
	}
	return
}

// InitHandle with log handle.
func InitHandle(hs ...Handler) {
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
