package log

// Verbose .
type Verbose bool

// DefaultVerboseLevel default Verbose level.
var DefaultVerboseLevel = 0

// V enable verbose log.
// v must be more than 0.
func V(v int) Verbose {
	return Verbose(v <= DefaultVerboseLevel)
}

// Infof logs a message at the info log level.
func (v Verbose) Infof(format string, args ...interface{}) {
	if v {
		logf(_infoLevel, format, args...)
	}
}

// Warnf logs a message at the warning log level.
func (v Verbose) Warnf(format string, args ...interface{}) {
	if v {
		logf(_warnLevel, format, args...)
	}
}

// Errorf logs a message at the error log level.
func (v Verbose) Errorf(format string, args ...interface{}) {
	if v {
		logf(_errorLevel, format, args...)
	}
}

// Info logs a message at the info log level.
func (v Verbose) Info(args ...interface{}) {
	if v {
		logs(_infoLevel, args...)
	}
}

// Warn logs a message at the warning log level.
func (v Verbose) Warn(args ...interface{}) {
	if v {
		logs(_warnLevel, args...)
	}
}

// Error logs a message at the error log level.
func (v Verbose) Error(args ...interface{}) {
	if v {
		logs(_errorLevel, args...)
	}
}

// Close close resource.
func (v Verbose) Close() error {
	return h.Close()
}
