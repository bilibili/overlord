package mesos

import (
	"path/filepath"
	"time"
)

func splitJobID(key string) string {
	_, file := filepath.Split(key)
	return file
}

// Duration parse toml time duration
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	tmp, err := time.ParseDuration(string(text))
	if err == nil {
		*d = Duration(tmp)
	}
	return err
}
