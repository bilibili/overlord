package mesos

import (
	"path/filepath"
)

func splitJobID(key string) string {
	_, file := filepath.Split(key)
	return file
}
