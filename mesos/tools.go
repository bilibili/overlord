package mesos

import (
	"fmt"
	"path/filepath"
)

func splitJobID(key string) string {
	keysp := filepath.SplitList(key)
	return fmt.Sprintf("%s/%s", keysp[len(keysp)-2], keysp[len(keysp)-1])
}
