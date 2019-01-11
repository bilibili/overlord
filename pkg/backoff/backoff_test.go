package backoff

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	for i, j := 0, 0; i < 120; i++ {
		if i == 10 {
			j = 0
		}
		tm := Backoff(j)
		j++
		t.Logf("backoff second: %d", tm/time.Second)
	}
}
