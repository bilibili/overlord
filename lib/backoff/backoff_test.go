package backoff_test

import (
	"testing"
	"time"

	"overlord/lib/backoff"
)

func TestBackoff(t *testing.T) {
	for i, j := 0, 0; i < 120; i++ {
		if i == 10 {
			j = 0
		}
		tm := backoff.Backoff(j)
		j++
		t.Logf("backoff second: %d", tm/time.Second)
	}
}
