package chunk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDist(t *testing.T) {
	offers := _createOffers(5, 100, 20, 1000, 2000)
	dist, err := DistIt(10, 10, 1, offers...)
	assert.NoError(t, err)
	assert.Len(t, dist.Addrs, 10)
}

func TestDistAppendIt(t *testing.T) {
	offers := _createOffers(7, 100, 20, 1000, 2000)
	dist, err := DistIt(10, 10, 1, offers[:5]...)
	assert.NoError(t, err)
	assert.Len(t, dist.Addrs, 10)
	err = DistAppendIt(dist, 2, 2, 2, offers...)
	assert.NoError(t, err)
	assert.Len(t, dist.Addrs, 12)
	t.Log(dist.Addrs)
}
