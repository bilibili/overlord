package mesos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeResource(t *testing.T) {
	rs := makeResources(0.1, 100, 31000)
	assert.Equal(t, "cpus:0.1;mem:100;ports:[31000-31001]", rs.String())
}
