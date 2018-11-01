package create

import (
	"overlord/lib/chunk"
)

// ClusterInfo is the arguments for create cluster task which was validated by apiserver.
type ClusterInfo struct {
	TaskID string
	// MaxMemory in MB
	MaxMemory float64

	MasterNum int

	// 1.0 means accept 1 cpu
	// default normal spped node acquires 0.5 cpu
	// default high spped node acquires 1 cpu
	// default slow speed node acquires 0.2 cpu
	NodeCPU float64

	Version string

	AppIDs []string

	Chunks []*chunk.Chunk
}

// ServerCreateCluster creates new cluster and wait for cluster done
func ServerCreateCluster(info *ClusterInfo) error {
	return nil
}
