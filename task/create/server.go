package create

import (
	"context"
	"encoding/json"
	"fmt"
	"overlord/config"
	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"strings"
	"text/template"
)

// maybe global defines
const (
	ClusterInstancesPath = "/clusters/%s/instances/"
	InstancePath         = "/instances/%s:%d"
	ClusterSlotsCount    = 16384
)

// RedisClusterInfo is the arguments for create cluster task which was validated by apiserver.
type RedisClusterInfo struct {
	TaskID      string
	ClusterName string
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

	IDMap map[string]map[int]int
}

// RedisClusterTask description a task for create new cluster.
type RedisClusterTask struct {
	e *etcd.Etcd
}

// ServerCreateCluster creates new cluster and wait for cluster done
func (c *RedisClusterTask) ServerCreateCluster(info *RedisClusterInfo) error {
	err := c.setupIDMap(info)
	if err != nil {
		return err
	}
	c.divideSlots(info)
	c.setupSlaveOf(info)
	// create new redis etcd tpl files
	return c.buildTplTree(info)
}

func (c *RedisClusterTask) buildTplTree(info *RedisClusterInfo) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, cc := range info.Chunks {
		for _, node := range cc.Nodes {
			instanceDir := fmt.Sprintf(InstancePath, node.Name, node.Port)

			err := c.e.Mkdir(ctx, instanceDir)
			if err != nil {
				return err
			}

			content := chunk.GenNodesConfFile(node.Name, node.Port, info.Chunks)
			err = c.e.Set(ctx, fmt.Sprintf("%s/nodes.conf", instanceDir), content)
			if err != nil {
				return err
			}

			tpl, err := template.New("redis.conf").Parse(config.RedisConfTpl)
			if err != nil {
				return err
			}

			var sb strings.Builder
			err = tpl.Execute(&sb, map[string]interface{}{
				"Port":             node.Port,
				"MaxMemoryInBytes": info.MaxMemory * 1024 * 1024,
			})
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/redis.conf", instanceDir), sb.String())
			if err != nil {
				return err
			}
			sb.Reset()
			err = json.NewEncoder(&sb).Encode(info)
			if err != nil {
				return err
			}
			err = c.e.Set(ctx, fmt.Sprintf("%s/info", instanceDir), sb.String())
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/taskid", instanceDir), info.TaskID)
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/version", instanceDir), info.Version)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *RedisClusterTask) divideSlots(info *RedisClusterInfo) {
	per := ClusterSlotsCount / info.MasterNum
	left := ClusterSlotsCount % info.MasterNum
	base := 0
	cursor := 0

	for _, cc := range info.Chunks {
		for _, node := range cc.Nodes {
			sc := per
			if cursor <= left {
				sc++
			}

			node.Slots = []chunk.Slot{{Begin: base, End: base + sc}}
			base += sc
			cursor++
		}
	}
}

func (c *RedisClusterTask) setupSlaveOf(info *RedisClusterInfo) {
	for _, chunk := range info.Chunks {
		for _, node := range chunk.Nodes {
			id := info.IDMap[node.Name][node.Port]
			node.RunID = fmt.Sprintf("%x", id)
		}
		chunk.Nodes[1].SlaveOf = chunk.Nodes[2].RunID
		chunk.Nodes[3].SlaveOf = chunk.Nodes[0].RunID
	}
}

func (c *RedisClusterTask) setupIDMap(info *RedisClusterInfo) error {
	path := fmt.Sprintf(ClusterInstancesPath, info.ClusterName)
	hostmap := chunk.GetHostCountInChunks(info.Chunks)
	idMap := make(map[string]map[int]int)
	for node, ports := range hostmap {
		for _, port := range ports {
			addr := fmt.Sprintf("%s:%d", node, port)
			id, err := c.e.GenID(context.Background(), path, addr)
			if err != nil {
				log.Infof("fail to create etcd path due to %s", err)
				return err
			}
			if maps, ok := idMap[node]; ok {
				maps[port] = id
			} else {
				idMap[addr] = map[int]int{port: id}
			}
		}
	}
	info.IDMap = idMap

	return nil
}
