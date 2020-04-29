package create

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"overlord/pkg/etcd"
	"overlord/pkg/log"
	"overlord/platform/chunk"
)

// maybe global defines
const (
	ClusterSlotsCount = 16384
)

// define subjob state
const (
	SubStatePending      = "pending"
	SubStateRunning      = "running"
	SubStateDivide       = "divide_slots"
	SubStateSetupIDMap   = "setup_idmap"
	SubStateSetupSlaveOf = "setup_slaveof"
)

// RedisClusterJob description a job for create new cluster.
type RedisClusterJob struct {
	e    *etcd.Etcd
	info *CacheInfo
}

// NewRedisClusterJob will create new Redis cluster.
func NewRedisClusterJob(e *etcd.Etcd, info *CacheInfo) *RedisClusterJob {
	return &RedisClusterJob{
		e:    e,
		info: info,
	}
}

// Create creates new cluster and wait for cluster done
func (c *RedisClusterJob) Create() error {
	err := c.setupIDMap()
	if err != nil {
		return err
	}
	c.divideSlots()
	c.setupSlaveOf()
	// create new redis etcd tpl files
	err = c.buildTplTree()

	return err
}

func (c *RedisClusterJob) buildTplTree() (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = c.e.SetJobState(ctx, c.info.Group, c.info.JobID, StateBuildTplTree)

	var sb strings.Builder
	err = json.NewEncoder(&sb).Encode(c.info)
	if err != nil {
		return err
	}
	err = c.e.Set(ctx, fmt.Sprintf("%s/%s/info", etcd.ClusterDir, c.info.Name), sb.String())
	if err != nil {
		return err
	}

	for _, cc := range c.info.Chunks {
		for _, node := range cc.Nodes {
			instanceDir := fmt.Sprintf(etcd.InstanceDir, node.Name, node.Port)
			err = cleanEtcdDirtyDir(ctx, c.e, fmt.Sprintf("%s:%d", node.Name, node.Port))
			if err != nil {
				log.Errorf("error clean dirty etcd dir %s", err)
			}

			err = c.e.Mkdir(ctx, instanceDir)
			if err != nil {
				return err
			}
			err = c.e.Set(ctx, fmt.Sprintf("%s/type", instanceDir), "redis_cluster")
			if err != nil {
				return err
			}
			content := chunk.GenNodesConfFile(node.Name, node.Port, c.info.Chunks)
			err = c.e.Set(ctx, fmt.Sprintf("%s/nodes.conf", instanceDir), content)
			if err != nil {
				return err
			}

			tpl, err := template.New("redis.conf").Parse(redisClusterConfTpl)
			if err != nil {
				return err
			}

			sb.Reset()
			err = tpl.Execute(&sb, map[string]interface{}{
				"Port":             node.Port,
				"MaxMemoryInBytes": int(c.info.MaxMemory * 1024 * 1024),
			})
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/redis.conf", instanceDir), sb.String())
			if err != nil {
				return err
			}

			// write role info
			err = c.e.Set(ctx, fmt.Sprintf("%s/role", instanceDir), node.Role)
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/jobid", instanceDir), c.info.JobID)
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/cluster", instanceDir), c.info.Name)
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/version", instanceDir), c.info.Version)
			if err != nil {
				return err
			}

			err = c.e.Set(ctx, fmt.Sprintf("%s/group", instanceDir), c.info.Group)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *RedisClusterJob) divideSlots() {
	_ = c.e.SetJobState(context.TODO(), c.info.Group, c.info.JobID, SubStateDivide)

	per := ClusterSlotsCount / c.info.Number
	left := ClusterSlotsCount % c.info.Number
	base := 0
	cursor := 0

	for _, cc := range c.info.Chunks {
		for _, node := range cc.Nodes {
			if node.Role == chunk.RoleSlave {
				node.Slots = []chunk.Slot{}
				continue
			}

			sc := per
			if cursor < left {
				sc++
			}

			end := base + sc
			if end >= ClusterSlotsCount {
				end = ClusterSlotsCount - 1
			}

			node.Slots = []chunk.Slot{{Begin: base, End: end}}
			base = end + 1
			cursor++
		}
	}
}

func (c *RedisClusterJob) setupSlaveOf() {
	_ = c.e.SetJobState(context.TODO(), c.info.Group, c.info.JobID, SubStateSetupSlaveOf)
	for _, chunk := range c.info.Chunks {
		for _, node := range chunk.Nodes {
			id := c.info.IDMap[node.Name][node.Port]
			node.RunID = id
			fmt.Println(node)
		}
		chunk.Nodes[1].SlaveOf = chunk.Nodes[2].RunID
		chunk.Nodes[3].SlaveOf = chunk.Nodes[0].RunID
	}
}

func (c *RedisClusterJob) setupIDMap() error {
	c.e.SetJobState(context.TODO(), c.info.Group, c.info.JobID, SubStateSetupIDMap)
	path := fmt.Sprintf(etcd.ClusterInstancesDir, c.info.Name)
	err := c.e.RMDir(context.Background(), path)
	if err != nil {
		log.Errorf("error clean previous data fail %s", err)
	}
	hostmap := chunk.GetHostCountInChunks(c.info.Chunks)
	idMap := make(map[string]map[int]string)
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
				idMap[node] = map[int]string{port: id}
			}
		}
	}
	c.info.IDMap = idMap
	return nil
}
