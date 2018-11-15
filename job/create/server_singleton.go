package create

import (
	"context"
	"encoding/json"
	"fmt"
	"overlord/config"
	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/proto"
	"strings"
	"text/template"
)

// CacheInfo is the server side create cache info.
type CacheInfo struct {
	JobID string

	Name string

	CacheType proto.CacheType

	MaxMemory float64

	Number int

	// for redis : it will be ignore becasue redis only run 1 cpu at all.
	Thread int

	Version string

	Dist *chunk.Dist
}

// NewCacheJob will create deploy cache job.
func NewCacheJob(e *etcd.Etcd, info *CacheInfo) *CacheJob {
	return &CacheJob{e: e, info: info}
}

// CacheJob is the job for framework running
type CacheJob struct {
	e    *etcd.Etcd
	info *CacheInfo
}

func (c *CacheJob) saveTplFile(ctx context.Context, path, conf, name string, data map[string]interface{}) error {
	tpl, err := template.New(name).Parse(conf)
	if err != nil {
		return err
	}

	var sb strings.Builder
	err = tpl.Execute(&sb, data)
	if err != nil {
		return err
	}
	err = c.e.Set(ctx, path, sb.String())
	return err
}

func (c *CacheJob) buildTplTree() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sb strings.Builder
	err := json.NewEncoder(&sb).Encode(c.info)
	if err != nil {
		return err
	}

	err = c.e.Set(ctx, fmt.Sprintf("%s/%s/info", etcd.ClusterDir, c.info.Name), sb.String())
	if err != nil {
		return err
	}

	for _, addr := range c.info.Dist.Addrs {
		instanceDir := fmt.Sprintf(etcd.InstanceDir, addr.IP, addr.Port)

		err = c.e.Set(ctx, fmt.Sprintf("%s/type", instanceDir), string(c.info.CacheType))
		if err != nil {
			return err
		}

		if c.info.CacheType == proto.CacheTypeRedis {
			data := map[string]interface{}{
				"Port":             addr.Port,
				"MaxMemoryInBytes": int(c.info.MaxMemory * 1024 * 1024),
			}
			err = c.saveTplFile(ctx,
				fmt.Sprintf("%s/redis.conf", instanceDir),
				config.RedisConfTpl, "redis.conf", data)

			if err != nil {
				return err
			}
		} else if c.info.CacheType == proto.CacheTypeMemcache {
			data := map[string]interface{}{
				"Port":      addr.Port,
				"Version":   c.info.Version,
				"MaxMemory": c.info.MaxMemory,
				"Thread":    c.info.Thread,
			}
			err = c.saveTplFile(ctx,
				fmt.Sprintf("%s/memcache.sh", instanceDir),
				config.MemcacheScriptTpl, "memcache.sh", data)
			if err != nil {
				return err
			}
		}

		err = c.e.Set(ctx, fmt.Sprintf("%s/version", instanceDir), c.info.Version)
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
	}

	return nil
}

func (c *CacheJob) setupInstanceDir() error {
	sub, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := fmt.Sprintf(etcd.ClusterInstancesDir, c.info.Name)

	for _, addr := range c.info.Dist.Addrs {
		addr := fmt.Sprintf("%s:%d", addr.IP, addr.Port)
		_, err := c.e.GenID(sub, path, addr)
		if err != nil {
			log.Infof("fail to create etcd path due to %s", err)
			return err
		}
	}
	return nil
}

// Create Cache instance
func (c *CacheJob) Create() error {
	err := c.setupInstanceDir()
	if err != nil {
		return err
	}
	return c.buildTplTree()
}
