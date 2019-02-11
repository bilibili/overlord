package anzi

import (
	"overlord/pkg/log"
	"overlord/proxy"
)

// Config is the struct which used by cmd/anzi
type Config struct {
	*log.Config
	Migrate *MigrateConfig `toml:"migrate"`
}

// MigrateConfig is the config file which nedd to read/write into target dir.
type MigrateConfig struct {
	From              []*proxy.ClusterConfig `toml:"from"`
	To                *proxy.ClusterConfig   `toml:"to"`
	MaxRDBConcurrency int                    `toml:"max_rdb_concurrency"`
}
