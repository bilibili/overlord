package config

import (
	"overlord/lib/log"
)

// ServerConfig is apiserver's config
type ServerConfig struct {
	Listen   string `toml:"listen"`
	Etcd     string `toml:"etcd"`
	Versions []*VersionConfig `toml:"versions"`
	*log.Config
}

// VersionConfig is the config for used version
type VersionConfig struct {
	CacheType string   `toml:"cache_type"`
	Versions  []string `toml:"versions"`
}
