package model

import (
	"fmt"
	"overlord/pkg/log"
)

// ServerConfig is apiserver's config
type ServerConfig struct {
	Listen   string                `toml:"listen"`
	Etcd     string                `toml:"etcd"`
	Versions []*VersionConfig      `toml:"versions"`
	Groups   map[string]string     `toml:"groups"`
	Monitor  *MonitorConfig        `toml:"monitor"`
	Cluster  *DefaultClusterConfig `toml:"cluster"`
	*log.Config
}

// DefaultClusterConfig is the config used to write into cluster
type DefaultClusterConfig struct {
	DialTimeout   int  `toml:"dial_timeout"`
	ReadTimeout   int  `toml:"read_timeout"`
	WriteTimeout  int  `toml:"write_timeout"`
	NodeConns     int  `toml:"node_connections"`
	PingFailLimit int  `toml:"ping_fail_limit"`
	PingAutoEject bool `toml:"ping_auto_eject"`
}

// MonitorConfig types
type MonitorConfig struct {
	URL     string `toml:"url"`
	Panel   string `toml:"panel"`
	NameVar string `toml:"name_var"`
	// in url is `orgId`
	OrgID int `toml:"org_id"`
}

// Href get monitory href
func (mc *MonitorConfig) Href(cname string) string {
	return fmt.Sprintf("%s/%s?orgId=%d&var-%s=%s", mc.URL, mc.Panel, mc.OrgID, mc.NameVar, cname)
}

// VersionConfig is the config for used version
type VersionConfig struct {
	CacheType string   `toml:"cache_type"`
	Versions  []string `toml:"versions"`
	Image     string   `toml:"image"`
}
