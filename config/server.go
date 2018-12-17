package config

import (
	"fmt"
	"overlord/lib/log"
)

// ServerConfig is apiserver's config
type ServerConfig struct {
	Listen   string           `toml:"listen"`
	Etcd     string           `toml:"etcd"`
	Versions []*VersionConfig `toml:"versions"`
	Monitor  *MonitorConfig   `toml:"monitor"`
	*log.Config
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
}
