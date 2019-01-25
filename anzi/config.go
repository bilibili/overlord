package anzi

// Config is the struct which used by cmd/anzi
type Config struct {
	Root *RootConfig `toml:"root"`
}

// RootConfig is the root configs of anzi.
type RootConfig struct {
	Listen  string `toml:"listen"`
	WorkDir string `toml:"workdir;default=./"`
}

// MigrateConfig is the config file which nedd to read/write into target dir.
type MigrateConfig struct {
	From              []*ServersConfig `toml:"from"`
	To                []*ServersConfig `toml:"to"`
	MaxRDBConcurrency int             `toml:"max_rdb_concurrency"`
}

type ServersConfig struct {
}
