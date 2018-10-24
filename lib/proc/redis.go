package proc

// Config redis conf info.
type Config struct {
	Path    string
	Master  bool
	Slave   bool
	SlaveOf string
	MemCap  int // memory capacity of M
}

// Redis redis proc info
type Redis struct {
	c    *Config
	proc *Proc
}

// NewRedis new redis proc
func NewRedis(c *Config) *Redis {
	r := &Redis{
		c: c,
	}
	r.proc = NewProc(r.cmd())
	return r
}

func (r *Redis) cmd() string {
	// TODO:build redis cmd with config arg.
	return r.c.Path
}

func (r *Redis) Start() {
	r.proc.Start()
}

func (r *Redis) Stop() {
	r.proc.Stop()
}
