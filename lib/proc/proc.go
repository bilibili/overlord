package proc

import (
	"context"
	"os/exec"
	"overlord/lib/log"
)

// Proc define process with cancel.
type Proc struct {
	ctx    context.Context
	cancel context.CancelFunc
	cmd    *exec.Cmd
}

// NewProc new and return proc with cancel.
func NewProc(name string, arg ...string) *Proc {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, name, arg...)
	return &Proc{
		ctx:    ctx,
		cancel: cancel,
		cmd:    cmd,
	}
}

// Start start proc.
func (p *Proc) Start() (err error) {
	log.Infof("start service %s %v", p.cmd.Path, p.cmd.Args)
	return p.cmd.Start()
}

// Stop stop process by useing cancel.Stop
func (p *Proc) Stop() {
	p.cancel()
}

// Wait wait proc to exit.
func (p *Proc) Wait() error {
	return p.cmd.Wait()
}
