package container

import (
	"context"
	"fmt"
	"io"
	"os"
	"overlord/pkg/log"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

// Container define container with cancel.
type Container struct {
	ctx    context.Context
	cancel context.CancelFunc
	cli    client.APIClient
	id     string
	image  string
	name   string
}

// New new and return container with cancel.
func New(image, name, workdir string, cmd []string) (c *Container, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	c = &Container{
		ctx:    ctx,
		cancel: cancel,
		image:  image,
		name:   name,
	}
	c.cli, err = client.NewEnvClient()
	if err != nil {
		return
	}

	var output io.ReadCloser
	output, err = c.cli.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return
	}
	io.Copy(os.Stdout, output)

	resp, err := c.cli.ContainerCreate(ctx, &container.Config{
		Image:      image,
		WorkingDir: workdir,
		Entrypoint: cmd,
	}, &container.HostConfig{
		NetworkMode: "host",
		Mounts: []mount.Mount{{
			Type:   mount.TypeBind,
			Source: workdir,
			Target: workdir,
		}},
	}, nil, name)
	if err != nil {
		return
	}
	c.id = resp.ID
	return
}

// Start start container.
func (c *Container) Start() (err error) {
	log.Infof("start service %s: %v", c.image, c.name)

	if c.id == "" {
		err = fmt.Errorf("container %s absent", c.id)
		return
	}

	err = c.cli.ContainerStart(c.ctx, c.id, types.ContainerStartOptions{})
	return
}

// Stop stop container by using cancel.Stop
func (c *Container) Stop() {
	if c.id != "" {
		timeout := 10 * time.Second
		c.cli.ContainerStop(c.ctx, c.id, &timeout)
	}
	c.cancel()
}

// Wait wait container to exit.
func (c *Container) Wait() error {
	if c.id == "" {
		return fmt.Errorf("container %s absent", c.id)
	}
	_, err := c.cli.ContainerWait(c.ctx, c.id)
	return err
}
