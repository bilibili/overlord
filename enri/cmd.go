package enri

import (
	"os"

	"github.com/urfave/cli"
)

var (
	nodes    cli.StringSlice
	seed     string
	slave    int
	src, dst string
	count    int64
	slot     int64
)

var app *cli.App

// Run run enri cli.
func Run() {
	app = cli.NewApp()
	add := cli.Command{
		Name:        "add",
		ShortName:   "a",
		Usage:       "add nodes to cluster",
		Description: "add nodes to cluster",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "cluster,c",
				Usage:       "origin node of cluster",
				Destination: &seed,
			},
			cli.StringSliceFlag{
				Name:  "node,n",
				Usage: "nodes to be added into cluster",
				Value: &nodes,
			},
		},
		Action: func(c *cli.Context) error {
			_, err := Add(seed, nodes)
			return err
		},
	}
	fix := cli.Command{
		Name:      "fix",
		ShortName: "f",
		Usage:     "fix cluster",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "node,n",
				Usage:       " addr of cluster",
				Destination: &seed,
			},
		},
		Action: func(c *cli.Context) error {
			_, err := Fix(seed)
			return err
		},
	}
	reshard := cli.Command{
		Name:      "reshard",
		ShortName: "r",
		Usage:     "reshard cluster",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "node,n",
				Usage:       " addr of cluster",
				Destination: &seed,
			},
		},
		Action: func(c *cli.Context) error {
			_, err := Reshard(seed)
			return err
		},
	}
	create := cli.Command{
		Name:        "create",
		ShortName:   "c",
		Usage:       "enri create [nodes]",
		Description: "add nodes to cluster",
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "node,n",
				Usage: "nodes to be added into cluster",
				Value: &nodes,
			},
			cli.IntFlag{
				Name:        "slave,s",
				Usage:       "slave count",
				Destination: &slave,
			},
		},
		Action: func(c *cli.Context) error {
			_, err := Create(nodes, slave)
			return err
		},
	}
	migrate := cli.Command{
		Name:        "migrate",
		ShortName:   "m",
		Usage:       "enri migrate ",
		Description: "migrate slot",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "origin,o",
				Usage:       "-s node",
				Destination: &src,
			},
			cli.StringFlag{
				Name:        "dst,d",
				Usage:       "-d node",
				Destination: &dst,
			},
			cli.Int64Flag{
				Name:        "count,c",
				Usage:       "slot count",
				Destination: &count,
			},
			cli.Int64Flag{
				Name:        "slot,s",
				Usage:       "slot num",
				Destination: &slot,
			},
		},
		Action: func(c *cli.Context) error {
			err := Migrate(src, dst, count, slot)
			return err
		},
	}
	del := cli.Command{
		Name:        "del",
		ShortName:   "d",
		Usage:       "del nodes from cluster",
		Description: "del nodes from  cluster",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "cluster,c",
				Usage:       "origin node of cluster",
				Destination: &seed,
			},
			cli.StringSliceFlag{
				Name:  "node,n",
				Usage: "nodes to be deleted from cluster",
				Value: &nodes,
			},
		},
		Action: func(c *cli.Context) error {
			_, err := Delete(seed, nodes)
			return err
		},
	}
	app.Commands = []cli.Command{add, create, del, migrate, fix, reshard}
	app.Run(os.Args)
}
