package enri

import (
	"errors"
	"os"

	"overlord/pkg/log"

	"github.com/urfave/cli"
)

var (
	errFlag  = errors.New("error flags")
	nodes    cli.StringSlice
	seed     string
	slave    int
	src, dst string
	count    int64
	slot     int64
	std      bool
	logFile  string
)

// Run run enri cli.
func Run() {

	app := cli.NewApp()
	app.Usage = "redis cluster manager tool"
	app.Version = "v0.1.0"
	app.Authors = []cli.Author{{Name: "lintanghui", Email: "xmutanghui@gmail.com"}}
	logFlag := []cli.Flag{
		cli.BoolFlag{
			Name:        "std",
			Usage:       "print log to std",
			Destination: &std,
		},
		cli.StringFlag{
			Name:        "log",
			Usage:       "print log to file",
			Destination: &logFile,
		},
	}
	logActon := func() {
		var handlers []log.Handler
		if std {
			handlers = append(handlers, log.NewStdHandler())
		}
		if logFile != "" {
			handlers = append(handlers, log.NewFileHandler(logFile))
		}
		if len(handlers) != 0 {
			log.InitHandle(handlers...)
		}
	}
	add := cli.Command{
		Name:        "add",
		ShortName:   "a",
		Usage:       "add nodes to cluster",
		Description: "add nodes into cluster",
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
			logActon()
			if seed == "" || len(nodes) == 0 {
				cli.ShowCommandHelp(c, "add")
				return errFlag
			}
			_, err := Add(seed, nodes)
			return err
		},
	}
	add.Flags = append(add.Flags, logFlag...)
	fix := cli.Command{
		Name:      "fix",
		ShortName: "f",
		Usage:     "fix cluster",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "node,n",
				Usage:       "addr of cluster",
				Destination: &seed,
			},
		},
		Action: func(c *cli.Context) error {
			logActon()
			if seed == "" {
				cli.ShowCommandHelp(c, "fix")
				return errFlag
			}
			_, err := Fix(seed)
			return err
		},
	}
	fix.Flags = append(fix.Flags, logFlag...)
	reshard := cli.Command{
		Name:      "reshard",
		ShortName: "r",
		Usage:     "reshard cluster",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "node,n",
				Usage:       "addr of cluster",
				Destination: &seed,
			},
		},
		Action: func(c *cli.Context) error {
			logActon()
			if seed == "" {
				cli.ShowCommandHelp(c, "reshard")
				return errFlag
			}
			_, err := Reshard(seed)
			return err
		},
	}
	reshard.Flags = append(reshard.Flags, logFlag...)
	create := cli.Command{
		Name:      "create",
		ShortName: "c",
		Usage:     "create cluster by nodes",
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
			logActon()
			if len(nodes) == 0 || slave == 0 {
				cli.ShowCommandHelp(c, "create")
				return errFlag
			}
			_, err := Create(nodes, slave)
			return err
		},
	}
	create.Flags = append(create.Flags, logFlag...)
	migrate := cli.Command{
		Name:      "migrate",
		ShortName: "m",
		Usage:     "migrate cluster slot of nodes",
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
				Value:       -1,
				Destination: &slot,
			},
		},
		Action: func(c *cli.Context) error {
			logActon()
			err := Migrate(src, dst, count, slot)
			if err != nil {
				log.Errorf("migrate slot err %v", err)
				cli.ShowCommandHelp(c, "migrate")
				return err
			}
			return nil
		},
	}
	migrate.Flags = append(migrate.Flags, logFlag...)
	replicate := cli.Command{
		Name:      "replicate",
		ShortName: "repl",
		Usage:     "migrate cluster slot of nodes",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "master,m",
				Usage:       "-s node",
				Destination: &src,
			},
			cli.StringFlag{
				Name:        "slave,s",
				Usage:       "-d node",
				Destination: &dst,
			},
		},
		Action: func(c *cli.Context) error {
			logActon()
			_, err := Replicate(src, dst)
			if err != nil {
				cli.ShowCommandHelp(c, "replicate")
				return errFlag
			}
			return err
		},
	}
	replicate.Flags = append(replicate.Flags, logFlag...)
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
			logActon()
			if seed == "" || len(nodes) == 0 {
				cli.ShowCommandHelp(c, "del")
				return errFlag
			}
			_, err := Delete(seed, nodes)
			return err
		},
	}
	del.Flags = append(del.Flags, logFlag...)
	info := cli.Command{
		Name:        "info",
		Usage:       "get cluster info",
		Description: "get cluster info",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "cluster,c",
				Usage:       "origin node of cluster",
				Destination: &seed,
			},
		},
		Action: func(c *cli.Context) error {
			logActon()
			if seed == "" {
				cli.ShowCommandHelp(c, "info")
				return errFlag
			}
			err := Info(seed)
			return err
		},
	}
	info.Flags = append(info.Flags, logFlag...)
	check := cli.Command{
		Name:        "check",
		Usage:       "check cluster state",
		Description: "check cluster state",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:        "cluster,c",
				Usage:       "origin node of cluster",
				Destination: &seed,
			},
		},
		Action: func(c *cli.Context) error {
			logActon()
			if seed == "" {
				cli.ShowCommandHelp(c, "check")
				return errFlag
			}
			err := Check(seed)
			return err
		},
	}
	check.Flags = append(check.Flags, logFlag...)
	app.Commands = []cli.Command{add, create, del, migrate, fix, reshard, replicate, info, check}
	app.Run(os.Args)
}
