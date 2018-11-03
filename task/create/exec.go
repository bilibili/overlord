// Package create is the package of create and make Ken happy.
package create

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"overlord/lib/dir"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"path/filepath"

	"overlord/config"

	"github.com/BurntSushi/toml"
	"overlord/proto"
)

func getDefaultServiceWorkDir() string {
	if config.GetRunMode() != config.RunModeTest {
		return "/data/%d"
	}
	return "/tmp/data/%d"
}

// DeployInfo is the struct to communicate between etcd and executor
// must be serialized and deserialized by json
type DeployInfo struct {
	// TaskID is the id of task
	TaskID string

	CacheType string

	Port    int
	Version string

	// TplTree is the Tree which contains a key as path of the file,
	// and value as the content of the file.
	TplTree map[string]string
}

// GenDeployInfo will create new deploy info from etcd
func GenDeployInfo(e *etcd.Etcd, ip string, port int) (info *DeployInfo, err error) {
	var (
		val         string
		instanceDir = fmt.Sprintf(InstancePath, ip, port)
		workdir     = fmt.Sprintf(getDefaultServiceWorkDir(), port)
	)

	info.TplTree = make(map[string]string)
	info.CacheType, err = e.Get(context.TODO(), fmt.Sprintf("%s/CacheType", instanceDir))
	if err != nil {
		return
	}


	if proto.CacheType(info.CacheType) == proto.CacheTypeRedisCluster {
		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/redis.conf", instanceDir))

		if err != nil {
			return
		}
		info.TplTree[fmt.Sprintf("%s/redis.conf", workdir)] = val

		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/nodes.conf", instanceDir))
		if err != nil {
			return
		}
		info.TplTree[fmt.Sprintf("%s/nodes.conf", workdir)] = val
	} else if proto.CacheType(info.CacheType) == proto.CacheTypeRedis {
		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/redis.conf", instanceDir))
		if err != nil {
			return
		}
		info.TplTree[fmt.Sprintf("%s/redis.conf", workdir)] = val
	} else {
		log.Errorf("unsupported cachetype %s", info.CacheType)
	}


	info.TaskID, err = e.Get(context.TODO(), fmt.Sprintf("%s/taskid", instanceDir))
	if err != nil {
		return
	}

	info.Version, err = e.Get(context.TODO(), fmt.Sprintf("%s/version", instanceDir))
	if err != nil {
		return
	}

	return
}

func renderTplTree(tplTree map[string]string) (err error) {
	var (
		basename string
		abs      string
	)

	for path, content := range tplTree {
		basename, err = dir.GetAbsDir(path)
		if err != nil {
			return
		}
		err = dir.MkDirAll(basename)
		if err != nil {
			return
		}
		abs, err = filepath.Abs(path)
		if err != nil {
			return
		}

		err = ioutil.WriteFile(abs, []byte(content), 0755)
		if err != nil {
			return
		}
	}

	return
}

func renderMetaIntoFile(workdir string, di *DeployInfo) error {
	file := fmt.Sprintf("%s/meta.toml", workdir)
	fd, err := os.Create(file)
	if err != nil {
		return err
	}
	err = fd.Chmod(0755)
	if err != nil {
		return err
	}
	encoder := toml.NewEncoder(fd)

	return encoder.Encode(di)
}

// func outputIntoFile(workdir string, data []byte) error {
// 	console := fmt.Sprintf("%s/console.log", workdir)
// 	return ioutil.WriteFile(console, data, 0755)
// }

// func wrapCmdWithBash(cmd string) string {
// 	return fmt.Sprintf("bash -c \"%s\"", cmd)
// }

// SetupCacheService will create new cache service
func SetupCacheService(info *DeployInfo) error {

	// 1. render template tree into the path
	//   1.1 foreach fpath, content in TplTree
	//   1.4 mkdir for fpath's basedir
	//   1.3 write content into
	err := renderTplTree(info.TplTree)
	if err != nil {
		log.Warnf("error when render template tree")
		return err
	}

	// 2. execute given command
	//   2.0 mk working dir
	workdir := fmt.Sprintf(getDefaultServiceWorkDir(), info.Port)
	err = dir.MkDirAll(workdir)
	if err != nil {
		log.Errorf("fail to create working dir")
		return err
	}
	err = renderMetaIntoFile(workdir, info)
	if err != nil {
		log.Errorf("fail to create meta data file due to %s", err)
		return err
	}

	// 2. setup systemd serivce
	//   2.1 check if binary was exists
	//   2.2 if not, pull it from scheduler and then setup systemd config

	// 3. spawn a new redis cluster service

	// argv, err := shlex.Split(info.ExecStart)
	// if err != nil {
	// 	return err
	// }

	// cmd := exec.Command(argv[0], argv[1:]...)
	// cmd.Dir = workdir

	// // must wait for remove defunc progress
	// defer func() {
	// 	err := cmd.Wait()
	// 	if err != nil {
	// 		log.Warnf("spawn wait sub command fail due to %s", err)
	// 	}
	// }()

	// output, err := cmd.CombinedOutput()
	// if err != nil {
	// 	log.Errorf("fail to create output file %s", err)
	// 	_ = outputIntoFile(workdir, output)
	// 	return err
	// }

	// return outputIntoFile(workdir, output)
	return nil
}
