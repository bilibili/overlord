package log_test

import (
	"testing"

	"overlord/lib/log"

	"github.com/pkg/errors"
)

func TestLog(t *testing.T) {
	std := log.NewStdHandler()
	f := log.NewFileHandler("/tmp/overlord.log")

	log.Init(std, f)

	log.Info("test1")
	log.Info("test1", "test2")
	log.Info("test1", "test2", "test3")

	log.Warn("test1")
	log.Warn("test1", "test2")
	log.Warn("test1", "test2", "test3")

	log.Error("test1")
	log.Error("test1", "test2")
	log.Error("test1", "test2", "test3")

	log.Infof("1(%s)", "test1")
	log.Infof("1(%s) 2(%s)", "test1", "test2")
	log.Infof("1(%s) 2(%s) 3(%s)", "test1", "test2", "test3")

	log.Warnf("1(%s)", "test1")
	log.Warnf("1(%s) 2(%s)", "test1", "test2")
	log.Warnf("1(%s) 2(%s) 3(%s)", "test1", "test2", "test3")

	log.Errorf("1(%s)", "test1")
	log.Errorf("1(%s) 2(%s)", "test1", "test2")
	log.Errorf("1(%s) 2(%s) 3(%s)", "test1", "test2", "test3")

	log.DefaultVerboseLevel = 3
	if log.V(5) {
		log.Info("this cannot be print")
	}
	if log.V(2) {
		log.Info("this will be printing1")
	}
	log.V(3).Info("this will be printing2")

	log.Errorf("stack:%+v", errors.New("this is a error"))
}
