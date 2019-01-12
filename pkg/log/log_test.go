package log_test

import (
	"testing"

	"overlord/pkg/log"

	"github.com/pkg/errors"
)

func TestLog(t *testing.T) {
	// std := log.NewStdHandler()
	// f := log.NewFileHandler("/tmp/overlord.log")

	log.Init(&log.Config{
		Stdout: true,
		Debug:  true,
		Log:    "/tmp/overlord.log",
		LogVL:  10,
	})

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
		log.Infof("this cannot be print:%s", "yeah")
		log.Warn("this cannot be print")
		log.Warnf("this cannot be print:%s", "yeah")
		log.Error("this cannot be print")
		log.Errorf("this cannot be print:%s", "yeah")
	}
	if log.V(2) {
		log.Info("this will be printing1")
		log.Infof("this will be printing1:%s", "yeah")
		log.Warn("this will be printing1")
		log.Warnf("this will be printing1:%s", "yeah")
		log.Error("this will be printing1")
		log.Errorf("this will be printing1:%s", "yeah")
	}
	log.V(3).Info("this will be printing2")
	log.V(3).Infof("this will be printing2:%s", "yeah")
	log.V(3).Warn("this will be printing2")
	log.V(3).Warnf("this will be printing2:%s", "yeah")
	log.V(3).Error("this will be printing2")
	log.V(3).Errorf("this will be printing2:%s", "yeah")

	log.Errorf("stack:%+v", errors.New("this is a error"))

	if err := log.Close(); err != nil {
		t.Error(err)
	}
}
