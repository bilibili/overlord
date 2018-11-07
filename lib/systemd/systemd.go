package systemd

import (
	"errors"
	"os/exec"
)

// defind errors
var (
	ErrNotSupportAction = errors.New("aciton not support")
)

// ActionType is the type which was used for systemd safe check
type ActionType string

// define actions
const (
	ActionStart        = "start"
	ActionRestart      = "restart"
	ActionStop         = "stop"
	ActionDaemonReload = "daemon-reload"
)

func checkSafe(action ActionType) bool {
	switch action {
	case ActionDaemonReload, ActionStop, ActionRestart, ActionStart:
		return true
	default:
		return false
	}
}

// Run limited allowed command
func Run(serviceName string, action ActionType) error {
	if checkSafe(action) {
		return ErrNotSupportAction
	}
	cmd := exec.Command("systemctl", string(action), serviceName)
	return cmd.Run()
}

// Start service
func Start(serviceName string) error {
	return Run(serviceName, ActionStart)
}

// Stop service
func Stop(serviceName string) error {
	return Run(serviceName, ActionStop)
}

// Restart service
func Restart(serviceName string) error {
	return Run(serviceName, ActionRestart)
}

// DaemonReload systemd
func DaemonReload() error {
	cmd := exec.Command("systemctl", string(ActionDaemonReload))
	return cmd.Run()
}
