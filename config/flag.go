package config

// define run mode
const (
	RunModeTest    = "test"
	RunModePreProd = "pre-prod"
	RunModeProd    = "prod"
)

// RunModeType is defined as a string with const
type RunModeType = string

// default runMode is test
var runMode = RunModeTest

// SetRunMode is very import to setup run mode
// if executor was run with prod must setup the RunMode first.
func SetRunMode(mode RunModeType) {
	if mode == RunModeProd || mode == RunModePreProd || mode == RunModeTest {
		runMode = mode
		return
	}
	panic("fail to setup unsupport run-mode")
}

// GetRunMode will get the runMode global variables.
func GetRunMode() string {
	return runMode
}
