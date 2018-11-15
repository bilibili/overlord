package job

// StateType is the state enum for job
type StateType = string

// define status enum
var (
	StatePending StateType = "pending"
	StateRunning StateType = "running"
	StateRecover StateType = "recover"

	// done status
	StateDone StateType = "done"

	// fail status
	StateLost StateType = "lost"
	StateFail StateType = "fail"
)

// define cluster deploying state enum
var (
	StateChunking StateType = "deploy_chunking"
	// StateNeedBalance is the state which all executer has ben succeed lunched the cache instance
	StateNeedBalance StateType = "deploy_needbalance"
)
