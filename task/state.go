package task

// StateType is the state enum for task
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
	StateSpawn StateType = "deploy_spawn"
	// first wait consistent
	StateWaiting StateType = "deploy_wait"
	StateConsistent StateType = "deploy_wait_consistent"
	StateBalancing  StateType = "deploy_balancing"
)
