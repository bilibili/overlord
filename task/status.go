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
