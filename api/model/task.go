package model

// Task is the json-encodable struct
type Task struct {
	ID    string `json:"id"`
	State string `json:"state"`
}
