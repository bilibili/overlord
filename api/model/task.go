package model

// Job is the json-encodable struct
type Job struct {
	ID    string `json:"id"`
	State string `json:"state"`
}
