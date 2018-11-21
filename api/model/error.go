package model

import "errors"

// define global errors
var (
	ErrConflict = errors.New("conflict")
	ErrNotFound = errors.New("not found")
)
