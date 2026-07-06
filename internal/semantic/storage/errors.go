package semantic

import "errors"

var (
	ErrInvalidInput = errors.New("invalid semantic store input")
	ErrNotFound     = errors.New("semantic resource not found")
	ErrConflict     = errors.New("semantic resource conflict")
)
