package spaces

import "errors"

var (
	ErrInvalidInput  = errors.New("invalid input")
	ErrSpaceNotFound = errors.New("space not found")
)
