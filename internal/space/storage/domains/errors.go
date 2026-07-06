package domains

import "errors"

var (
	ErrInvalidInput   = errors.New("invalid input")
	ErrDomainNotFound = errors.New("domain not found")
	ErrConflict       = errors.New("domain conflict")
)
