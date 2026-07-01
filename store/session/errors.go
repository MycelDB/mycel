package session

import "errors"

var (
	ErrInvalidInput       = errors.New("invalid input")
	ErrSessionNotFound    = errors.New("refresh session not found")
	ErrDuplicateSessionID = errors.New("duplicate refresh session id")
	ErrDuplicateTokenHash = errors.New("duplicate refresh token hash")
)
