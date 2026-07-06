package store

import "errors"

var (
	ErrInvalidInput  = errors.New("invalid embedding input")
	ErrKeyNotFound   = errors.New("embedding provider key not found")
	ErrInvalidKey    = errors.New("invalid encryption key")
	ErrDecryptFailed = errors.New("embedding secret decrypt failed")
)
