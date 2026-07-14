package wal

import "errors"

var (
	ErrCorrupt        = errors.New("wal corrupt")
	ErrInvalidRecord  = errors.New("invalid wal record")
	ErrIteratorClosed = errors.New("wal iterator closed")
)
