package model

import "errors"

// ErrNotFound reports that a requested blob does not exist in the requested
// scope.
var ErrNotFound = errors.New("blob not found")
