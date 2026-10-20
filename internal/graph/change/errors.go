package graphchange

import "errors"

// ErrConflict reports that a graph commit could not be applied because its
// expected revision conflicts with a concurrent graph change.
var ErrConflict = errors.New("graph conflict")
