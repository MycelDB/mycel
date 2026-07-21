package wal

import "fmt"

// LSN is a monotonically increasing write-ahead log sequence number.
type LSN uint64

const ZeroLSN LSN = 0

func (l LSN) IsZero() bool   { return l == 0 }
func (l LSN) Next() LSN      { return l + 1 }
func (l LSN) String() string { return fmt.Sprintf("%016d", uint64(l)) }
