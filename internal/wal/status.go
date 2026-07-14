package wal

// Status describes replication-relevant WAL positions.
type Status struct {
	LastCommittedLSN    LSN
	OldestRetainedLSN   LSN
	CurrentSegmentStart LSN
	CurrentSegmentBytes int64
}
