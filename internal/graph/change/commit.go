package graphchange

import "time"

// CommitResult reports the durable effects of a committed graph transaction.
type CommitResult struct {
	OperationCount       int32
	CommittedRevision    int64
	Changes              []Change
	CommitID             string
	TransactionID        string
	SessionID            string
	SpaceID              string
	DomainID             string
	BaseRevision         int64
	OperationID          string
	CommittedAt          time.Time
	IdempotencyKey       string
	IdempotencyExpiresAt time.Time
	IdempotentReplay     bool
}
