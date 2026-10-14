package graphchange

// CommitResult reports the durable effects of a committed graph transaction.
type CommitResult struct {
	OperationCount    int32
	CommittedRevision int64
	Changes           []Change
}
