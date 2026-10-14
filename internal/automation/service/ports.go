package service

import (
	"context"

	"github.com/myceldb/mycel/internal/automation/actions"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

// SessionRuntime is the session capability set required by automation execution.
// It is intentionally narrower than session/service.Manager so automation does
// not depend on unrelated session lifecycle operations.
type SessionRuntime interface {
	OpenSession(ctx context.Context, input sessionmodel.OpenSessionInput) (sessionmodel.GraphSession, error)
	BeginTransaction(ctx context.Context, input sessionmodel.BeginTransactionInput) (sessionmodel.GraphTransaction, error)
	CommitTransactionAtRevision(ctx context.Context, principalID string, transactionID string, operationCount int32, committedRevision int64) (sessionmodel.TransactionCommit, error)
	RollbackTransaction(ctx context.Context, principalID string, transactionID string) (sessionmodel.GraphTransaction, error)
}

// GraphRuntime is the graph capability set required by automation execution and
// condition evaluation. Real graph managers satisfy this interface implicitly.
type GraphRuntime interface {
	actions.Graphs
	CurrentRevision(ctx context.Context, spaceID string) (int64, error)
	ScanAdjacency(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphservice.AdjacencyScan) ([]graph.Edge, string, graphservice.IndexedReadStats, error)
	CommitTransactionGraph(ctx context.Context, tx sessionmodel.GraphTransaction) (graphservice.CommitResult, error)
	DiscardTransactionGraph(ctx context.Context, transactionID string)
}
