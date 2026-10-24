package service

import (
	"context"
	"io"

	"github.com/myceldb/mycel/internal/automation/actions"
	blobmodel "github.com/myceldb/mycel/internal/blob/model"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	schema "github.com/myceldb/mycel/internal/schema/model"
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

// SchemaReader is the schema capability required by automation conditions.
// Concrete schema managers satisfy this interface implicitly.
type SchemaReader interface {
	GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error)
}

// BlobReader is the blob capability required by automation image inputs.
// Concrete blob managers satisfy this interface implicitly.
type BlobReader interface {
	OpenBlobInDomain(ctx context.Context, spaceID string, domainID string, blobID string) (blobmodel.BlobMeta, io.ReadCloser, error)
}

// InferenceInvoker is the inference capability required by automation runs.
// Concrete inference runtimes satisfy this interface implicitly.
type InferenceInvoker interface {
	Invoke(ctx context.Context, req inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error)
}

type inferenceAuthorizer interface {
	Resolve(ctx context.Context, req inferenceruntime.ResolveRequest) (inferenceruntime.ResolveResult, error)
}

type inferenceAuthorizationRevisionReader interface {
	CurrentAuthorizationRevisions(ctx context.Context, spaceID string) (globalRevision uint64, spaceRevision uint64, err error)
	AssertGlobalAuthorizationRevision(ctx context.Context, expected uint64) error
}

// GraphRuntime is the graph capability set required by automation execution and
// condition evaluation. Real graph managers satisfy this interface implicitly.
type GraphRuntime interface {
	actions.Graphs
	CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error)
	LookupIdempotentCommit(ctx context.Context, principalID, spaceID, domainID, key string) (graphchange.CommitResult, bool, error)
	ScanAdjacency(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error)
	CommitTransactionGraph(ctx context.Context, tx sessionmodel.GraphTransaction) (graphchange.CommitResult, error)
	DiscardTransactionGraph(ctx context.Context, transactionID string) error
	AcknowledgeTransactionFinalization(ctx context.Context, tx sessionmodel.GraphTransaction) error
}
