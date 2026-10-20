package service

import (
	"context"
	"errors"
	"time"

	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const ModuleName = "session"

var (
	ErrInvalidInput        = errors.New("invalid session input")
	ErrSessionNotFound     = errors.New("session not found")
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrUnauthorized        = errors.New("session unauthorized")
	ErrClosed              = errors.New("session closed")
	ErrInvalidState        = errors.New("invalid session state")
)

type Manager interface {
	OpenSession(ctx context.Context, input sessionmodel.OpenSessionInput) (sessionmodel.GraphSession, error)
	GetSession(ctx context.Context, principalID string, sessionID string) (sessionmodel.GraphSession, error)
	HeartbeatSession(ctx context.Context, principalID string, sessionID string, extension time.Duration) (sessionmodel.GraphSession, error)
	CloseSession(ctx context.Context, principalID string, sessionID string) (sessionmodel.GraphSession, error)
	BeginTransaction(ctx context.Context, input sessionmodel.BeginTransactionInput) (sessionmodel.GraphTransaction, error)
	GetTransaction(ctx context.Context, principalID string, transactionID string) (sessionmodel.GraphTransaction, error)
	CommitTransaction(ctx context.Context, principalID string, transactionID string, operationCount int32) (sessionmodel.TransactionCommit, error)
	CommitTransactionAtRevision(ctx context.Context, principalID string, transactionID string, operationCount int32, committedRevision int64) (sessionmodel.TransactionCommit, error)
	RollbackTransaction(ctx context.Context, principalID string, transactionID string) (sessionmodel.GraphTransaction, error)
	CloseTransaction(ctx context.Context, principalID string, transactionID string) (sessionmodel.GraphTransaction, error)
}

type RouteInspector interface {
	SessionRoute(sessionID string) (sessionmodel.SessionRouteRecord, bool)
	TransactionRoute(transactionID string) (sessionmodel.TransactionRouteRecord, bool)
	RouteDiagnostics() sessionmodel.RouteDiagnostics
}
