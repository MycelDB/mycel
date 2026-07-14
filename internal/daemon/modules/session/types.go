package session

import (
	"context"
	"errors"
	"time"
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
	OpenSession(ctx context.Context, input OpenSessionInput) (GraphSession, error)
	GetSession(ctx context.Context, userID string, sessionID string) (GraphSession, error)
	HeartbeatSession(ctx context.Context, userID string, sessionID string, extension time.Duration) (GraphSession, error)
	CloseSession(ctx context.Context, userID string, sessionID string) (GraphSession, error)
	BeginTransaction(ctx context.Context, input BeginTransactionInput) (GraphTransaction, error)
	GetTransaction(ctx context.Context, userID string, transactionID string) (GraphTransaction, error)
	CommitTransaction(ctx context.Context, userID string, transactionID string, operationCount int32) (TransactionCommit, error)
	CommitTransactionAtRevision(ctx context.Context, userID string, transactionID string, operationCount int32, committedRevision int64) (TransactionCommit, error)
	RollbackTransaction(ctx context.Context, userID string, transactionID string) (GraphTransaction, error)
	CloseTransaction(ctx context.Context, userID string, transactionID string) (GraphTransaction, error)
}

type OpenSessionInput struct {
	UserID      string
	SpaceID     string
	DomainID    string
	IdleTimeout time.Duration
}

type BeginTransactionInput struct {
	UserID       string
	SessionID    string
	Mode         TransactionMode
	BaseRevision *int64
}

type SessionState string

const (
	SessionStateActive  SessionState = "active"
	SessionStateClosed  SessionState = "closed"
	SessionStateExpired SessionState = "expired"
)

type TransactionMode string

const (
	TransactionModeReadOnly  TransactionMode = "read_only"
	TransactionModeReadWrite TransactionMode = "read_write"
)

type TransactionState string

const (
	TransactionStateActive     TransactionState = "active"
	TransactionStateCommitted  TransactionState = "committed"
	TransactionStateRolledBack TransactionState = "rolled_back"
	TransactionStateClosed     TransactionState = "closed"
	TransactionStateExpired    TransactionState = "expired"
	TransactionStateAborted    TransactionState = "aborted"
)

type GraphSession struct {
	ID        string
	UserID    string
	SpaceID   string
	DomainID  string
	State     SessionState
	CreatedAt time.Time
	LastSeen  time.Time
	ExpiresAt time.Time
}

type GraphTransaction struct {
	ID           string
	SessionID    string
	UserID       string
	SpaceID      string
	DomainID     string
	Mode         TransactionMode
	State        TransactionState
	BaseRevision int64
	CreatedAt    time.Time
	LastSeen     time.Time
	ExpiresAt    time.Time
}

type TransactionCommit struct {
	ID                string
	TransactionID     string
	SessionID         string
	UserID            string
	SpaceID           string
	DomainID          string
	BaseRevision      int64
	CommittedRevision int64
	OperationCount    int32
	CommittedAt       time.Time
}
