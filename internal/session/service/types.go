package service

import (
	"context"
	"errors"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
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
	GetSession(ctx context.Context, principalID string, sessionID string) (GraphSession, error)
	HeartbeatSession(ctx context.Context, principalID string, sessionID string, extension time.Duration) (GraphSession, error)
	CloseSession(ctx context.Context, principalID string, sessionID string) (GraphSession, error)
	BeginTransaction(ctx context.Context, input BeginTransactionInput) (GraphTransaction, error)
	GetTransaction(ctx context.Context, principalID string, transactionID string) (GraphTransaction, error)
	CommitTransaction(ctx context.Context, principalID string, transactionID string, operationCount int32) (TransactionCommit, error)
	CommitTransactionAtRevision(ctx context.Context, principalID string, transactionID string, operationCount int32, committedRevision int64) (TransactionCommit, error)
	RollbackTransaction(ctx context.Context, principalID string, transactionID string) (GraphTransaction, error)
	CloseTransaction(ctx context.Context, principalID string, transactionID string) (GraphTransaction, error)
}

type OpenSessionInput struct {
	PrincipalID string
	SpaceID     string
	DomainID    string
	IdleTimeout time.Duration
	Origin      graphchange.OriginMetadata
}

type BeginTransactionInput struct {
	PrincipalID  string
	SessionID    string
	Mode         TransactionMode
	BaseRevision *int64
	Origin       graphchange.OriginMetadata
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
	ID          string
	PrincipalID string
	SpaceID     string
	DomainID    string
	HomeNodeID  consensus.NodeID
	State       SessionState
	Origin      graphchange.OriginMetadata
	CreatedAt   time.Time
	LastSeen    time.Time
	ExpiresAt   time.Time
}

type GraphTransaction struct {
	ID           string
	SessionID    string
	PrincipalID  string
	SpaceID      string
	DomainID     string
	HomeNodeID   consensus.NodeID
	Mode         TransactionMode
	State        TransactionState
	BaseRevision int64
	Origin       graphchange.OriginMetadata
	CreatedAt    time.Time
	LastSeen     time.Time
	ExpiresAt    time.Time
}

type SessionRouteRecord struct {
	SessionID   string
	PrincipalID string
	SpaceID     string
	DomainID    string
	HomeNodeID  consensus.NodeID
	State       SessionState
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ExpiresAt   time.Time
}

type TransactionRouteRecord struct {
	TransactionID string
	SessionID     string
	PrincipalID   string
	SpaceID       string
	DomainID      string
	HomeNodeID    consensus.NodeID
	State         TransactionState
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ExpiresAt     time.Time
}

type RouteDiagnostics struct {
	LocalHomeNodeID          consensus.NodeID
	SessionRoutes            int
	TransactionRoutes        int
	LocalSessionRoutes       int
	RemoteSessionRoutes      int
	LocalTransactionRoutes   int
	RemoteTransactionRoutes  int
	ActiveLocalSessions      int
	ActiveRemoteSessions     int
	ActiveLocalTransactions  int
	ActiveRemoteTransactions int
}

type RouteInspector interface {
	SessionRoute(sessionID string) (SessionRouteRecord, bool)
	TransactionRoute(transactionID string) (TransactionRouteRecord, bool)
	RouteDiagnostics() RouteDiagnostics
}

type TransactionCommit struct {
	ID                string
	TransactionID     string
	SessionID         string
	PrincipalID       string
	SpaceID           string
	DomainID          string
	BaseRevision      int64
	CommittedRevision int64
	OperationCount    int32
	Origin            graphchange.OriginMetadata
	CommittedAt       time.Time
}
