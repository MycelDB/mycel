package model

import (
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
)

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
