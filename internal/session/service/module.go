package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const (
	defaultSessionIdleTimeout = 30 * time.Minute
	maxSessionIdleTimeout     = 24 * time.Hour
)

type Module struct {
	mu                sync.Mutex
	sessions          map[string]sessionmodel.GraphSession
	transactions      map[string]sessionmodel.GraphTransaction
	sessionRoutes     map[string]sessionmodel.SessionRouteRecord
	transactionRoutes map[string]sessionmodel.TransactionRouteRecord
	revisions         map[string]int64
	commits           map[string]sessionmodel.TransactionCommit
	localHomeNodeID   consensus.NodeID
	gate              *quiesce.Gate
}

func NewModule() *Module {
	return &Module{sessions: map[string]sessionmodel.GraphSession{}, transactions: map[string]sessionmodel.GraphTransaction{}, sessionRoutes: map[string]sessionmodel.SessionRouteRecord{}, transactionRoutes: map[string]sessionmodel.TransactionRouteRecord{}, revisions: map[string]int64{}, commits: map[string]sessionmodel.TransactionCommit{}, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	if m.sessions == nil {
		m.sessions = map[string]sessionmodel.GraphSession{}
	}
	if m.transactions == nil {
		m.transactions = map[string]sessionmodel.GraphTransaction{}
	}
	if m.sessionRoutes == nil {
		m.sessionRoutes = map[string]sessionmodel.SessionRouteRecord{}
	}
	if m.transactionRoutes == nil {
		m.transactionRoutes = map[string]sessionmodel.TransactionRouteRecord{}
	}
	if m.revisions == nil {
		m.revisions = map[string]int64{}
	}
	if m.commits == nil {
		m.commits = map[string]sessionmodel.TransactionCommit{}
	}
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if identityProvider, ok := host.(runtime.LocalRouteIdentityProvider); ok {
		identity := identityProvider.LocalRouteIdentity()
		if identity.RaftMode {
			m.localHomeNodeID = consensus.NodeID(identity.RaftNodeID)
		}
	}
	if registrar, ok := host.(runtime.QuiesceRegistrar); ok {
		if err := registrar.RegisterQuiesceParticipant(m.gate); err != nil {
			return runtime.Abort(ModuleName, "quiesce", "register session quiesce participant", err)
		}
	}
	if logger := host.Log(); logger != nil {
		logger.Info("session module initialized", "storage", "memory")
	}
	return runtime.OK(ModuleName)
}

func (m *Module) OpenSession(ctx context.Context, input sessionmodel.OpenSessionInput) (sessionmodel.GraphSession, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return sessionmodel.GraphSession{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return sessionmodel.GraphSession{}, err
	}
	if strings.TrimSpace(input.PrincipalID) == "" || strings.TrimSpace(input.SpaceID) == "" || strings.TrimSpace(input.DomainID) == "" {
		return sessionmodel.GraphSession{}, fmt.Errorf("%w: principal_id, space_id, and domain_id are required", ErrInvalidInput)
	}
	idle := normalizeTimeout(input.IdleTimeout)
	now := time.Now().UTC()
	origin := input.Origin
	origin.PrincipalID = strings.TrimSpace(input.PrincipalID)
	s := sessionmodel.GraphSession{ID: routing.NewSessionID(m.localHomeNodeID), PrincipalID: strings.TrimSpace(input.PrincipalID), SpaceID: strings.TrimSpace(input.SpaceID), DomainID: strings.TrimSpace(input.DomainID), HomeNodeID: m.localHomeNodeID, State: sessionmodel.SessionStateActive, Origin: origin, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(idle)}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[s.ID] = s
	m.sessionRoutes[s.ID] = sessionRouteFromSession(s, now)
	return s, nil
}

func (m *Module) GetSession(ctx context.Context, principalID string, sessionID string) (sessionmodel.GraphSession, error) {
	if err := m.requireLocalSessionHome(sessionID); err != nil {
		return sessionmodel.GraphSession{}, err
	}
	if err := ctx.Err(); err != nil {
		return sessionmodel.GraphSession{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(principalID, sessionID)
	if err != nil {
		return sessionmodel.GraphSession{}, err
	}
	return s, nil
}

func (m *Module) HeartbeatSession(ctx context.Context, principalID string, sessionID string, extension time.Duration) (sessionmodel.GraphSession, error) {
	if err := m.requireLocalSessionHome(sessionID); err != nil {
		return sessionmodel.GraphSession{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return sessionmodel.GraphSession{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return sessionmodel.GraphSession{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(principalID, sessionID)
	if err != nil {
		return sessionmodel.GraphSession{}, err
	}
	if s.State != sessionmodel.SessionStateActive {
		return s, ErrClosed
	}
	now := time.Now().UTC()
	s.LastSeen = now
	s.ExpiresAt = now.Add(normalizeTimeout(extension))
	m.sessions[s.ID] = s
	m.sessionRoutes[s.ID] = sessionRouteFromSession(s, now)
	for id, tx := range m.transactions {
		if tx.SessionID == s.ID && tx.State == sessionmodel.TransactionStateActive {
			tx.LastSeen = now
			tx.ExpiresAt = s.ExpiresAt
			m.transactions[id] = tx
			m.transactionRoutes[id] = transactionRouteFromTransaction(tx, now)
		}
	}
	return s, nil
}

func (m *Module) CloseSession(ctx context.Context, principalID string, sessionID string) (sessionmodel.GraphSession, error) {
	if err := m.requireLocalSessionHome(sessionID); err != nil {
		return sessionmodel.GraphSession{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return sessionmodel.GraphSession{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return sessionmodel.GraphSession{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(principalID, sessionID)
	if err != nil {
		return sessionmodel.GraphSession{}, err
	}
	now := time.Now().UTC()
	if s.State == sessionmodel.SessionStateActive {
		s.State = sessionmodel.SessionStateClosed
		s.LastSeen = now
		m.sessions[s.ID] = s
	}
	m.sessionRoutes[s.ID] = sessionRouteFromSession(s, now)
	for id, tx := range m.transactions {
		if tx.SessionID != s.ID || tx.State != sessionmodel.TransactionStateActive {
			continue
		}
		tx.LastSeen = now
		if tx.Mode == sessionmodel.TransactionModeReadWrite {
			tx.State = sessionmodel.TransactionStateRolledBack
		} else {
			tx.State = sessionmodel.TransactionStateClosed
		}
		m.transactions[id] = tx
		m.transactionRoutes[id] = transactionRouteFromTransaction(tx, now)
	}
	return s, nil
}

func (m *Module) BeginTransaction(ctx context.Context, input sessionmodel.BeginTransactionInput) (sessionmodel.GraphTransaction, error) {
	if err := m.requireLocalSessionHome(input.SessionID); err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	if input.Mode != sessionmodel.TransactionModeReadOnly && input.Mode != sessionmodel.TransactionModeReadWrite {
		return sessionmodel.GraphTransaction{}, fmt.Errorf("%w: transaction mode is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(input.PrincipalID, input.SessionID)
	if err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	if s.State != sessionmodel.SessionStateActive {
		return sessionmodel.GraphTransaction{}, ErrClosed
	}
	now := time.Now().UTC()
	s.LastSeen = now
	m.sessions[s.ID] = s
	m.sessionRoutes[s.ID] = sessionRouteFromSession(s, now)
	baseRevision := m.revisions[revisionKey(s.SpaceID, s.DomainID)]
	if input.BaseRevision != nil && *input.BaseRevision > baseRevision {
		baseRevision = *input.BaseRevision
	}
	origin := mergeOrigin(s.Origin, input.Origin)
	operationID, err := normalizeOperationID(origin.OperationID)
	if err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	origin.OperationID = operationID
	origin.PrincipalID = s.PrincipalID
	origin.SessionID = s.ID
	txID := routing.NewTransactionID(s.HomeNodeID)
	origin.TransactionID = txID
	tx := sessionmodel.GraphTransaction{ID: txID, SessionID: s.ID, PrincipalID: s.PrincipalID, SpaceID: s.SpaceID, DomainID: s.DomainID, HomeNodeID: s.HomeNodeID, Mode: input.Mode, State: sessionmodel.TransactionStateActive, BaseRevision: baseRevision, Origin: origin, CreatedAt: now, LastSeen: now, ExpiresAt: s.ExpiresAt}
	m.transactions[tx.ID] = tx
	m.transactionRoutes[tx.ID] = transactionRouteFromTransaction(tx, now)
	return tx, nil
}

func (m *Module) GetTransaction(ctx context.Context, principalID string, transactionID string) (sessionmodel.GraphTransaction, error) {
	if err := m.requireLocalTransactionHome(transactionID); err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	if err := ctx.Err(); err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getTransactionLocked(principalID, transactionID)
}

func (m *Module) CommitTransaction(ctx context.Context, principalID string, transactionID string, operationCount int32) (sessionmodel.TransactionCommit, error) {
	return m.commitTransaction(ctx, principalID, transactionID, operationCount, 0)
}

func (m *Module) CommitTransactionAtRevision(ctx context.Context, principalID string, transactionID string, operationCount int32, committedRevision int64) (sessionmodel.TransactionCommit, error) {
	return m.commitTransaction(ctx, principalID, transactionID, operationCount, committedRevision)
}

func (m *Module) commitTransaction(ctx context.Context, principalID string, transactionID string, operationCount int32, committedRevision int64) (sessionmodel.TransactionCommit, error) {
	if err := m.requireLocalTransactionHome(transactionID); err != nil {
		return sessionmodel.TransactionCommit{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return sessionmodel.TransactionCommit{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return sessionmodel.TransactionCommit{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := m.getTransactionLocked(principalID, transactionID)
	if err != nil {
		return sessionmodel.TransactionCommit{}, err
	}
	if tx.State != sessionmodel.TransactionStateActive {
		return sessionmodel.TransactionCommit{}, ErrInvalidState
	}
	if tx.Mode != sessionmodel.TransactionModeReadWrite {
		return sessionmodel.TransactionCommit{}, fmt.Errorf("%w: only read-write transactions can commit", ErrInvalidState)
	}
	now := time.Now().UTC()
	key := revisionKey(tx.SpaceID, tx.DomainID)
	if committedRevision > 0 {
		if committedRevision < m.revisions[key] {
			return sessionmodel.TransactionCommit{}, ErrInvalidState
		}
		m.revisions[key] = committedRevision
	} else if operationCount > 0 {
		m.revisions[key]++
		committedRevision = m.revisions[key]
	} else {
		// A read-write transaction may execute only reads. Committing that no-op
		// transaction must not advance the session subsystem's view of the graph
		// revision, or the next write can start from a base revision that is ahead
		// of the graph store and fail as a false transaction conflict.
		committedRevision = m.revisions[key]
	}
	tx.State = sessionmodel.TransactionStateCommitted
	tx.LastSeen = now
	m.transactions[tx.ID] = tx
	m.transactionRoutes[tx.ID] = transactionRouteFromTransaction(tx, now)
	origin := tx.Origin
	origin.PrincipalID = tx.PrincipalID
	origin.SessionID = tx.SessionID
	origin.TransactionID = tx.ID
	commit := sessionmodel.TransactionCommit{ID: uuid.NewString(), TransactionID: tx.ID, SessionID: tx.SessionID, PrincipalID: tx.PrincipalID, SpaceID: tx.SpaceID, DomainID: tx.DomainID, BaseRevision: tx.BaseRevision, CommittedRevision: committedRevision, OperationCount: operationCount, Origin: origin, CommittedAt: now}
	m.commits[commit.ID] = commit
	return commit, nil
}

func (m *Module) RollbackTransaction(ctx context.Context, principalID string, transactionID string) (sessionmodel.GraphTransaction, error) {
	return m.finalizeTransaction(ctx, principalID, transactionID, sessionmodel.TransactionStateRolledBack)
}

func (m *Module) CloseTransaction(ctx context.Context, principalID string, transactionID string) (sessionmodel.GraphTransaction, error) {
	return m.finalizeTransaction(ctx, principalID, transactionID, sessionmodel.TransactionStateClosed)
}

func (m *Module) finalizeTransaction(ctx context.Context, principalID string, transactionID string, readOnlyState sessionmodel.TransactionState) (sessionmodel.GraphTransaction, error) {
	if err := m.requireLocalTransactionHome(transactionID); err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := m.getTransactionLocked(principalID, transactionID)
	if err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	if tx.State == sessionmodel.TransactionStateActive {
		now := time.Now().UTC()
		tx.State = sessionmodel.TransactionStateRolledBack
		if tx.Mode != sessionmodel.TransactionModeReadWrite {
			tx.State = readOnlyState
		}
		tx.LastSeen = now
		m.transactions[tx.ID] = tx
		m.transactionRoutes[tx.ID] = transactionRouteFromTransaction(tx, now)
	}
	return tx, nil
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) getSessionLocked(principalID string, sessionID string) (sessionmodel.GraphSession, error) {
	if strings.TrimSpace(sessionID) == "" {
		return sessionmodel.GraphSession{}, fmt.Errorf("%w: session_id is required", ErrInvalidInput)
	}
	s, ok := m.sessions[strings.TrimSpace(sessionID)]
	if !ok {
		return sessionmodel.GraphSession{}, ErrSessionNotFound
	}
	if s.PrincipalID != strings.TrimSpace(principalID) {
		return sessionmodel.GraphSession{}, ErrUnauthorized
	}
	if s.State == sessionmodel.SessionStateActive && !s.ExpiresAt.IsZero() && !time.Now().UTC().Before(s.ExpiresAt) {
		now := time.Now().UTC()
		s.State = sessionmodel.SessionStateExpired
		m.sessions[s.ID] = s
		m.sessionRoutes[s.ID] = sessionRouteFromSession(s, now)
		for id, tx := range m.transactions {
			if tx.SessionID == s.ID && tx.State == sessionmodel.TransactionStateActive {
				tx.State = sessionmodel.TransactionStateExpired
				m.transactions[id] = tx
				m.transactionRoutes[id] = transactionRouteFromTransaction(tx, now)
			}
		}
	}
	return s, nil
}

func (m *Module) getTransactionLocked(principalID string, transactionID string) (sessionmodel.GraphTransaction, error) {
	if strings.TrimSpace(transactionID) == "" {
		return sessionmodel.GraphTransaction{}, fmt.Errorf("%w: transaction_id is required", ErrInvalidInput)
	}
	tx, ok := m.transactions[strings.TrimSpace(transactionID)]
	if !ok {
		return sessionmodel.GraphTransaction{}, ErrTransactionNotFound
	}
	if tx.PrincipalID != strings.TrimSpace(principalID) {
		return sessionmodel.GraphTransaction{}, ErrUnauthorized
	}
	if tx.State == sessionmodel.TransactionStateActive && !tx.ExpiresAt.IsZero() && !time.Now().UTC().Before(tx.ExpiresAt) {
		now := time.Now().UTC()
		tx.State = sessionmodel.TransactionStateExpired
		m.transactions[tx.ID] = tx
		m.transactionRoutes[tx.ID] = transactionRouteFromTransaction(tx, now)
	}
	return tx, nil
}

func (m *Module) SessionRoute(sessionID string) (sessionmodel.SessionRouteRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.sessionRoutes[strings.TrimSpace(sessionID)]
	return record, ok
}

func (m *Module) TransactionRoute(transactionID string) (sessionmodel.TransactionRouteRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.transactionRoutes[strings.TrimSpace(transactionID)]
	return record, ok
}

func (m *Module) RouteDiagnostics() sessionmodel.RouteDiagnostics {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := sessionmodel.RouteDiagnostics{LocalHomeNodeID: m.localHomeNodeID, SessionRoutes: len(m.sessionRoutes), TransactionRoutes: len(m.transactionRoutes)}
	for _, route := range m.sessionRoutes {
		if route.HomeNodeID == m.localHomeNodeID {
			out.LocalSessionRoutes++
			if route.State == sessionmodel.SessionStateActive {
				out.ActiveLocalSessions++
			}
		} else {
			out.RemoteSessionRoutes++
			if route.State == sessionmodel.SessionStateActive {
				out.ActiveRemoteSessions++
			}
		}
	}
	for _, route := range m.transactionRoutes {
		if route.HomeNodeID == m.localHomeNodeID {
			out.LocalTransactionRoutes++
			if route.State == sessionmodel.TransactionStateActive {
				out.ActiveLocalTransactions++
			}
		} else {
			out.RemoteTransactionRoutes++
			if route.State == sessionmodel.TransactionStateActive {
				out.ActiveRemoteTransactions++
			}
		}
	}
	return out
}

func (m *Module) requireLocalSessionHome(sessionID string) error {
	if strings.TrimSpace(sessionID) == "" || m.localHomeNodeID == 0 {
		return nil
	}
	home, ok, err := routing.ParseSessionHomeNode(sessionID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if !ok {
		return routing.NewUnknownSessionHome("session id does not encode home node", routing.WithLocalNode(m.localHomeNodeID))
	}
	if home != m.localHomeNodeID {
		return routing.NewSessionHomeMismatch("session belongs to another home node", routing.WithHomeNode(home), routing.WithLocalNode(m.localHomeNodeID), routing.WithTargetNode(home))
	}
	return nil
}

func (m *Module) requireLocalTransactionHome(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" || m.localHomeNodeID == 0 {
		return nil
	}
	home, ok, err := routing.ParseTransactionHomeNode(transactionID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if !ok {
		return routing.NewUnknownSessionHome("transaction id does not encode home node", routing.WithLocalNode(m.localHomeNodeID))
	}
	if home != m.localHomeNodeID {
		return routing.NewSessionHomeMismatch("transaction belongs to another home node", routing.WithHomeNode(home), routing.WithLocalNode(m.localHomeNodeID), routing.WithTargetNode(home))
	}
	return nil
}

func sessionRouteFromSession(s sessionmodel.GraphSession, updatedAt time.Time) sessionmodel.SessionRouteRecord {
	return sessionmodel.SessionRouteRecord{SessionID: s.ID, PrincipalID: s.PrincipalID, SpaceID: s.SpaceID, DomainID: s.DomainID, HomeNodeID: s.HomeNodeID, State: s.State, CreatedAt: s.CreatedAt, UpdatedAt: updatedAt, ExpiresAt: s.ExpiresAt}
}

func transactionRouteFromTransaction(tx sessionmodel.GraphTransaction, updatedAt time.Time) sessionmodel.TransactionRouteRecord {
	return sessionmodel.TransactionRouteRecord{TransactionID: tx.ID, SessionID: tx.SessionID, PrincipalID: tx.PrincipalID, SpaceID: tx.SpaceID, DomainID: tx.DomainID, HomeNodeID: tx.HomeNodeID, State: tx.State, CreatedAt: tx.CreatedAt, UpdatedAt: updatedAt, ExpiresAt: tx.ExpiresAt}
}

func normalizeOperationID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return uuid.NewString(), nil
	}
	if _, err := uuid.Parse(value); err != nil {
		return "", fmt.Errorf("%w: operation_id must be a UUID", ErrInvalidInput)
	}
	return value, nil
}

func mergeOrigin(sessionOrigin graphchange.OriginMetadata, txOrigin graphchange.OriginMetadata) graphchange.OriginMetadata {
	out := sessionOrigin
	if txOrigin.ClientID != "" {
		out.ClientID = txOrigin.ClientID
	}
	if txOrigin.ClientInstanceID != "" {
		out.ClientInstanceID = txOrigin.ClientInstanceID
	}
	if txOrigin.OperationID != "" {
		out.OperationID = txOrigin.OperationID
	}
	if txOrigin.Label != "" {
		out.Label = txOrigin.Label
	}
	return out
}

func normalizeTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultSessionIdleTimeout
	}
	if value > maxSessionIdleTimeout {
		return maxSessionIdleTimeout
	}
	return value
}

func revisionKey(spaceID string, domainID string) string { return spaceID + ":" + domainID }
