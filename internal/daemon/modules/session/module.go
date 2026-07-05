package session

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

const (
	defaultSessionIdleTimeout = 30 * time.Minute
	maxSessionIdleTimeout     = 24 * time.Hour
)

type Module struct {
	mu           sync.Mutex
	sessions     map[string]GraphSession
	transactions map[string]GraphTransaction
	revisions    map[string]int64
	commits      map[string]TransactionCommit
}

func NewModule() *Module {
	return &Module{sessions: map[string]GraphSession{}, transactions: map[string]GraphTransaction{}, revisions: map[string]int64{}, commits: map[string]TransactionCommit{}}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	if m.sessions == nil {
		m.sessions = map[string]GraphSession{}
	}
	if m.transactions == nil {
		m.transactions = map[string]GraphTransaction{}
	}
	if m.revisions == nil {
		m.revisions = map[string]int64{}
	}
	if m.commits == nil {
		m.commits = map[string]TransactionCommit{}
	}
	rt.Logger.Info("session module initialized", "storage", "memory")
	return daemonruntime.OK(ModuleName)
}

func (m *Module) OpenSession(ctx context.Context, input OpenSessionInput) (GraphSession, error) {
	if err := ctx.Err(); err != nil {
		return GraphSession{}, err
	}
	if strings.TrimSpace(input.UserID) == "" || strings.TrimSpace(input.SpaceID) == "" || strings.TrimSpace(input.DomainID) == "" {
		return GraphSession{}, fmt.Errorf("%w: user_id, space_id, and domain_id are required", ErrInvalidInput)
	}
	idle := normalizeTimeout(input.IdleTimeout)
	now := time.Now().UTC()
	s := GraphSession{ID: uuid.NewString(), UserID: strings.TrimSpace(input.UserID), SpaceID: strings.TrimSpace(input.SpaceID), DomainID: strings.TrimSpace(input.DomainID), State: SessionStateActive, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(idle)}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[s.ID] = s
	return s, nil
}

func (m *Module) GetSession(ctx context.Context, userID string, sessionID string) (GraphSession, error) {
	if err := ctx.Err(); err != nil {
		return GraphSession{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(userID, sessionID)
	if err != nil {
		return GraphSession{}, err
	}
	return s, nil
}

func (m *Module) HeartbeatSession(ctx context.Context, userID string, sessionID string, extension time.Duration) (GraphSession, error) {
	if err := ctx.Err(); err != nil {
		return GraphSession{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(userID, sessionID)
	if err != nil {
		return GraphSession{}, err
	}
	if s.State != SessionStateActive {
		return s, ErrClosed
	}
	now := time.Now().UTC()
	s.LastSeen = now
	s.ExpiresAt = now.Add(normalizeTimeout(extension))
	m.sessions[s.ID] = s
	for id, tx := range m.transactions {
		if tx.SessionID == s.ID && tx.State == TransactionStateActive {
			tx.LastSeen = now
			tx.ExpiresAt = s.ExpiresAt
			m.transactions[id] = tx
		}
	}
	return s, nil
}

func (m *Module) CloseSession(ctx context.Context, userID string, sessionID string) (GraphSession, error) {
	if err := ctx.Err(); err != nil {
		return GraphSession{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(userID, sessionID)
	if err != nil {
		return GraphSession{}, err
	}
	now := time.Now().UTC()
	if s.State == SessionStateActive {
		s.State = SessionStateClosed
		s.LastSeen = now
		m.sessions[s.ID] = s
	}
	for id, tx := range m.transactions {
		if tx.SessionID != s.ID || tx.State != TransactionStateActive {
			continue
		}
		tx.LastSeen = now
		if tx.Mode == TransactionModeReadWrite {
			tx.State = TransactionStateRolledBack
		} else {
			tx.State = TransactionStateClosed
		}
		m.transactions[id] = tx
	}
	return s, nil
}

func (m *Module) BeginTransaction(ctx context.Context, input BeginTransactionInput) (GraphTransaction, error) {
	if err := ctx.Err(); err != nil {
		return GraphTransaction{}, err
	}
	if input.Mode != TransactionModeReadOnly && input.Mode != TransactionModeReadWrite {
		return GraphTransaction{}, fmt.Errorf("%w: transaction mode is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.getSessionLocked(input.UserID, input.SessionID)
	if err != nil {
		return GraphTransaction{}, err
	}
	if s.State != SessionStateActive {
		return GraphTransaction{}, ErrClosed
	}
	now := time.Now().UTC()
	s.LastSeen = now
	m.sessions[s.ID] = s
	baseRevision := m.revisions[revisionKey(s.SpaceID, s.DomainID)]
	if input.BaseRevision != nil && *input.BaseRevision > baseRevision {
		baseRevision = *input.BaseRevision
	}
	tx := GraphTransaction{ID: uuid.NewString(), SessionID: s.ID, UserID: s.UserID, SpaceID: s.SpaceID, DomainID: s.DomainID, Mode: input.Mode, State: TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: s.ExpiresAt}
	m.transactions[tx.ID] = tx
	return tx, nil
}

func (m *Module) GetTransaction(ctx context.Context, userID string, transactionID string) (GraphTransaction, error) {
	if err := ctx.Err(); err != nil {
		return GraphTransaction{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getTransactionLocked(userID, transactionID)
}

func (m *Module) CommitTransaction(ctx context.Context, userID string, transactionID string, operationCount int32) (TransactionCommit, error) {
	return m.commitTransaction(ctx, userID, transactionID, operationCount, 0)
}

func (m *Module) CommitTransactionAtRevision(ctx context.Context, userID string, transactionID string, operationCount int32, committedRevision int64) (TransactionCommit, error) {
	return m.commitTransaction(ctx, userID, transactionID, operationCount, committedRevision)
}

func (m *Module) commitTransaction(ctx context.Context, userID string, transactionID string, operationCount int32, committedRevision int64) (TransactionCommit, error) {
	if err := ctx.Err(); err != nil {
		return TransactionCommit{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := m.getTransactionLocked(userID, transactionID)
	if err != nil {
		return TransactionCommit{}, err
	}
	if tx.State != TransactionStateActive {
		return TransactionCommit{}, ErrInvalidState
	}
	if tx.Mode != TransactionModeReadWrite {
		return TransactionCommit{}, fmt.Errorf("%w: only read-write transactions can commit", ErrInvalidState)
	}
	now := time.Now().UTC()
	key := revisionKey(tx.SpaceID, tx.DomainID)
	if committedRevision > 0 {
		if committedRevision < m.revisions[key] {
			return TransactionCommit{}, ErrInvalidState
		}
		m.revisions[key] = committedRevision
	} else {
		m.revisions[key]++
		committedRevision = m.revisions[key]
	}
	tx.State = TransactionStateCommitted
	tx.LastSeen = now
	m.transactions[tx.ID] = tx
	commit := TransactionCommit{ID: uuid.NewString(), TransactionID: tx.ID, SessionID: tx.SessionID, UserID: tx.UserID, SpaceID: tx.SpaceID, DomainID: tx.DomainID, BaseRevision: tx.BaseRevision, CommittedRevision: committedRevision, OperationCount: operationCount, CommittedAt: now}
	m.commits[commit.ID] = commit
	return commit, nil
}

func (m *Module) RollbackTransaction(ctx context.Context, userID string, transactionID string) (GraphTransaction, error) {
	if err := ctx.Err(); err != nil {
		return GraphTransaction{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := m.getTransactionLocked(userID, transactionID)
	if err != nil {
		return GraphTransaction{}, err
	}
	if tx.State == TransactionStateActive {
		tx.State = TransactionStateRolledBack
		tx.LastSeen = time.Now().UTC()
		m.transactions[tx.ID] = tx
	}
	return tx, nil
}

func (m *Module) CloseTransaction(ctx context.Context, userID string, transactionID string) (GraphTransaction, error) {
	if err := ctx.Err(); err != nil {
		return GraphTransaction{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := m.getTransactionLocked(userID, transactionID)
	if err != nil {
		return GraphTransaction{}, err
	}
	if tx.State == TransactionStateActive {
		if tx.Mode == TransactionModeReadWrite {
			tx.State = TransactionStateRolledBack
		} else {
			tx.State = TransactionStateClosed
		}
		tx.LastSeen = time.Now().UTC()
		m.transactions[tx.ID] = tx
	}
	return tx, nil
}

func (m *Module) getSessionLocked(userID string, sessionID string) (GraphSession, error) {
	if strings.TrimSpace(sessionID) == "" {
		return GraphSession{}, fmt.Errorf("%w: session_id is required", ErrInvalidInput)
	}
	s, ok := m.sessions[strings.TrimSpace(sessionID)]
	if !ok {
		return GraphSession{}, ErrSessionNotFound
	}
	if s.UserID != strings.TrimSpace(userID) {
		return GraphSession{}, ErrUnauthorized
	}
	if s.State == SessionStateActive && !s.ExpiresAt.IsZero() && !time.Now().UTC().Before(s.ExpiresAt) {
		s.State = SessionStateExpired
		m.sessions[s.ID] = s
		for id, tx := range m.transactions {
			if tx.SessionID == s.ID && tx.State == TransactionStateActive {
				tx.State = TransactionStateExpired
				m.transactions[id] = tx
			}
		}
	}
	return s, nil
}

func (m *Module) getTransactionLocked(userID string, transactionID string) (GraphTransaction, error) {
	if strings.TrimSpace(transactionID) == "" {
		return GraphTransaction{}, fmt.Errorf("%w: transaction_id is required", ErrInvalidInput)
	}
	tx, ok := m.transactions[strings.TrimSpace(transactionID)]
	if !ok {
		return GraphTransaction{}, ErrTransactionNotFound
	}
	if tx.UserID != strings.TrimSpace(userID) {
		return GraphTransaction{}, ErrUnauthorized
	}
	if tx.State == TransactionStateActive && !tx.ExpiresAt.IsZero() && !time.Now().UTC().Before(tx.ExpiresAt) {
		tx.State = TransactionStateExpired
		m.transactions[tx.ID] = tx
	}
	return tx, nil
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
