package service

import (
	"context"
	"time"

	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
)

const (
	DefaultCheckpointPolicyInterval          = time.Minute
	DefaultCheckpointPolicyRevisionThreshold = uint64(10000)
	DefaultCheckpointPolicyTimeout           = 30 * time.Second
)

// CheckpointPolicyConfig controls local automatic domain graph checkpointing.
type CheckpointPolicyConfig struct {
	Enabled           bool
	Interval          time.Duration
	RevisionThreshold uint64
	Timeout           time.Duration
}

type checkpointRuntimeState struct {
	LastAttemptAt time.Time
	LastSuccessAt time.Time
	LastDuration  time.Duration
	LastError     string
	Running       bool
}

func normalizeCheckpointPolicyConfig(cfg CheckpointPolicyConfig) CheckpointPolicyConfig {
	if cfg.Interval == 0 {
		cfg.Interval = DefaultCheckpointPolicyInterval
	}
	if cfg.RevisionThreshold == 0 {
		cfg.RevisionThreshold = DefaultCheckpointPolicyRevisionThreshold
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultCheckpointPolicyTimeout
	}
	return cfg
}

func (m *Module) Start(ctx context.Context) error {
	policy := normalizeCheckpointPolicyConfig(m.checkpointPolicy)
	if !policy.Enabled {
		return nil
	}
	m.mu.Lock()
	if m.checkpointWorkerCancel != nil {
		m.mu.Unlock()
		return nil
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	m.checkpointPolicy = policy
	m.checkpointWorkerCancel = cancel
	m.checkpointWorkerDone = done
	m.mu.Unlock()
	if m.logger != nil {
		m.logger.Info("graph checkpoint worker started", "interval", policy.Interval.String(), "revision_threshold", policy.RevisionThreshold, "timeout", policy.Timeout.String())
	}
	go m.runCheckpointWorker(workerCtx, done, policy)
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	m.mu.Lock()
	cancel := m.checkpointWorkerCancel
	done := m.checkpointWorkerDone
	m.checkpointWorkerCancel = nil
	m.checkpointWorkerDone = nil
	m.mu.Unlock()
	if cancel == nil || done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Module) runCheckpointWorker(ctx context.Context, done chan<- struct{}, policy CheckpointPolicyConfig) {
	defer close(done)
	m.runCheckpointPolicyOnce(ctx, policy)
	ticker := time.NewTicker(policy.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runCheckpointPolicyOnce(ctx, policy)
		}
	}
}

func (m *Module) runCheckpointPolicyOnce(ctx context.Context, policy CheckpointPolicyConfig) {
	stores := m.openDomainStoresSnapshot()
	for key, store := range stores {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := m.maybeAutoCheckpointStore(ctx, key, store, policy); err != nil && m.logger != nil {
			m.logger.Warn("automatic graph checkpoint failed", "space_id", key.SpaceID, "domain_id", key.DomainID, "error", err)
		}
	}
}

func (m *Module) openDomainStoresSnapshot() map[domainStoreKey]*graphstorage.LocalStore {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[domainStoreKey]*graphstorage.LocalStore, len(m.stores))
	for key, store := range m.stores {
		if store != nil {
			out[key] = store
		}
	}
	return out
}

func (m *Module) maybeAutoCheckpointStore(ctx context.Context, key domainStoreKey, store *graphstorage.LocalStore, policy CheckpointPolicyConfig) error {
	status, err := store.CheckpointStatus(ctx)
	if err != nil {
		m.recordCheckpointFailure(key, 0, err)
		return mapStorageError(err)
	}
	if !checkpointEligible(status, policy.RevisionThreshold) {
		return nil
	}
	attemptCtx := ctx
	cancel := func() {}
	if policy.Timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, policy.Timeout)
	}
	defer cancel()
	started, err := m.writeGraphCheckpoint(attemptCtx, key, store)
	if err != nil {
		return err
	}
	duration := time.Since(started)
	m.recordCheckpointSuccess(key, duration)
	if m.logger != nil {
		m.logger.Info("automatic graph checkpoint written", "space_id", key.SpaceID, "domain_id", key.DomainID, "duration", duration.String(), "previous_tail_revisions", status.TailRevisions, "current_revision", status.CurrentRevision)
	}
	return nil
}

func checkpointEligible(status graphstorage.CheckpointStatus, threshold uint64) bool {
	if status.CurrentRevision == 0 || threshold == 0 {
		return false
	}
	if !status.CheckpointPresent {
		return status.CurrentRevision >= threshold
	}
	return status.TailRevisions >= threshold
}

func (m *Module) markCheckpointRunning(key domainStoreKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.checkpointStates[key]
	state.LastAttemptAt = time.Now().UTC()
	state.LastError = ""
	state.Running = true
	m.checkpointStates[key] = state
}

func (m *Module) recordCheckpointSuccess(key domainStoreKey, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.checkpointStates[key]
	if state.LastAttemptAt.IsZero() {
		state.LastAttemptAt = time.Now().UTC()
	}
	state.LastSuccessAt = time.Now().UTC()
	state.LastDuration = duration
	state.LastError = ""
	state.Running = false
	m.checkpointStates[key] = state
}

func (m *Module) recordCheckpointFailure(key domainStoreKey, duration time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.checkpointStates[key]
	if state.LastAttemptAt.IsZero() {
		state.LastAttemptAt = time.Now().UTC()
	}
	state.LastDuration = duration
	if err != nil {
		state.LastError = err.Error()
	}
	state.Running = false
	m.checkpointStates[key] = state
}

func (m *Module) checkpointRuntimeState(key domainStoreKey) checkpointRuntimeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.checkpointStates[key]
}
