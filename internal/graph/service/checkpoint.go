package service

import (
	"context"
	"strings"
	"time"

	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
)

// GraphCheckpointStatus is a local operational status summary for one
// domain-scoped graph store checkpoint.
type GraphCheckpointStatus struct {
	SpaceID                         string
	DomainID                        string
	CurrentRevision                 uint64
	CheckpointPresent               bool
	CheckpointRevision              uint64
	CheckpointCreatedAt             time.Time
	NodeCount                       int
	EdgeCount                       int
	GraphChecksum                   string
	ChecksumAlgorithm               string
	TailRevisions                   uint64
	Source                          string
	AutoCheckpointEnabled           bool
	AutoCheckpointRevisionThreshold uint64
	AutoCheckpointInterval          time.Duration
	LastCheckpointAttemptAt         time.Time
	LastCheckpointSuccessAt         time.Time
	LastCheckpointDuration          time.Duration
	LastCheckpointError             string
	CheckpointAge                   time.Duration
}

// CreateGraphCheckpoint writes a local derived checkpoint for one domain graph
// store and returns status for the newly-written checkpoint. It is not a Raft
// proposal, Raft snapshot, backup, or graph mutation.
func (m *Module) CreateGraphCheckpoint(ctx context.Context, spaceID string, domainID string) (GraphCheckpointStatus, error) {
	key, err := newDomainStoreKey(spaceID, domainID)
	if err != nil {
		return GraphCheckpointStatus{}, err
	}
	store, err := m.existingDomainStore(ctx, key)
	if err != nil {
		return GraphCheckpointStatus{}, err
	}
	m.markCheckpointRunning(key)
	started := time.Now()
	if err := store.WriteCheckpoint(ctx); err != nil {
		m.recordCheckpointFailure(key, time.Since(started), err)
		return GraphCheckpointStatus{}, mapStorageError(err)
	}
	m.recordCheckpointSuccess(key, time.Since(started))
	return m.GraphCheckpointStatus(ctx, key.SpaceID, key.DomainID)
}

// GraphCheckpointStatus returns local latest checkpoint status for one domain
// graph store. It is local-only and does not collect peer status.
func (m *Module) GraphCheckpointStatus(ctx context.Context, spaceID string, domainID string) (GraphCheckpointStatus, error) {
	key, err := newDomainStoreKey(spaceID, domainID)
	if err != nil {
		return GraphCheckpointStatus{}, err
	}
	store, err := m.existingDomainStore(ctx, key)
	if err != nil {
		return GraphCheckpointStatus{}, err
	}
	status, err := store.CheckpointStatus(ctx)
	if err != nil {
		return GraphCheckpointStatus{}, mapStorageError(err)
	}
	return m.graphCheckpointStatusFromStorage(key, status), nil
}

func (m *Module) graphCheckpointStatusFromStorage(key domainStoreKey, status graphstorage.CheckpointStatus) GraphCheckpointStatus {
	policy := normalizeCheckpointPolicyConfig(m.checkpointPolicy)
	runtimeState := m.checkpointRuntimeState(key)
	out := GraphCheckpointStatus{
		SpaceID:                         strings.TrimSpace(key.SpaceID),
		DomainID:                        strings.TrimSpace(key.DomainID),
		CurrentRevision:                 status.CurrentRevision,
		CheckpointPresent:               status.CheckpointPresent,
		CheckpointRevision:              status.CheckpointRevision,
		CheckpointCreatedAt:             status.CreatedAt,
		NodeCount:                       status.NodeCount,
		EdgeCount:                       status.EdgeCount,
		GraphChecksum:                   status.GraphChecksum,
		ChecksumAlgorithm:               status.ChecksumAlgorithm,
		TailRevisions:                   status.TailRevisions,
		Source:                          "local_checkpoint",
		AutoCheckpointEnabled:           policy.Enabled,
		AutoCheckpointRevisionThreshold: policy.RevisionThreshold,
		AutoCheckpointInterval:          policy.Interval,
		LastCheckpointAttemptAt:         runtimeState.LastAttemptAt,
		LastCheckpointSuccessAt:         runtimeState.LastSuccessAt,
		LastCheckpointDuration:          runtimeState.LastDuration,
		LastCheckpointError:             runtimeState.LastError,
	}
	if status.CheckpointPresent && !status.CreatedAt.IsZero() {
		out.CheckpointAge = time.Since(status.CreatedAt)
	}
	return out
}
