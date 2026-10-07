package service

import (
	"context"
	"strings"
	"time"

	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
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
	PersistentIndex                 GraphPersistentIndexStatus
}

type GraphPersistentIndexStatus struct {
	Present           bool
	IndexSetID        string
	IndexFormat       string
	GraphRevision     uint64
	GraphChecksum     string
	ChecksumAlgorithm string
	GraphCheckpointID string
	CreatedAt         time.Time
	LoadResult        string
	FallbackReason    string
	Entries           []GraphPersistentIndexEntryStatus
	QueryIndexes      []GraphPersistentQueryIndexStatus
}

type GraphPersistentIndexEntryStatus struct {
	Kind       string
	Path       string
	EntryCount int
	Checksum   string
}

type GraphPersistentQueryIndexStatus struct {
	Identity                 string
	Name                     string
	DomainID                 string
	SchemaHash               string
	DefinitionFingerprint    string
	TargetKind               string
	TargetType               string
	Labels                   []string
	FieldNamespace           string
	FieldName                string
	IndexKind                string
	Direction                string
	BuildState               string
	LastIndexedGraphRevision uint64
	KeyEncodingVersion       int
	EntryCount               uint64
	LoadResult               string
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
	started, err := m.writeGraphCheckpoint(ctx, key, store)
	if err != nil {
		return GraphCheckpointStatus{}, err
	}
	m.recordCheckpointSuccess(key, time.Since(started))
	return m.GraphCheckpointStatus(ctx, key.SpaceID, key.DomainID)
}

// GraphCheckpointStatus returns local latest checkpoint status for one domain
// graph store. It is local-only and does not collect peer status.
func (m *Module) writeGraphCheckpoint(ctx context.Context, key domainStoreKey, store *graphstorage.LocalStore) (time.Time, error) {
	if m.gate != nil {
		release, err := m.gate.Enter(ctx)
		if err != nil {
			mapped := quiesce.GRPCError(err)
			m.recordCheckpointFailure(key, 0, mapped)
			return time.Time{}, mapped
		}
		defer release()
	}
	m.markCheckpointRunning(key)
	started := time.Now()
	if err := store.WriteCheckpoint(ctx); err != nil {
		mapped := mapStorageError(err)
		m.recordCheckpointFailure(key, time.Since(started), mapped)
		return time.Time{}, mapped
	}
	return started, nil
}

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
		PersistentIndex:                 graphPersistentIndexStatusFromStorage(status.PersistentIndex),
	}
	if status.CheckpointPresent && !status.CreatedAt.IsZero() {
		out.CheckpointAge = time.Since(status.CreatedAt)
	}
	return out
}

func graphPersistentIndexStatusFromStorage(status graphstorage.PersistentIndexStatus) GraphPersistentIndexStatus {
	out := GraphPersistentIndexStatus{
		Present:           status.Present,
		IndexSetID:        status.IndexSetID,
		IndexFormat:       status.IndexFormat,
		GraphRevision:     status.GraphRevision,
		GraphChecksum:     status.GraphChecksum,
		ChecksumAlgorithm: status.ChecksumAlgorithm,
		GraphCheckpointID: status.GraphCheckpointID,
		CreatedAt:         status.CreatedAt,
		LoadResult:        status.LoadResult,
		FallbackReason:    status.FallbackReason,
	}
	if len(status.Entries) > 0 {
		out.Entries = make([]GraphPersistentIndexEntryStatus, 0, len(status.Entries))
		for _, entry := range status.Entries {
			out.Entries = append(out.Entries, GraphPersistentIndexEntryStatus{Kind: entry.Kind, Path: entry.Path, EntryCount: entry.EntryCount, Checksum: entry.Checksum})
		}
	}
	if len(status.QueryIndexes) > 0 {
		out.QueryIndexes = make([]GraphPersistentQueryIndexStatus, 0, len(status.QueryIndexes))
		for _, idx := range status.QueryIndexes {
			out.QueryIndexes = append(out.QueryIndexes, GraphPersistentQueryIndexStatus{Identity: idx.Identity, Name: idx.Name, DomainID: idx.DomainID, SchemaHash: idx.SchemaHash, DefinitionFingerprint: idx.DefinitionFingerprint, TargetKind: idx.TargetKind, TargetType: idx.TargetType, Labels: append([]string(nil), idx.Labels...), FieldNamespace: idx.FieldNamespace, FieldName: idx.FieldName, IndexKind: idx.IndexKind, Direction: idx.Direction, BuildState: idx.BuildState, LastIndexedGraphRevision: idx.LastIndexedGraphRevision, KeyEncodingVersion: idx.KeyEncodingVersion, EntryCount: idx.EntryCount, LoadResult: idx.LoadResult})
		}
	}
	return out
}
