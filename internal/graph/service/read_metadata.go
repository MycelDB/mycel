package service

import (
	"context"
	"sync"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

type ReadConsistency string

const (
	ReadConsistencyStrong  ReadConsistency = "strong"
	ReadConsistencyOverlay ReadConsistency = "overlay"
	ReadConsistencyStale   ReadConsistency = "stale"
)

type ReadMetadata struct {
	Consistency      ReadConsistency   `json:"consistency,omitempty"`
	GroupID          consensus.GroupID `json:"group_id,omitempty"`
	PartitionID      uint32            `json:"partition_id,omitempty"`
	LeaderNodeID     consensus.NodeID  `json:"leader_node_id,omitempty"`
	ReadIndex        uint64            `json:"read_index,omitempty"`
	AppliedIndex     uint64            `json:"applied_index,omitempty"`
	ObservedRevision int64             `json:"observed_revision,omitempty"`
	Stale            bool              `json:"stale,omitempty"`
	StaleReason      string            `json:"stale_reason,omitempty"`
}

type readMetadataContextKey struct{}

type ReadMetadataRecorder struct {
	mu      sync.Mutex
	entries []ReadMetadata
}

func WithReadMetadataRecorder(ctx context.Context) (context.Context, *ReadMetadataRecorder) {
	recorder := &ReadMetadataRecorder{}
	return context.WithValue(ctx, readMetadataContextKey{}, recorder), recorder
}

func RecordReadMetadata(ctx context.Context, metadata ReadMetadata) {
	recorder, _ := ctx.Value(readMetadataContextKey{}).(*ReadMetadataRecorder)
	if recorder == nil || metadata.Consistency == "" {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.entries = append(recorder.entries, metadata)
}

func (r *ReadMetadataRecorder) Snapshot() []ReadMetadata {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ReadMetadata, len(r.entries))
	copy(out, r.entries)
	return out
}

func (r *ReadMetadataRecorder) Summary() *ReadMetadata {
	entries := r.Snapshot()
	if len(entries) == 0 {
		return nil
	}
	out := entries[len(entries)-1]
	for _, entry := range entries {
		if entry.Stale {
			out = entry
			break
		}
		if entry.Consistency == ReadConsistencyStrong {
			out = entry
		}
		if entry.ObservedRevision > out.ObservedRevision {
			out.ObservedRevision = entry.ObservedRevision
		}
		if entry.ReadIndex > out.ReadIndex {
			out.ReadIndex = entry.ReadIndex
		}
		if entry.AppliedIndex > out.AppliedIndex {
			out.AppliedIndex = entry.AppliedIndex
		}
	}
	return &out
}

func readMetadataFromStrong(read *StrongReadContext) *ReadMetadata {
	if read == nil {
		return nil
	}
	return &ReadMetadata{Consistency: ReadConsistencyStrong, GroupID: read.GroupID, PartitionID: read.PartitionID, LeaderNodeID: read.LeaderNodeID, ReadIndex: read.ReadIndex, AppliedIndex: read.AppliedIndex, ObservedRevision: read.ObservedRevision}
}

func RecordStrongReadContext(ctx context.Context, read *StrongReadContext) {
	metadata := readMetadataFromStrong(read)
	if metadata == nil {
		return
	}
	RecordReadMetadata(ctx, *metadata)
}

func RecordOverlayRead(ctx context.Context) {
	RecordReadMetadata(ctx, ReadMetadata{Consistency: ReadConsistencyOverlay})
}
