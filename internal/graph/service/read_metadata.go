package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

type ReadConsistency string

const (
	ReadConsistencyStrong  ReadConsistency = "strong"
	ReadConsistencyOverlay ReadConsistency = "overlay"
	ReadConsistencyStale   ReadConsistency = "stale"
)

type RevisionDependency struct {
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Revision uint64 `json:"revision"`
}

type revisionWitness struct {
	Version      int                  `json:"version"`
	SpaceID      string               `json:"space_id,omitempty"`
	DomainID     string               `json:"domain_id,omitempty"`
	Dependencies []RevisionDependency `json:"dependencies"`
	Checksum     string               `json:"checksum,omitempty"`
	MAC          string               `json:"mac,omitempty"`
}

type ReadMetadata struct {
	Consistency      ReadConsistency      `json:"consistency,omitempty"`
	GroupID          consensus.GroupID    `json:"group_id,omitempty"`
	PartitionID      uint32               `json:"partition_id,omitempty"`
	LeaderNodeID     consensus.NodeID     `json:"leader_node_id,omitempty"`
	ReadIndex        uint64               `json:"read_index,omitempty"`
	AppliedIndex     uint64               `json:"applied_index,omitempty"`
	ObservedRevision int64                `json:"observed_revision,omitempty"`
	Stale            bool                 `json:"stale,omitempty"`
	StaleReason      string               `json:"stale_reason,omitempty"`
	RevisionWitness  []byte               `json:"revision_witness,omitempty"`
	Dependencies     []RevisionDependency `json:"-"`
}

type readMetadataContextKey struct{}

type revisionWitnessKeyContext struct{}

// WithRevisionWitnessKey attaches the daemon-local cluster-stable MAC key to a
// read context. The key never leaves the daemon and is not included in metadata.
func WithRevisionWitnessKey(ctx context.Context, key []byte) context.Context {
	if len(key) == 0 {
		return ctx
	}
	return context.WithValue(ctx, revisionWitnessKeyContext{}, append([]byte(nil), key...))
}

func revisionWitnessKey(ctx context.Context) []byte {
	key, _ := ctx.Value(revisionWitnessKeyContext{}).([]byte)
	return key
}

type ReadMetadataRecorder struct {
	witnessKey     []byte
	mu             sync.Mutex
	entries        []ReadMetadata
	spaceID        string
	domainID       string
	dependencies   map[string]RevisionDependency
	queryReadFence bool
}

func WithReadMetadataRecorder(ctx context.Context) (context.Context, *ReadMetadataRecorder) {
	recorder := &ReadMetadataRecorder{dependencies: map[string]RevisionDependency{}, witnessKey: revisionWitnessKey(ctx)}
	return context.WithValue(ctx, readMetadataContextKey{}, recorder), recorder
}

func RecordReadScope(ctx context.Context, spaceID, domainID string) {
	recorder, _ := ctx.Value(readMetadataContextKey{}).(*ReadMetadataRecorder)
	if recorder == nil {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.spaceID == "" {
		recorder.spaceID = spaceID
	}
	if recorder.domainID == "" {
		recorder.domainID = domainID
	}
}

// RecordReadPredicate marks a query read whose predicate/candidate set must be
// fenced at the observed domain revision. The witness contains only the opaque
// domain revision dependency, never the predicate text or query payload.
func RecordReadPredicate(ctx context.Context) {
	recorder, _ := ctx.Value(readMetadataContextKey{}).(*ReadMetadataRecorder)
	if recorder == nil {
		return
	}
	recorder.mu.Lock()
	recorder.queryReadFence = true
	recorder.mu.Unlock()
}

func RecordReadDependency(ctx context.Context, dependency RevisionDependency) {
	if dependency.Kind == "" || dependency.ID == "" {
		return
	}
	recorder, _ := ctx.Value(readMetadataContextKey{}).(*ReadMetadataRecorder)
	if recorder == nil {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.dependencies == nil {
		recorder.dependencies = map[string]RevisionDependency{}
	}
	key := dependency.Kind + ":" + dependency.ID
	if existing, ok := recorder.dependencies[key]; !ok || dependency.Revision > existing.Revision {
		recorder.dependencies[key] = dependency
	}
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
	r.mu.Lock()
	dependencies := make([]RevisionDependency, 0, len(r.dependencies))
	for _, dependency := range r.dependencies {
		dependencies = append(dependencies, dependency)
	}
	scopeSpace, scopeDomain := r.spaceID, r.domainID
	queryReadFence := r.queryReadFence
	r.mu.Unlock()
	if queryReadFence && scopeDomain != "" {
		var observedRevision uint64
		for _, entry := range entries {
			if entry.ObservedRevision > 0 && uint64(entry.ObservedRevision) > observedRevision {
				observedRevision = uint64(entry.ObservedRevision)
			}
		}
		if observedRevision > 0 {
			dependencies = append(dependencies, RevisionDependency{Kind: "domain", ID: scopeDomain, Revision: observedRevision})
		}
	}
	sort.Slice(dependencies, func(i, j int) bool {
		if dependencies[i].Kind != dependencies[j].Kind {
			return dependencies[i].Kind < dependencies[j].Kind
		}
		return dependencies[i].ID < dependencies[j].ID
	})
	out.Dependencies = dependencies
	out.RevisionWitness = encodeRevisionWitness(scopeSpace, scopeDomain, dependencies, r.witnessKey)
	return &out
}

func encodeRevisionWitness(spaceID, domainID string, dependencies []RevisionDependency, key []byte) []byte {
	if len(dependencies) == 0 {
		return nil
	}
	witness := revisionWitness{Version: 2, SpaceID: spaceID, DomainID: domainID, Dependencies: dependencies}
	if len(key) == 0 {
		witness.Version = 1
	}
	unsigned, err := json.Marshal(witness)
	if err != nil {
		return nil
	}
	if len(key) > 0 {
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write(unsigned)
		witness.MAC = hex.EncodeToString(mac.Sum(nil))
	} else {
		// Unit-test and non-cluster fallback. Production daemon instances derive
		// a key from the shared cluster backend authentication secret.
		digest := sha256.Sum256(unsigned)
		witness.Checksum = hex.EncodeToString(digest[:])
	}
	payload, err := json.Marshal(witness)
	if err != nil {
		return nil
	}
	return []byte(base64.RawURLEncoding.EncodeToString(payload))
}

func DecodeRevisionWitness(raw []byte, key ...[]byte) (spaceID, domainID string, dependencies []RevisionDependency, err error) {
	if len(raw) == 0 {
		return "", "", nil, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(string(raw))
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid revision witness encoding")
	}
	var witness revisionWitness
	if err := json.Unmarshal(payload, &witness); err != nil || (witness.Version != 1 && witness.Version != 2) {
		return "", "", nil, fmt.Errorf("invalid revision witness")
	}
	unsigned := revisionWitness{Version: witness.Version, SpaceID: witness.SpaceID, DomainID: witness.DomainID, Dependencies: witness.Dependencies}
	unsignedPayload, err := json.Marshal(unsigned)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid revision witness")
	}
	if witness.Version == 2 {
		if witness.MAC == "" || len(key) == 0 || len(key[0]) == 0 {
			return "", "", nil, fmt.Errorf("invalid revision witness authentication")
		}
		mac := hmac.New(sha256.New, key[0])
		_, _ = mac.Write(unsignedPayload)
		if !hmac.Equal([]byte(witness.MAC), []byte(hex.EncodeToString(mac.Sum(nil)))) {
			return "", "", nil, fmt.Errorf("invalid revision witness authentication")
		}
	} else {
		if witness.Checksum == "" {
			return "", "", nil, fmt.Errorf("invalid revision witness checksum")
		}
		digest := sha256.Sum256(unsignedPayload)
		if witness.Checksum != hex.EncodeToString(digest[:]) {
			return "", "", nil, fmt.Errorf("invalid revision witness checksum")
		}
	}
	return witness.SpaceID, witness.DomainID, witness.Dependencies, nil
}

func readMetadataFromStrong(read *StrongReadContext) *ReadMetadata {
	if read == nil {
		return nil
	}
	return &ReadMetadata{Consistency: ReadConsistencyStrong, GroupID: read.GroupID, PartitionID: read.PartitionID, LeaderNodeID: read.LeaderNodeID, ReadIndex: read.ReadIndex, AppliedIndex: read.AppliedIndex, ObservedRevision: read.ObservedRevision}
}

func RecordStrongReadContext(ctx context.Context, read *StrongReadContext) {
	if read != nil {
		for _, dependency := range read.Dependencies {
			RecordReadDependency(ctx, dependency)
		}
	}
	metadata := readMetadataFromStrong(read)
	if metadata == nil {
		return
	}
	RecordReadMetadata(ctx, *metadata)
}

func RecordOverlayRead(ctx context.Context) {
	RecordReadMetadata(ctx, ReadMetadata{Consistency: ReadConsistencyOverlay})
}
