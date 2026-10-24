package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const (
	MaxGraphCommitIdempotencyKeyBytes = 128
	GraphCommitIdempotencyRetention   = 30 * 24 * time.Hour
)

type IdempotencyMetrics struct {
	ReplayHits         uint64
	ReplayMisses       uint64
	Mismatches         uint64
	CapacityRejections uint64
	Evictions          uint64
	ActiveRecords      uint64
	CapacityRecords    uint64
}

type graphIdempotencyAcknowledgment struct {
	Scope string `json:"scope"`
	Key   string `json:"key"`
}

type graphCommitIdempotency struct {
	Scope                   string                         `json:"scope"`
	Key                     string                         `json:"key"`
	Digest                  string                         `json:"digest"`
	Metadata                graphCommitIdempotencyMetadata `json:"metadata"`
	CommittedAt             time.Time                      `json:"committed_at"`
	ExpiresAt               time.Time                      `json:"expires_at"`
	RetainUntilAcknowledged bool                           `json:"retain_until_acknowledged,omitempty"`
}

type graphCommitIdempotencyMetadata struct {
	CommitID       string                      `json:"commit_id"`
	TransactionID  string                      `json:"transaction_id"`
	SessionID      string                      `json:"session_id"`
	SpaceID        string                      `json:"space_id"`
	DomainID       string                      `json:"domain_id"`
	BaseRevision   int64                       `json:"base_revision"`
	OperationCount int32                       `json:"operation_count"`
	OperationID    string                      `json:"operation_id,omitempty"`
	Event          *graphchange.CommittedEvent `json:"event,omitempty"`
}

type canonicalGraphMutation struct {
	OperationCount         int32                `json:"operation_count"`
	PutNodes               []domaingraph.Node   `json:"put_nodes,omitempty"`
	DeleteNodeIDs          []domaingraph.NodeID `json:"delete_node_ids,omitempty"`
	PutEdges               []domaingraph.Edge   `json:"put_edges,omitempty"`
	DeleteEdgeIDs          []domaingraph.EdgeID `json:"delete_edge_ids,omitempty"`
	AutomationFenceNodeIDs []domaingraph.NodeID `json:"automation_fence_node_ids,omitempty"`
	AutomationFenceEdgeIDs []domaingraph.EdgeID `json:"automation_fence_edge_ids,omitempty"`
}

func (m *Module) lockGraphCommitIdempotencyScope(tx sessionmodel.GraphTransaction) func() {
	return m.lockGraphCommitIdempotencyIdentity(tx.PrincipalID, tx.SpaceID, tx.DomainID, tx.IdempotencyKey)
}

func (m *Module) lockGraphCommitIdempotencyIdentity(principalID, spaceID, domainID, key string) func() {
	if key == "" {
		return func() {}
	}
	sum := sha256.Sum256([]byte(principalID + "\x00" + spaceID + "\x00" + domainID + "\x00" + key))
	lock := &m.idempotencyScopeLocks[int(sum[0])%len(m.idempotencyScopeLocks)]
	lock.Lock()
	return lock.Unlock
}

func validateGraphCommitIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if key != strings.TrimSpace(key) {
		return fmt.Errorf("%w: idempotency_key must not have leading or trailing whitespace", ErrInvalidInput)
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("%w: idempotency_key must be valid UTF-8", ErrInvalidInput)
	}
	if len(key) > MaxGraphCommitIdempotencyKeyBytes {
		return fmt.Errorf("%w: idempotency_key exceeds %d bytes", ErrInvalidInput, MaxGraphCommitIdempotencyKeyBytes)
	}
	return nil
}

func newGraphCommitIdempotency(tx sessionmodel.GraphTransaction, record graphCommitRecord) (*graphCommitIdempotency, error) {
	if tx.IdempotencyKey == "" {
		return nil, nil
	}
	if err := validateGraphCommitIdempotencyKey(tx.IdempotencyKey); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tx.PrincipalID) == "" {
		return nil, fmt.Errorf("%w: authenticated principal is required for idempotency", ErrInvalidInput)
	}
	digest, err := graphMutationDigest(record)
	if err != nil {
		return nil, err
	}
	commitID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	committedAt := time.Now().UTC()
	return &graphCommitIdempotency{
		Scope:  tx.PrincipalID,
		Key:    tx.IdempotencyKey,
		Digest: digest,
		Metadata: graphCommitIdempotencyMetadata{
			CommitID:       commitID.String(),
			TransactionID:  tx.ID,
			SessionID:      tx.SessionID,
			SpaceID:        tx.SpaceID,
			DomainID:       tx.DomainID,
			BaseRevision:   tx.BaseRevision,
			OperationCount: record.OperationCount,
			OperationID:    tx.Origin.OperationID,
		},
		CommittedAt:             committedAt,
		ExpiresAt:               committedAt.Add(GraphCommitIdempotencyRetention),
		RetainUntilAcknowledged: tx.RetainIdempotencyUntilAcknowledged,
	}, nil
}

func graphMutationDigest(record graphCommitRecord) (string, error) {
	nodes := append([]domaingraph.Node(nil), record.PutNodes...)
	for i := range nodes {
		nodes[i].CreatedAt = time.Time{}
		nodes[i].UpdatedAt = time.Time{}
		nodes[i].Labels = canonicalStringSet(nodes[i].Labels)
		nodes[i].Properties = idempotencyCanonicalMap(nodes[i].Properties)
		nodes[i].Payload = idempotencyCanonicalMap(nodes[i].Payload)
		nodes[i].Meta = idempotencyCanonicalMap(nodes[i].Meta)
		nodes[i].Props = idempotencyCanonicalMap(nodes[i].Props)
	}
	edges := append([]domaingraph.Edge(nil), record.PutEdges...)
	for i := range edges {
		edges[i].CreatedAt = time.Time{}
		edges[i].UpdatedAt = time.Time{}
		edges[i].Labels = canonicalStringSet(edges[i].Labels)
		edges[i].Properties = idempotencyCanonicalMap(edges[i].Properties)
		edges[i].Payload = idempotencyCanonicalMap(edges[i].Payload)
		edges[i].Meta = idempotencyCanonicalMap(edges[i].Meta)
	}
	payload, err := json.Marshal(canonicalGraphMutation{
		OperationCount:         record.OperationCount,
		PutNodes:               nodes,
		DeleteNodeIDs:          record.DeleteNodeIDs,
		PutEdges:               edges,
		DeleteEdgeIDs:          record.DeleteEdgeIDs,
		AutomationFenceNodeIDs: record.AutomationFenceNodeIDs,
		AutomationFenceEdgeIDs: record.AutomationFenceEdgeIDs,
	})
	if err != nil {
		return "", fmt.Errorf("canonical graph mutation: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalStringSet(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

func idempotencyCanonicalMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = idempotencyCanonicalValue(value)
	}
	return out
}

func idempotencyCanonicalValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return idempotencyCanonicalMap(typed)
	case []any:
		if len(typed) == 0 {
			return nil
		}
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = idempotencyCanonicalValue(item)
		}
		return out
	case []string:
		if len(typed) == 0 {
			return nil
		}
		return append([]string(nil), typed...)
	default:
		return value
	}
}

func attachIdempotencyEvent(record *graphCommitIdempotency, event graphchange.CommittedEvent, changes []graphchange.Change) error {
	if record == nil {
		return nil
	}
	commitID, err := uuid.Parse(record.Metadata.CommitID)
	if err != nil {
		return fmt.Errorf("invalid idempotency commit ID: %w", err)
	}
	event.ID = commitID
	event.CommitID = commitID
	event.TxnID = commitID
	event.TransactionID = commitID
	event.CommittedAt = record.CommittedAt
	event.Changes = changes
	event.Normalize()
	record.Metadata.Event = &event
	return nil
}

func idempotencyEvent(record graphstorage.IdempotencyRecord) (graphchange.CommittedEvent, bool, error) {
	var metadata graphCommitIdempotencyMetadata
	if err := json.Unmarshal(record.Metadata, &metadata); err != nil {
		return graphchange.CommittedEvent{}, false, fmt.Errorf("decode graph idempotency metadata: %w", err)
	}
	if metadata.Event == nil || metadata.Event.Empty() {
		return graphchange.CommittedEvent{}, false, nil
	}
	event := *metadata.Event
	event.GraphRevision = record.CommittedRevision
	event.Revision = record.CommittedRevision
	event.CommittedAt = record.CommittedAt
	event.Normalize()
	return event, true, nil
}

func (m *Module) deliverIdempotencyEvent(ctx context.Context, store *graphstorage.LocalStore, record graphstorage.IdempotencyRecord) error {
	if record.Delivered {
		return nil
	}
	event, ok, err := idempotencyEvent(record)
	if err != nil || !ok {
		return err
	}
	m.mu.Lock()
	sink := m.changeSink
	if m.raftGroups != nil {
		sink = m.raftApplyChangeSink
	}
	m.mu.Unlock()
	delivered, err := m.notifyChangeSink(ctx, sink, graphstorage.CommitInfo{TxnID: event.TxnID, NextRevision: record.CommittedRevision}, event)
	if err != nil || !delivered {
		return err
	}
	if err := store.MarkIdempotencyDelivered(context.WithoutCancel(ctx), record.Scope, record.Key, record.Digest); err != nil {
		m.mu.Lock()
		m.lastGraphChangeSinkErr = err
		m.mu.Unlock()
		return err
	}
	return nil
}

func (record graphCommitIdempotency) storageRecord() (graphstorage.IdempotencyRecord, error) {
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return graphstorage.IdempotencyRecord{}, err
	}
	return graphstorage.IdempotencyRecord{
		Scope:                   record.Scope,
		Key:                     record.Key,
		Digest:                  record.Digest,
		Metadata:                metadata,
		CommittedAt:             record.CommittedAt,
		ExpiresAt:               record.ExpiresAt,
		RetainUntilAcknowledged: record.RetainUntilAcknowledged,
	}, nil
}

// AcknowledgeIdempotentCommit durably releases a retained publication receipt
// only after its caller has persisted the terminal publication disposition.
func (m *Module) AcknowledgeIdempotentCommit(ctx context.Context, principalID, spaceID, domainID, key string) error {
	principalID = strings.TrimSpace(principalID)
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	key = strings.TrimSpace(key)
	if principalID == "" || spaceID == "" || domainID == "" || key == "" {
		return fmt.Errorf("%w: idempotency acknowledgement scope is required", ErrInvalidInput)
	}
	if len(key) > MaxGraphCommitIdempotencyKeyBytes || !utf8.ValidString(key) {
		return fmt.Errorf("%w: invalid idempotency acknowledgement key", ErrInvalidInput)
	}
	if err := m.requireRaftGraphWriteRoute(spaceID); err != nil {
		return err
	}
	record := graphCommitRecord{
		SpaceID:  spaceID,
		DomainID: domainID,
		IdempotencyAcknowledgment: &graphIdempotencyAcknowledgment{
			Scope: principalID,
			Key:   key,
		},
	}
	if m.raftGroups != nil {
		sum := sha256.Sum256([]byte(principalID + "\x00" + spaceID + "\x00" + domainID + "\x00" + key))
		cmd, err := m.buildGraphCommitRaftCommand(record, m.raftPartitionCount, "graph-idempotency-ack-"+hex.EncodeToString(sum[:]))
		if err != nil {
			return err
		}
		_, err = m.proposeGraphRaftCommand(ctx, cmd)
		return err
	}
	store, err := m.store(ctx, spaceID, domainID)
	if err != nil {
		return err
	}
	err = store.MarkIdempotencyPublicationAcknowledged(ctx, principalID, key)
	if errors.Is(err, graphstorage.ErrNotFound) {
		return nil
	}
	return mapStorageError(err)
}

func (m *Module) observeIdempotencyError(record *graphCommitIdempotency, err error) error {
	if record == nil || err == nil {
		return err
	}
	if errors.Is(err, ErrIdempotencyConflict) || errors.Is(err, graphstorage.ErrIdempotencyConflict) {
		m.idempotencyMismatches.Add(1)
	}
	if errors.Is(err, ErrIdempotencyCapacity) || errors.Is(err, graphstorage.ErrIdempotencyCapacity) {
		m.idempotencyCapacityRejections.Add(1)
	}
	return err
}

func graphCommitResultFromIdempotency(record graphstorage.IdempotencyRecord, replay bool) (graphchange.CommitResult, error) {
	var metadata graphCommitIdempotencyMetadata
	if err := json.Unmarshal(record.Metadata, &metadata); err != nil {
		return graphchange.CommitResult{}, fmt.Errorf("decode graph idempotency metadata: %w", err)
	}
	return graphchange.CommitResult{
		OperationCount:       metadata.OperationCount,
		CommittedRevision:    int64(record.CommittedRevision),
		CommitID:             metadata.CommitID,
		TransactionID:        metadata.TransactionID,
		SessionID:            metadata.SessionID,
		SpaceID:              metadata.SpaceID,
		DomainID:             metadata.DomainID,
		BaseRevision:         metadata.BaseRevision,
		OperationID:          metadata.OperationID,
		CommittedAt:          record.CommittedAt,
		IdempotencyKey:       record.Key,
		IdempotencyExpiresAt: record.ExpiresAt,
		IdempotentReplay:     replay,
	}, nil
}

func (m *Module) LookupIdempotentCommit(ctx context.Context, principalID, spaceID, domainID, key string) (graphchange.CommitResult, bool, error) {
	if strings.TrimSpace(principalID) == "" {
		return graphchange.CommitResult{}, false, fmt.Errorf("%w: authenticated principal is required for idempotency", ErrInvalidInput)
	}
	if err := validateGraphCommitIdempotencyKey(key); err != nil {
		return graphchange.CommitResult{}, false, err
	}
	if key == "" {
		return graphchange.CommitResult{}, false, nil
	}
	if _, err := newDomainStoreKey(spaceID, domainID); err != nil {
		return graphchange.CommitResult{}, false, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphRead(spaceID); err != nil {
		return graphchange.CommitResult{}, false, err
	} else if forward {
		req := raftGraphReadRequest{Op: "get_idempotent_commit", SpaceID: spaceID, DomainID: domainID, PrincipalID: principalID, IdempotencyKey: key}
		var res raftGraphIdempotencyResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return graphchange.CommitResult{}, false, err
		}
		return res.Commit, res.Found, ctx.Err()
	}
	if _, err := m.strongGraphRead(ctx, spaceID); err != nil {
		return graphchange.CommitResult{}, false, err
	}
	return m.lookupIdempotentCommitWithDelivery(ctx, principalID, spaceID, domainID, key)
}

func (m *Module) IdempotencyMetrics() IdempotencyMetrics {
	metrics := IdempotencyMetrics{
		ReplayHits:         m.idempotencyReplayHits.Load(),
		ReplayMisses:       m.idempotencyReplayMisses.Load(),
		Mismatches:         m.idempotencyMismatches.Load(),
		CapacityRejections: m.idempotencyCapacityRejections.Load(),
	}
	m.mu.Lock()
	for _, store := range m.stores {
		active, capacity, evictions := store.IdempotencyStats()
		metrics.ActiveRecords += uint64(active)
		metrics.CapacityRecords += uint64(capacity)
		metrics.Evictions += evictions
	}
	m.mu.Unlock()
	return metrics
}

func (m *Module) lookupIdempotentCommitLocal(ctx context.Context, principalID, spaceID, domainID, key string) (graphchange.CommitResult, bool, error) {
	store, err := m.store(ctx, spaceID, domainID)
	if err != nil {
		return graphchange.CommitResult{}, false, err
	}
	record, ok, err := store.GetIdempotencyRecord(ctx, principalID, key)
	if err != nil {
		return graphchange.CommitResult{}, false, mapStorageError(err)
	}
	if !ok {
		m.idempotencyReplayMisses.Add(1)
		return graphchange.CommitResult{}, false, nil
	}
	m.idempotencyReplayHits.Add(1)
	result, err := graphCommitResultFromIdempotency(record, true)
	return result, true, err
}

func (m *Module) lookupIdempotentCommitWithDelivery(ctx context.Context, principalID, spaceID, domainID, key string) (graphchange.CommitResult, bool, error) {
	release := m.lockGraphCommitIdempotencyIdentity(principalID, spaceID, domainID, key)
	defer release()
	result, found, err := m.lookupIdempotentCommitLocal(ctx, principalID, spaceID, domainID, key)
	if err != nil || !found {
		return result, found, err
	}
	store, err := m.store(ctx, spaceID, domainID)
	if err != nil {
		return graphchange.CommitResult{}, false, err
	}
	record, found, err := store.GetIdempotencyRecord(ctx, principalID, key)
	if err != nil {
		return graphchange.CommitResult{}, false, mapStorageError(err)
	}
	if found {
		_ = m.deliverIdempotencyEvent(ctx, store, record)
	}
	return result, true, nil
}
