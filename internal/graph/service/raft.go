package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/metadata"
)

type RaftStateMachine struct {
	Module         *Module
	PartitionID    uint32
	PartitionCount uint32
}

func (s RaftStateMachine) RaftStateMachineName() string { return "graph" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	return scope == consensus.CommandScopeSpacePartition && recordType == recordTypeGraphCommit
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applyGraphRaftCommand(ctx, apply, cmd, s.PartitionCount)
}

func (m *Module) graphChangeEventFromRaftRecord(ctx context.Context, record graphCommitRecord, commandID string) (graphchange.CommittedEvent, []GraphChange, error) {
	if err := normalizeGraphCommitRecordDomain(&record); err != nil {
		return graphchange.CommittedEvent{}, nil, err
	}
	store, err := m.store(ctx, record.SpaceID, record.DomainID)
	if err != nil {
		return graphchange.CommittedEvent{}, nil, err
	}
	snapshot := &overlay{putNodes: map[domaingraph.NodeID]domaingraph.Node{}, deleteNodes: map[domaingraph.NodeID]struct{}{}, putEdges: map[domaingraph.EdgeID]domaingraph.Edge{}, deleteEdges: map[domaingraph.EdgeID]struct{}{}, opCount: record.OperationCount}
	for _, node := range record.PutNodes {
		snapshot.putNodes[node.ID] = node
	}
	for _, id := range record.DeleteNodeIDs {
		snapshot.deleteNodes[id] = struct{}{}
	}
	for _, edge := range record.PutEdges {
		snapshot.putEdges[edge.ID] = edge
	}
	for _, id := range record.DeleteEdgeIDs {
		snapshot.deleteEdges[id] = struct{}{}
	}
	domainID := record.DomainID
	if strings.TrimSpace(domainID) == "" {
		domainID = firstRecordDomainID(record)
	}
	tx := daemonsession.GraphTransaction{ID: commandID, SpaceID: record.SpaceID, DomainID: domainID, Origin: record.Origin}
	event, err := m.graphChangeEvent(ctx, tx, store, snapshot)
	if err != nil {
		return graphchange.CommittedEvent{}, nil, err
	}
	if strings.TrimSpace(commandID) != "" {
		event.ID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("mycel:graph-change:"+commandID))
	}
	if event.TxnID == uuid.Nil {
		if strings.TrimSpace(commandID) != "" {
			event.TxnID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("mycel:graph-txn:"+commandID))
		} else if event.ID != uuid.Nil {
			event.TxnID = event.ID
		}
		event.TransactionID = event.TxnID
	}
	changes, err := m.overlayChanges(ctx, store, snapshot)
	if err != nil {
		return graphchange.CommittedEvent{}, nil, err
	}
	return event, changes, nil
}

func normalizeGraphCommitRecordDomain(record *graphCommitRecord) error {
	if record == nil {
		return fmt.Errorf("graph commit record is required")
	}
	if strings.TrimSpace(record.DomainID) == "" {
		record.DomainID = firstRecordDomainID(*record)
	}
	if _, err := newDomainStoreKey(record.SpaceID, record.DomainID); err != nil {
		return err
	}
	for _, node := range record.PutNodes {
		if node.DomainID.String() != record.DomainID {
			return fmt.Errorf("%w: node domain_id %s does not match transaction domain_id %s", ErrInvalidInput, node.DomainID, record.DomainID)
		}
	}
	for _, edge := range record.PutEdges {
		if edge.DomainID.String() != record.DomainID {
			return fmt.Errorf("%w: edge domain_id %s does not match transaction domain_id %s", ErrInvalidInput, edge.DomainID, record.DomainID)
		}
	}
	return nil
}

func firstRecordDomainID(record graphCommitRecord) string {
	for _, node := range record.PutNodes {
		if node.DomainID != uuid.Nil {
			return node.DomainID.String()
		}
	}
	for _, edge := range record.PutEdges {
		if edge.DomainID != uuid.Nil {
			return edge.DomainID.String()
		}
	}
	return ""
}

func graphRaftCommandID(ctx context.Context, txID string) string {
	if key := graphIdempotencyKeyFromContext(ctx); key != "" {
		return "graph-commit-idempotency-" + key
	}
	return "graph-commit-" + txID
}

func graphIdempotencyKeyFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, key := range []string{"idempotency-key", "x-idempotency-key"} {
		values := md.Get(key)
		if len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup, partitionCount uint32) {
	m.raftGroups = groups
	m.raftPartitionCount = partitionCount
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	m.mu.Unlock()
}

func (m *Module) proposeGraphRaftCommand(ctx context.Context, cmd consensus.RaftCommand) (consensus.ProposalResult, error) {
	if m.raftGroups == nil {
		return consensus.ProposalResult{}, fmt.Errorf("raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return consensus.ProposalResult{}, raftGraphUnavailable("raft partition group %d is not available", cmd.PartitionID)
	}
	leader := group.Leader()
	if leader == 0 {
		return consensus.ProposalResult{}, raftGraphUnavailable("raft partition group %d has no leader", cmd.PartitionID)
	}
	local := m.raftLocalNode
	if local == 0 && m.raftGroups != nil {
		local = m.raftGroups.NodeID()
	}
	if local == 0 {
		return consensus.ProposalResult{}, raftGraphUnavailable("raft graph local node id is not configured")
	}
	result, err := group.Propose(ctx, cmd)
	if err != nil {
		return consensus.ProposalResult{}, raftGraphUnavailable("raft graph proposal for partition %d failed: %v", cmd.PartitionID, err)
	}
	return result, nil
}

func (m *Module) buildGraphCommitRaftCommand(record graphCommitRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	spaceID, err := uuid.Parse(strings.TrimSpace(record.SpaceID))
	if err != nil || spaceID == uuid.Nil {
		return consensus.RaftCommand{}, fmt.Errorf("space_id must be a UUID")
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewSpaceCommand(domainspace.SpaceID(spaceID), partitionCount, recordTypeGraphCommit, payload, commandID)
}

func (m *Module) applyGraphRaftCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand, partitionCount uint32) error {
	if err := cmd.Validate(partitionCount); err != nil {
		return err
	}
	if cmd.RecordType != recordTypeGraphCommit {
		return fmt.Errorf("unsupported graph raft record type %s", cmd.RecordType)
	}
	m.mu.Lock()
	if _, ok := m.raftAppliedCommands[cmd.CommandID]; ok {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	var record graphCommitRecord
	if err := json.Unmarshal(cmd.Payload, &record); err != nil {
		return err
	}
	if strings.TrimSpace(record.SpaceID) != strings.TrimSpace(cmd.SpaceID) {
		return fmt.Errorf("graph raft command space_id mismatch: command=%s payload=%s", cmd.SpaceID, record.SpaceID)
	}
	if err := m.validateBlobReferences(ctx, record.SpaceID, record.PutNodes); err != nil {
		return err
	}
	if err := m.validateAutomationOutputFences(ctx, record); err != nil {
		return err
	}
	event, changes, err := m.graphChangeEventFromRaftRecord(ctx, record, cmd.CommandID)
	if err != nil {
		return err
	}
	revision, _, _, err := m.applyGraphCommitRecord(ctx, record)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			alreadyApplied, verifyErr := m.graphCommitRecordEffectsPresent(ctx, record)
			if verifyErr != nil {
				return verifyErr
			}
			if alreadyApplied {
				return m.rememberRaftAppliedCommand(context.WithoutCancel(ctx), cmd.CommandID)
			}
		}
		return err
	}
	if err := m.rememberRaftAppliedCommand(context.WithoutCancel(ctx), cmd.CommandID); err != nil {
		return err
	}
	event.Changes = changes
	m.notifyRaftApplyChangeSink(ctx, graphstorage.CommitInfo{TxnID: event.TxnID, NextRevision: uint64(revision)}, event)
	return nil
}

func (m *Module) graphCommitRecordEffectsPresent(ctx context.Context, record graphCommitRecord) (bool, error) {
	if err := normalizeGraphCommitRecordDomain(&record); err != nil {
		return false, err
	}
	store, err := m.store(ctx, record.SpaceID, record.DomainID)
	if err != nil {
		return false, err
	}
	for _, node := range record.PutNodes {
		got, err := store.GetNode(ctx, node.ID)
		if err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				return false, nil
			}
			return false, mapStorageError(err)
		}
		if !graphNodeEffectEqual(got, node) {
			return false, nil
		}
	}
	for _, edge := range record.PutEdges {
		got, err := store.GetEdge(ctx, edge.ID)
		if err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				return false, nil
			}
			return false, mapStorageError(err)
		}
		if !graphEdgeEffectEqual(got, edge) {
			return false, nil
		}
	}
	for _, id := range record.DeleteNodeIDs {
		if _, err := store.GetNode(ctx, id); err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				continue
			}
			return false, mapStorageError(err)
		}
		return false, nil
	}
	for _, id := range record.DeleteEdgeIDs {
		if _, err := store.GetEdge(ctx, id); err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				continue
			}
			return false, mapStorageError(err)
		}
		return false, nil
	}
	return true, nil
}

type graphEntityEffect struct {
	labels     []string
	properties map[string]any
	payload    map[string]any
	meta       map[string]any
	createdAt  time.Time
	updatedAt  time.Time
}

func graphNodeEffectEqual(got, want domaingraph.Node) bool {
	return got.ID == want.ID && got.DomainID == want.DomainID && blobRefsEqual(got.BlobRef, want.BlobRef) && got.Content == want.Content && mapsEqual(got.Props, want.Props) && graphEntityEffectEqual(nodeEffect(got), nodeEffect(want))
}

func graphEdgeEffectEqual(got, want domaingraph.Edge) bool {
	return got.ID == want.ID && got.DomainID == want.DomainID && got.FromID == want.FromID && got.ToID == want.ToID && graphEntityEffectEqual(edgeEffect(got), edgeEffect(want))
}

func nodeEffect(n domaingraph.Node) graphEntityEffect {
	return graphEntityEffect{labels: n.Labels, properties: n.Properties, payload: n.Payload, meta: n.Meta, createdAt: n.CreatedAt, updatedAt: n.UpdatedAt}
}

func edgeEffect(e domaingraph.Edge) graphEntityEffect {
	return graphEntityEffect{labels: e.Labels, properties: e.Properties, payload: e.Payload, meta: e.Meta, createdAt: e.CreatedAt, updatedAt: e.UpdatedAt}
}

func graphEntityEffectEqual(got, want graphEntityEffect) bool {
	if !want.createdAt.IsZero() && !got.createdAt.Equal(want.createdAt) {
		return false
	}
	if !want.updatedAt.IsZero() && !got.updatedAt.Equal(want.updatedAt) {
		return false
	}
	return stringSlicesEqual(got.labels, want.labels) && mapsEqual(got.properties, want.properties) && mapsEqual(got.payload, want.payload) && mapsEqual(got.meta, want.meta)
}

func blobRefsEqual(a, b *domaingraph.BlobID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mapsEqual(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
