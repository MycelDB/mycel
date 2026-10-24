package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	"github.com/myceldb/mycel/internal/wal"
)

const recordTypeGraphCommit wal.RecordType = "graph.commit.v1"

type graphCommitRecord struct {
	SpaceID                   string                           `json:"space_id"`
	DomainID                  string                           `json:"domain_id,omitempty"`
	Origin                    graphchange.OriginMetadata       `json:"origin,omitempty"`
	BaseRevision              int64                            `json:"base_revision"`
	PutNodes                  []domaingraph.Node               `json:"put_nodes,omitempty"`
	DeleteNodeIDs             []domaingraph.NodeID             `json:"delete_node_ids,omitempty"`
	PutEdges                  []domaingraph.Edge               `json:"put_edges,omitempty"`
	DeleteEdgeIDs             []domaingraph.EdgeID             `json:"delete_edge_ids,omitempty"`
	AutomationFenceNodeIDs    []domaingraph.NodeID             `json:"automation_fence_node_ids,omitempty"`
	AutomationFenceEdgeIDs    []domaingraph.EdgeID             `json:"automation_fence_edge_ids,omitempty"`
	OperationCount            int32                            `json:"operation_count"`
	Idempotency               *graphCommitIdempotency          `json:"idempotency,omitempty"`
	IdempotencyAcknowledgment *graphIdempotencyRecordReference `json:"idempotency_acknowledgment,omitempty"`
	IdempotencyRetention      *graphIdempotencyRetention       `json:"idempotency_retention,omitempty"`
}

func graphCommitRecordFromSnapshot(tx sessionmodel.GraphTransaction, snapshot *overlay) graphCommitRecord {
	return graphCommitRecord{SpaceID: tx.SpaceID, DomainID: tx.DomainID, Origin: tx.Origin, BaseRevision: tx.BaseRevision, PutNodes: sortedNodes(snapshot.putNodes), DeleteNodeIDs: sortedNodeIDs(snapshot.deleteNodes), PutEdges: sortedEdges(snapshot.putEdges), DeleteEdgeIDs: sortedEdgeIDs(snapshot.deleteEdges), AutomationFenceNodeIDs: sortedNodeIDs(snapshot.automationFenceNodes), AutomationFenceEdgeIDs: sortedEdgeIDs(snapshot.automationFenceEdges), OperationCount: snapshot.opCount}
}

func (m *Module) applyGraphCommit(ctx context.Context, rec wal.Record) error {
	var payload graphCommitRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, _, _, err := m.applyGraphCommitRecord(ctx, payload)
	return err
}

func (m *Module) applyGraphCommitRecord(ctx context.Context, payload graphCommitRecord) (int64, graphCommitRecord, graphstorage.CommitInfo, error) {
	totalStart := time.Now()
	timing := graphApplyTiming{}
	stepStart := time.Now()
	if err := normalizeGraphCommitRecordDomain(&payload); err != nil {
		return 0, payload, graphstorage.CommitInfo{}, err
	}
	timing.Normalize = time.Since(stepStart)
	stepStart = time.Now()
	store, err := m.store(ctx, payload.SpaceID, payload.DomainID)
	if err != nil {
		return 0, payload, graphstorage.CommitInfo{}, err
	}
	timing.StoreOpen = time.Since(stepStart)
	if payload.IdempotencyRetention != nil {
		retain := payload.IdempotencyRetention
		if retain.Record.Scope != retain.Scope || retain.Record.Key != retain.Key || !retain.Record.RetainUntilAcknowledged {
			return 0, payload, graphstorage.CommitInfo{}, fmt.Errorf("invalid idempotency retention record")
		}
		err := store.RetainIdempotencyUntilAcknowledged(ctx, retain.Scope, retain.Key)
		if errors.Is(err, graphstorage.ErrNotFound) {
			err = store.RestoreIdempotencyRecords(ctx, []graphstorage.IdempotencyRecord{retain.Record})
		}
		return int64(store.Revision()), payload, graphstorage.CommitInfo{}, mapStorageError(err)
	}
	if payload.IdempotencyAcknowledgment != nil {
		ack := payload.IdempotencyAcknowledgment
		err := store.MarkIdempotencyPublicationAcknowledged(ctx, ack.Scope, ack.Key)
		return int64(store.Revision()), payload, graphstorage.CommitInfo{}, mapStorageError(err)
	}
	stepStart = time.Now()
	storageTx, err := store.Begin(ctx)
	if err != nil {
		return 0, payload, graphstorage.CommitInfo{}, mapStorageError(err)
	}
	if payload.Idempotency != nil {
		record, err := payload.Idempotency.storageRecord()
		if err != nil {
			_ = storageTx.Rollback()
			return 0, payload, graphstorage.CommitInfo{}, err
		}
		storageTx.SetIdempotency(record)
	}
	timing.StorageBegin = time.Since(stepStart)
	stepStart = time.Now()
	storageTx.ExpectRevision(uint64(payload.BaseRevision))
	timing.ExpectRevision = time.Since(stepStart)
	stepStart = time.Now()
	for _, node := range payload.PutNodes {
		if err := storageTx.PutNode(node); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, graphstorage.CommitInfo{}, mapStorageError(err)
		}
	}
	timing.PutNodes = time.Since(stepStart)
	stepStart = time.Now()
	for _, edge := range payload.PutEdges {
		if err := storageTx.PutEdge(edge); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, graphstorage.CommitInfo{}, mapStorageError(err)
		}
	}
	timing.PutEdges = time.Since(stepStart)
	stepStart = time.Now()
	for _, id := range payload.DeleteNodeIDs {
		if err := storageTx.DeleteNode(id); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, graphstorage.CommitInfo{}, mapStorageError(err)
		}
	}
	timing.DeleteNodes = time.Since(stepStart)
	stepStart = time.Now()
	for _, id := range payload.DeleteEdgeIDs {
		if err := storageTx.DeleteEdge(id); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, graphstorage.CommitInfo{}, mapStorageError(err)
		}
	}
	timing.DeleteEdges = time.Since(stepStart)
	stepStart = time.Now()
	info, err := storageTx.CommitWithInfo()
	timing.StorageCommit = time.Since(stepStart)
	if err != nil {
		return 0, payload, graphstorage.CommitInfo{}, mapStorageError(err)
	}
	timing.CommitTiming = info.Timing
	timing.Total = time.Since(totalStart)
	m.logGraphApplyTiming(payload, info.NextRevision, timing)
	return int64(store.Revision()), payload, info, nil
}

func (m *Module) markWALApplied(ctx context.Context, lsn wal.LSN) error {
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return nil
}
