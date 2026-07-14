package graph

import (
	"context"
	"encoding/json"

	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/wal"
)

const recordTypeGraphCommit wal.RecordType = "graph.commit.v1"

type graphCommitRecord struct {
	SpaceID        string               `json:"space_id"`
	BaseRevision   int64                `json:"base_revision"`
	PutNodes       []domaingraph.Node   `json:"put_nodes,omitempty"`
	DeleteNodeIDs  []domaingraph.NodeID `json:"delete_node_ids,omitempty"`
	PutEdges       []domaingraph.Edge   `json:"put_edges,omitempty"`
	DeleteEdgeIDs  []domaingraph.EdgeID `json:"delete_edge_ids,omitempty"`
	OperationCount int32                `json:"operation_count"`
}

func graphCommitRecordFromSnapshot(tx daemonsession.GraphTransaction, snapshot *overlay) graphCommitRecord {
	return graphCommitRecord{SpaceID: tx.SpaceID, BaseRevision: tx.BaseRevision, PutNodes: sortedNodes(snapshot.putNodes), DeleteNodeIDs: sortedNodeIDs(snapshot.deleteNodes), PutEdges: sortedEdges(snapshot.putEdges), DeleteEdgeIDs: sortedEdgeIDs(snapshot.deleteEdges), OperationCount: snapshot.opCount}
}

func (m *Module) applyGraphCommit(ctx context.Context, rec wal.Record) error {
	var payload graphCommitRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, _, err := m.applyGraphCommitRecord(ctx, payload)
	return err
}

func (m *Module) applyGraphCommitRecord(ctx context.Context, payload graphCommitRecord) (int64, graphCommitRecord, error) {
	store, err := m.store(ctx, payload.SpaceID)
	if err != nil {
		return 0, payload, err
	}
	storageTx, err := store.Begin(ctx)
	if err != nil {
		return 0, payload, mapStorageError(err)
	}
	storageTx.ExpectRevision(uint64(payload.BaseRevision))
	for _, node := range payload.PutNodes {
		if err := storageTx.PutNode(node); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, mapStorageError(err)
		}
	}
	for _, edge := range payload.PutEdges {
		if err := storageTx.PutEdge(edge); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, mapStorageError(err)
		}
	}
	for _, id := range payload.DeleteNodeIDs {
		if err := storageTx.DeleteNode(id); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, mapStorageError(err)
		}
	}
	for _, id := range payload.DeleteEdgeIDs {
		if err := storageTx.DeleteEdge(id); err != nil {
			_ = storageTx.Rollback()
			return 0, payload, mapStorageError(err)
		}
	}
	if _, err := storageTx.CommitWithInfo(); err != nil {
		return 0, payload, mapStorageError(err)
	}
	return int64(store.Revision()), payload, nil
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
