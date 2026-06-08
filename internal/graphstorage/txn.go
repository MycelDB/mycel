package graphstorage

import (
	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
)

type localTxn struct {
	store       *LocalStore
	id          uuid.UUID
	nodePuts    []graph.Node
	nodeDeletes []graph.NodeID
	edgePuts    []graph.Edge
	edgeDeletes []graph.EdgeID
	closed      bool
}

func (t *localTxn) PutNode(node graph.Node) error {
	if t.closed {
		return ErrTxnClosed
	}
	t.nodePuts = append(t.nodePuts, cloneNode(node))
	return nil
}
func (t *localTxn) DeleteNode(id graph.NodeID) error {
	if t.closed {
		return ErrTxnClosed
	}
	t.nodeDeletes = append(t.nodeDeletes, id)
	return nil
}
func (t *localTxn) PutEdge(edge graph.Edge) error {
	if t.closed {
		return ErrTxnClosed
	}
	t.edgePuts = append(t.edgePuts, cloneEdge(edge))
	return nil
}
func (t *localTxn) DeleteEdge(id graph.EdgeID) error {
	if t.closed {
		return ErrTxnClosed
	}
	t.edgeDeletes = append(t.edgeDeletes, id)
	return nil
}
func (t *localTxn) Rollback() error { t.closed = true; return nil }

func (t *localTxn) Commit() error {
	if t.closed {
		return ErrTxnClosed
	}
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.store.ensureReady(); err != nil {
		return err
	}
	zero := uuid.Nil
	if _, err := t.store.txns.appendRecord(RecordKindTxnBegin, t.id, zero, nil); err != nil {
		return err
	}
	nodeLocs := make([]RecordLocation, len(t.nodePuts))
	edgeLocs := make([]RecordLocation, len(t.edgePuts))
	nodeDelLocs := make([]RecordLocation, len(t.nodeDeletes))
	edgeDelLocs := make([]RecordLocation, len(t.edgeDeletes))
	for i, n := range t.nodePuts {
		payload, err := encodeNode(n)
		if err != nil {
			return err
		}
		loc, err := t.store.nodes.appendRecord(RecordKindNodePut, t.id, n.ID, payload)
		if err != nil {
			return err
		}
		nodeLocs[i] = loc
	}
	for i, id := range t.nodeDeletes {
		loc, err := t.store.nodes.appendRecord(RecordKindNodeTombstone, t.id, id, nil)
		if err != nil {
			return err
		}
		nodeDelLocs[i] = loc
	}
	for i, e := range t.edgePuts {
		payload, err := encodeEdge(e)
		if err != nil {
			return err
		}
		loc, err := t.store.edges.appendRecord(RecordKindEdgePut, t.id, e.ID, payload)
		if err != nil {
			return err
		}
		edgeLocs[i] = loc
	}
	for i, id := range t.edgeDeletes {
		loc, err := t.store.edges.appendRecord(RecordKindEdgeTombstone, t.id, id, nil)
		if err != nil {
			return err
		}
		edgeDelLocs[i] = loc
	}
	if err := t.store.nodes.sync(); err != nil {
		return err
	}
	if err := t.store.edges.sync(); err != nil {
		return err
	}
	if _, err := t.store.txns.appendRecord(RecordKindTxnCommit, t.id, zero, nil); err != nil {
		return err
	}
	if err := t.store.txns.sync(); err != nil {
		return err
	}
	for i, n := range t.nodePuts {
		t.store.applyNodePut(n, nodeLocs[i])
	}
	for i, id := range t.nodeDeletes {
		t.store.applyNodeDelete(id, nodeDelLocs[i])
	}
	for i, e := range t.edgePuts {
		t.store.applyEdgePut(e, edgeLocs[i])
	}
	for i, id := range t.edgeDeletes {
		t.store.applyEdgeDelete(id, edgeDelLocs[i])
	}
	t.closed = true
	return nil
}
