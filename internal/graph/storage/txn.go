package graphstorage

import (
	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
)

type localTxn struct {
	store            *LocalStore
	id               uuid.UUID
	nodePuts         []graph.Node
	nodeDeletes      []graph.NodeID
	edgePuts         []graph.Edge
	edgeDeletes      []graph.EdgeID
	expectedRevision *uint64
	commitHook       CommitHook
	closed           bool
}

func (t *localTxn) ExpectRevision(revision uint64) {
	if t.closed {
		return
	}
	t.expectedRevision = &revision
}

func (t *localTxn) SetCommitHook(hook CommitHook) {
	if t.closed {
		return
	}
	t.commitHook = hook
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
	_, err := t.CommitWithInfo()
	return err
}

func (t *localTxn) CommitWithInfo() (CommitInfo, error) {
	if t.closed {
		return CommitInfo{}, ErrTxnClosed
	}
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.store.ensureReady(); err != nil {
		return CommitInfo{}, err
	}
	if t.expectedRevision != nil && *t.expectedRevision > t.store.revision {
		return CommitInfo{}, ErrConflict
	}
	if _, ok := t.writeSetConflict(); ok {
		return CommitInfo{}, ErrConflict
	}
	if _, ok := t.invariantConflict(); ok {
		return CommitInfo{}, ErrConflict
	}
	zero := uuid.Nil
	if _, err := t.store.txns.appendRecord(RecordKindTxnBegin, t.id, zero, nil); err != nil {
		return CommitInfo{}, err
	}
	nodeLocs := make([]RecordLocation, len(t.nodePuts))
	edgeLocs := make([]RecordLocation, len(t.edgePuts))
	nodeDelLocs := make([]RecordLocation, len(t.nodeDeletes))
	edgeDelLocs := make([]RecordLocation, len(t.edgeDeletes))
	for i, n := range t.nodePuts {
		payload, err := encodeNode(n)
		if err != nil {
			return CommitInfo{}, err
		}
		loc, err := t.store.nodes.appendRecord(RecordKindNodePut, t.id, n.ID, payload)
		if err != nil {
			return CommitInfo{}, err
		}
		nodeLocs[i] = loc
	}
	for i, id := range t.nodeDeletes {
		loc, err := t.store.nodes.appendRecord(RecordKindNodeTombstone, t.id, id, nil)
		if err != nil {
			return CommitInfo{}, err
		}
		nodeDelLocs[i] = loc
	}
	for i, e := range t.edgePuts {
		payload, err := encodeEdge(e)
		if err != nil {
			return CommitInfo{}, err
		}
		loc, err := t.store.edges.appendRecord(RecordKindEdgePut, t.id, e.ID, payload)
		if err != nil {
			return CommitInfo{}, err
		}
		edgeLocs[i] = loc
	}
	for i, id := range t.edgeDeletes {
		loc, err := t.store.edges.appendRecord(RecordKindEdgeTombstone, t.id, id, nil)
		if err != nil {
			return CommitInfo{}, err
		}
		edgeDelLocs[i] = loc
	}
	if err := t.store.nodes.sync(); err != nil {
		return CommitInfo{}, err
	}
	if err := t.store.edges.sync(); err != nil {
		return CommitInfo{}, err
	}
	info := CommitInfo{TxnID: t.id, NextRevision: t.store.revision + 1}
	if t.commitHook != nil {
		if err := t.commitHook(info); err != nil {
			return CommitInfo{}, err
		}
	}
	if _, err := t.store.txns.appendRecord(RecordKindTxnCommit, t.id, zero, nil); err != nil {
		return CommitInfo{}, err
	}
	if err := t.store.txns.sync(); err != nil {
		return CommitInfo{}, err
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
	t.store.revision++
	newRev := t.store.revision
	for _, n := range t.nodePuts {
		t.store.nodeModRev[n.ID] = newRev
	}
	for _, id := range t.nodeDeletes {
		t.store.nodeModRev[id] = newRev
	}
	for _, e := range t.edgePuts {
		t.store.edgeModRev[e.ID] = newRev
	}
	for _, id := range t.edgeDeletes {
		t.store.edgeModRev[id] = newRev
	}
	t.closed = true
	return info, nil
}

// invariantConflict reports graph-shape conflicts that are not captured by
// entity ID write-set overlap, such as edge endpoints deleted by a concurrent
// commit or two different contains edges claiming the same child.
func (t *localTxn) invariantConflict() (string, bool) {
	nodePuts := make(map[graph.NodeID]struct{}, len(t.nodePuts))
	for _, node := range t.nodePuts {
		nodePuts[node.ID] = struct{}{}
	}
	nodeDeletes := make(map[graph.NodeID]struct{}, len(t.nodeDeletes))
	for _, id := range t.nodeDeletes {
		nodeDeletes[id] = struct{}{}
	}
	edgeDeletes := make(map[graph.EdgeID]struct{}, len(t.edgeDeletes))
	for _, id := range t.edgeDeletes {
		edgeDeletes[id] = struct{}{}
	}
	edgePuts := make(map[graph.EdgeID]graph.Edge, len(t.edgePuts))
	for _, edge := range t.edgePuts {
		edgePuts[edge.ID] = edge
	}
	containsChild := map[graph.NodeID]graph.EdgeID{}
	for _, edge := range t.edgePuts {
		if _, deleting := nodeDeletes[edge.FromID]; deleting {
			return edge.ID.String(), true
		}
		if _, deleting := nodeDeletes[edge.ToID]; deleting {
			return edge.ID.String(), true
		}
		if _, ok := t.store.nodeRecords[edge.FromID]; !ok {
			if _, created := nodePuts[edge.FromID]; !created {
				return edge.ID.String(), true
			}
		}
		if _, ok := t.store.nodeRecords[edge.ToID]; !ok {
			if _, created := nodePuts[edge.ToID]; !created {
				return edge.ID.String(), true
			}
		}
		if edge.Kind == graph.EdgeKindContains {
			if existing, ok := containsChild[edge.ToID]; ok && existing != edge.ID {
				return edge.ToID.String(), true
			}
			containsChild[edge.ToID] = edge.ID
			if existing, ok := t.store.containsParent[edge.ToID]; ok && existing != edge.ID {
				if _, deleting := edgeDeletes[existing]; !deleting {
					return edge.ToID.String(), true
				}
			}
		}
	}
	for _, id := range t.nodeDeletes {
		for edgeID, edge := range t.store.edgeRecords {
			if edge.FromID != id && edge.ToID != id {
				continue
			}
			if replacement, replacing := edgePuts[edgeID]; replacing && replacement.FromID != id && replacement.ToID != id {
				continue
			}
			if _, deleting := edgeDeletes[edgeID]; !deleting {
				return id.String(), true
			}
		}
		for _, edge := range t.edgePuts {
			if edge.FromID == id || edge.ToID == id {
				return id.String(), true
			}
		}
	}
	return "", false
}

// writeSetConflict reports whether any entity this transaction writes or deletes
// was modified by another committed transaction after this transaction's base
// revision. Transactions whose write-sets are disjoint from concurrent commits
// do not conflict; new entities (never written before) never conflict.
func (t *localTxn) writeSetConflict() (string, bool) {
	if t.expectedRevision == nil {
		return "", false
	}
	base := *t.expectedRevision
	for _, n := range t.nodePuts {
		if t.store.nodeModRev[n.ID] > base {
			return n.ID.String(), true
		}
	}
	for _, id := range t.nodeDeletes {
		if t.store.nodeModRev[id] > base {
			return id.String(), true
		}
	}
	for _, e := range t.edgePuts {
		if t.store.edgeModRev[e.ID] > base {
			return e.ID.String(), true
		}
	}
	for _, id := range t.edgeDeletes {
		if t.store.edgeModRev[id] > base {
			return id.String(), true
		}
	}
	return "", false
}
