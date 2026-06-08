package graphstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
)

type manifest struct {
	FormatVersion     int      `json:"format_version"`
	NodeSegments      []string `json:"node_segments"`
	EdgeSegments      []string `json:"edge_segments"`
	TxnSegments       []string `json:"txn_segments"`
	ActiveNodeSegment string   `json:"active_node_segment"`
	ActiveEdgeSegment string   `json:"active_edge_segment"`
	ActiveTxnSegment  string   `json:"active_txn_segment"`
}

type LocalStore struct {
	mu               sync.RWMutex
	path             string
	state            StoreState
	manifest         manifest
	nodes            *segment
	edges            *segment
	txns             *segment
	nodeRecords      map[graph.NodeID]graph.Node
	edgeRecords      map[graph.EdgeID]graph.Edge
	nodeMeta         map[graph.NodeID]NodeMeta
	edgeMeta         map[graph.EdgeID]EdgeMeta
	nodesByTemplate  map[graph.TemplateID]map[graph.NodeID]struct{}
	containsChildren map[graph.NodeID][]graph.EdgeID
	containsParent   map[graph.NodeID]graph.EdgeID
	journalDay       map[int]map[graph.NodeID]struct{}
}

func Open(ctx context.Context, spacePath string) (*LocalStore, error) {
	s := &LocalStore{path: spacePath, state: StoreStateOpening}
	if err := s.open(ctx); err != nil {
		s.state = StoreStateError
		return nil, err
	}
	return s, nil
}
func (s *LocalStore) State() StoreState { s.mu.RLock(); defer s.mu.RUnlock(); return s.state }

func (s *LocalStore) open(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Join(s.path, "segments"), 0o700); err != nil {
		return err
	}
	m, err := s.loadManifest()
	if err != nil {
		return err
	}
	s.manifest = m
	s.txns, err = openSegment(filepath.Join(s.path, m.ActiveTxnSegment), SegmentKindTxn)
	if err != nil {
		return err
	}
	s.nodes, err = openSegment(filepath.Join(s.path, m.ActiveNodeSegment), SegmentKindNode)
	if err != nil {
		return err
	}
	s.edges, err = openSegment(filepath.Join(s.path, m.ActiveEdgeSegment), SegmentKindEdge)
	if err != nil {
		return err
	}
	s.state = StoreStateRebuildingIndex
	if err := s.rebuildIndexes(ctx); err != nil {
		return err
	}
	s.state = StoreStateReady
	return nil
}

func (s *LocalStore) loadManifest() (manifest, error) {
	path := filepath.Join(s.path, "manifest.knot")
	raw, err := os.ReadFile(path)
	if err == nil {
		var m manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return manifest{}, err
		}
		return m, nil
	}
	if !os.IsNotExist(err) {
		return manifest{}, err
	}
	m := manifest{FormatVersion: 1, NodeSegments: []string{"segments/nodes-000001.kseg"}, EdgeSegments: []string{"segments/edges-000001.kseg"}, TxnSegments: []string{"segments/txns-000001.kseg"}, ActiveNodeSegment: "segments/nodes-000001.kseg", ActiveEdgeSegment: "segments/edges-000001.kseg", ActiveTxnSegment: "segments/txns-000001.kseg"}
	b, _ := json.MarshalIndent(m, "", "  ")
	b = append(b, '\n')
	return m, os.WriteFile(path, b, 0o600)
}

func (s *LocalStore) rebuildIndexes(ctx context.Context) error {
	s.resetIndexes()
	committed := map[uuid.UUID]struct{}{}
	for _, seg := range s.manifest.TxnSegments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := scanSegment(filepath.Join(s.path, seg), SegmentKindTxn, func(r scannedRecord) error {
			if r.header.kind == RecordKindTxnCommit {
				committed[r.header.txnID] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, seg := range s.manifest.NodeSegments {
		if err := scanSegment(filepath.Join(s.path, seg), SegmentKindNode, func(r scannedRecord) error {
			if _, ok := committed[r.header.txnID]; !ok {
				return nil
			}
			switch r.header.kind {
			case RecordKindNodePut:
				n, err := decodeNode(r.payload)
				if err != nil {
					return err
				}
				s.applyNodePut(n, r.location)
			case RecordKindNodeTombstone:
				s.applyNodeDelete(graph.NodeID(r.header.entityID), r.location)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, seg := range s.manifest.EdgeSegments {
		if err := scanSegment(filepath.Join(s.path, seg), SegmentKindEdge, func(r scannedRecord) error {
			if _, ok := committed[r.header.txnID]; !ok {
				return nil
			}
			switch r.header.kind {
			case RecordKindEdgePut:
				e, err := decodeEdge(r.payload)
				if err != nil {
					return err
				}
				s.applyEdgePut(e, r.location)
			case RecordKindEdgeTombstone:
				s.applyEdgeDelete(graph.EdgeID(r.header.entityID), r.location)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *LocalStore) resetIndexes() {
	s.nodeRecords = map[graph.NodeID]graph.Node{}
	s.edgeRecords = map[graph.EdgeID]graph.Edge{}
	s.nodeMeta = map[graph.NodeID]NodeMeta{}
	s.edgeMeta = map[graph.EdgeID]EdgeMeta{}
	s.nodesByTemplate = map[graph.TemplateID]map[graph.NodeID]struct{}{}
	s.containsChildren = map[graph.NodeID][]graph.EdgeID{}
	s.containsParent = map[graph.NodeID]graph.EdgeID{}
	s.journalDay = map[int]map[graph.NodeID]struct{}{}
}
func (s *LocalStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = StoreStateClosed
	var err error
	if s.nodes != nil {
		err = s.nodes.close()
	}
	if s.edges != nil {
		if e := s.edges.close(); err == nil {
			err = e
		}
	}
	if s.txns != nil {
		if e := s.txns.close(); err == nil {
			err = e
		}
	}
	return err
}
func (s *LocalStore) ensureReady() error {
	if s.state == StoreStateClosed {
		return ErrClosed
	}
	if s.state != StoreStateReady {
		return fmt.Errorf("graph storage not ready: %s", s.state)
	}
	return nil
}

func (s *LocalStore) Begin(ctx context.Context) (Txn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	return &localTxn{store: s, id: id}, nil
}
func (s *LocalStore) GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodeRecords[id]
	if !ok {
		return graph.Node{}, ErrNotFound
	}
	return cloneNode(n), ctx.Err()
}
func (s *LocalStore) ListNodes(ctx context.Context) ([]graph.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]graph.Node, 0, len(s.nodeRecords))
	for _, n := range s.nodeRecords {
		out = append(out, cloneNode(n))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, ctx.Err()
}
func (s *LocalStore) GetEdge(ctx context.Context, id graph.EdgeID) (graph.Edge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.edgeRecords[id]
	if !ok {
		return graph.Edge{}, ErrNotFound
	}
	return cloneEdge(e), ctx.Err()
}
func (s *LocalStore) ListEdges(ctx context.Context) ([]graph.Edge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]graph.Edge, 0, len(s.edgeRecords))
	for _, e := range s.edgeRecords {
		out = append(out, cloneEdge(e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, ctx.Err()
}
func (s *LocalStore) Children(ctx context.Context, parentID graph.NodeID) ([]graph.Edge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.containsChildren[parentID]
	out := make([]graph.Edge, 0, len(ids))
	for _, id := range ids {
		if e, ok := s.edgeRecords[id]; ok {
			out = append(out, cloneEdge(e))
		}
	}
	return out, ctx.Err()
}
func (s *LocalStore) Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.containsParent[childID]
	if !ok {
		return nil, ctx.Err()
	}
	e := cloneEdge(s.edgeRecords[id])
	return &e, ctx.Err()
}
func (s *LocalStore) NodesByTemplate(ctx context.Context, tid graph.TemplateID) ([]graph.NodeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.nodesByTemplate[tid]
	out := make([]graph.NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out, ctx.Err()
}
func (s *LocalStore) JournalNodesByDayRange(ctx context.Context, from, to int) ([]graph.NodeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []graph.NodeID{}
	for day, set := range s.journalDay {
		if day >= from && day <= to {
			for id := range set {
				out = append(out, id)
			}
		}
	}
	return out, ctx.Err()
}

func (s *LocalStore) applyNodePut(n graph.Node, loc RecordLocation) {
	if old, ok := s.nodeRecords[n.ID]; ok {
		s.removeNodeIndexes(old)
	}
	s.nodeRecords[n.ID] = cloneNode(n)
	s.nodeMeta[n.ID] = NodeMeta{ID: n.ID, TemplateID: n.TemplateID, Location: loc}
	if n.TemplateID != nil {
		ensureNodeSet(s.nodesByTemplate, *n.TemplateID)[n.ID] = struct{}{}
	}
	if day, ok := numberPropInt(n.Props["journal_day"]); ok {
		ensureNodeSet(s.journalDay, day)[n.ID] = struct{}{}
	}
}
func (s *LocalStore) applyNodeDelete(id graph.NodeID, loc RecordLocation) {
	if old, ok := s.nodeRecords[id]; ok {
		s.removeNodeIndexes(old)
	}
	delete(s.nodeRecords, id)
	s.nodeMeta[id] = NodeMeta{ID: id, Deleted: true, Location: loc}
}
func (s *LocalStore) removeNodeIndexes(n graph.Node) {
	if n.TemplateID != nil {
		delete(s.nodesByTemplate[*n.TemplateID], n.ID)
	}
	if day, ok := numberPropInt(n.Props["journal_day"]); ok {
		delete(s.journalDay[day], n.ID)
	}
}
func (s *LocalStore) applyEdgePut(e graph.Edge, loc RecordLocation) {
	if old, ok := s.edgeRecords[e.ID]; ok {
		s.removeEdgeIndexes(old)
	}
	s.edgeRecords[e.ID] = cloneEdge(e)
	s.edgeMeta[e.ID] = EdgeMeta{ID: e.ID, FromID: e.FromID, ToID: e.ToID, Kind: e.Kind, Location: loc}
	if e.Kind == graph.EdgeKindContains {
		s.containsChildren[e.FromID] = append(s.containsChildren[e.FromID], e.ID)
		s.containsParent[e.ToID] = e.ID
		s.sortChildren(e.FromID)
	}
}
func (s *LocalStore) applyEdgeDelete(id graph.EdgeID, loc RecordLocation) {
	if old, ok := s.edgeRecords[id]; ok {
		s.removeEdgeIndexes(old)
	}
	delete(s.edgeRecords, id)
	s.edgeMeta[id] = EdgeMeta{ID: id, Deleted: true, Location: loc}
}
func (s *LocalStore) removeEdgeIndexes(e graph.Edge) {
	if e.Kind == graph.EdgeKindContains {
		ids := s.containsChildren[e.FromID]
		out := ids[:0]
		for _, id := range ids {
			if id != e.ID {
				out = append(out, id)
			}
		}
		if len(out) == 0 {
			delete(s.containsChildren, e.FromID)
		} else {
			s.containsChildren[e.FromID] = out
		}
		if s.containsParent[e.ToID] == e.ID {
			delete(s.containsParent, e.ToID)
		}
	}
}
func (s *LocalStore) sortChildren(parent graph.NodeID) {
	sort.SliceStable(s.containsChildren[parent], func(i, j int) bool {
		ei := s.edgeRecords[s.containsChildren[parent][i]]
		ej := s.edgeRecords[s.containsChildren[parent][j]]
		oi, iok := numberPropInt(ei.Props["order"])
		oj, jok := numberPropInt(ej.Props["order"])
		if iok && jok && oi != oj {
			return oi < oj
		}
		if iok != jok {
			return iok
		}
		return ei.ID.String() < ej.ID.String()
	})
}

func ensureNodeSet[K comparable](m map[K]map[graph.NodeID]struct{}, key K) map[graph.NodeID]struct{} {
	set := m[key]
	if set == nil {
		set = map[graph.NodeID]struct{}{}
		m[key] = set
	}
	return set
}
func numberPropInt(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int8:
		return int(x), true
	case int16:
		return int(x), true
	case int32:
		return int(x), true
	case int64:
		return int(x), true
	case uint:
		return int(x), true
	case uint8:
		return int(x), true
	case uint16:
		return int(x), true
	case uint32:
		return int(x), true
	case uint64:
		return int(x), true
	case float32:
		return int(x), true
	case float64:
		return int(x), true
	default:
		return 0, false
	}
}
func cloneNode(n graph.Node) graph.Node { n.Props = cloneProps(n.Props); return n }
func cloneEdge(e graph.Edge) graph.Edge { e.Props = cloneProps(e.Props); return e }
func cloneProps(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
