package graphstorage

import (
	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

func (s *LocalStore) validateEdgeIndexes(edge graph.Edge) error {
	for _, idx := range s.configuredIndexes[edge.DomainID] {
		if idx.TargetKind != schema.IndexTargetEdge {
			continue
		}
		if _, err := s.edgePropertyIndexEntry(edge, idx); err != nil {
			return err
		}
	}
	return nil
}

func (s *LocalStore) addEdgeAdjacencyIndexes(edge graph.Edge) {
	if edge.DomainID == graph.DomainID(uuid.Nil) {
		return
	}
	key := encodeAdjacencyKey(edge)
	for _, label := range edge.Labels {
		if label == "" {
			continue
		}
		ensureAdjacencySet(s.edgeAdjacencyOut, edge.DomainID, edge.FromID, label)[key] = edge.ID
		ensureAdjacencySet(s.edgeAdjacencyIn, edge.DomainID, edge.ToID, label)[key] = edge.ID
	}
}

func (s *LocalStore) removeEdgeAdjacencyIndexes(edge graph.Edge) {
	key := encodeAdjacencyKey(edge)
	for _, label := range edge.Labels {
		deleteAdjacencyKey(s.edgeAdjacencyOut, edge.DomainID, edge.FromID, label, key)
		deleteAdjacencyKey(s.edgeAdjacencyIn, edge.DomainID, edge.ToID, label, key)
	}
}

func (s *LocalStore) addEdgePropertyIndexEntry(edge graph.Edge, idx schema.IndexDefinition) error {
	entry, err := s.edgePropertyIndexEntry(edge, idx)
	if err != nil || entry == nil {
		return err
	}
	identity := indexIdentity(edge.DomainID, idx.Name)
	entries := s.edgePropertyIndex[identity]
	if entries == nil {
		entries = map[string]edgePropertyIndexEntry{}
		s.edgePropertyIndex[identity] = entries
	}
	entries[entry.Key] = *entry
	return nil
}

func (s *LocalStore) removeEdgePropertyIndexEntry(edge graph.Edge, idx schema.IndexDefinition) {
	entry, err := s.edgePropertyIndexEntry(edge, idx)
	if err != nil || entry == nil {
		return
	}
	identity := indexIdentity(edge.DomainID, idx.Name)
	delete(s.edgePropertyIndex[identity], entry.Key)
}

func (s *LocalStore) edgePropertyIndexEntry(edge graph.Edge, idx schema.IndexDefinition) (*edgePropertyIndexEntry, error) {
	if idx.TargetKind != schema.IndexTargetEdge {
		return nil, nil
	}
	if !edgeHasAnyLabel(edge, idx.Labels) {
		return nil, nil
	}
	value, ok := graph.EdgeProperty(edge, idx.Field.Name)
	if !ok || value == nil {
		if idx.Required {
			return nil, ErrUnsupported
		}
		return nil, nil
	}
	key, err := encodeOrderedEdgeKey(value, edge.ID)
	if err != nil {
		return nil, err
	}
	return &edgePropertyIndexEntry{EdgeID: edge.ID, Value: value, Key: key}, nil
}

func edgeHasAnyLabel(edge graph.Edge, labels []string) bool {
	if len(labels) == 0 {
		return false
	}
	seen := map[string]struct{}{}
	for _, label := range edge.Labels {
		seen[label] = struct{}{}
	}
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			return true
		}
	}
	return false
}

func ensureAdjacencySet(index map[graph.DomainID]map[graph.NodeID]map[string]map[string]graph.EdgeID, domainID graph.DomainID, nodeID graph.NodeID, label string) map[string]graph.EdgeID {
	byNode := index[domainID]
	if byNode == nil {
		byNode = map[graph.NodeID]map[string]map[string]graph.EdgeID{}
		index[domainID] = byNode
	}
	byLabel := byNode[nodeID]
	if byLabel == nil {
		byLabel = map[string]map[string]graph.EdgeID{}
		byNode[nodeID] = byLabel
	}
	set := byLabel[label]
	if set == nil {
		set = map[string]graph.EdgeID{}
		byLabel[label] = set
	}
	return set
}

func deleteAdjacencyKey(index map[graph.DomainID]map[graph.NodeID]map[string]map[string]graph.EdgeID, domainID graph.DomainID, nodeID graph.NodeID, label string, key string) {
	byNode := index[domainID]
	byLabel := byNode[nodeID]
	set := byLabel[label]
	delete(set, key)
	if len(set) == 0 {
		delete(byLabel, label)
	}
	if len(byLabel) == 0 {
		delete(byNode, nodeID)
	}
	if len(byNode) == 0 {
		delete(index, domainID)
	}
}
