package graphstorage

import (
	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

func (s *LocalStore) validateNodeIndexes(node graph.Node) error {
	for _, idx := range s.configuredIndexes[node.DomainID] {
		if _, err := s.nodePropertyIndexEntry(node, idx); err != nil {
			return err
		}
	}
	return nil
}

func (s *LocalStore) addNodeLabelIndexes(node graph.Node) {
	if node.DomainID == graph.DomainID(uuid.Nil) {
		return
	}
	byLabel := s.labelIndex[node.DomainID]
	if byLabel == nil {
		byLabel = map[string]map[graph.NodeID]struct{}{}
		s.labelIndex[node.DomainID] = byLabel
	}
	for _, label := range node.Labels {
		if label == "" {
			continue
		}
		set := byLabel[label]
		if set == nil {
			set = map[graph.NodeID]struct{}{}
			byLabel[label] = set
		}
		set[node.ID] = struct{}{}
	}
}

func (s *LocalStore) removeNodeLabelIndexes(node graph.Node) {
	byLabel := s.labelIndex[node.DomainID]
	for _, label := range node.Labels {
		set := byLabel[label]
		delete(set, node.ID)
		if len(set) == 0 {
			delete(byLabel, label)
		}
	}
	if len(byLabel) == 0 {
		delete(s.labelIndex, node.DomainID)
	}
}

func (s *LocalStore) addNodeTagIndexes(node graph.Node) {
	if node.DomainID == graph.DomainID(uuid.Nil) {
		return
	}
	byTag := s.tagIndex[node.DomainID]
	if byTag == nil {
		byTag = map[string]map[graph.NodeID]struct{}{}
		s.tagIndex[node.DomainID] = byTag
	}
	for _, tag := range tagsForStorageIndex(node) {
		set := byTag[tag]
		if set == nil {
			set = map[graph.NodeID]struct{}{}
			byTag[tag] = set
		}
		set[node.ID] = struct{}{}
	}
}

func (s *LocalStore) removeNodeTagIndexes(node graph.Node) {
	byTag := s.tagIndex[node.DomainID]
	for _, tag := range tagsForStorageIndex(node) {
		set := byTag[tag]
		delete(set, node.ID)
		if len(set) == 0 {
			delete(byTag, tag)
		}
	}
	if len(byTag) == 0 {
		delete(s.tagIndex, node.DomainID)
	}
}

func tagsForStorageIndex(node graph.Node) []string {
	value := any(nil)
	if node.Properties != nil {
		value = node.Properties[graph.NodePropTags]
	}
	if value == nil && node.Props != nil {
		value = node.Props[graph.NodePropTags]
	}
	tags, err := graph.NormalizeTagsValue(value)
	if err != nil {
		return nil
	}
	return tags
}

func (s *LocalStore) addNodePropertyIndexEntry(node graph.Node, idx schema.IndexDefinition) error {
	entry, err := s.nodePropertyIndexEntry(node, idx)
	if err != nil || entry == nil {
		return err
	}
	identity := indexIdentity(node.DomainID, idx.Name)
	entries := s.nodePropertyIndex[identity]
	if entries == nil {
		entries = map[string]nodePropertyIndexEntry{}
		s.nodePropertyIndex[identity] = entries
	}
	entries[entry.Key] = *entry
	s.ensureNodePropertyOrder(identity).Put(entry.Key, *entry)
	s.ensureHistoryNodePropertyOrder(identity).Put(entry.Key, node.ID)
	return nil
}

func (s *LocalStore) removeNodePropertyIndexEntry(node graph.Node, idx schema.IndexDefinition) {
	entry, err := s.nodePropertyIndexEntry(node, idx)
	if err != nil || entry == nil {
		return
	}
	identity := indexIdentity(node.DomainID, idx.Name)
	delete(s.nodePropertyIndex[identity], entry.Key)
	if order := s.nodePropertyOrder[identity]; order != nil {
		order.Delete(entry.Key)
	}
}

func newNodePropertyOrder[V any]() *orderedTree[string, V] {
	return newOrderedTree[string, V](
		func(left, right string) bool { return left < right },
		func(key string) uint64 { return hashOrderedBytes([]byte(key)) },
	)
}

func (s *LocalStore) ensureNodePropertyOrder(identity string) *orderedTree[string, nodePropertyIndexEntry] {
	order := s.nodePropertyOrder[identity]
	if order == nil {
		order = newNodePropertyOrder[nodePropertyIndexEntry]()
		s.nodePropertyOrder[identity] = order
	}
	return order
}

func (s *LocalStore) ensureHistoryNodePropertyOrder(identity string) *orderedTree[string, graph.NodeID] {
	order := s.historyNodePropertyOrder[identity]
	if order == nil {
		order = newNodePropertyOrder[graph.NodeID]()
		s.historyNodePropertyOrder[identity] = order
	}
	return order
}

func (s *LocalStore) rebuildNodePropertyOrdersLocked() {
	s.nodePropertyOrder = map[string]*orderedTree[string, nodePropertyIndexEntry]{}
	s.historyNodePropertyOrder = map[string]*orderedTree[string, graph.NodeID]{}
	for identity, entries := range s.nodePropertyIndex {
		order := s.ensureNodePropertyOrder(identity)
		for key, entry := range entries {
			order.Put(key, entry)
		}
	}
	for domainID, indexes := range s.configuredIndexes {
		for _, idx := range indexes {
			if idx.TargetKind != schema.IndexTargetNode {
				continue
			}
			identity := indexIdentity(domainID, idx.Name)
			order := s.ensureHistoryNodePropertyOrder(identity)
			for _, versions := range s.nodeHistory {
				for _, version := range versions {
					if version.Deleted || version.Entity.DomainID != domainID {
						continue
					}
					entry, err := s.nodePropertyIndexEntry(version.Entity, idx)
					if err == nil && entry != nil {
						order.Put(entry.Key, entry.NodeID)
					}
				}
			}
		}
	}
}

func (s *LocalStore) nodePropertyIndexEntry(node graph.Node, idx schema.IndexDefinition) (*nodePropertyIndexEntry, error) {
	if idx.TargetKind != schema.IndexTargetNode {
		return nil, nil
	}
	if !nodeHasAnyLabel(node, idx.Labels) {
		return nil, nil
	}
	value, ok := graph.Property(node, idx.Field.Name)
	if !ok || value == nil {
		if idx.Required {
			return nil, ErrUnsupported
		}
		return nil, nil
	}
	key, err := encodeOrderedNodeKey(value, node.ID)
	if err != nil {
		return nil, err
	}
	return &nodePropertyIndexEntry{NodeID: node.ID, Value: value, Key: key}, nil
}

func nodeHasAnyLabel(node graph.Node, labels []string) bool {
	if len(labels) == 0 {
		return false
	}
	seen := map[string]struct{}{}
	for _, label := range node.Labels {
		seen[label] = struct{}{}
	}
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			return true
		}
	}
	return false
}
