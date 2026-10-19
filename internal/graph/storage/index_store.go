package graphstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/myceldb/mycel/internal/fsperm"

	"github.com/google/uuid"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

type indexManifestFile struct {
	FormatVersion int                   `json:"format_version"`
	Domains       []domainIndexManifest `json:"domains"`
}

type domainIndexManifest struct {
	DomainID   string                   `json:"domain_id"`
	SchemaHash string                   `json:"schema_hash"`
	Indexes    []schema.IndexDefinition `json:"indexes"`
}

type nodePropertyIndexEntry struct {
	NodeID graph.NodeID
	Value  any
	Key    string
}

type edgePropertyIndexEntry struct {
	EdgeID graph.EdgeID
	Value  any
	Key    string
}

func (s *LocalStore) ConfigureIndexes(ctx context.Context, domainID graph.DomainID, schemaHash string, indexes []schema.IndexDefinition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return err
	}
	indexes = normalizeConfiguredIndexes(indexes)
	if err := s.configureIndexesLocked(domainID, schemaHash, indexes); err != nil {
		return err
	}
	return s.writeIndexManifestLocked()
}

func (s *LocalStore) ScanNodePropertyOrdered(ctx context.Context, scan OrderedNodePropertyScan) ([]NodeIndexEntry, string, error) {
	entries, next, _, err := s.ScanNodePropertyOrderedBounded(ctx, scan)
	return entries, next, err
}

// ScanNodePropertyOrderedBounded walks the derived ordered index directly and
// reports every examined index entry. It never materializes or sorts the full
// index for a page request.
func (s *LocalStore) ScanNodePropertyOrderedBounded(ctx context.Context, scan OrderedNodePropertyScan) ([]NodeIndexEntry, string, int, error) {
	return s.scanNodePropertyOrdered(ctx, scan, nil)
}

// ScanNodePropertyOrderedAt walks every historical property key in index order
// and resolves the node version visible at revision. Obsolete historical keys
// consume ExamineLimit and advance the cursor without forcing whole-domain
// materialization.
func (s *LocalStore) ScanNodePropertyOrderedAt(ctx context.Context, scan OrderedNodePropertyScan, revision uint64) ([]NodeIndexEntry, string, int, error) {
	return s.scanNodePropertyOrdered(ctx, scan, &revision)
}

func (s *LocalStore) scanNodePropertyOrdered(ctx context.Context, scan OrderedNodePropertyScan, revision *uint64) ([]NodeIndexEntry, string, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", 0, err
	}
	if err := normalizeOrderedNodePropertyScan(&scan); err != nil {
		return nil, "", 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureReady(); err != nil {
		return nil, "", 0, err
	}
	identity := indexIdentity(scan.DomainID, scan.IndexName)
	meta, ok := s.indexMetadata[identity]
	if !ok || meta.BuildState != IndexBuildStateReady {
		return nil, "", 0, ErrIndexUnavailable
	}
	cursor, low, high, err := orderedNodeScanKeys(scan)
	if err != nil {
		return nil, "", 0, err
	}
	if revision == nil {
		order := s.nodePropertyOrder[identity]
		if order == nil {
			return []NodeIndexEntry{}, "", 0, nil
		}
		return scanOrderedNodePropertyTree(ctx, order, scan, cursor, low, high, func(key string, entry nodePropertyIndexEntry) (NodeIndexEntry, bool) {
			return NodeIndexEntry{NodeID: entry.NodeID, Value: s.nodePropertyIndexEntryValue(entry, meta), Cursor: encodeIndexCursor(key)}, true
		})
	}
	order := s.historyNodePropertyOrder[identity]
	if order == nil {
		return []NodeIndexEntry{}, "", 0, nil
	}
	return scanOrderedNodePropertyTree(ctx, order, scan, cursor, low, high, func(key string, nodeID graph.NodeID) (NodeIndexEntry, bool) {
		node, visible := nodeAtRevision(s.nodeHistory[nodeID], *revision)
		if !visible || node.DomainID != scan.DomainID || !nodeHasAnyLabel(node, meta.Labels) {
			return NodeIndexEntry{}, false
		}
		value, exists := graph.Property(node, meta.Field.Name)
		if !exists || value == nil {
			return NodeIndexEntry{}, false
		}
		visibleKey, encodeErr := encodeOrderedNodeKey(value, node.ID)
		if encodeErr != nil || visibleKey != key {
			return NodeIndexEntry{}, false
		}
		return NodeIndexEntry{NodeID: node.ID, Value: value, Cursor: encodeIndexCursor(key)}, true
	})
}

func scanOrderedNodePropertyTree[V any](ctx context.Context, order *orderedTree[string, V], scan OrderedNodePropertyScan, cursor, low, high string, resolve func(string, V) (NodeIndexEntry, bool)) ([]NodeIndexEntry, string, int, error) {
	iterator := orderedNodeIterator(order, scan, cursor, low, high)
	out := make([]NodeIndexEntry, 0, scan.Limit)
	examined := 0
	lastExamined := ""
	exhausted := false
	for iterator.HasNext() && examined < scan.ExamineLimit && len(out) < scan.Limit {
		if err := ctx.Err(); err != nil {
			return nil, "", examined, err
		}
		key, value, _ := iterator.Next()
		if !orderedNodeKeyInBounds(key, low, high, scan) {
			if orderedNodeKeyPastBounds(key, low, high, scan) {
				exhausted = true
				break
			}
			continue
		}
		examined++
		lastExamined = key
		if entry, ok := resolve(key, value); ok {
			out = append(out, entry)
		}
	}
	next := ""
	if !exhausted && iterator.HasNext() && lastExamined != "" {
		next = encodeIndexCursor(lastExamined)
	}
	return out, next, examined, nil
}

func normalizeOrderedNodePropertyScan(scan *OrderedNodePropertyScan) error {
	if scan.Limit <= 0 {
		return fmt.Errorf("%w: ordered node property scan limit must be positive", ErrInvalidRecord)
	}
	if scan.Limit > 500 {
		scan.Limit = 500
	}
	if scan.ExamineLimit <= 0 {
		scan.ExamineLimit = scan.Limit
	} else if scan.ExamineLimit > 500 {
		scan.ExamineLimit = 500
	}
	return nil
}

func orderedNodeScanKeys(scan OrderedNodePropertyScan) (string, string, string, error) {
	cursor, err := decodeIndexCursor(scan.Cursor)
	if err != nil {
		return "", "", "", err
	}
	low, high, err := encodedBounds(scan.HasLow, scan.Low, scan.HasHigh, scan.High)
	return cursor, low, high, err
}

func orderedNodeIterator[V any](order *orderedTree[string, V], scan OrderedNodePropertyScan, cursor, low, high string) orderedTreeIterator[string, V] {
	if scan.Direction == schema.IndexSortDirectionDesc {
		start := cursor
		if start == "" && high != "" {
			start = high
			if !scan.HighExclusive {
				start += "\x01"
			}
		}
		return order.IteratorBefore(start, start != "")
	}
	start := cursor
	if start == "" && low != "" {
		start = low
		if scan.LowExclusive {
			start += "\x01"
		}
	}
	return order.IteratorAfter(start, start != "")
}

func orderedNodeKeyInBounds(key, low, high string, scan OrderedNodePropertyScan) bool {
	valueKey := orderedValueKey(key)
	if low != "" && (valueKey < low || scan.LowExclusive && valueKey == low) {
		return false
	}
	if high != "" && (valueKey > high || scan.HighExclusive && valueKey == high) {
		return false
	}
	return true
}

func orderedNodeKeyPastBounds(key, low, high string, scan OrderedNodePropertyScan) bool {
	valueKey := orderedValueKey(key)
	if scan.Direction == schema.IndexSortDirectionDesc {
		return low != "" && (valueKey < low || scan.LowExclusive && valueKey == low)
	}
	return high != "" && (valueKey > high || scan.HighExclusive && valueKey == high)
}

func (s *LocalStore) nodePropertyIndexEntryValue(entry nodePropertyIndexEntry, meta IndexMetadata) any {
	if entry.Value != nil {
		return entry.Value
	}
	node, ok := s.nodeRecords[entry.NodeID]
	if !ok {
		return nil
	}
	value, _ := graph.Property(node, meta.Field.Name)
	return value
}

func (s *LocalStore) ScanEdgePropertyOrdered(ctx context.Context, scan OrderedEdgePropertyScan) ([]EdgeIndexEntry, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureReady(); err != nil {
		return nil, "", err
	}
	meta, ok := s.indexMetadata[indexIdentity(scan.DomainID, scan.IndexName)]
	if !ok || meta.BuildState != IndexBuildStateReady || meta.TargetKind != schema.IndexTargetEdge {
		return nil, "", ErrIndexUnavailable
	}
	entries := s.edgePropertyIndex[indexIdentity(scan.DomainID, scan.IndexName)]
	if entries == nil {
		return nil, "", nil
	}
	cursor, err := decodeIndexCursor(scan.Cursor)
	if err != nil {
		return nil, "", err
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		if cursor != "" {
			if scan.Direction == schema.IndexSortDirectionDesc {
				if key >= cursor {
					continue
				}
			} else if key <= cursor {
				continue
			}
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if scan.Direction == schema.IndexSortDirectionDesc {
		reverseStrings(keys)
	}
	limit := scan.Limit
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}
	out := make([]EdgeIndexEntry, 0, limit)
	for _, key := range keys[:limit] {
		entry := entries[key]
		out = append(out, EdgeIndexEntry{EdgeID: entry.EdgeID, Value: s.edgePropertyIndexEntryValue(entry, meta), Cursor: encodeIndexCursor(key)})
	}
	next := ""
	if limit < len(keys) && limit > 0 {
		next = encodeIndexCursor(keys[limit-1])
	}
	return out, next, nil
}

func (s *LocalStore) edgePropertyIndexEntryValue(entry edgePropertyIndexEntry, meta IndexMetadata) any {
	if entry.Value != nil {
		return entry.Value
	}
	edge, ok := s.edgeRecords[entry.EdgeID]
	if !ok {
		return nil
	}
	value, _ := graph.EdgeProperty(edge, meta.Field.Name)
	return value
}

func (s *LocalStore) ScanAdjacency(ctx context.Context, scan AdjacencyScan) ([]graph.EdgeID, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	cursor, err := decodeIndexCursor(scan.Cursor)
	if err != nil {
		return nil, "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureReady(); err != nil {
		return nil, "", err
	}
	identity := adjacencyOrderIdentity{DomainID: scan.DomainID, NodeID: scan.NodeID, Label: scan.Label, Direction: scan.Direction}
	order := s.currentAdjacencyOrder[identity]
	if order == nil {
		return []graph.EdgeID{}, "", nil
	}
	iterator := order.IteratorAfter(cursor, cursor != "")
	limit := scan.Limit
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	out := make([]graph.EdgeID, 0, min(limit, 500))
	last := ""
	for iterator.HasNext() && len(out) < limit {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		key, id, _ := iterator.Next()
		last = key
		out = append(out, id)
	}
	if iterator.HasNext() {
		return out, encodeIndexCursor(last), nil
	}
	return out, "", nil
}

// ScanAdjacencyAt makes bounded progress through the retained multiversion
// adjacency order and returns only entries visible at revision. The examined
// count includes historical keys skipped because a different edge version is
// visible, allowing query budgets to charge the actual index work.
func (s *LocalStore) ScanAdjacencyAt(ctx context.Context, scan AdjacencyScan, revision uint64, examineLimit int) ([]graph.EdgeID, string, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", 0, err
	}
	if scan.Limit <= 0 {
		return nil, "", 0, fmt.Errorf("%w: adjacency scan limit must be positive", ErrInvalidRecord)
	}
	if examineLimit <= 0 {
		examineLimit = scan.Limit
	}
	cursor, err := decodeIndexCursor(scan.Cursor)
	if err != nil {
		return nil, "", 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureReady(); err != nil {
		return nil, "", 0, err
	}
	identity := adjacencyOrderIdentity{DomainID: scan.DomainID, NodeID: scan.NodeID, Label: scan.Label, Direction: scan.Direction}
	order := s.historyAdjacencyOrder[identity]
	if order == nil {
		return []graph.EdgeID{}, "", 0, nil
	}
	iterator := order.IteratorAfter(cursor, cursor != "")
	out := make([]graph.EdgeID, 0, scan.Limit)
	examined := 0
	last := ""
	for iterator.HasNext() && examined < examineLimit && len(out) < scan.Limit {
		if err := ctx.Err(); err != nil {
			return nil, "", examined, err
		}
		key, id, _ := iterator.Next()
		examined++
		last = key
		edge, ok := entityAtRevision(s.edgeHistory[id], revision, func(edge graph.Edge) graph.Edge { return edge })
		if !ok || encodeAdjacencyKey(edge) != key || !edgeMatchesAdjacencyScan(edge, scan) {
			continue
		}
		out = append(out, id)
	}
	if iterator.HasNext() {
		return out, encodeIndexCursor(last), examined, nil
	}
	return out, "", examined, nil
}

func edgeMatchesAdjacencyScan(edge graph.Edge, scan AdjacencyScan) bool {
	if edge.DomainID != scan.DomainID || scan.Label != "" && !edgeHasAnyLabel(edge, []string{scan.Label}) {
		return false
	}
	if scan.Direction == AdjacencyDirectionIn {
		return edge.ToID == scan.NodeID
	}
	return edge.FromID == scan.NodeID
}

func (s *LocalStore) ScanLabel(ctx context.Context, scan LabelScan) ([]graph.NodeID, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureReady(); err != nil {
		return nil, "", err
	}
	return scanNodeIDSet(s.labelIndex[scan.DomainID][scan.Label], scan.Limit, scan.Cursor)
}

func (s *LocalStore) ScanTag(ctx context.Context, scan TagScan) ([]graph.NodeID, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureReady(); err != nil {
		return nil, "", err
	}
	return scanNodeIDSet(s.tagIndex[scan.DomainID][scan.Tag], scan.Limit, scan.Cursor)
}

func scanNodeIDSet(idsMap map[graph.NodeID]struct{}, limit int, cursor string) ([]graph.NodeID, string, error) {
	ids := make([]graph.NodeID, 0, len(idsMap))
	for id := range idsMap {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	if cursor != "" {
		cursorID, err := parseLabelCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		filtered := ids[:0]
		for _, id := range ids {
			if id.String() > cursorID.String() {
				filtered = append(filtered, id)
			}
		}
		ids = filtered
	}
	if limit <= 0 || limit > len(ids) {
		limit = len(ids)
	}
	out := append([]graph.NodeID(nil), ids[:limit]...)
	next := ""
	if limit < len(ids) && limit > 0 {
		next = encodeIndexCursor(labelCursorKey(ids[limit-1]))
	}
	return out, next, nil
}

func (s *LocalStore) IndexStatuses(ctx context.Context) ([]IndexMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]IndexMetadata, 0, len(s.indexMetadata))
	for _, meta := range s.indexMetadata {
		meta.Labels = append([]string(nil), meta.Labels...)
		out = append(out, meta)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DomainID == out[j].DomainID {
			return out[i].Name < out[j].Name
		}
		return out[i].DomainID.String() < out[j].DomainID.String()
	})
	return out, nil
}

func (s *LocalStore) configureIndexesLocked(domainID graph.DomainID, schemaHash string, indexes []schema.IndexDefinition) error {
	if s.configuredIndexes == nil {
		s.configuredIndexes = map[graph.DomainID][]schema.IndexDefinition{}
	}
	if s.indexMetadata == nil {
		s.indexMetadata = map[string]IndexMetadata{}
	}
	if s.nodePropertyIndex == nil {
		s.nodePropertyIndex = map[string]map[string]nodePropertyIndexEntry{}
	}
	if s.nodePropertyOrder == nil {
		s.nodePropertyOrder = map[string]*orderedTree[string, nodePropertyIndexEntry]{}
	}
	if s.historyNodePropertyOrder == nil {
		s.historyNodePropertyOrder = map[string]*orderedTree[string, graph.NodeID]{}
	}
	if s.edgePropertyIndex == nil {
		s.edgePropertyIndex = map[string]map[string]edgePropertyIndexEntry{}
	}
	old := s.configuredIndexes[domainID]
	if configuredIndexesMatch(old, indexes) && configuredSchemaHash(s.indexMetadata, domainID, old) == schemaHash {
		return nil
	}
	for _, idx := range old {
		delete(s.indexMetadata, indexIdentity(domainID, idx.Name))
		delete(s.nodePropertyIndex, indexIdentity(domainID, idx.Name))
		delete(s.nodePropertyOrder, indexIdentity(domainID, idx.Name))
		delete(s.historyNodePropertyOrder, indexIdentity(domainID, idx.Name))
		delete(s.edgePropertyIndex, indexIdentity(domainID, idx.Name))
	}
	s.configuredIndexes[domainID] = cloneIndexDefinitions(indexes)
	for _, idx := range indexes {
		identity := indexIdentity(domainID, idx.Name)
		s.indexMetadata[identity] = IndexMetadata{Name: idx.Name, DomainID: domainID, SchemaHash: schemaHash, TargetKind: idx.TargetKind, TargetType: idx.TargetType, Labels: append([]string(nil), idx.Labels...), Field: idx.Field, Kind: idx.Kind, Direction: idx.Direction, BuildState: IndexBuildStateBuilding, KeyEncodingVersion: indexKeyEncodingVersion}
		switch idx.TargetKind {
		case schema.IndexTargetNode:
			s.nodePropertyIndex[identity] = map[string]nodePropertyIndexEntry{}
		case schema.IndexTargetEdge:
			s.edgePropertyIndex[identity] = map[string]edgePropertyIndexEntry{}
		}
	}
	for _, node := range s.nodeRecords {
		if node.DomainID != domainID {
			continue
		}
		for _, idx := range indexes {
			if idx.TargetKind != schema.IndexTargetNode {
				continue
			}
			if err := s.addNodePropertyIndexEntry(node, idx); err != nil {
				meta := s.indexMetadata[indexIdentity(domainID, idx.Name)]
				meta.BuildState = IndexBuildStateFailed
				meta.Error = err.Error()
				s.indexMetadata[indexIdentity(domainID, idx.Name)] = meta
				return err
			}
		}
	}
	for _, edge := range s.edgeRecords {
		if edge.DomainID != domainID {
			continue
		}
		for _, idx := range indexes {
			if idx.TargetKind != schema.IndexTargetEdge {
				continue
			}
			if err := s.addEdgePropertyIndexEntry(edge, idx); err != nil {
				meta := s.indexMetadata[indexIdentity(domainID, idx.Name)]
				meta.BuildState = IndexBuildStateFailed
				meta.Error = err.Error()
				s.indexMetadata[indexIdentity(domainID, idx.Name)] = meta
				return err
			}
		}
	}
	s.rebuildNodePropertyOrdersLocked()
	for _, idx := range indexes {
		identity := indexIdentity(domainID, idx.Name)
		meta := s.indexMetadata[identity]
		meta.BuildState = IndexBuildStateReady
		meta.LastIndexedGraphRevision = s.revision
		s.indexMetadata[identity] = meta
	}
	return nil
}

func (s *LocalStore) loadIndexManifestLocked() error {
	path := filepath.Join(s.path, "indexes", "manifest.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var manifest indexManifestFile
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return err
	}
	for _, domain := range manifest.Domains {
		parsed, err := uuid.Parse(domain.DomainID)
		if err != nil {
			return err
		}
		id := graph.DomainID(parsed)
		indexes := normalizeConfiguredIndexes(domain.Indexes)
		s.configuredIndexes[id] = indexes
		for _, idx := range indexes {
			identity := indexIdentity(id, idx.Name)
			s.indexMetadata[identity] = IndexMetadata{Name: idx.Name, DomainID: id, SchemaHash: domain.SchemaHash, TargetKind: idx.TargetKind, TargetType: idx.TargetType, Labels: append([]string(nil), idx.Labels...), Field: idx.Field, Kind: idx.Kind, Direction: idx.Direction, BuildState: IndexBuildStateReady, KeyEncodingVersion: indexKeyEncodingVersion}
			switch idx.TargetKind {
			case schema.IndexTargetNode:
				s.nodePropertyIndex[identity] = map[string]nodePropertyIndexEntry{}
			case schema.IndexTargetEdge:
				s.edgePropertyIndex[identity] = map[string]edgePropertyIndexEntry{}
			}
		}
	}
	return nil
}

func (s *LocalStore) writeIndexManifestLocked() error {
	if err := os.MkdirAll(filepath.Join(s.path, "indexes"), fsperm.PrivateDir); err != nil {
		return err
	}
	manifest := indexManifestFile{FormatVersion: 1}
	domains := make([]graph.DomainID, 0, len(s.configuredIndexes))
	for domainID := range s.configuredIndexes {
		domains = append(domains, domainID)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].String() < domains[j].String() })
	for _, domainID := range domains {
		schemaHash := ""
		for _, idx := range s.configuredIndexes[domainID] {
			if meta, ok := s.indexMetadata[indexIdentity(domainID, idx.Name)]; ok {
				schemaHash = meta.SchemaHash
				break
			}
		}
		manifest.Domains = append(manifest.Domains, domainIndexManifest{DomainID: domainID.String(), SchemaHash: schemaHash, Indexes: cloneIndexDefinitions(s.configuredIndexes[domainID])})
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(s.path, "indexes", "manifest.json"), data, fsperm.PrivateFile)
}

func normalizeConfiguredIndexes(indexes []schema.IndexDefinition) []schema.IndexDefinition {
	out := cloneIndexDefinitions(indexes)
	for i := range out {
		if out[i].Kind == "" {
			out[i].Kind = schema.IndexKindEquality
		}
		if out[i].Kind == schema.IndexKindOrdered && out[i].Direction == "" {
			out[i].Direction = schema.IndexSortDirectionAsc
		}
	}
	return out
}

func configuredIndexesMatch(left, right []schema.IndexDefinition) bool {
	left = normalizeConfiguredIndexes(left)
	right = normalizeConfiguredIndexes(right)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].TargetKind != right[i].TargetKind || left[i].TargetType != right[i].TargetType || left[i].Field != right[i].Field || left[i].Kind != right[i].Kind || left[i].Direction != right[i].Direction || left[i].Unique != right[i].Unique || left[i].Required != right[i].Required {
			return false
		}
		if !stringSlicesEqual(left[i].Labels, right[i].Labels) {
			return false
		}
	}
	return true
}

func configuredSchemaHash(metadata map[string]IndexMetadata, domainID graph.DomainID, indexes []schema.IndexDefinition) string {
	for _, idx := range indexes {
		if meta, ok := metadata[indexIdentity(domainID, idx.Name)]; ok {
			return meta.SchemaHash
		}
	}
	return ""
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func cloneIndexDefinitions(indexes []schema.IndexDefinition) []schema.IndexDefinition {
	out := append([]schema.IndexDefinition(nil), indexes...)
	for i := range out {
		out[i].Labels = append([]string(nil), out[i].Labels...)
	}
	return out
}

func indexIdentity(domainID graph.DomainID, name string) string {
	return domainID.String() + "\x00" + name
}

func encodedBounds(hasLow bool, low any, hasHigh bool, high any) (string, string, error) {
	var lowKey, highKey string
	var err error
	if hasLow {
		lowKey, err = encodeSortableValue(low)
		if err != nil {
			return "", "", err
		}
	}
	if hasHigh {
		highKey, err = encodeSortableValue(high)
		if err != nil {
			return "", "", err
		}
	}
	return lowKey, highKey, nil
}

func orderedValueKey(key string) string {
	if idx := strings.LastIndex(key, "\x00"); idx >= 0 {
		return key[:idx]
	}
	return key
}

func (s *LocalStore) markReadyIndexesIndexedThrough(revision uint64, domains map[graph.DomainID]struct{}) {
	if len(domains) == 0 {
		return
	}
	for key, meta := range s.indexMetadata {
		if meta.BuildState != IndexBuildStateReady {
			continue
		}
		if _, ok := domains[meta.DomainID]; !ok {
			continue
		}
		meta.LastIndexedGraphRevision = revision
		s.indexMetadata[key] = meta
	}
}

func reverseStrings(values []string) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}
