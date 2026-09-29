package graphstorage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

const (
	persistentIndexQueryMetadataPayload = "query-index-metadata.kidx"
	persistentIndexQueryNodePayload     = "query-node-property.kidx"
	persistentIndexQueryEdgePayload     = "query-edge-property.kidx"

	persistentIndexKindQueryMetadata = "query_metadata"
	persistentIndexKindQueryNode     = "query_node_property"
	persistentIndexKindQueryEdge     = "query_edge_property"
)

type persistentQueryIndexMetadataRecord struct {
	Identity                 string
	Name                     string
	DomainID                 graph.DomainID
	SchemaHash               string
	DefinitionFingerprint    string
	TargetKind               schema.IndexTargetKind
	TargetType               string
	Labels                   []string
	Field                    schema.FieldPath
	Kind                     schema.IndexKind
	Direction                schema.IndexSortDirection
	BuildState               IndexBuildState
	LastIndexedGraphRevision uint64
	KeyEncodingVersion       int
	Error                    string
	EntryCount               uint64
}

type persistentQueryIndexEntry struct {
	Identity string
	Key      string
}

func (s *LocalStore) exportPersistentQueryIndexes(revision uint64) ([]persistentQueryIndexMetadataRecord, []persistentQueryIndexEntry, []persistentQueryIndexEntry, error) {
	metadata := make([]persistentQueryIndexMetadataRecord, 0, len(s.indexMetadata))
	nodeEntries := []persistentQueryIndexEntry{}
	edgeEntries := []persistentQueryIndexEntry{}
	identities := make([]string, 0, len(s.indexMetadata))
	for identity, meta := range s.indexMetadata {
		if meta.BuildState != IndexBuildStateReady || meta.LastIndexedGraphRevision < revision || meta.KeyEncodingVersion != indexKeyEncodingVersion {
			continue
		}
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	for _, identity := range identities {
		meta := s.indexMetadata[identity]
		fingerprint, err := persistentQueryIndexDefinitionFingerprint(meta)
		if err != nil {
			return nil, nil, nil, err
		}
		count := uint64(0)
		switch meta.TargetKind {
		case schema.IndexTargetNode:
			entries := s.nodePropertyIndex[identity]
			keys := make([]string, 0, len(entries))
			for key := range entries {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			count = uint64(len(keys))
			for _, key := range keys {
				nodeEntries = append(nodeEntries, persistentQueryIndexEntry{Identity: identity, Key: key})
			}
		case schema.IndexTargetEdge:
			entries := s.edgePropertyIndex[identity]
			keys := make([]string, 0, len(entries))
			for key := range entries {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			count = uint64(len(keys))
			for _, key := range keys {
				edgeEntries = append(edgeEntries, persistentQueryIndexEntry{Identity: identity, Key: key})
			}
		default:
			continue
		}
		metadata = append(metadata, persistentQueryIndexMetadataRecord{Identity: identity, Name: meta.Name, DomainID: meta.DomainID, SchemaHash: meta.SchemaHash, DefinitionFingerprint: fingerprint, TargetKind: meta.TargetKind, TargetType: meta.TargetType, Labels: append([]string(nil), meta.Labels...), Field: meta.Field, Kind: meta.Kind, Direction: meta.Direction, BuildState: meta.BuildState, LastIndexedGraphRevision: meta.LastIndexedGraphRevision, KeyEncodingVersion: meta.KeyEncodingVersion, Error: meta.Error, EntryCount: count})
	}
	return metadata, nodeEntries, edgeEntries, nil
}

func marshalPersistentQueryMetadataPayload(records []persistentQueryIndexMetadataRecord) ([]byte, int, error) {
	var buf bytes.Buffer
	if err := writePersistentBinaryHeader(&buf, persistentIndexKindQueryMetadata, len(records)); err != nil {
		return nil, 0, err
	}
	for _, record := range records {
		if err := writePersistentBinaryString(&buf, record.Identity); err != nil {
			return nil, 0, err
		}
		if err := writePersistentBinaryString(&buf, record.Name); err != nil {
			return nil, 0, err
		}
		domainUUID := uuid.UUID(record.DomainID)
		buf.Write(domainUUID[:])
		for _, value := range []string{record.SchemaHash, record.DefinitionFingerprint, string(record.TargetKind), record.TargetType, record.Field.Namespace, record.Field.Name, string(record.Kind), string(record.Direction), string(record.BuildState), record.Error} {
			if err := writePersistentBinaryString(&buf, value); err != nil {
				return nil, 0, err
			}
		}
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(record.Labels))); err != nil {
			return nil, 0, err
		}
		for _, label := range record.Labels {
			if err := writePersistentBinaryString(&buf, label); err != nil {
				return nil, 0, err
			}
		}
		if err := binary.Write(&buf, binary.BigEndian, record.LastIndexedGraphRevision); err != nil {
			return nil, 0, err
		}
		if err := binary.Write(&buf, binary.BigEndian, int32(record.KeyEncodingVersion)); err != nil {
			return nil, 0, err
		}
		if err := binary.Write(&buf, binary.BigEndian, record.EntryCount); err != nil {
			return nil, 0, err
		}
	}
	return buf.Bytes(), len(records), nil
}

func marshalPersistentQueryEntriesPayload(kind string, entries []persistentQueryIndexEntry) ([]byte, int, error) {
	groups := persistentQueryEntryGroups(entries)
	var buf bytes.Buffer
	if err := writePersistentBinaryHeader(&buf, kind, len(groups)); err != nil {
		return nil, 0, err
	}
	for _, group := range groups {
		if err := writePersistentBinaryString(&buf, group.identity); err != nil {
			return nil, 0, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint64(len(group.keys))); err != nil {
			return nil, 0, err
		}
		for _, key := range group.keys {
			if err := writePersistentBinaryString(&buf, key); err != nil {
				return nil, 0, err
			}
		}
	}
	return buf.Bytes(), len(groups), nil
}

type persistentQueryEntryGroup struct {
	identity string
	keys     []string
}

func persistentQueryEntryGroups(entries []persistentQueryIndexEntry) []persistentQueryEntryGroup {
	groups := []persistentQueryEntryGroup{}
	for _, entry := range entries {
		if len(groups) == 0 || groups[len(groups)-1].identity != entry.Identity {
			groups = append(groups, persistentQueryEntryGroup{identity: entry.Identity})
		}
		groups[len(groups)-1].keys = append(groups[len(groups)-1].keys, entry.Key)
	}
	return groups
}

func (s *LocalStore) loadPersistentQueryIndexes(dir string, manifest persistentIndexManifest, checkpoint CheckpointManifest) ([]PersistentQueryIndexStatus, error) {
	metadataEntry, ok := manifest.Indexes[persistentIndexKindQueryMetadata]
	if !ok {
		return nil, nil
	}
	metadataRaw, err := readPersistentIndexPayloadFile(dir, metadataEntry)
	if err != nil {
		return nil, err
	}
	metadata, err := s.readPersistentQueryMetadata(metadataRaw, metadataEntry.EntryCount, checkpoint.GraphRevision)
	if err != nil {
		return nil, err
	}
	nodeIndex := make(map[string]map[string]nodePropertyIndexEntry, len(metadata))
	edgeIndex := make(map[string]map[string]edgePropertyIndexEntry, len(metadata))
	for identity, record := range metadata {
		switch record.TargetKind {
		case schema.IndexTargetNode:
			nodeIndex[identity] = make(map[string]nodePropertyIndexEntry, persistentQueryEntryMapCapacity(record.EntryCount))
		case schema.IndexTargetEdge:
			edgeIndex[identity] = make(map[string]edgePropertyIndexEntry, persistentQueryEntryMapCapacity(record.EntryCount))
		}
	}
	if entry, ok := manifest.Indexes[persistentIndexKindQueryNode]; ok {
		raw, err := readPersistentIndexPayloadFile(dir, entry)
		if err != nil {
			return nil, err
		}
		if err := s.readPersistentQueryNodeEntries(raw, entry.EntryCount, metadata, nodeIndex); err != nil {
			return nil, err
		}
	}
	if entry, ok := manifest.Indexes[persistentIndexKindQueryEdge]; ok {
		raw, err := readPersistentIndexPayloadFile(dir, entry)
		if err != nil {
			return nil, err
		}
		if err := s.readPersistentQueryEdgeEntries(raw, entry.EntryCount, metadata, edgeIndex); err != nil {
			return nil, err
		}
	}
	for identity, record := range metadata {
		if record.TargetKind == schema.IndexTargetNode && uint64(len(nodeIndex[identity])) != record.EntryCount {
			return nil, fmt.Errorf("%w: persistent query node entry count mismatch", ErrInvalidRecord)
		}
		if record.TargetKind == schema.IndexTargetEdge && uint64(len(edgeIndex[identity])) != record.EntryCount {
			return nil, fmt.Errorf("%w: persistent query edge entry count mismatch", ErrInvalidRecord)
		}
	}
	for identity, record := range metadata {
		meta := s.indexMetadata[identity]
		meta.SchemaHash = record.SchemaHash
		meta.TargetKind = record.TargetKind
		meta.TargetType = record.TargetType
		meta.Labels = append([]string(nil), record.Labels...)
		meta.Field = record.Field
		meta.Kind = record.Kind
		meta.Direction = record.Direction
		meta.BuildState = IndexBuildStateReady
		meta.LastIndexedGraphRevision = checkpoint.GraphRevision
		meta.KeyEncodingVersion = record.KeyEncodingVersion
		meta.Error = ""
		s.indexMetadata[identity] = meta
		if record.TargetKind == schema.IndexTargetNode {
			s.nodePropertyIndex[identity] = nodeIndex[identity]
		} else if record.TargetKind == schema.IndexTargetEdge {
			s.edgePropertyIndex[identity] = edgeIndex[identity]
		}
	}
	return persistentQueryIndexStatusesFromMetadata(metadata, PersistentIndexLoadUsed), nil
}

func (s *LocalStore) persistentQueryIndexStatusesFromManifest(dir string, manifest persistentIndexManifest, checkpoint CheckpointManifest, loadResult string) ([]PersistentQueryIndexStatus, error) {
	metadataEntry, ok := manifest.Indexes[persistentIndexKindQueryMetadata]
	if !ok {
		return nil, nil
	}
	metadataRaw, err := readPersistentIndexPayloadFile(dir, metadataEntry)
	if err != nil {
		return nil, err
	}
	metadata, err := s.readPersistentQueryMetadata(metadataRaw, metadataEntry.EntryCount, checkpoint.GraphRevision)
	if err != nil {
		return nil, err
	}
	return persistentQueryIndexStatusesFromMetadata(metadata, loadResult), nil
}

func persistentQueryIndexStatusesFromMetadata(metadata map[string]persistentQueryIndexMetadataRecord, loadResult string) []PersistentQueryIndexStatus {
	identities := make([]string, 0, len(metadata))
	for identity := range metadata {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	out := make([]PersistentQueryIndexStatus, 0, len(identities))
	for _, identity := range identities {
		record := metadata[identity]
		out = append(out, PersistentQueryIndexStatus{Identity: record.Identity, Name: record.Name, DomainID: record.DomainID.String(), SchemaHash: record.SchemaHash, DefinitionFingerprint: record.DefinitionFingerprint, TargetKind: string(record.TargetKind), TargetType: record.TargetType, Labels: append([]string(nil), record.Labels...), FieldNamespace: record.Field.Namespace, FieldName: record.Field.Name, IndexKind: string(record.Kind), Direction: string(record.Direction), BuildState: string(record.BuildState), LastIndexedGraphRevision: record.LastIndexedGraphRevision, KeyEncodingVersion: record.KeyEncodingVersion, EntryCount: record.EntryCount, LoadResult: loadResult})
	}
	return out
}

func persistentQueryEntryMapCapacity(count uint64) int {
	maxInt := int(^uint(0) >> 1)
	if count > uint64(maxInt) {
		return maxInt
	}
	return int(count)
}

func readPersistentIndexPayloadFile(dir string, entry persistentIndexManifestEntry) ([]byte, error) {
	raw, err := os.ReadFile(filepath.Join(dir, entry.Path))
	if err != nil {
		return nil, err
	}
	if persistentIndexChecksum(raw) != entry.Checksum {
		return nil, fmt.Errorf("%w: persistent index checksum mismatch", ErrInvalidRecord)
	}
	return raw, nil
}

func (s *LocalStore) readPersistentQueryMetadata(raw []byte, entryCount int, checkpointRevision uint64) (map[string]persistentQueryIndexMetadataRecord, error) {
	r := bytes.NewReader(raw)
	if err := readPersistentBinaryHeader(r, persistentIndexKindQueryMetadata, entryCount); err != nil {
		return nil, err
	}
	out := make(map[string]persistentQueryIndexMetadataRecord, entryCount)
	for i := 0; i < entryCount; i++ {
		identity, err := readPersistentBinaryString(r)
		if err != nil {
			return nil, err
		}
		name, err := readPersistentBinaryString(r)
		if err != nil {
			return nil, err
		}
		domainID, err := readPersistentBinaryUUID(r)
		if err != nil {
			return nil, err
		}
		strings := make([]string, 10)
		for i := range strings {
			strings[i], err = readPersistentBinaryString(r)
			if err != nil {
				return nil, err
			}
		}
		var labelCount uint32
		if err := binary.Read(r, binary.BigEndian, &labelCount); err != nil {
			return nil, err
		}
		if labelCount > 1024 {
			return nil, fmt.Errorf("%w: too many persistent query index labels", ErrInvalidRecord)
		}
		labels := make([]string, 0, labelCount)
		for j := uint32(0); j < labelCount; j++ {
			label, err := readPersistentBinaryString(r)
			if err != nil {
				return nil, err
			}
			labels = append(labels, label)
		}
		var lastRevision uint64
		if err := binary.Read(r, binary.BigEndian, &lastRevision); err != nil {
			return nil, err
		}
		var keyVersion int32
		if err := binary.Read(r, binary.BigEndian, &keyVersion); err != nil {
			return nil, err
		}
		var recordEntryCount uint64
		if err := binary.Read(r, binary.BigEndian, &recordEntryCount); err != nil {
			return nil, err
		}
		record := persistentQueryIndexMetadataRecord{Identity: identity, Name: name, DomainID: graph.DomainID(domainID), SchemaHash: strings[0], DefinitionFingerprint: strings[1], TargetKind: schema.IndexTargetKind(strings[2]), TargetType: strings[3], Labels: labels, Field: schema.FieldPath{Namespace: strings[4], Name: strings[5]}, Kind: schema.IndexKind(strings[6]), Direction: schema.IndexSortDirection(strings[7]), BuildState: IndexBuildState(strings[8]), Error: strings[9], LastIndexedGraphRevision: lastRevision, KeyEncodingVersion: int(keyVersion), EntryCount: recordEntryCount}
		if err := s.validatePersistentQueryMetadataRecord(record, checkpointRevision); err != nil {
			return nil, err
		}
		out[identity] = record
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing persistent query metadata bytes", ErrInvalidRecord)
	}
	return out, nil
}

func (s *LocalStore) validatePersistentQueryMetadataRecord(record persistentQueryIndexMetadataRecord, checkpointRevision uint64) error {
	if record.Identity == "" || record.Name == "" || record.DomainID == uuid.Nil || record.BuildState != IndexBuildStateReady || record.LastIndexedGraphRevision < checkpointRevision || record.KeyEncodingVersion != indexKeyEncodingVersion {
		return fmt.Errorf("%w: invalid persistent query index metadata", ErrInvalidRecord)
	}
	configured := s.configuredIndexes[record.DomainID]
	var matched *schema.IndexDefinition
	for i := range configured {
		if configured[i].Name == record.Name {
			matched = &configured[i]
			break
		}
	}
	if matched == nil || indexIdentity(record.DomainID, record.Name) != record.Identity {
		return fmt.Errorf("%w: persistent query index definition missing", ErrInvalidRecord)
	}
	meta, ok := s.indexMetadata[record.Identity]
	if !ok || meta.SchemaHash != record.SchemaHash {
		return fmt.Errorf("%w: persistent query index schema mismatch", ErrInvalidRecord)
	}
	wantMeta := IndexMetadata{Name: matched.Name, DomainID: record.DomainID, SchemaHash: record.SchemaHash, TargetKind: matched.TargetKind, TargetType: matched.TargetType, Labels: append([]string(nil), matched.Labels...), Field: matched.Field, Kind: matched.Kind, Direction: matched.Direction, BuildState: IndexBuildStateReady, LastIndexedGraphRevision: checkpointRevision, KeyEncodingVersion: indexKeyEncodingVersion}
	fingerprint, err := persistentQueryIndexDefinitionFingerprint(wantMeta)
	if err != nil {
		return err
	}
	if fingerprint != record.DefinitionFingerprint || record.TargetKind != matched.TargetKind || record.Field != matched.Field || record.Kind != matched.Kind || record.Direction != matched.Direction {
		return fmt.Errorf("%w: persistent query index definition mismatch", ErrInvalidRecord)
	}
	return nil
}

func (s *LocalStore) readPersistentQueryNodeEntries(raw []byte, groupCount int, metadata map[string]persistentQueryIndexMetadataRecord, out map[string]map[string]nodePropertyIndexEntry) error {
	r := bytes.NewReader(raw)
	if err := readPersistentBinaryHeader(r, persistentIndexKindQueryNode, groupCount); err != nil {
		return err
	}
	for i := 0; i < groupCount; i++ {
		identity, err := readPersistentBinaryString(r)
		if err != nil {
			return err
		}
		record, ok := metadata[identity]
		if !ok || record.TargetKind != schema.IndexTargetNode {
			return fmt.Errorf("%w: invalid persistent query node group", ErrInvalidRecord)
		}
		var keyCount uint64
		if err := binary.Read(r, binary.BigEndian, &keyCount); err != nil {
			return err
		}
		entries := out[identity]
		if entries == nil {
			entries = make(map[string]nodePropertyIndexEntry, persistentQueryEntryMapCapacity(record.EntryCount))
			out[identity] = entries
		}
		for j := uint64(0); j < keyCount; j++ {
			key, err := readPersistentBinaryString(r)
			if err != nil {
				return err
			}
			nodeID, err := parseNodeIDFromOrderedKey(key)
			if err != nil {
				return fmt.Errorf("%w: persistent query node key mismatch", ErrInvalidRecord)
			}
			node, exists := s.nodeRecords[nodeID]
			if !exists || node.DomainID != record.DomainID {
				return fmt.Errorf("%w: invalid persistent query node entry", ErrInvalidRecord)
			}
			entries[key] = nodePropertyIndexEntry{NodeID: nodeID, Key: key}
		}
	}
	if r.Len() != 0 {
		return fmt.Errorf("%w: trailing persistent query node bytes", ErrInvalidRecord)
	}
	return nil
}

func (s *LocalStore) readPersistentQueryEdgeEntries(raw []byte, groupCount int, metadata map[string]persistentQueryIndexMetadataRecord, out map[string]map[string]edgePropertyIndexEntry) error {
	r := bytes.NewReader(raw)
	if err := readPersistentBinaryHeader(r, persistentIndexKindQueryEdge, groupCount); err != nil {
		return err
	}
	for i := 0; i < groupCount; i++ {
		identity, err := readPersistentBinaryString(r)
		if err != nil {
			return err
		}
		record, ok := metadata[identity]
		if !ok || record.TargetKind != schema.IndexTargetEdge {
			return fmt.Errorf("%w: invalid persistent query edge group", ErrInvalidRecord)
		}
		var keyCount uint64
		if err := binary.Read(r, binary.BigEndian, &keyCount); err != nil {
			return err
		}
		entries := out[identity]
		if entries == nil {
			entries = make(map[string]edgePropertyIndexEntry, persistentQueryEntryMapCapacity(record.EntryCount))
			out[identity] = entries
		}
		for j := uint64(0); j < keyCount; j++ {
			key, err := readPersistentBinaryString(r)
			if err != nil {
				return err
			}
			edgeID, err := parseEdgeIDFromOrderedKey(key)
			if err != nil {
				return fmt.Errorf("%w: persistent query edge key mismatch", ErrInvalidRecord)
			}
			edge, exists := s.edgeRecords[edgeID]
			if !exists || edge.DomainID != record.DomainID {
				return fmt.Errorf("%w: invalid persistent query edge entry", ErrInvalidRecord)
			}
			entries[key] = edgePropertyIndexEntry{EdgeID: edgeID, Key: key}
		}
	}
	if r.Len() != 0 {
		return fmt.Errorf("%w: trailing persistent query edge bytes", ErrInvalidRecord)
	}
	return nil
}

func persistentQueryIndexDefinitionFingerprint(meta IndexMetadata) (string, error) {
	labels := append([]string(nil), meta.Labels...)
	sort.Strings(labels)
	raw, err := json.Marshal(struct {
		Name               string                    `json:"name"`
		TargetKind         schema.IndexTargetKind    `json:"target_kind"`
		TargetType         string                    `json:"target_type"`
		Labels             []string                  `json:"labels"`
		Field              schema.FieldPath          `json:"field"`
		Kind               schema.IndexKind          `json:"kind"`
		Direction          schema.IndexSortDirection `json:"direction"`
		KeyEncodingVersion int                       `json:"key_encoding_version"`
	}{Name: meta.Name, TargetKind: meta.TargetKind, TargetType: meta.TargetType, Labels: labels, Field: meta.Field, Kind: meta.Kind, Direction: meta.Direction, KeyEncodingVersion: meta.KeyEncodingVersion})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
