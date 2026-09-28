package graphstorage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/fsperm"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

const (
	persistentIndexManifestVersion = 1
	persistentIndexFormat          = "domain-graph-index-v1-binary"
	persistentIndexJSONFormat      = "domain-graph-index-v1-json"
	persistentIndexChecksumAlgo    = "domain-graph-index-v1-sha256"
	persistentIndexBinaryMagic     = "KIDX"
	persistentIndexLatestPointer   = "LATEST"
	persistentIndexManifestName    = "manifest.json"
	persistentIndexLabelsPayload   = "labels.kidx"
	persistentIndexTagsPayload     = "tags.kidx"
	persistentIndexAdjOutPayload   = "adjacency-out.kidx"
	persistentIndexAdjInPayload    = "adjacency-in.kidx"
)

const (
	PersistentIndexLoadMissing   = "missing"
	PersistentIndexLoadAvailable = "available"
	PersistentIndexLoadNotLoaded = "not_loaded"
	PersistentIndexLoadUsed      = "used"
	PersistentIndexLoadFallback  = "fallback"
)

type PersistentIndexStatus struct {
	Present           bool
	IndexSetID        string
	IndexFormat       string
	GraphRevision     uint64
	GraphChecksum     string
	ChecksumAlgorithm string
	GraphCheckpointID string
	CreatedAt         time.Time
	LoadResult        string
	FallbackReason    string
	Entries           []PersistentIndexEntryStatus
}

type PersistentIndexEntryStatus struct {
	Kind       string
	Path       string
	EntryCount int
	Checksum   string
}

type persistentIndexManifest struct {
	FormatVersion     int                                     `json:"format_version"`
	SpaceID           string                                  `json:"space_id,omitempty"`
	DomainID          string                                  `json:"domain_id,omitempty"`
	IndexSetID        string                                  `json:"index_set_id"`
	CreatedAt         time.Time                               `json:"created_at"`
	GraphCheckpointID string                                  `json:"graph_checkpoint_id"`
	GraphRevision     uint64                                  `json:"graph_revision"`
	GraphChecksum     string                                  `json:"graph_checksum"`
	IndexFormat       string                                  `json:"index_format"`
	ChecksumAlgorithm string                                  `json:"checksum_algorithm"`
	Indexes           map[string]persistentIndexManifestEntry `json:"indexes"`
}

type persistentIndexManifestEntry struct {
	Path       string `json:"path"`
	EntryCount int    `json:"entry_count"`
	Checksum   string `json:"checksum"`
}

type persistentNodeIndexPayload struct {
	FormatVersion int                        `json:"format_version"`
	Kind          string                     `json:"kind"`
	Entries       []persistentNodeIndexEntry `json:"entries"`
}

type persistentNodeIndexEntry struct {
	DomainID string   `json:"domain_id"`
	Key      string   `json:"key"`
	NodeIDs  []string `json:"node_ids"`
}

type persistentAdjacencyPayload struct {
	FormatVersion int                        `json:"format_version"`
	Kind          string                     `json:"kind"`
	Entries       []persistentAdjacencyEntry `json:"entries"`
}

type persistentAdjacencyEntry struct {
	DomainID string                         `json:"domain_id"`
	NodeID   string                         `json:"node_id"`
	Label    string                         `json:"label"`
	Edges    []persistentAdjacencyEdgeEntry `json:"edges"`
}

type persistentAdjacencyEdgeEntry struct {
	Key    string `json:"key"`
	EdgeID string `json:"edge_id"`
}

func (s *LocalStore) WriteIndexSet(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return err
	}
	manifest, checkpointID, err := s.latestCheckpointManifestLocked()
	if err != nil {
		return err
	}
	if manifest.GraphRevision != s.revision {
		return fmt.Errorf("%w: latest checkpoint revision %d does not match current graph revision %d", ErrInvalidRecord, manifest.GraphRevision, s.revision)
	}
	return s.writeIndexSetLocked(ctx, manifest, checkpointID)
}

func (s *LocalStore) writeIndexSetLocked(ctx context.Context, checkpoint CheckpointManifest, checkpointID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root := filepath.Join(s.path, "indexes")
	if err := os.MkdirAll(root, fsperm.PrivateDir); err != nil {
		return err
	}
	indexSetID := fmt.Sprintf("idx-%s", uuid.NewString())
	tmpDir := filepath.Join(root, ".tmp-"+indexSetID)
	if err := os.MkdirAll(tmpDir, fsperm.PrivateDir); err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(tmpDir)
		}
	}()
	labelsRaw, labelsCount, err := marshalPersistentNodeIndexPayload("labels", s.exportNodeIndexPayload(s.labelIndex))
	if err != nil {
		return err
	}
	tagsRaw, tagsCount, err := marshalPersistentNodeIndexPayload("tags", s.exportNodeIndexPayload(s.tagIndex))
	if err != nil {
		return err
	}
	adjOutRaw, adjOutCount, err := marshalPersistentAdjacencyPayload("adjacency_out", s.exportAdjacencyPayload(s.edgeAdjacencyOut))
	if err != nil {
		return err
	}
	adjInRaw, adjInCount, err := marshalPersistentAdjacencyPayload("adjacency_in", s.exportAdjacencyPayload(s.edgeAdjacencyIn))
	if err != nil {
		return err
	}
	payloads := map[string]struct {
		path  string
		raw   []byte
		count int
	}{
		"labels":        {path: persistentIndexLabelsPayload, raw: labelsRaw, count: labelsCount},
		"tags":          {path: persistentIndexTagsPayload, raw: tagsRaw, count: tagsCount},
		"adjacency_out": {path: persistentIndexAdjOutPayload, raw: adjOutRaw, count: adjOutCount},
		"adjacency_in":  {path: persistentIndexAdjInPayload, raw: adjInRaw, count: adjInCount},
	}
	entries := map[string]persistentIndexManifestEntry{}
	for kind, payload := range payloads {
		if err := writeFileSync(filepath.Join(tmpDir, payload.path), payload.raw, fsperm.PrivateFile); err != nil {
			return err
		}
		entries[kind] = persistentIndexManifestEntry{Path: payload.path, EntryCount: payload.count, Checksum: persistentIndexChecksum(payload.raw)}
	}
	spaceID, domainID := inferCheckpointStoreIDs(s.path)
	manifest := persistentIndexManifest{
		FormatVersion:     persistentIndexManifestVersion,
		SpaceID:           spaceID,
		DomainID:          domainID,
		IndexSetID:        indexSetID,
		CreatedAt:         time.Now().UTC(),
		GraphCheckpointID: checkpointID,
		GraphRevision:     checkpoint.GraphRevision,
		GraphChecksum:     checkpoint.GraphChecksum,
		IndexFormat:       persistentIndexFormat,
		ChecksumAlgorithm: persistentIndexChecksumAlgo,
		Indexes:           entries,
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := writeFileSync(filepath.Join(tmpDir, persistentIndexManifestName), raw, fsperm.PrivateFile); err != nil {
		return err
	}
	if err := syncDir(tmpDir); err != nil {
		return err
	}
	finalDir := filepath.Join(root, indexSetID)
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return err
	}
	cleanup = false
	pointerTmp := filepath.Join(root, persistentIndexLatestPointer+".tmp")
	if err := writeFileSync(pointerTmp, []byte(indexSetID+"\n"), fsperm.PrivateFile); err != nil {
		return err
	}
	if err := os.Rename(pointerTmp, filepath.Join(root, persistentIndexLatestPointer)); err != nil {
		return err
	}
	_ = syncDir(root)
	s.cleanupOldIndexSets(root, indexSetID)
	return nil
}

func (s *LocalStore) latestCheckpointManifestLocked() (CheckpointManifest, string, error) {
	dir, err := s.latestCheckpointDir()
	if err != nil {
		return CheckpointManifest{}, "", err
	}
	if dir == "" {
		return CheckpointManifest{}, "", fmt.Errorf("%w: no graph checkpoint is present", ErrNotFound)
	}
	raw, err := os.ReadFile(filepath.Join(dir, checkpointManifestName))
	if err != nil {
		return CheckpointManifest{}, "", err
	}
	var manifest CheckpointManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return CheckpointManifest{}, "", err
	}
	if err := s.validateCheckpointManifest(manifest); err != nil {
		return CheckpointManifest{}, "", err
	}
	return manifest, filepath.Base(dir), nil
}

func (s *LocalStore) tryLoadPersistentIndexSet(ctx context.Context, checkpoint CheckpointManifest) (PersistentIndexStatus, bool) {
	status := PersistentIndexStatus{LoadResult: PersistentIndexLoadMissing}
	if err := ctx.Err(); err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	dir, err := s.latestIndexSetDir()
	if err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	if dir == "" {
		return status, false
	}
	raw, err := os.ReadFile(filepath.Join(dir, persistentIndexManifestName))
	if err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	var manifest persistentIndexManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	status = persistentIndexStatusFromManifest(manifest)
	if err := s.validatePersistentIndexManifest(manifest, checkpoint); err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	labels, err := s.readPersistentNodeIndex(dir, manifest, "labels")
	if err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	tags, err := s.readPersistentNodeIndex(dir, manifest, "tags")
	if err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	adjOut, err := s.readPersistentAdjacencyIndex(dir, manifest, "adjacency_out")
	if err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	adjIn, err := s.readPersistentAdjacencyIndex(dir, manifest, "adjacency_in")
	if err != nil {
		return persistentIndexStatusFallback(status, err), false
	}
	s.labelIndex = labels
	s.tagIndex = tags
	s.edgeAdjacencyOut = adjOut
	s.edgeAdjacencyIn = adjIn
	status.LoadResult = PersistentIndexLoadUsed
	status.FallbackReason = ""
	return status, true
}

func (s *LocalStore) latestIndexSetDir() (string, error) {
	root := filepath.Join(s.path, "indexes")
	raw, err := os.ReadFile(filepath.Join(root, persistentIndexLatestPointer))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	name := strings.TrimSpace(string(raw))
	if !validPersistentIndexSetName(name) {
		return "", fmt.Errorf("%w: invalid persistent index pointer", ErrInvalidRecord)
	}
	return filepath.Join(root, name), nil
}

func (s *LocalStore) persistentIndexManifestCandidateForCheckpoint(checkpoint CheckpointManifest) PersistentIndexStatus {
	status := PersistentIndexStatus{LoadResult: PersistentIndexLoadMissing}
	dir, err := s.latestIndexSetDir()
	if err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	if dir == "" {
		return status
	}
	raw, err := os.ReadFile(filepath.Join(dir, persistentIndexManifestName))
	if err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	var manifest persistentIndexManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	status = persistentIndexStatusFromManifest(manifest)
	if err := s.validatePersistentIndexManifest(manifest, checkpoint); err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	status.LoadResult = PersistentIndexLoadAvailable
	return status
}

func (s *LocalStore) persistentIndexStatusForCheckpoint(checkpoint CheckpointManifest, lastLoad PersistentIndexStatus) PersistentIndexStatus {
	status := PersistentIndexStatus{LoadResult: PersistentIndexLoadMissing}
	dir, err := s.latestIndexSetDir()
	if err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	if dir == "" {
		return status
	}
	raw, err := os.ReadFile(filepath.Join(dir, persistentIndexManifestName))
	if err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	var manifest persistentIndexManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	status = persistentIndexStatusFromManifest(manifest)
	if err := s.validatePersistentIndexManifest(manifest, checkpoint); err != nil {
		return persistentIndexStatusFallback(status, err)
	}
	for _, entry := range manifest.Indexes {
		raw, err := os.ReadFile(filepath.Join(dir, entry.Path))
		if err != nil {
			return persistentIndexStatusFallback(status, err)
		}
		if persistentIndexChecksum(raw) != entry.Checksum {
			return persistentIndexStatusFallback(status, fmt.Errorf("%w: persistent index checksum mismatch", ErrInvalidRecord))
		}
	}
	status.LoadResult = PersistentIndexLoadAvailable
	if lastLoad.IndexSetID == status.IndexSetID && lastLoad.LoadResult != "" {
		status.LoadResult = lastLoad.LoadResult
		status.FallbackReason = lastLoad.FallbackReason
	} else {
		status.LoadResult = PersistentIndexLoadNotLoaded
	}
	return status
}

func persistentIndexStatusFromManifest(manifest persistentIndexManifest) PersistentIndexStatus {
	status := PersistentIndexStatus{
		Present:           true,
		IndexSetID:        manifest.IndexSetID,
		IndexFormat:       manifest.IndexFormat,
		GraphRevision:     manifest.GraphRevision,
		GraphChecksum:     manifest.GraphChecksum,
		ChecksumAlgorithm: manifest.ChecksumAlgorithm,
		GraphCheckpointID: manifest.GraphCheckpointID,
		CreatedAt:         manifest.CreatedAt.UTC(),
		LoadResult:        PersistentIndexLoadAvailable,
	}
	kinds := make([]string, 0, len(manifest.Indexes))
	for kind := range manifest.Indexes {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		entry := manifest.Indexes[kind]
		status.Entries = append(status.Entries, PersistentIndexEntryStatus{Kind: kind, Path: entry.Path, EntryCount: entry.EntryCount, Checksum: entry.Checksum})
	}
	return status
}

func persistentIndexStatusFallback(status PersistentIndexStatus, err error) PersistentIndexStatus {
	if err == nil && !status.Present {
		status.LoadResult = PersistentIndexLoadMissing
		return status
	}
	status.LoadResult = PersistentIndexLoadFallback
	if err != nil {
		status.FallbackReason = err.Error()
	}
	return status
}

func validPersistentIndexSetName(name string) bool {
	return strings.HasPrefix(name, "idx-") && name == filepath.Base(name) && !strings.Contains(name, "..") && strings.TrimSpace(name) == name
}

func (s *LocalStore) validatePersistentIndexManifest(manifest persistentIndexManifest, checkpoint CheckpointManifest) error {
	if manifest.FormatVersion != persistentIndexManifestVersion {
		return fmt.Errorf("%w: unsupported persistent index manifest version", ErrUnsupported)
	}
	if (manifest.IndexFormat != persistentIndexFormat && manifest.IndexFormat != persistentIndexJSONFormat) || manifest.ChecksumAlgorithm != persistentIndexChecksumAlgo {
		return fmt.Errorf("%w: unsupported persistent index format", ErrUnsupported)
	}
	if !validPersistentIndexSetName(manifest.IndexSetID) {
		return fmt.Errorf("%w: invalid persistent index set ID", ErrInvalidRecord)
	}
	if manifest.GraphRevision != checkpoint.GraphRevision || manifest.GraphChecksum != checkpoint.GraphChecksum {
		return fmt.Errorf("%w: persistent index graph baseline mismatch", ErrInvalidRecord)
	}
	spaceID, domainID := inferCheckpointStoreIDs(s.path)
	if manifest.SpaceID != "" && spaceID != "" && manifest.SpaceID != spaceID {
		return fmt.Errorf("%w: persistent index space mismatch", ErrInvalidRecord)
	}
	if manifest.DomainID != "" && domainID != "" && manifest.DomainID != domainID {
		return fmt.Errorf("%w: persistent index domain mismatch", ErrInvalidRecord)
	}
	for _, kind := range []string{"labels", "tags", "adjacency_out", "adjacency_in"} {
		entry, ok := manifest.Indexes[kind]
		if !ok {
			return fmt.Errorf("%w: persistent index missing %s", ErrInvalidRecord, kind)
		}
		if err := validatePersistentIndexPayloadPath(entry.Path); err != nil {
			return err
		}
	}
	return nil
}

func validatePersistentIndexPayloadPath(path string) error {
	if strings.TrimSpace(path) != path || path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || strings.Contains(path, string(filepath.Separator)) || strings.Contains(path, "..") {
		return fmt.Errorf("%w: invalid persistent index payload path", ErrInvalidRecord)
	}
	return nil
}

func (s *LocalStore) readPersistentNodeIndex(dir string, manifest persistentIndexManifest, kind string) (map[graph.DomainID]map[string]map[graph.NodeID]struct{}, error) {
	entry := manifest.Indexes[kind]
	raw, err := os.ReadFile(filepath.Join(dir, entry.Path))
	if err != nil {
		return nil, err
	}
	if persistentIndexChecksum(raw) != entry.Checksum {
		return nil, fmt.Errorf("%w: persistent index checksum mismatch", ErrInvalidRecord)
	}
	if manifest.IndexFormat == persistentIndexFormat {
		return s.readPersistentBinaryNodeIndex(raw, entry.EntryCount, kind)
	}
	var payload persistentNodeIndexPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if payload.FormatVersion != persistentIndexManifestVersion || payload.Kind != kind || len(payload.Entries) != entry.EntryCount {
		return nil, fmt.Errorf("%w: persistent node index payload mismatch", ErrInvalidRecord)
	}
	out := map[graph.DomainID]map[string]map[graph.NodeID]struct{}{}
	for _, item := range payload.Entries {
		domainUUID, err := uuid.Parse(item.DomainID)
		if err != nil || domainUUID == uuid.Nil || item.Key == "" {
			return nil, fmt.Errorf("%w: invalid persistent node index entry", ErrInvalidRecord)
		}
		set := ensureNodeSetByString(out, graph.DomainID(domainUUID), item.Key)
		last := ""
		for _, rawID := range item.NodeIDs {
			id, err := uuid.Parse(rawID)
			nodeID := graph.NodeID(id)
			node, exists := s.nodeRecords[nodeID]
			if err != nil || id == uuid.Nil || (last != "" && rawID <= last) || !exists || node.DomainID != graph.DomainID(domainUUID) {
				return nil, fmt.Errorf("%w: invalid persistent node index ID", ErrInvalidRecord)
			}
			set[nodeID] = struct{}{}
			last = rawID
		}
	}
	return out, nil
}

func (s *LocalStore) readPersistentAdjacencyIndex(dir string, manifest persistentIndexManifest, kind string) (map[graph.DomainID]map[graph.NodeID]map[string]map[string]graph.EdgeID, error) {
	entry := manifest.Indexes[kind]
	raw, err := os.ReadFile(filepath.Join(dir, entry.Path))
	if err != nil {
		return nil, err
	}
	if persistentIndexChecksum(raw) != entry.Checksum {
		return nil, fmt.Errorf("%w: persistent adjacency index checksum mismatch", ErrInvalidRecord)
	}
	if manifest.IndexFormat == persistentIndexFormat {
		return s.readPersistentBinaryAdjacencyIndex(raw, entry.EntryCount, kind)
	}
	var payload persistentAdjacencyPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if payload.FormatVersion != persistentIndexManifestVersion || payload.Kind != kind || len(payload.Entries) != entry.EntryCount {
		return nil, fmt.Errorf("%w: persistent adjacency payload mismatch", ErrInvalidRecord)
	}
	out := map[graph.DomainID]map[graph.NodeID]map[string]map[string]graph.EdgeID{}
	for _, item := range payload.Entries {
		domainUUID, err := uuid.Parse(item.DomainID)
		if err != nil || domainUUID == uuid.Nil || item.Label == "" {
			return nil, fmt.Errorf("%w: invalid persistent adjacency domain/label", ErrInvalidRecord)
		}
		nodeUUID, err := uuid.Parse(item.NodeID)
		if err != nil || nodeUUID == uuid.Nil {
			return nil, fmt.Errorf("%w: invalid persistent adjacency node", ErrInvalidRecord)
		}
		set := ensureAdjacencySet(out, graph.DomainID(domainUUID), graph.NodeID(nodeUUID), item.Label)
		last := ""
		for _, edge := range item.Edges {
			edgeUUID, err := uuid.Parse(edge.EdgeID)
			edgeID := graph.EdgeID(edgeUUID)
			stored, exists := s.edgeRecords[edgeID]
			if err != nil || edgeUUID == uuid.Nil || edge.Key == "" || (last != "" && edge.Key <= last) || !exists || stored.DomainID != graph.DomainID(domainUUID) {
				return nil, fmt.Errorf("%w: invalid persistent adjacency edge", ErrInvalidRecord)
			}
			set[edge.Key] = edgeID
			last = edge.Key
		}
	}
	return out, nil
}

func (s *LocalStore) exportNodeIndexPayload(index map[graph.DomainID]map[string]map[graph.NodeID]struct{}) []persistentNodeIndexEntry {
	entries := []persistentNodeIndexEntry{}
	domains := make([]graph.DomainID, 0, len(index))
	for domainID := range index {
		domains = append(domains, domainID)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].String() < domains[j].String() })
	for _, domainID := range domains {
		keys := make([]string, 0, len(index[domainID]))
		for key := range index[domainID] {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			ids := nodeIDStrings(index[domainID][key])
			entries = append(entries, persistentNodeIndexEntry{DomainID: domainID.String(), Key: key, NodeIDs: ids})
		}
	}
	return entries
}

func (s *LocalStore) exportAdjacencyPayload(index map[graph.DomainID]map[graph.NodeID]map[string]map[string]graph.EdgeID) []persistentAdjacencyEntry {
	entries := []persistentAdjacencyEntry{}
	domains := make([]graph.DomainID, 0, len(index))
	for domainID := range index {
		domains = append(domains, domainID)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].String() < domains[j].String() })
	for _, domainID := range domains {
		nodes := make([]graph.NodeID, 0, len(index[domainID]))
		for nodeID := range index[domainID] {
			nodes = append(nodes, nodeID)
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].String() < nodes[j].String() })
		for _, nodeID := range nodes {
			labels := make([]string, 0, len(index[domainID][nodeID]))
			for label := range index[domainID][nodeID] {
				labels = append(labels, label)
			}
			sort.Strings(labels)
			for _, label := range labels {
				edges := persistentAdjacencyEdges(index[domainID][nodeID][label])
				entries = append(entries, persistentAdjacencyEntry{DomainID: domainID.String(), NodeID: nodeID.String(), Label: label, Edges: edges})
			}
		}
	}
	return entries
}

func marshalPersistentNodeIndexPayload(kind string, entries []persistentNodeIndexEntry) ([]byte, int, error) {
	raw, err := marshalPersistentBinaryNodeIndexPayload(kind, entries)
	if err != nil {
		return nil, 0, err
	}
	return raw, len(entries), nil
}

func marshalPersistentAdjacencyPayload(kind string, entries []persistentAdjacencyEntry) ([]byte, int, error) {
	raw, err := marshalPersistentBinaryAdjacencyPayload(kind, entries)
	if err != nil {
		return nil, 0, err
	}
	return raw, len(entries), nil
}

func marshalPersistentBinaryNodeIndexPayload(kind string, entries []persistentNodeIndexEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := writePersistentBinaryHeader(&buf, kind, len(entries)); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		domainID, err := uuid.Parse(entry.DomainID)
		if err != nil {
			return nil, err
		}
		buf.Write(domainID[:])
		if err := writePersistentBinaryString(&buf, entry.Key); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(entry.NodeIDs))); err != nil {
			return nil, err
		}
		for _, rawID := range entry.NodeIDs {
			id, err := uuid.Parse(rawID)
			if err != nil {
				return nil, err
			}
			buf.Write(id[:])
		}
	}
	return buf.Bytes(), nil
}

func marshalPersistentBinaryAdjacencyPayload(kind string, entries []persistentAdjacencyEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := writePersistentBinaryHeader(&buf, kind, len(entries)); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		domainID, err := uuid.Parse(entry.DomainID)
		if err != nil {
			return nil, err
		}
		nodeID, err := uuid.Parse(entry.NodeID)
		if err != nil {
			return nil, err
		}
		buf.Write(domainID[:])
		buf.Write(nodeID[:])
		if err := writePersistentBinaryString(&buf, entry.Label); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(entry.Edges))); err != nil {
			return nil, err
		}
		for _, edge := range entry.Edges {
			if err := writePersistentBinaryString(&buf, edge.Key); err != nil {
				return nil, err
			}
			edgeID, err := uuid.Parse(edge.EdgeID)
			if err != nil {
				return nil, err
			}
			buf.Write(edgeID[:])
		}
	}
	return buf.Bytes(), nil
}

func writePersistentBinaryHeader(buf *bytes.Buffer, kind string, entryCount int) error {
	buf.WriteString(persistentIndexBinaryMagic)
	if err := binary.Write(buf, binary.BigEndian, uint16(persistentIndexManifestVersion)); err != nil {
		return err
	}
	if err := writePersistentBinaryString(buf, kind); err != nil {
		return err
	}
	return binary.Write(buf, binary.BigEndian, uint64(entryCount))
}

func writePersistentBinaryString(buf *bytes.Buffer, value string) error {
	if len(value) > int(^uint32(0)) {
		return fmt.Errorf("%w: persistent index string too large", ErrInvalidRecord)
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := buf.WriteString(value)
	return err
}

func (s *LocalStore) readPersistentBinaryNodeIndex(raw []byte, entryCount int, kind string) (map[graph.DomainID]map[string]map[graph.NodeID]struct{}, error) {
	r := bytes.NewReader(raw)
	if err := readPersistentBinaryHeader(r, kind, entryCount); err != nil {
		return nil, err
	}
	out := map[graph.DomainID]map[string]map[graph.NodeID]struct{}{}
	for i := 0; i < entryCount; i++ {
		domainUUID, err := readPersistentBinaryUUID(r)
		if err != nil {
			return nil, err
		}
		key, err := readPersistentBinaryString(r)
		if err != nil {
			return nil, err
		}
		var nodeCount uint32
		if err := binary.Read(r, binary.BigEndian, &nodeCount); err != nil {
			return nil, err
		}
		if domainUUID == uuid.Nil || key == "" || int(nodeCount) > r.Len()/16 {
			return nil, fmt.Errorf("%w: invalid persistent node index entry", ErrInvalidRecord)
		}
		set := ensureNodeSetByString(out, graph.DomainID(domainUUID), key)
		var last uuid.UUID
		hasLast := false
		for j := uint32(0); j < nodeCount; j++ {
			id, err := readPersistentBinaryUUID(r)
			if err != nil {
				return nil, err
			}
			nodeID := graph.NodeID(id)
			node, exists := s.nodeRecords[nodeID]
			if id == uuid.Nil || (hasLast && bytes.Compare(id[:], last[:]) <= 0) || !exists || node.DomainID != graph.DomainID(domainUUID) {
				return nil, fmt.Errorf("%w: invalid persistent node index ID", ErrInvalidRecord)
			}
			set[nodeID] = struct{}{}
			last = id
			hasLast = true
		}
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing persistent node index bytes", ErrInvalidRecord)
	}
	return out, nil
}

func (s *LocalStore) readPersistentBinaryAdjacencyIndex(raw []byte, entryCount int, kind string) (map[graph.DomainID]map[graph.NodeID]map[string]map[string]graph.EdgeID, error) {
	r := bytes.NewReader(raw)
	if err := readPersistentBinaryHeader(r, kind, entryCount); err != nil {
		return nil, err
	}
	out := map[graph.DomainID]map[graph.NodeID]map[string]map[string]graph.EdgeID{}
	for i := 0; i < entryCount; i++ {
		domainUUID, err := readPersistentBinaryUUID(r)
		if err != nil {
			return nil, err
		}
		nodeUUID, err := readPersistentBinaryUUID(r)
		if err != nil {
			return nil, err
		}
		label, err := readPersistentBinaryString(r)
		if err != nil {
			return nil, err
		}
		var edgeCount uint32
		if err := binary.Read(r, binary.BigEndian, &edgeCount); err != nil {
			return nil, err
		}
		if domainUUID == uuid.Nil || nodeUUID == uuid.Nil || label == "" || int(edgeCount) > r.Len()/20 {
			return nil, fmt.Errorf("%w: invalid persistent adjacency entry", ErrInvalidRecord)
		}
		set := ensureAdjacencySet(out, graph.DomainID(domainUUID), graph.NodeID(nodeUUID), label)
		last := ""
		for j := uint32(0); j < edgeCount; j++ {
			key, err := readPersistentBinaryString(r)
			if err != nil {
				return nil, err
			}
			edgeUUID, err := readPersistentBinaryUUID(r)
			if err != nil {
				return nil, err
			}
			edgeID := graph.EdgeID(edgeUUID)
			stored, exists := s.edgeRecords[edgeID]
			if edgeUUID == uuid.Nil || key == "" || (last != "" && key <= last) || !exists || stored.DomainID != graph.DomainID(domainUUID) {
				return nil, fmt.Errorf("%w: invalid persistent adjacency edge", ErrInvalidRecord)
			}
			set[key] = edgeID
			last = key
		}
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing persistent adjacency index bytes", ErrInvalidRecord)
	}
	return out, nil
}

func readPersistentBinaryHeader(r *bytes.Reader, wantKind string, wantEntryCount int) error {
	magic := make([]byte, len(persistentIndexBinaryMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return err
	}
	if string(magic) != persistentIndexBinaryMagic {
		return fmt.Errorf("%w: invalid persistent index magic", ErrInvalidRecord)
	}
	var version uint16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return err
	}
	kind, err := readPersistentBinaryString(r)
	if err != nil {
		return err
	}
	var entryCount uint64
	if err := binary.Read(r, binary.BigEndian, &entryCount); err != nil {
		return err
	}
	if version != persistentIndexManifestVersion || kind != wantKind || entryCount != uint64(wantEntryCount) {
		return fmt.Errorf("%w: persistent index binary header mismatch", ErrInvalidRecord)
	}
	return nil
}

func readPersistentBinaryString(r *bytes.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if int(length) < 0 || int(length) > r.Len() || length > 1<<20 {
		return "", fmt.Errorf("%w: invalid persistent index string length", ErrInvalidRecord)
	}
	buf := make([]byte, int(length))
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readPersistentBinaryUUID(r *bytes.Reader) (uuid.UUID, error) {
	var id uuid.UUID
	_, err := io.ReadFull(r, id[:])
	return id, err
}

func nodeIDStrings(set map[graph.NodeID]struct{}) []string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id.String())
	}
	sort.Strings(ids)
	return ids
}

func persistentAdjacencyEdges(set map[string]graph.EdgeID) []persistentAdjacencyEdgeEntry {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]persistentAdjacencyEdgeEntry, 0, len(keys))
	for _, key := range keys {
		out = append(out, persistentAdjacencyEdgeEntry{Key: key, EdgeID: set[key].String()})
	}
	return out
}

func ensureNodeSetByString(index map[graph.DomainID]map[string]map[graph.NodeID]struct{}, domainID graph.DomainID, key string) map[graph.NodeID]struct{} {
	byKey := index[domainID]
	if byKey == nil {
		byKey = map[string]map[graph.NodeID]struct{}{}
		index[domainID] = byKey
	}
	set := byKey[key]
	if set == nil {
		set = map[graph.NodeID]struct{}{}
		byKey[key] = set
	}
	return set
}

func persistentIndexChecksum(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *LocalStore) cleanupOldIndexSets(root, keep string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == keep || name == persistentIndexLatestPointer || name == persistentIndexLatestPointer+".tmp" {
			continue
		}
		if entry.IsDir() && (strings.HasPrefix(name, "idx-") || strings.HasPrefix(name, ".tmp-")) {
			_ = os.RemoveAll(filepath.Join(root, name))
		}
	}
}
