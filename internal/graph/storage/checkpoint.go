package graphstorage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
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
	checkpointManifestVersion = 1
	checkpointPayloadVersion  = 1
	checkpointLatestPointer   = "LATEST"
	checkpointNodePayload     = "nodes.kchk"
	checkpointEdgePayload     = "edges.kchk"
	checkpointManifestName    = "manifest.json"
	checkpointChecksumAlgo    = "graph-checkpoint-v1-sha256"
)

var checkpointMagic = [4]byte{'K', 'C', 'H', 'K'}

const (
	checkpointKindNode uint8 = 1
	checkpointKindEdge uint8 = 2
)

type CheckpointManifest struct {
	FormatVersion         int                    `json:"format_version"`
	SpaceID               string                 `json:"space_id,omitempty"`
	DomainID              string                 `json:"domain_id,omitempty"`
	GraphRevision         uint64                 `json:"graph_revision"`
	CreatedAt             time.Time              `json:"created_at"`
	NodeCount             int                    `json:"node_count"`
	EdgeCount             int                    `json:"edge_count"`
	AppliedSegmentOffsets CheckpointSegmentState `json:"applied_segment_offsets"`
	ChecksumAlgorithm     string                 `json:"checksum_algorithm"`
	NodeChecksum          string                 `json:"node_checksum"`
	EdgeChecksum          string                 `json:"edge_checksum"`
	GraphChecksum         string                 `json:"graph_checksum"`
}

type CheckpointSegmentState struct {
	Txns  []CheckpointSegmentOffset `json:"txns"`
	Nodes []CheckpointSegmentOffset `json:"nodes"`
	Edges []CheckpointSegmentOffset `json:"edges"`
}

type CheckpointSegmentOffset struct {
	Segment string `json:"segment"`
	Offset  int64  `json:"offset"`
}

// CheckpointStatus describes the latest local checkpoint for a store.
type CheckpointStatus struct {
	CurrentRevision    uint64
	CheckpointPresent  bool
	CheckpointRevision uint64
	CreatedAt          time.Time
	NodeCount          int
	EdgeCount          int
	GraphChecksum      string
	ChecksumAlgorithm  string
	TailRevisions      uint64
	PersistentIndex    PersistentIndexStatus
}

// CheckpointStatus returns current store revision and latest checkpoint metadata,
// if a checkpoint is present.
func (s *LocalStore) CheckpointStatus(ctx context.Context) (CheckpointStatus, error) {
	if err := ctx.Err(); err != nil {
		return CheckpointStatus{}, err
	}
	s.mu.RLock()
	currentRevision := s.revision
	lastIndexLoad := s.persistentIndexLoadStatus
	err := s.ensureReady()
	s.mu.RUnlock()
	if err != nil {
		return CheckpointStatus{}, err
	}
	status := CheckpointStatus{CurrentRevision: currentRevision}
	dir, err := s.latestCheckpointDir()
	if err != nil {
		if os.IsNotExist(err) {
			return status, nil
		}
		return CheckpointStatus{}, err
	}
	if dir == "" {
		return status, nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, checkpointManifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return status, nil
		}
		return CheckpointStatus{}, err
	}
	var manifest CheckpointManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return CheckpointStatus{}, err
	}
	if err := s.validateCheckpointManifest(manifest); err != nil {
		return CheckpointStatus{}, err
	}
	status.CheckpointPresent = true
	status.CheckpointRevision = manifest.GraphRevision
	status.CreatedAt = manifest.CreatedAt.UTC()
	status.NodeCount = manifest.NodeCount
	status.EdgeCount = manifest.EdgeCount
	status.GraphChecksum = manifest.GraphChecksum
	status.ChecksumAlgorithm = manifest.ChecksumAlgorithm
	if currentRevision >= manifest.GraphRevision {
		status.TailRevisions = currentRevision - manifest.GraphRevision
	}
	status.PersistentIndex = s.persistentIndexStatusForCheckpoint(manifest, lastIndexLoad)
	return status, nil
}

// WriteCheckpoint writes a compact latest-state checkpoint for this local store.
// The checkpoint is a local derived artifact; the append-only segments remain the
// authoritative mutation log until a future compaction phase removes old records.
func (s *LocalStore) WriteCheckpoint(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return err
	}
	nodes := make([]graph.Node, 0, len(s.nodeRecords))
	for _, node := range s.nodeRecords {
		nodes = append(nodes, cloneNode(node))
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID.String() < nodes[j].ID.String() })
	edges := make([]graph.Edge, 0, len(s.edgeRecords))
	for _, edge := range s.edgeRecords {
		edges = append(edges, cloneEdge(edge))
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID.String() < edges[j].ID.String() })
	offsets, err := s.captureCheckpointSegmentOffsetsLocked()
	if err != nil {
		return err
	}
	nodeChecksum, err := checkpointNodeChecksum(nodes)
	if err != nil {
		return err
	}
	edgeChecksum, err := checkpointEdgeChecksum(edges)
	if err != nil {
		return err
	}
	manifest := CheckpointManifest{
		FormatVersion:         checkpointManifestVersion,
		GraphRevision:         s.revision,
		CreatedAt:             time.Now().UTC(),
		NodeCount:             len(nodes),
		EdgeCount:             len(edges),
		AppliedSegmentOffsets: offsets,
		ChecksumAlgorithm:     checkpointChecksumAlgo,
		NodeChecksum:          nodeChecksum,
		EdgeChecksum:          edgeChecksum,
		GraphChecksum:         checkpointGraphChecksum(nodeChecksum, edgeChecksum),
	}
	manifest.SpaceID, manifest.DomainID = inferCheckpointStoreIDs(s.path)
	return s.writeCheckpointLocked(ctx, manifest, nodes, edges)
}

func (s *LocalStore) captureCheckpointSegmentOffsetsLocked() (CheckpointSegmentState, error) {
	capture := func(segments []string) ([]CheckpointSegmentOffset, error) {
		out := make([]CheckpointSegmentOffset, 0, len(segments))
		for _, seg := range segments {
			st, err := os.Stat(filepath.Join(s.path, seg))
			if err != nil {
				return nil, err
			}
			out = append(out, CheckpointSegmentOffset{Segment: seg, Offset: st.Size()})
		}
		return out, nil
	}
	var state CheckpointSegmentState
	var err error
	if state.Txns, err = capture(s.manifest.TxnSegments); err != nil {
		return CheckpointSegmentState{}, err
	}
	if state.Nodes, err = capture(s.manifest.NodeSegments); err != nil {
		return CheckpointSegmentState{}, err
	}
	if state.Edges, err = capture(s.manifest.EdgeSegments); err != nil {
		return CheckpointSegmentState{}, err
	}
	return state, nil
}

func (s *LocalStore) writeCheckpointLocked(ctx context.Context, manifest CheckpointManifest, nodes []graph.Node, edges []graph.Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root := filepath.Join(s.path, "checkpoints")
	if err := os.MkdirAll(root, fsperm.PrivateDir); err != nil {
		return err
	}
	id := fmt.Sprintf("chk-%s", uuid.NewString())
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, fsperm.PrivateDir); err != nil {
		return err
	}
	if err := writeCheckpointNodes(filepath.Join(dir, checkpointNodePayload), nodes); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	if err := writeCheckpointEdges(filepath.Join(dir, checkpointEdgePayload), edges); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	raw = append(raw, '\n')
	if err := writeFileSync(filepath.Join(dir, checkpointManifestName), raw, fsperm.PrivateFile); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	if err := syncDir(dir); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	pointerTmp := filepath.Join(root, checkpointLatestPointer+".tmp")
	if err := writeFileSync(pointerTmp, []byte(id+"\n"), fsperm.PrivateFile); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	if err := os.Rename(pointerTmp, filepath.Join(root, checkpointLatestPointer)); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	_ = syncDir(root)
	s.cleanupOldCheckpoints(root, id)
	// Persistent indexes are local derived artifacts. A failure to write them must
	// not invalidate the graph checkpoint, because the store can rebuild indexes
	// from checkpoint payloads and tail segment replay.
	_ = s.writeIndexSetLocked(ctx, manifest, id)
	return nil
}

func (s *LocalStore) cleanupOldCheckpoints(root, keep string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == keep || name == checkpointLatestPointer || name == checkpointLatestPointer+".tmp" {
			continue
		}
		if entry.IsDir() && strings.HasPrefix(name, "chk-") {
			_ = os.RemoveAll(filepath.Join(root, name))
		}
		if strings.HasPrefix(name, ".tmp-") {
			_ = os.RemoveAll(filepath.Join(root, name))
		}
	}
}

func (s *LocalStore) loadCheckpoint(ctx context.Context) (bool, error) {
	loaded, err := s.tryLoadCheckpoint(ctx)
	if err == nil {
		return loaded, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	// Invalid or incomplete checkpoints are ignored for the initial phase. The
	// caller falls back to authoritative full segment replay.
	s.resetIndexes()
	return false, nil
}

func (s *LocalStore) tryLoadCheckpoint(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	dir, err := s.latestCheckpointDir()
	if err != nil || dir == "" {
		return false, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, checkpointManifestName))
	if err != nil {
		return false, err
	}
	var manifest CheckpointManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return false, err
	}
	if err := s.validateCheckpointManifest(manifest); err != nil {
		return false, err
	}
	nodes, err := readCheckpointNodes(filepath.Join(dir, checkpointNodePayload))
	if err != nil {
		return false, err
	}
	edges, err := readCheckpointEdges(filepath.Join(dir, checkpointEdgePayload))
	if err != nil {
		return false, err
	}
	if len(nodes) != manifest.NodeCount || len(edges) != manifest.EdgeCount {
		return false, fmt.Errorf("%w: checkpoint count mismatch", ErrInvalidRecord)
	}
	nodeChecksum, err := checkpointNodeChecksum(nodes)
	if err != nil {
		return false, err
	}
	edgeChecksum, err := checkpointEdgeChecksum(edges)
	if err != nil {
		return false, err
	}
	if nodeChecksum != manifest.NodeChecksum || edgeChecksum != manifest.EdgeChecksum || checkpointGraphChecksum(nodeChecksum, edgeChecksum) != manifest.GraphChecksum {
		return false, fmt.Errorf("%w: checkpoint checksum mismatch", ErrInvalidRecord)
	}
	indexCandidate := s.persistentIndexManifestCandidateForCheckpoint(manifest)
	usePersistentCandidate := indexCandidate.Present && indexCandidate.LoadResult != PersistentIndexLoadFallback
	s.resetIndexes()
	if err := s.loadIndexManifestLocked(); err != nil {
		return false, err
	}
	checkpointLoc := RecordLocation{Segment: "checkpoint", Offset: 0, Length: 0}
	if usePersistentCandidate {
		for _, node := range nodes {
			s.applyCheckpointNodePut(node, checkpointLoc, true)
			s.nodeModRev[node.ID] = manifest.GraphRevision
		}
		for _, edge := range edges {
			s.applyCheckpointEdgePut(edge, checkpointLoc, true)
			s.edgeModRev[edge.ID] = manifest.GraphRevision
		}
		s.persistentIndexLoadStatus, _ = s.tryLoadPersistentIndexSet(ctx, manifest)
		if s.persistentIndexLoadStatus.LoadResult == PersistentIndexLoadFallback {
			s.resetIndexes()
			if err := s.loadIndexManifestLocked(); err != nil {
				return false, err
			}
			s.hydrateCheckpointFull(nodes, edges, checkpointLoc, manifest.GraphRevision)
		}
	} else {
		s.persistentIndexLoadStatus = indexCandidate
		s.hydrateCheckpointFull(nodes, edges, checkpointLoc, manifest.GraphRevision)
	}
	s.revision = manifest.GraphRevision
	if err := s.replayCheckpointTail(ctx, manifest.AppliedSegmentOffsets); err != nil {
		return false, err
	}
	for key, meta := range s.indexMetadata {
		if meta.BuildState == IndexBuildStateReady {
			meta.LastIndexedGraphRevision = s.revision
			s.indexMetadata[key] = meta
		}
	}
	return true, nil
}

func (s *LocalStore) hydrateCheckpointFull(nodes []graph.Node, edges []graph.Edge, loc RecordLocation, revision uint64) {
	for _, node := range nodes {
		s.applyNodePut(node, loc)
		s.nodeModRev[node.ID] = revision
	}
	for _, edge := range edges {
		s.applyEdgePut(edge, loc)
		s.edgeModRev[edge.ID] = revision
	}
}

func (s *LocalStore) applyCheckpointNodePut(n graph.Node, loc RecordLocation, persistentLabelTagIndexes bool) {
	s.nodeRecords[n.ID] = cloneNode(n)
	s.nodeMeta[n.ID] = NodeMeta{ID: n.ID, DomainID: n.DomainID, Location: loc}
	if n.DomainID != uuid.Nil {
		ensureNodeSet(s.nodesByDomain, n.DomainID)[n.ID] = struct{}{}
		if !persistentLabelTagIndexes {
			s.addNodeLabelIndexes(n)
			s.addNodeTagIndexes(n)
		}
		for _, idx := range s.configuredIndexes[n.DomainID] {
			_ = s.addNodePropertyIndexEntry(n, idx)
		}
	}
	propsForIndex := n.Properties
	if len(propsForIndex) == 0 {
		propsForIndex = n.Props
	}
	if day, ok := numberPropInt(propsForIndex["journal_day"]); ok {
		ensureNodeSet(s.journalDay, day)[n.ID] = struct{}{}
	}
	if n.BlobRef != nil {
		ensureNodeSet(s.blobRefs, *n.BlobRef)[n.ID] = struct{}{}
	}
}

func (s *LocalStore) applyCheckpointEdgePut(e graph.Edge, loc RecordLocation, persistentAdjacencyIndexes bool) {
	stored := cloneEdge(e)
	s.edgeRecords[e.ID] = stored
	s.edgeMeta[e.ID] = EdgeMeta{ID: e.ID, DomainID: e.DomainID, FromID: e.FromID, ToID: e.ToID, Labels: append([]string(nil), e.Labels...), Location: loc}
	_ = s.edgeIndex.Put(context.Background(), stored)
	if !persistentAdjacencyIndexes {
		s.addEdgeAdjacencyIndexes(stored)
	}
	for _, idx := range s.configuredIndexes[e.DomainID] {
		_ = s.addEdgePropertyIndexEntry(stored, idx)
	}
	if graph.EdgeHasLabels(e, []string{"contains"}) {
		s.containsChildren[e.FromID] = append(s.containsChildren[e.FromID], e.ID)
		s.containsParent[e.ToID] = e.ID
		s.sortChildren(e.FromID)
	}
}

func (s *LocalStore) latestCheckpointDir() (string, error) {
	root := filepath.Join(s.path, "checkpoints")
	raw, err := os.ReadFile(filepath.Join(root, checkpointLatestPointer))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	name := strings.TrimSpace(string(raw))
	if name == "" || strings.Contains(name, "/") || strings.Contains(name, string(os.PathSeparator)) || strings.Contains(name, "..") {
		return "", fmt.Errorf("%w: invalid checkpoint pointer", ErrInvalidRecord)
	}
	return filepath.Join(root, name), nil
}

func (s *LocalStore) validateCheckpointManifest(manifest CheckpointManifest) error {
	if manifest.FormatVersion != checkpointManifestVersion {
		return fmt.Errorf("%w: unsupported checkpoint format %d", ErrUnsupported, manifest.FormatVersion)
	}
	if manifest.GraphRevision == 0 && (manifest.NodeCount > 0 || manifest.EdgeCount > 0) {
		return fmt.Errorf("%w: checkpoint has entities at revision 0", ErrInvalidRecord)
	}
	if manifest.NodeCount < 0 || manifest.EdgeCount < 0 {
		return fmt.Errorf("%w: negative checkpoint counts", ErrInvalidRecord)
	}
	if manifest.ChecksumAlgorithm != checkpointChecksumAlgo {
		return fmt.Errorf("%w: unsupported checkpoint checksum algorithm", ErrUnsupported)
	}
	if manifest.SpaceID != "" || manifest.DomainID != "" {
		spaceID, domainID := inferCheckpointStoreIDs(s.path)
		if manifest.SpaceID != "" && spaceID != "" && manifest.SpaceID != spaceID {
			return fmt.Errorf("%w: checkpoint space mismatch", ErrInvalidRecord)
		}
		if manifest.DomainID != "" && domainID != "" && manifest.DomainID != domainID {
			return fmt.Errorf("%w: checkpoint domain mismatch", ErrInvalidRecord)
		}
	}
	if err := validateCheckpointSegmentOffsets(s.manifest.TxnSegments, manifest.AppliedSegmentOffsets.Txns); err != nil {
		return err
	}
	if err := validateCheckpointSegmentOffsets(s.manifest.NodeSegments, manifest.AppliedSegmentOffsets.Nodes); err != nil {
		return err
	}
	if err := validateCheckpointSegmentOffsets(s.manifest.EdgeSegments, manifest.AppliedSegmentOffsets.Edges); err != nil {
		return err
	}
	return nil
}

func validateCheckpointSegmentOffsets(segments []string, offsets []CheckpointSegmentOffset) error {
	if len(segments) != len(offsets) {
		return fmt.Errorf("%w: checkpoint segment offset count mismatch", ErrInvalidRecord)
	}
	seen := map[string]struct{}{}
	for _, offset := range offsets {
		if offset.Offset < segmentHeaderLen {
			return fmt.Errorf("%w: checkpoint offset before segment header", ErrInvalidRecord)
		}
		seen[offset.Segment] = struct{}{}
	}
	for _, seg := range segments {
		if _, ok := seen[seg]; !ok {
			return fmt.Errorf("%w: checkpoint missing segment offset %s", ErrInvalidRecord, seg)
		}
	}
	return nil
}

func (s *LocalStore) replayCheckpointTail(ctx context.Context, state CheckpointSegmentState) error {
	commitRevision := map[uuid.UUID]uint64{}
	committed := map[uuid.UUID]struct{}{}
	for _, seg := range s.manifest.TxnSegments {
		if err := ctx.Err(); err != nil {
			return err
		}
		offset, ok := checkpointOffsetFor(state.Txns, seg)
		if !ok {
			return fmt.Errorf("%w: missing txn checkpoint offset %s", ErrInvalidRecord, seg)
		}
		if err := scanSegmentFrom(filepath.Join(s.path, seg), SegmentKindTxn, s.encryption, offset, func(r scannedRecord) error {
			if r.header.kind == RecordKindTxnCommit {
				if _, exists := committed[r.header.txnID]; !exists {
					s.revision++
					commitRevision[r.header.txnID] = s.revision
				}
				committed[r.header.txnID] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, seg := range s.manifest.NodeSegments {
		if err := ctx.Err(); err != nil {
			return err
		}
		offset, ok := checkpointOffsetFor(state.Nodes, seg)
		if !ok {
			return fmt.Errorf("%w: missing node checkpoint offset %s", ErrInvalidRecord, seg)
		}
		if err := scanSegmentFrom(filepath.Join(s.path, seg), SegmentKindNode, s.encryption, offset, func(r scannedRecord) error {
			rev, ok := commitRevision[r.header.txnID]
			if !ok {
				return nil
			}
			switch r.header.kind {
			case RecordKindNodePut:
				n, err := decodeNode(r.payload)
				if err != nil {
					return err
				}
				s.applyNodePut(n, r.location)
				s.nodeModRev[n.ID] = rev
			case RecordKindNodeTombstone:
				id := graph.NodeID(r.header.entityID)
				s.applyNodeDelete(id, r.location)
				s.nodeModRev[id] = rev
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, seg := range s.manifest.EdgeSegments {
		if err := ctx.Err(); err != nil {
			return err
		}
		offset, ok := checkpointOffsetFor(state.Edges, seg)
		if !ok {
			return fmt.Errorf("%w: missing edge checkpoint offset %s", ErrInvalidRecord, seg)
		}
		if err := scanSegmentFrom(filepath.Join(s.path, seg), SegmentKindEdge, s.encryption, offset, func(r scannedRecord) error {
			rev, ok := commitRevision[r.header.txnID]
			if !ok {
				return nil
			}
			switch r.header.kind {
			case RecordKindEdgePut:
				e, err := decodeEdge(r.payload)
				if err != nil {
					return err
				}
				s.applyEdgePut(e, r.location)
				s.edgeModRev[e.ID] = rev
			case RecordKindEdgeTombstone:
				id := graph.EdgeID(r.header.entityID)
				s.applyEdgeDelete(id, r.location)
				s.edgeModRev[id] = rev
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func checkpointOffsetFor(offsets []CheckpointSegmentOffset, segment string) (int64, bool) {
	for _, offset := range offsets {
		if offset.Segment == segment {
			return offset.Offset, true
		}
	}
	return 0, false
}

func writeCheckpointNodes(path string, nodes []graph.Node) error {
	return writeCheckpointPayload(path, checkpointKindNode, len(nodes), func(w io.Writer) error {
		for _, node := range nodes {
			payload, err := encodeNode(node)
			if err != nil {
				return err
			}
			if err := writeCheckpointPayloadRecord(w, payload); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeCheckpointEdges(path string, edges []graph.Edge) error {
	return writeCheckpointPayload(path, checkpointKindEdge, len(edges), func(w io.Writer) error {
		for _, edge := range edges {
			payload, err := encodeEdge(edge)
			if err != nil {
				return err
			}
			if err := writeCheckpointPayloadRecord(w, payload); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeCheckpointPayload(path string, kind uint8, count int, writeRecords func(io.Writer) error) error {
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	header := make([]byte, segmentHeaderLen)
	copy(header[segmentMagicOffset:segmentVersionOffset], checkpointMagic[:])
	binary.BigEndian.PutUint16(header[segmentVersionOffset:segmentKindOffset], checkpointPayloadVersion)
	header[segmentKindOffset] = kind
	if _, err := bw.Write(header); err != nil {
		_ = f.Close()
		return err
	}
	if err := binary.Write(bw, binary.BigEndian, uint64(count)); err != nil {
		_ = f.Close()
		return err
	}
	if err := writeRecords(bw); err != nil {
		_ = f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func writeCheckpointPayloadRecord(w io.Writer, payload []byte) error {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readCheckpointNodes(path string) ([]graph.Node, error) {
	payloads, err := readCheckpointPayload(path, checkpointKindNode)
	if err != nil {
		return nil, err
	}
	nodes := make([]graph.Node, 0, len(payloads))
	for _, payload := range payloads {
		node, err := decodeNode(payload)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func readCheckpointEdges(path string) ([]graph.Edge, error) {
	payloads, err := readCheckpointPayload(path, checkpointKindEdge)
	if err != nil {
		return nil, err
	}
	edges := make([]graph.Edge, 0, len(payloads))
	for _, payload := range payloads {
		edge, err := decodeEdge(payload)
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func readCheckpointPayload(path string, wantKind uint8) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	header := make([]byte, segmentHeaderLen)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	if string(header[segmentMagicOffset:segmentVersionOffset]) != string(checkpointMagic[:]) || binary.BigEndian.Uint16(header[segmentVersionOffset:segmentKindOffset]) != checkpointPayloadVersion || header[segmentKindOffset] != wantKind {
		return nil, fmt.Errorf("%w: bad checkpoint payload header", ErrInvalidRecord)
	}
	var count uint64
	if err := binary.Read(f, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	if count > maxEncodedMapEntries {
		return nil, fmt.Errorf("%w: checkpoint payload count too large", ErrInvalidRecord)
	}
	out := make([][]byte, 0, count)
	for i := uint64(0); i < count; i++ {
		recHeader := make([]byte, 8)
		if _, err := io.ReadFull(f, recHeader); err != nil {
			return nil, err
		}
		length := binary.BigEndian.Uint32(recHeader[0:4])
		crc := binary.BigEndian.Uint32(recHeader[4:8])
		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(f, payload); err != nil {
				return nil, err
			}
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return nil, fmt.Errorf("%w: checkpoint payload crc mismatch", ErrInvalidRecord)
		}
		out = append(out, payload)
	}
	trail := make([]byte, 1)
	if n, err := f.Read(trail); err != io.EOF || n != 0 {
		if err == nil {
			return nil, fmt.Errorf("%w: trailing checkpoint payload bytes", ErrInvalidRecord)
		}
		return nil, err
	}
	return out, nil
}

func checkpointNodeChecksum(nodes []graph.Node) (string, error) {
	h := sha256.New()
	for _, node := range nodes {
		payload, err := encodeNode(node)
		if err != nil {
			return "", err
		}
		writeChecksumRecord(h, payload)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func checkpointEdgeChecksum(edges []graph.Edge) (string, error) {
	h := sha256.New()
	for _, edge := range edges {
		payload, err := encodeEdge(edge)
		if err != nil {
			return "", err
		}
		writeChecksumRecord(h, payload)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeChecksumRecord(w io.Writer, payload []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	_, _ = w.Write(lenBuf[:])
	_, _ = w.Write(payload)
}

func checkpointGraphChecksum(nodeChecksum, edgeChecksum string) string {
	h := sha256.Sum256([]byte(nodeChecksum + "\n" + edgeChecksum))
	return hex.EncodeToString(h[:])
}

func writeFileSync(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func inferCheckpointStoreIDs(path string) (spaceID, domainID string) {
	clean := filepath.Clean(path)
	parts := strings.Split(clean, string(os.PathSeparator))
	for i := 0; i+3 < len(parts); i++ {
		if parts[i] == "graphs" && parts[i+2] == "domains" {
			return parts[i+1], parts[i+3]
		}
	}
	return "", ""
}

func canonicalizeCheckpointNodes(nodes []graph.Node) []graph.Node {
	out := make([]graph.Node, len(nodes))
	for i, node := range nodes {
		out[i] = cloneNode(node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}

func canonicalizeCheckpointEdges(edges []graph.Edge) []graph.Edge {
	out := make([]graph.Edge, len(edges))
	for i, edge := range edges {
		out[i] = cloneEdge(edge)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}

func checkpointPayloadBytesForTest(nodes []graph.Node, edges []graph.Edge) ([]byte, error) {
	var b bytes.Buffer
	for _, node := range canonicalizeCheckpointNodes(nodes) {
		payload, err := encodeNode(node)
		if err != nil {
			return nil, err
		}
		b.Write(payload)
	}
	for _, edge := range canonicalizeCheckpointEdges(edges) {
		payload, err := encodeEdge(edge)
		if err != nil {
			return nil, err
		}
		b.Write(payload)
	}
	return b.Bytes(), nil
}
