package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const GraphChecksumAlgorithmV1 = "graph-v1-sha256"

// LocalGraphStats is a deterministic, read-only summary of the committed graph
// state currently visible on one daemon for one space/domain. It is intended for
// Phase G divergence diagnostics; it is local evidence, not by itself a
// cluster-level consistency proof.
type LocalGraphStats struct {
	SpaceID           string
	DomainID          string
	PartitionID       uint32
	Revision          uint64
	NodeCount         int
	EdgeCount         int
	NodeChecksum      string
	EdgeChecksum      string
	GraphChecksum     string
	ChecksumAlgorithm string
	CollectedAt       time.Time
	Source            string
}

// LocalGraphConsistencyStats returns deterministic latest-state graph counts
// and checksums for the local committed store. It intentionally does not route
// to a raft leader or compare peers; later Phase G tranches aggregate these
// local reports with raft status to distinguish lag from divergence.
func (m *Module) ExecuteLocalGraphConsistency(ctx context.Context, spaceID string, domainID string) ([]byte, error) {
	stats, err := m.LocalGraphConsistencyStats(ctx, spaceID, domainID)
	if err != nil {
		return nil, err
	}
	return json.Marshal(stats)
}

func (m *Module) LocalGraphConsistencyStats(ctx context.Context, spaceID string, domainID string) (LocalGraphStats, error) {
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	if spaceID == "" {
		return LocalGraphStats{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	parsedDomain, err := uuid.Parse(domainID)
	if err != nil || parsedDomain == uuid.Nil {
		return LocalGraphStats{}, fmt.Errorf("%w: domain_id must be a UUID", ErrInvalidInput)
	}
	store, err := m.existingStoreForConsistencyStats(ctx, spaceID)
	if err != nil {
		return LocalGraphStats{}, err
	}
	domain := domaingraph.DomainID(parsedDomain)
	nodes, err := store.ListNodesByDomain(ctx, domain)
	if err != nil {
		return LocalGraphStats{}, mapStorageError(err)
	}
	allEdges, err := store.ListEdges(ctx)
	if err != nil {
		return LocalGraphStats{}, mapStorageError(err)
	}
	edges := make([]domaingraph.Edge, 0, len(allEdges))
	for _, edge := range allEdges {
		if edge.DomainID == domain {
			edges = append(edges, edge)
		}
	}
	partitionID := uint32(0)
	if m.raftPartitionCount > 0 {
		if parsedSpace, err := uuid.Parse(spaceID); err == nil && parsedSpace != uuid.Nil {
			if cmd, err := consensus.NewSpaceCommand(domainspace.SpaceID(parsedSpace), m.raftPartitionCount, recordTypeGraphCommit, nil, "graph-consistency-stats"); err == nil {
				partitionID = cmd.PartitionID
			}
		}
	}
	return buildLocalGraphStats(spaceID, domainID, partitionID, store.Revision(), nodes, edges, time.Now().UTC())
}

type graphConsistencyManifest struct {
	NodeSegments      []string `json:"node_segments"`
	EdgeSegments      []string `json:"edge_segments"`
	TxnSegments       []string `json:"txn_segments"`
	ActiveNodeSegment string   `json:"active_node_segment"`
	ActiveEdgeSegment string   `json:"active_edge_segment"`
	ActiveTxnSegment  string   `json:"active_txn_segment"`
}

func (m *Module) existingStoreForConsistencyStats(ctx context.Context, spaceID string) (*graphstorage.LocalStore, error) {
	m.mu.Lock()
	if store := m.stores[spaceID]; store != nil {
		m.mu.Unlock()
		return store, nil
	}
	m.mu.Unlock()
	spacePath := filepath.Join(m.dataDir, spaceID)
	if err := validateExistingGraphStoreForReadOnlyOpen(spacePath); err != nil {
		return nil, err
	}
	store, err := graphstorage.Open(ctx, spacePath)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if existing := m.stores[spaceID]; existing != nil {
		m.mu.Unlock()
		_ = store.Close()
		return existing, nil
	}
	m.stores[spaceID] = store
	m.mu.Unlock()
	return store, nil
}

func validateExistingGraphStoreForReadOnlyOpen(spacePath string) error {
	manifestPath := filepath.Join(spacePath, "manifest.mycel")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: graph store manifest does not exist", ErrNotFound)
		}
		return err
	}
	var manifest graphConsistencyManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return err
	}
	segments := append([]string{}, manifest.NodeSegments...)
	segments = append(segments, manifest.EdgeSegments...)
	segments = append(segments, manifest.TxnSegments...)
	segments = append(segments, manifest.ActiveNodeSegment, manifest.ActiveEdgeSegment, manifest.ActiveTxnSegment)
	seen := map[string]struct{}{}
	for _, segment := range segments {
		if err := validateGraphStoreManifestSegment(segment); err != nil {
			return err
		}
		if _, ok := seen[segment]; ok {
			continue
		}
		seen[segment] = struct{}{}
		info, err := os.Stat(filepath.Join(spacePath, segment))
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%w: graph store segment %s does not exist", ErrNotFound, segment)
			}
			return err
		}
		if info.IsDir() || info.Size() == 0 {
			return fmt.Errorf("%w: graph store segment %s is not readable", ErrInvalidState, segment)
		}
	}
	return nil
}

func validateGraphStoreManifestSegment(segment string) error {
	if segment == "" || strings.TrimSpace(segment) != segment || strings.IndexFunc(segment, unicode.IsSpace) >= 0 {
		return fmt.Errorf("%w: graph store manifest contains invalid segment path", ErrInvalidState)
	}
	if filepath.IsAbs(segment) || filepath.Clean(segment) != segment || strings.HasPrefix(segment, ".."+string(filepath.Separator)) || segment == ".." {
		return fmt.Errorf("%w: graph store manifest contains unsafe segment path", ErrInvalidState)
	}
	return nil
}

func buildLocalGraphStats(spaceID string, domainID string, partitionID uint32, revision uint64, nodes []domaingraph.Node, edges []domaingraph.Edge, collectedAt time.Time) (LocalGraphStats, error) {
	nodeChecksum, err := checksumNodes(nodes)
	if err != nil {
		return LocalGraphStats{}, err
	}
	edgeChecksum, err := checksumEdges(edges)
	if err != nil {
		return LocalGraphStats{}, err
	}
	graphChecksum, err := checksumJSON(map[string]any{
		"algorithm":     GraphChecksumAlgorithmV1,
		"node_count":    len(nodes),
		"edge_count":    len(edges),
		"node_checksum": nodeChecksum,
		"edge_checksum": edgeChecksum,
	})
	if err != nil {
		return LocalGraphStats{}, err
	}
	return LocalGraphStats{SpaceID: strings.TrimSpace(spaceID), DomainID: strings.TrimSpace(domainID), PartitionID: partitionID, Revision: revision, NodeCount: len(nodes), EdgeCount: len(edges), NodeChecksum: nodeChecksum, EdgeChecksum: edgeChecksum, GraphChecksum: graphChecksum, ChecksumAlgorithm: GraphChecksumAlgorithmV1, CollectedAt: collectedAt.UTC(), Source: "local_latest"}, nil
}

func checksumNodes(nodes []domaingraph.Node) (string, error) {
	canonical := make([]canonicalNode, 0, len(nodes))
	for _, node := range nodes {
		canonical = append(canonical, canonicalizeNode(node))
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	return checksumJSON(canonical)
}

func checksumEdges(edges []domaingraph.Edge) (string, error) {
	canonical := make([]canonicalEdge, 0, len(edges))
	for _, edge := range edges {
		canonical = append(canonical, canonicalizeEdge(edge))
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	return checksumJSON(canonical)
}

type canonicalNode struct {
	ID         string         `json:"id"`
	DomainID   string         `json:"domain_id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
	Payload    map[string]any `json:"payload"`
	Meta       map[string]any `json:"meta"`
	BlobRef    string         `json:"blob_ref,omitempty"`
	Content    string         `json:"content"`
	Props      map[string]any `json:"props"`
	CreatedAt  string         `json:"created_at,omitempty"`
	UpdatedAt  string         `json:"updated_at,omitempty"`
}

type canonicalEdge struct {
	ID         string         `json:"id"`
	DomainID   string         `json:"domain_id"`
	FromID     string         `json:"from_id"`
	ToID       string         `json:"to_id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
	Payload    map[string]any `json:"payload"`
	Meta       map[string]any `json:"meta"`
	CreatedAt  string         `json:"created_at,omitempty"`
	UpdatedAt  string         `json:"updated_at,omitempty"`
}

func canonicalizeNode(node domaingraph.Node) canonicalNode {
	blobRef := ""
	if node.BlobRef != nil {
		blobRef = string(*node.BlobRef)
	}
	return canonicalNode{ID: node.ID.String(), DomainID: node.DomainID.String(), Labels: canonicalLabels(node.Labels), Properties: canonicalMap(node.Properties), Payload: canonicalMap(node.Payload), Meta: canonicalMap(node.Meta), BlobRef: blobRef, Content: node.Content, Props: canonicalMap(node.Props), CreatedAt: canonicalTime(node.CreatedAt), UpdatedAt: canonicalTime(node.UpdatedAt)}
}

func canonicalizeEdge(edge domaingraph.Edge) canonicalEdge {
	return canonicalEdge{ID: edge.ID.String(), DomainID: edge.DomainID.String(), FromID: edge.FromID.String(), ToID: edge.ToID.String(), Labels: canonicalLabels(edge.Labels), Properties: canonicalMap(edge.Properties), Payload: canonicalMap(edge.Payload), Meta: canonicalMap(edge.Meta), CreatedAt: canonicalTime(edge.CreatedAt), UpdatedAt: canonicalTime(edge.UpdatedAt)}
}

func canonicalLabels(labels []string) []string {
	out := normalizeLabels(labels)
	sort.Strings(out)
	return out
}

func canonicalMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = canonicalValue(value)
	}
	return out
}

func canonicalValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return canonicalMap(typed)
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, val := range typed {
			out[key] = val
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, val := range typed {
			out[i] = canonicalValue(val)
		}
		return out
	case []string:
		out := append([]string(nil), typed...)
		return out
	case time.Time:
		return canonicalTime(typed)
	default:
		return typed
	}
}

func canonicalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func checksumJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
