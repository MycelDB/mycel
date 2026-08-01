package service

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestLocalGraphStatsChecksumStableRegardlessOfInsertionOrder(t *testing.T) {
	spaceID := "11111111-1111-1111-1111-111111111111"
	domainID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	n1 := testChecksumNode("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", domainID, "first", []string{"Task", "Note"}, map[string]any{"nested": map[string]any{"b": 2, "a": 1}, "tags": []any{"b", "a"}})
	n2 := testChecksumNode("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", domainID, "second", []string{"Journal"}, map[string]any{"title": "hello"})
	e1 := testChecksumEdge("cccccccc-cccc-cccc-cccc-cccccccccccc", domainID, n1.ID, n2.ID, []string{"contains", "primary"}, map[string]any{"order": 1, "kind": "child"})

	statsA, err := buildLocalGraphStats(spaceID, domainID.String(), 1, 2, []domaingraph.Node{n1, n2}, []domaingraph.Edge{e1}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatalf("build stats A: %v", err)
	}
	// Same semantic graph, different entity order, label order, and map insertion order.
	n1Reordered := testChecksumNode("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", domainID, "first", []string{"Note", "Task"}, map[string]any{"tags": []any{"b", "a"}, "nested": map[string]any{"a": 1, "b": 2}})
	e1Reordered := testChecksumEdge("cccccccc-cccc-cccc-cccc-cccccccccccc", domainID, n1.ID, n2.ID, []string{"primary", "contains"}, map[string]any{"kind": "child", "order": 1})
	statsB, err := buildLocalGraphStats(spaceID, domainID.String(), 1, 2, []domaingraph.Node{n2, n1Reordered}, []domaingraph.Edge{e1Reordered}, time.Unix(20, 0).UTC())
	if err != nil {
		t.Fatalf("build stats B: %v", err)
	}
	if statsA.NodeChecksum != statsB.NodeChecksum || statsA.EdgeChecksum != statsB.EdgeChecksum || statsA.GraphChecksum != statsB.GraphChecksum {
		t.Fatalf("checksums changed for reordered equivalent graph:\nA=%#v\nB=%#v", statsA, statsB)
	}
	if statsA.ChecksumAlgorithm != GraphChecksumAlgorithmV1 || statsA.Source != "local_latest" {
		t.Fatalf("unexpected stats metadata: %#v", statsA)
	}
}

func TestLocalGraphStatsChecksumChangesWhenGraphChanges(t *testing.T) {
	spaceID := "11111111-1111-1111-1111-111111111111"
	domainID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	n1 := testChecksumNode("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", domainID, "first", []string{"Note"}, nil)
	n2 := testChecksumNode("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", domainID, "second", []string{"Note"}, nil)
	e1 := testChecksumEdge("cccccccc-cccc-cccc-cccc-cccccccccccc", domainID, n1.ID, n2.ID, []string{"contains"}, nil)
	base, err := buildLocalGraphStats(spaceID, domainID.String(), 1, 2, []domaingraph.Node{n1, n2}, []domaingraph.Edge{e1}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatalf("build base stats: %v", err)
	}

	n1Changed := n1
	n1Changed.Content = "changed"
	changedNode, err := buildLocalGraphStats(spaceID, domainID.String(), 1, 2, []domaingraph.Node{n1Changed, n2}, []domaingraph.Edge{e1}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatalf("build changed-node stats: %v", err)
	}
	if base.NodeChecksum == changedNode.NodeChecksum || base.GraphChecksum == changedNode.GraphChecksum {
		t.Fatalf("node/content change did not affect checksums: base=%#v changed=%#v", base, changedNode)
	}
	if base.EdgeChecksum != changedNode.EdgeChecksum {
		t.Fatalf("node-only change unexpectedly changed edge checksum")
	}

	e1Changed := e1
	e1Changed.ToID = n1.ID
	changedEdge, err := buildLocalGraphStats(spaceID, domainID.String(), 1, 2, []domaingraph.Node{n1, n2}, []domaingraph.Edge{e1Changed}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatalf("build changed-edge stats: %v", err)
	}
	if base.EdgeChecksum == changedEdge.EdgeChecksum || base.GraphChecksum == changedEdge.GraphChecksum {
		t.Fatalf("edge endpoint change did not affect checksums: base=%#v changed=%#v", base, changedEdge)
	}
}

func TestLocalGraphStatsEmptyDomainStable(t *testing.T) {
	spaceID := "11111111-1111-1111-1111-111111111111"
	domainID := "22222222-2222-2222-2222-222222222222"
	statsA, err := buildLocalGraphStats(spaceID, domainID, 0, 0, nil, nil, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatalf("build empty stats A: %v", err)
	}
	statsB, err := buildLocalGraphStats(spaceID, domainID, 0, 0, []domaingraph.Node{}, []domaingraph.Edge{}, time.Unix(20, 0).UTC())
	if err != nil {
		t.Fatalf("build empty stats B: %v", err)
	}
	if statsA.NodeCount != 0 || statsA.EdgeCount != 0 || statsA.NodeChecksum == "" || statsA.EdgeChecksum == "" || statsA.GraphChecksum == "" {
		t.Fatalf("unexpected empty stats: %#v", statsA)
	}
	if statsA.NodeChecksum != statsB.NodeChecksum || statsA.EdgeChecksum != statsB.EdgeChecksum || statsA.GraphChecksum != statsB.GraphChecksum {
		t.Fatalf("empty stats not stable: A=%#v B=%#v", statsA, statsB)
	}
}

func TestModuleLocalGraphConsistencyStatsDoesNotCreateMissingSpace(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	_, err := m.LocalGraphConsistencyStats(ctx, spaceID, uuid.NewString())
	if err == nil {
		t.Fatal("LocalGraphConsistencyStats() expected missing-space error")
	}
	if _, statErr := os.Stat(filepath.Join(dataDir, "graphs", spaceID)); !os.IsNotExist(statErr) {
		t.Fatalf("consistency stats created or touched missing graph store: statErr=%v", statErr)
	}
}

func TestValidateGraphStoreManifestSegmentRejectsWhitespaceAndUnsafePaths(t *testing.T) {
	for _, segment := range []string{" segments/nodes-000001.kseg", "segments/nodes-000001.kseg ", "segments/nodes 000001.kseg", "segments/nodes\t000001.kseg", "../segments/nodes-000001.kseg", "/data/nodes-000001.kseg", "segments/../nodes-000001.kseg"} {
		if err := validateGraphStoreManifestSegment(segment); err == nil {
			t.Fatalf("validateGraphStoreManifestSegment(%q) succeeded, want error", segment)
		}
	}
	if err := validateGraphStoreManifestSegment("segments/nodes-000001.kseg"); err != nil {
		t.Fatalf("validateGraphStoreManifestSegment(valid) error = %v", err)
	}
}

func TestModuleLocalGraphConsistencyStatsRejectsUnsafeManifestWithoutCreatingSegments(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	spacePath := filepath.Join(dataDir, "graphs", spaceID)
	if err := os.MkdirAll(filepath.Join(spacePath, "segments"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nodes-000001.kseg", "edges-000001.kseg", "txns-000001.kseg"} {
		if err := os.WriteFile(filepath.Join(spacePath, "segments", name), []byte("existing"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := `{"format_version":1,"node_segments":["segments/nodes-000001.kseg "],"edge_segments":["segments/edges-000001.kseg"],"txn_segments":["segments/txns-000001.kseg"],"active_node_segment":"segments/nodes-000001.kseg ","active_edge_segment":"segments/edges-000001.kseg","active_txn_segment":"segments/txns-000001.kseg"}`
	if err := os.WriteFile(filepath.Join(spacePath, "manifest.mycel"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := m.LocalGraphConsistencyStats(ctx, spaceID, uuid.NewString())
	if err == nil {
		t.Fatal("LocalGraphConsistencyStats() expected unsafe-manifest error")
	}
	if _, statErr := os.Stat(filepath.Join(spacePath, "segments", "nodes-000001.kseg ")); !os.IsNotExist(statErr) {
		t.Fatalf("consistency stats created unsafe trailing-space segment: statErr=%v", statErr)
	}
}

func TestModuleLocalGraphConsistencyStatsScansCommittedDomain(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	otherDomainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	parent, err := m.CreateNode(ctx, tx, NodeInput{NodeID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", Content: "parent", Labels: []string{"Note"}, Properties: map[string]any{"title": "parent"}})
	if err != nil {
		t.Fatalf("CreateNode(parent) error = %v", err)
	}
	child, err := m.CreateNode(ctx, tx, NodeInput{NodeID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", Content: "child", Labels: []string{"Note"}, Properties: map[string]any{"title": "child"}})
	if err != nil {
		t.Fatalf("CreateNode(child) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{EdgeID: "cccccccc-cccc-cccc-cccc-cccccccccccc", FromNodeID: parent.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph(domain) error = %v", err)
	}
	otherTx := graphTx(spaceID, otherDomainID, 1)
	if _, err := m.CreateNode(ctx, otherTx, NodeInput{NodeID: "dddddddd-dddd-dddd-dddd-dddddddddddd", Content: "other domain"}); err != nil {
		t.Fatalf("CreateNode(other domain) error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, otherTx); err != nil {
		t.Fatalf("CommitTransactionGraph(other domain) error = %v", err)
	}

	stats, err := m.LocalGraphConsistencyStats(ctx, spaceID, domainID)
	if err != nil {
		t.Fatalf("LocalGraphConsistencyStats() error = %v", err)
	}
	if stats.SpaceID != spaceID || stats.DomainID != domainID || stats.Revision != 2 || stats.NodeCount != 2 || stats.EdgeCount != 1 || stats.NodeChecksum == "" || stats.EdgeChecksum == "" || stats.GraphChecksum == "" {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	otherStats, err := m.LocalGraphConsistencyStats(ctx, spaceID, otherDomainID)
	if err != nil {
		t.Fatalf("LocalGraphConsistencyStats(other) error = %v", err)
	}
	if otherStats.NodeCount != 1 || otherStats.EdgeCount != 0 || otherStats.GraphChecksum == stats.GraphChecksum {
		t.Fatalf("unexpected other-domain stats: %#v vs %#v", otherStats, stats)
	}
}

func TestModuleLocalGraphForensicExportIsBoundedAndCanonical(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	parent, err := m.CreateNode(ctx, tx, NodeInput{NodeID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", Content: "parent", Labels: []string{"Note", "Task"}, Properties: map[string]any{"title": "parent"}})
	if err != nil {
		t.Fatalf("CreateNode(parent) error = %v", err)
	}
	child, err := m.CreateNode(ctx, tx, NodeInput{NodeID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", Content: "child", Labels: []string{"Task"}, Properties: map[string]any{"title": "child"}})
	if err != nil {
		t.Fatalf("CreateNode(child) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{EdgeID: "cccccccc-cccc-cccc-cccc-cccccccccccc", FromNodeID: parent.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	export, err := m.LocalGraphForensicExport(ctx, spaceID, domainID, LocalGraphForensicExportOptions{PageSize: 2})
	if err != nil {
		t.Fatalf("LocalGraphForensicExport() error = %v", err)
	}
	if !export.Truncated || export.NextPageToken == "" || export.Stats.NodeCount != 2 || export.Stats.EdgeCount != 1 || len(export.Nodes) != 2 || len(export.Edges) != 0 {
		t.Fatalf("unexpected first page export: %#v", export)
	}
	if export.Nodes[0].ID != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" || export.Nodes[0].Checksum == "" || export.Nodes[0].CanonicalJSON == "" {
		t.Fatalf("unexpected canonical node export: %#v", export.Nodes)
	}
	second, err := m.LocalGraphForensicExport(ctx, spaceID, domainID, LocalGraphForensicExportOptions{PageSize: 2, PageToken: export.NextPageToken})
	if err != nil {
		t.Fatalf("LocalGraphForensicExport(second) error = %v", err)
	}
	if second.Truncated || second.NextPageToken != "" || len(second.Nodes) != 0 || len(second.Edges) != 1 || second.Edges[0].ID != "cccccccc-cccc-cccc-cccc-cccccccccccc" {
		t.Fatalf("unexpected second page export: %#v", second)
	}
}

func testChecksumNode(id string, domainID uuid.UUID, content string, labels []string, props map[string]any) domaingraph.Node {
	return domaingraph.Node{ID: uuid.MustParse(id), DomainID: domaingraph.DomainID(domainID), Content: content, Labels: labels, Properties: props, Payload: map[string]any{}, Meta: map[string]any{"source": "test"}, Props: map[string]any{}, CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)}
}

func testChecksumEdge(id string, domainID uuid.UUID, fromID domaingraph.NodeID, toID domaingraph.NodeID, labels []string, props map[string]any) domaingraph.Edge {
	return domaingraph.Edge{ID: uuid.MustParse(id), DomainID: domaingraph.DomainID(domainID), FromID: fromID, ToID: toID, Labels: labels, Properties: props, Payload: map[string]any{}, Meta: map[string]any{"source": "test"}, CreatedAt: time.Date(2026, 1, 2, 3, 4, 7, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 2, 3, 4, 8, 0, time.UTC)}
}
