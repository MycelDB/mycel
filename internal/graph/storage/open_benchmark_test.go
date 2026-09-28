package graphstorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

const (
	benchmarkOpenNodes        = 10_000
	benchmarkOpenEdges        = 20_000
	benchmarkOpenUpdateRounds = 3
)

func BenchmarkLocalStoreQueryIndexOpenAndScan(b *testing.B) {
	ctx := context.Background()
	fixture := prepareQueryIndexOpenBenchmarkFixture(b, ctx, benchmarkOpenNodes, benchmarkOpenEdges)
	cases := []struct {
		name string
		path string
	}{
		{name: "full-replay-query-index-rebuild", path: fixture.fullReplayDir},
		{name: "checkpoint-query-index-rebuild", path: fixture.checkpointOnlyDir},
		{name: "checkpoint-persistent-query-indexes", path: fixture.checkpointWithIndexesDir},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(fixture.nodeCount), "live_nodes/op")
			b.ReportMetric(float64(fixture.edgeCount), "live_edges/op")
			for i := 0; i < b.N; i++ {
				store, err := Open(ctx, tc.path)
				if err != nil {
					b.Fatalf("Open() error = %v", err)
				}
				if store.Revision() != fixture.revision {
					b.Fatalf("Revision() = %d, want %d", store.Revision(), fixture.revision)
				}
				nodeEntries, _, err := store.ScanNodePropertyOrdered(ctx, OrderedNodePropertyScan{DomainID: fixture.domainID, IndexName: fixture.nodeIndexName, Direction: schema.IndexSortDirectionAsc, Limit: 100})
				if err != nil || len(nodeEntries) == 0 {
					b.Fatalf("ScanNodePropertyOrdered() len=%d err=%v", len(nodeEntries), err)
				}
				edgeEntries, _, err := store.ScanEdgePropertyOrdered(ctx, OrderedEdgePropertyScan{DomainID: fixture.domainID, IndexName: fixture.edgeIndexName, Direction: schema.IndexSortDirectionAsc, Limit: 100})
				if err != nil || len(edgeEntries) == 0 {
					b.Fatalf("ScanEdgePropertyOrdered() len=%d err=%v", len(edgeEntries), err)
				}
				if err := store.Close(); err != nil {
					b.Fatalf("Close() error = %v", err)
				}
			}
		})
	}
}

func BenchmarkLocalStoreOpen(b *testing.B) {
	ctx := context.Background()
	fixture := prepareOpenBenchmarkFixture(b, ctx, benchmarkOpenNodes, benchmarkOpenEdges)
	cases := []struct {
		name string
		path string
	}{
		{name: "full-replay", path: fixture.fullReplayDir},
		{name: "checkpoint-only", path: fixture.checkpointOnlyDir},
		{name: "checkpoint-persistent-indexes", path: fixture.checkpointWithIndexesDir},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(fixture.nodeCount), "live_nodes/op")
			b.ReportMetric(float64(fixture.edgeCount), "live_edges/op")
			b.ReportMetric(float64(fixture.historicalNodePuts), "node_put_records/op")
			for i := 0; i < b.N; i++ {
				store, err := Open(ctx, tc.path)
				if err != nil {
					b.Fatalf("Open() error = %v", err)
				}
				if store.Revision() != fixture.revision {
					b.Fatalf("Revision() = %d, want %d", store.Revision(), fixture.revision)
				}
				if err := store.Close(); err != nil {
					b.Fatalf("Close() error = %v", err)
				}
			}
		})
	}
}

type openBenchmarkFixture struct {
	fullReplayDir            string
	checkpointOnlyDir        string
	checkpointWithIndexesDir string
	domainID                 graph.DomainID
	nodeIndexName            string
	edgeIndexName            string
	nodeCount                int
	edgeCount                int
	historicalNodePuts       int
	revision                 uint64
}

func prepareOpenBenchmarkFixture(b *testing.B, ctx context.Context, nodeCount, edgeCount int) openBenchmarkFixture {
	b.Helper()
	root := b.TempDir()
	fullReplayDir := filepath.Join(root, "full-replay")
	if err := os.MkdirAll(fullReplayDir, 0o700); err != nil {
		b.Fatalf("mkdir fixture: %v", err)
	}
	domainID, revision := seedOpenBenchmarkStore(b, ctx, fullReplayDir, nodeCount, edgeCount, benchmarkOpenUpdateRounds)
	checkpointWithIndexesDir := filepath.Join(root, "checkpoint-persistent-indexes")
	if err := copyDir(fullReplayDir, checkpointWithIndexesDir); err != nil {
		b.Fatalf("copy checkpoint fixture: %v", err)
	}
	store, err := Open(ctx, checkpointWithIndexesDir)
	if err != nil {
		b.Fatalf("open checkpoint fixture: %v", err)
	}
	if err := store.WriteCheckpoint(ctx); err != nil {
		b.Fatalf("WriteCheckpoint() error = %v", err)
	}
	if err := store.Close(); err != nil {
		b.Fatalf("Close() error = %v", err)
	}
	checkpointOnlyDir := filepath.Join(root, "checkpoint-only")
	if err := copyDir(checkpointWithIndexesDir, checkpointOnlyDir); err != nil {
		b.Fatalf("copy checkpoint-only fixture: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(checkpointOnlyDir, "indexes")); err != nil {
		b.Fatalf("remove indexes from checkpoint-only fixture: %v", err)
	}
	return openBenchmarkFixture{fullReplayDir: fullReplayDir, checkpointOnlyDir: checkpointOnlyDir, checkpointWithIndexesDir: checkpointWithIndexesDir, domainID: domainID, nodeCount: nodeCount, edgeCount: edgeCount, historicalNodePuts: nodeCount * (benchmarkOpenUpdateRounds + 1), revision: revision}
}

func seedOpenBenchmarkStore(b *testing.B, ctx context.Context, dir string, nodeCount, edgeCount, updateRounds int) (graph.DomainID, uint64) {
	b.Helper()
	store, err := Open(ctx, dir)
	if err != nil {
		b.Fatalf("Open(seed) error = %v", err)
	}
	domainID := graph.DomainID(uuid.New())
	nodes := make([]graph.Node, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		labels := []string{"BenchmarkNode"}
		if i%5 == 0 {
			labels = append(labels, "BenchmarkTask")
		}
		nodes = append(nodes, graph.Node{
			ID:       graph.NodeID(uuid.New()),
			DomainID: domainID,
			Content:  fmt.Sprintf("benchmark node %d", i),
			Labels:   labels,
			Properties: map[string]any{
				graph.NodePropTags: []string{fmt.Sprintf("tag-%02d", i%32)},
				"ordinal":          i,
			},
			Payload: map[string]any{"text": fmt.Sprintf("benchmark payload %d", i)},
		})
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		b.Fatalf("Begin(seed) error = %v", err)
	}
	for _, node := range nodes {
		if err := tx.PutNode(node); err != nil {
			b.Fatalf("PutNode(seed) error = %v", err)
		}
	}
	for i := 0; i < edgeCount; i++ {
		edge := graph.Edge{
			ID:       graph.EdgeID(uuid.New()),
			DomainID: domainID,
			FromID:   nodes[i%nodeCount].ID,
			ToID:     nodes[(i*7+1)%nodeCount].ID,
			Labels:   []string{"REFERENCES"},
			Properties: map[string]any{
				"ordinal": i,
			},
		}
		if err := tx.PutEdge(edge); err != nil {
			b.Fatalf("PutEdge(seed) error = %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("Commit(seed) error = %v", err)
	}
	for round := 0; round < updateRounds; round++ {
		tx, err := store.Begin(ctx)
		if err != nil {
			b.Fatalf("Begin(update round %d) error = %v", round, err)
		}
		for i := range nodes {
			nodes[i].Content = fmt.Sprintf("benchmark node %d update round %d", i, round)
			nodes[i].Payload = map[string]any{"text": nodes[i].Content}
			if err := tx.PutNode(nodes[i]); err != nil {
				b.Fatalf("PutNode(update round %d) error = %v", round, err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit(update round %d) error = %v", round, err)
		}
	}
	revision := store.Revision()
	if err := store.Close(); err != nil {
		b.Fatalf("Close(seed) error = %v", err)
	}
	return domainID, revision
}

func prepareQueryIndexOpenBenchmarkFixture(b *testing.B, ctx context.Context, nodeCount, edgeCount int) openBenchmarkFixture {
	b.Helper()
	root := b.TempDir()
	fullReplayDir := filepath.Join(root, "full-replay-query")
	if err := os.MkdirAll(fullReplayDir, 0o700); err != nil {
		b.Fatalf("mkdir fixture: %v", err)
	}
	domainID, revision := seedOpenBenchmarkStore(b, ctx, fullReplayDir, nodeCount, edgeCount, benchmarkOpenUpdateRounds)
	store, err := Open(ctx, fullReplayDir)
	if err != nil {
		b.Fatalf("open query fixture: %v", err)
	}
	nodeIdx := schema.IndexDefinition{Name: "benchmark_nodes_by_ordinal", TargetKind: schema.IndexTargetNode, TargetType: "BenchmarkNode", Labels: []string{"BenchmarkNode"}, Field: schema.FieldPath{Namespace: "properties", Name: "ordinal"}, Kind: schema.IndexKindOrdered, Direction: schema.IndexSortDirectionAsc}
	edgeIdx := schema.IndexDefinition{Name: "benchmark_edges_by_ordinal", TargetKind: schema.IndexTargetEdge, TargetType: "REFERENCES", Labels: []string{"REFERENCES"}, Field: schema.FieldPath{Namespace: "properties", Name: "ordinal"}, Kind: schema.IndexKindOrdered, Direction: schema.IndexSortDirectionAsc}
	if err := store.ConfigureIndexes(ctx, domainID, "schema-query-benchmark", []schema.IndexDefinition{nodeIdx, edgeIdx}); err != nil {
		b.Fatalf("ConfigureIndexes() error = %v", err)
	}
	if err := store.Close(); err != nil {
		b.Fatalf("Close(query fixture) error = %v", err)
	}
	checkpointWithIndexesDir := filepath.Join(root, "checkpoint-persistent-query")
	if err := copyDir(fullReplayDir, checkpointWithIndexesDir); err != nil {
		b.Fatalf("copy query checkpoint fixture: %v", err)
	}
	store, err = Open(ctx, checkpointWithIndexesDir)
	if err != nil {
		b.Fatalf("open query checkpoint fixture: %v", err)
	}
	if err := store.WriteCheckpoint(ctx); err != nil {
		b.Fatalf("WriteCheckpoint() error = %v", err)
	}
	if err := store.Close(); err != nil {
		b.Fatalf("Close(query checkpoint fixture) error = %v", err)
	}
	checkpointOnlyDir := filepath.Join(root, "checkpoint-query-rebuild")
	if err := copyDir(checkpointWithIndexesDir, checkpointOnlyDir); err != nil {
		b.Fatalf("copy query checkpoint-only fixture: %v", err)
	}
	if err := removePersistentIndexSets(checkpointOnlyDir); err != nil {
		b.Fatalf("remove persistent query index sets: %v", err)
	}
	return openBenchmarkFixture{fullReplayDir: fullReplayDir, checkpointOnlyDir: checkpointOnlyDir, checkpointWithIndexesDir: checkpointWithIndexesDir, domainID: domainID, nodeIndexName: nodeIdx.Name, edgeIndexName: edgeIdx.Name, nodeCount: nodeCount, edgeCount: edgeCount, historicalNodePuts: nodeCount * (benchmarkOpenUpdateRounds + 1), revision: revision}
}

func removePersistentIndexSets(dir string) error {
	root := filepath.Join(dir, "indexes")
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "manifest.json" {
			continue
		}
		if name == persistentIndexLatestPointer || name == persistentIndexLatestPointer+".tmp" || entry.IsDir() && (strings.HasPrefix(name, "idx-") || strings.HasPrefix(name, ".tmp-")) {
			if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return os.MkdirAll(dst, 0o700)
		}
		target := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, info.Mode().Perm())
	})
}
