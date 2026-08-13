package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/identity/model"
	semanticmodel "github.com/myceldb/mycel/internal/semantic/model"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	spacemodel "github.com/myceldb/mycel/internal/space/model"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
)

func TestSyntheticLogseqDatastoreImportWithSemanticMaintenance(t *testing.T) {
	cfg := syntheticLogseqImportConfig{
		Pages:         4,
		BlocksPerPage: 10,
		RefsPerBlock:  1,
		ChunkOps:      50,
		Partitions:    4,
		Timeout:       25 * time.Second,
	}
	if os.Getenv("MYCEL_RUN_LARGE_LOGSEQ_IMPORT_TEST") == "1" {
		cfg.Pages = intEnv("MYCEL_LOGSEQ_IMPORT_PAGES", 100)
		cfg.BlocksPerPage = intEnv("MYCEL_LOGSEQ_IMPORT_BLOCKS_PER_PAGE", 100)
		cfg.RefsPerBlock = intEnv("MYCEL_LOGSEQ_IMPORT_REFS_PER_BLOCK", 2)
		cfg.ChunkOps = intEnv("MYCEL_LOGSEQ_IMPORT_CHUNK_OPS", 200)
		cfg.Partitions = uint32(intEnv("MYCEL_LOGSEQ_IMPORT_PARTITIONS", 16))
		cfg.Timeout = time.Duration(intEnv("MYCEL_LOGSEQ_IMPORT_TIMEOUT_SECONDS", 300)) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	cluster := newPhaseDIntegrationCluster(t, cfg.Partitions)
	cluster.semanticConfig = semanticservice.Config{MaintenanceConfig: semanticservice.MaintenanceConfig{
		Enabled:          true,
		AnalyzerInterval: time.Hour,
		WorkerInterval:   time.Hour,
		WorkerCount:      1,
		MaxBatchSize:     cfg.ChunkOps,
	}}
	cluster.start(ctx, t)
	defer cluster.close()
	cluster.waitForLeaders(ctx, t)
	cluster.installSemanticDirtyEventSinks()

	created, err := cluster.nodes[1].space.CreateSpaceWithResult(ctx, spaceservice.CreateSpaceInput{
		Name:              "synthetic-logseq-import",
		OwnerPrincipalID:  identity.PrincipalID(uuid.NewString()),
		DefaultDomainKey:  "logseq",
		DefaultDomainName: "Logseq",
		CommandID:         "synthetic-logseq-space",
	})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	spaceID := created.Space.SpaceID.String()
	domainID := created.Domain.ID
	cluster.waitForSpace(ctx, t, spaceID)

	semanticLeader := cluster.leaderForSpace(ctx, t, spaceID)
	index := cluster.createSyntheticSemanticIndex(ctx, t, semanticLeader, created.Space.SpaceID, domainID)
	cluster.waitForSemanticIndex(ctx, t, created.Space.SpaceID, index.ID)

	ops := buildSyntheticLogseqOps(cfg, domainID)
	result := cluster.applySyntheticLogseqOps(ctx, t, spaceID, domainID.String(), cfg.ChunkOps, ops)
	if result.Commits == 0 {
		t.Fatalf("synthetic import made no commits")
	}
	if result.CommittedOps != len(ops) {
		t.Fatalf("synthetic import committed %d ops, want %d", result.CommittedOps, len(ops))
	}
	cluster.waitForGraphRevision(ctx, t, spaceID, result.FinalRevision)
	cluster.waitForSemanticDirtyEvents(ctx, t, created.Space.SpaceID, result.Commits)

	analyze, err := cluster.nodes[semanticLeader].semantic.AnalyzeDirtyWork(ctx, semanticservice.AnalyzeInput{SpaceID: created.Space.SpaceID, SemanticIndexID: index.ID})
	if err != nil {
		t.Fatalf("AnalyzeDirtyWork() error = %v", err)
	}
	if analyze.ProcessedEvents == 0 || analyze.EnqueuedItems == 0 {
		t.Fatalf("AnalyzeDirtyWork() = %+v, want processed events and enqueued work", analyze)
	}
	cluster.waitForSemanticWork(ctx, t, created.Space.SpaceID, index.ID)

	nodes, edges := cluster.countGraph(ctx, t, spaceID, domainID.String(), result.FinalRevision)
	if nodes != result.ExpectedNodes || edges != result.ExpectedEdges {
		t.Fatalf("final graph counts nodes=%d edges=%d, want nodes=%d edges=%d", nodes, edges, result.ExpectedNodes, result.ExpectedEdges)
	}
	for id, node := range cluster.nodes {
		if err := node.graph.LastGraphChangeSinkError(); err != nil {
			t.Fatalf("node %d graph change sink error: %v", id, err)
		}
	}
	t.Logf("synthetic Logseq import committed pages=%d blocks_per_page=%d ops=%d commits=%d final_revision=%d semantic_events=%d semantic_work=%d", cfg.Pages, cfg.BlocksPerPage, len(ops), result.Commits, result.FinalRevision, analyze.ProcessedEvents, analyze.EnqueuedItems)
}

type syntheticLogseqImportConfig struct {
	Pages         int
	BlocksPerPage int
	RefsPerBlock  int
	ChunkOps      int
	Partitions    uint32
	Timeout       time.Duration
}

type syntheticLogseqResult struct {
	Commits       int
	FinalRevision int64
	ExpectedNodes int
	ExpectedEdges int
	CommittedOps  int
}

type syntheticLogseqOp struct {
	Kind       string
	ID         string
	FromID     string
	ToID       string
	Labels     []string
	Properties map[string]any
	Content    string
}

func buildSyntheticLogseqOps(cfg syntheticLogseqImportConfig, domainID graphmodel.DomainID) []syntheticLogseqOp {
	ops := make([]syntheticLogseqOp, 0, cfg.Pages+cfg.Pages*cfg.BlocksPerPage*(2+cfg.RefsPerBlock))
	for p := 0; p < cfg.Pages; p++ {
		ops = append(ops, syntheticLogseqOp{
			Kind:   "node",
			ID:     syntheticLogseqUUID("page", p),
			Labels: []string{"Page"},
			Properties: map[string]any{
				"record_type": "page",
				"title":       fmt.Sprintf("Page %03d", p),
				"path":        fmt.Sprintf("pages/page-%03d.md", p),
				"domain_id":   domainID.String(),
			},
			Content: fmt.Sprintf("Page %03d", p),
		})
	}
	for p := 0; p < cfg.Pages; p++ {
		pageID := syntheticLogseqUUID("page", p)
		for b := 0; b < cfg.BlocksPerPage; b++ {
			blockID := syntheticLogseqUUID("block", p, b)
			parentID := pageID
			depth := b % 5
			if depth > 0 {
				parentID = syntheticLogseqUUID("block", p, b-1)
			}
			ops = append(ops, syntheticLogseqOp{
				Kind:   "node",
				ID:     blockID,
				Labels: []string{"Block"},
				Properties: map[string]any{
					"record_type": "block",
					"page":        fmt.Sprintf("Page %03d", p),
					"page_index":  p,
					"block_index": b,
					"depth":       depth,
					"tags":        []any{fmt.Sprintf("tag-%02d", (p+b)%13)},
				},
				Content: fmt.Sprintf("- synthetic Logseq block p=%03d b=%03d #tag-%02d", p, b, (p+b)%13),
			})
			ops = append(ops, syntheticLogseqOp{
				Kind:   "edge",
				ID:     syntheticLogseqUUID("contains", p, b),
				FromID: parentID,
				ToID:   blockID,
				Labels: []string{"contains"},
				Properties: map[string]any{
					"order": b,
					"depth": depth,
				},
			})
			for r := 0; r < cfg.RefsPerBlock; r++ {
				targetPage := (p + b + r) % cfg.Pages
				ops = append(ops, syntheticLogseqOp{
					Kind:   "edge",
					ID:     syntheticLogseqUUID("ref", p, b, r),
					FromID: blockID,
					ToID:   syntheticLogseqUUID("page", targetPage),
					Labels: []string{"references"},
					Properties: map[string]any{
						"kind": "page-ref",
					},
				})
			}
		}
	}
	return ops
}

func (c *phaseDIntegrationCluster) installSemanticDirtyEventSinks() {
	for _, node := range c.nodes {
		n := node
		n.graph.SetChangeSink(graphchange.SinkFunc(func(ctx context.Context, event graphchange.CommittedEvent) error {
			appender, err := n.semantic.DirtyEventAppender(ctx, event.SpaceID)
			if err != nil {
				return err
			}
			return appender.OnGraphCommitted(ctx, event)
		}))
	}
}

func (c *phaseDIntegrationCluster) createSyntheticSemanticIndex(ctx context.Context, t *testing.T, nodeID consensus.NodeID, spaceID spacemodel.SpaceID, domainID graphmodel.DomainID) semanticmodel.SemanticIndex {
	t.Helper()
	mgr, err := c.nodes[nodeID].semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("SpaceManager() error = %v", err)
	}
	idx, err := mgr.UpsertSemanticIndex(ctx, semanticmodel.SemanticIndex{
		ID:              semanticmodel.SemanticIndexID(uuid.MustParse(syntheticLogseqUUID("semantic-index", 0))),
		SpaceID:         spaceID,
		DomainID:        domainID,
		Key:             "synthetic-logseq",
		Name:            "Synthetic Logseq",
		Purpose:         semanticmodel.SemanticIndexPurposeSearch,
		SourcePolicy:    semanticmodel.SemanticSourcePolicy{Extraction: semanticmodel.SourceExtractionSelf},
		ModelEndpointID: uuid.MustParse(syntheticLogseqUUID("model-endpoint", 0)),
		ModelID:         uuid.MustParse(syntheticLogseqUUID("model", 0)),
		VectorStoreID:   semanticmodel.VectorStoreID(uuid.MustParse(syntheticLogseqUUID("vector-store", 0))),
		Enabled:         true,
	})
	if err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	return idx
}

func (c *phaseDIntegrationCluster) applySyntheticLogseqOps(ctx context.Context, t *testing.T, spaceID, domainID string, chunkOps int, ops []syntheticLogseqOp) syntheticLogseqResult {
	t.Helper()
	leader := c.leaderForSpace(ctx, t, spaceID)
	graph := c.nodes[leader].graph
	result := syntheticLogseqResult{}
	for start := 0; start < len(ops); start += chunkOps {
		end := start + chunkOps
		if end > len(ops) {
			end = len(ops)
		}
		rev, err := graph.CurrentRevision(ctx, spaceID)
		if err != nil {
			t.Fatalf("CurrentRevision() before ops[%d:%d] error = %v", start, end, err)
		}
		tx := phaseDGraphTx(spaceID, domainID, rev)
		for i, op := range ops[start:end] {
			switch op.Kind {
			case "node":
				if _, err := graph.CreateNode(ctx, tx, graphservice.NodeInput{NodeID: op.ID, Labels: op.Labels, Properties: op.Properties, Content: op.Content}); err != nil {
					t.Fatalf("CreateNode op %d (%s) error = %v", start+i, op.ID, err)
				}
				result.ExpectedNodes++
			case "edge":
				if _, err := graph.CreateEdge(ctx, tx, graphservice.EdgeInput{EdgeID: op.ID, FromNodeID: op.FromID, ToNodeID: op.ToID, Labels: op.Labels, Properties: op.Properties}); err != nil {
					t.Fatalf("CreateEdge op %d (%s %s->%s labels=%v) error = %v", start+i, op.ID, op.FromID, op.ToID, op.Labels, err)
				}
				result.ExpectedEdges++
			default:
				t.Fatalf("unknown synthetic op kind %q", op.Kind)
			}
		}
		commit, err := graph.CommitTransactionGraph(ctx, tx)
		if err != nil {
			t.Fatalf("CommitTransactionGraph() for ops[%d:%d] error = %v", start, end, err)
		}
		wantOps := int32(end - start)
		if commit.OperationCount != wantOps {
			t.Fatalf("CommitTransactionGraph() for ops[%d:%d] operation_count=%d, want %d", start, end, commit.OperationCount, wantOps)
		}
		result.Commits++
		result.CommittedOps += int(commit.OperationCount)
		result.FinalRevision = commit.CommittedRevision
	}
	return result
}

func (c *phaseDIntegrationCluster) waitForGraphRevision(ctx context.Context, t *testing.T, spaceID string, revision int64) {
	t.Helper()
	leader := c.leaderForSpace(ctx, t, spaceID)
	for _, node := range c.nodes {
		node.graph.EnableExperimentalRaftNetworking(leader, nil, "")
	}
	defer c.configureGraphLocalIDs()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			got, err := syntheticLogseqLocalRevision(ctx, node.graph, spaceID)
			if err != nil || got < revision {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("graph revision %d did not converge: %v", revision, err)
	}
}

func (c *phaseDIntegrationCluster) waitForSemanticIndex(ctx context.Context, t *testing.T, spaceID spacemodel.SpaceID, indexID semanticmodel.SemanticIndexID) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			mgr, err := node.semantic.SpaceManager(ctx, spaceID)
			if err != nil {
				return false
			}
			indexes, err := mgr.ListSemanticIndexes(ctx)
			if err != nil {
				return false
			}
			found := false
			for _, idx := range indexes {
				if idx.ID == indexID {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("semantic index %s did not converge: %v", indexID, err)
	}
}

func (c *phaseDIntegrationCluster) waitForSemanticDirtyEvents(ctx context.Context, t *testing.T, spaceID spacemodel.SpaceID, want int) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			mgr, err := node.semantic.MaintenanceManager(ctx, spaceID)
			if err != nil {
				return false
			}
			events, err := mgr.ListGraphDirtyEvents(ctx)
			if err != nil || len(events) < want {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("semantic dirty events did not converge to %d: %v", want, err)
	}
}

func (c *phaseDIntegrationCluster) waitForSemanticWork(ctx context.Context, t *testing.T, spaceID spacemodel.SpaceID, indexID semanticmodel.SemanticIndexID) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			mgr, err := node.semantic.MaintenanceManager(ctx, spaceID)
			if err != nil {
				return false
			}
			items, err := mgr.ListDirtyWorkItems(ctx)
			if err != nil {
				return false
			}
			for _, item := range items {
				if item.SemanticIndexID == indexID {
					return true
				}
			}
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("semantic dirty work for index %s did not converge: %v", indexID, err)
	}
}

func (c *phaseDIntegrationCluster) countGraph(ctx context.Context, t *testing.T, spaceID, domainID string, revision int64) (int, int) {
	t.Helper()
	leader := c.leaderForSpace(ctx, t, spaceID)
	graph := c.nodes[leader].graph
	tx := syntheticLogseqReadTx(spaceID, domainID, revision)
	nodes := 0
	for token := ""; ; {
		page, next, err := graph.ListNodes(ctx, tx, 500, token)
		if err != nil {
			t.Fatalf("ListNodes() error = %v", err)
		}
		nodes += len(page)
		if next == "" {
			break
		}
		token = next
	}
	edges := 0
	for token := ""; ; {
		page, next, err := graph.ListEdges(ctx, tx, 500, token)
		if err != nil {
			t.Fatalf("ListEdges() error = %v", err)
		}
		edges += len(page)
		if next == "" {
			break
		}
		token = next
	}
	return nodes, edges
}

func syntheticLogseqReadTx(spaceID, domainID string, revision int64) sessionservice.GraphTransaction {
	tx := phaseDGraphTx(spaceID, domainID, revision)
	tx.Mode = sessionservice.TransactionModeReadOnly
	return tx
}

func syntheticLogseqLocalRevision(ctx context.Context, graph *graphservice.Module, spaceID string) (int64, error) {
	tx := phaseDGraphTx(spaceID, uuid.Nil.String(), 0)
	req := map[string]any{"op": "current_revision", "space_id": spaceID, "tx": map[string]any{"ID": tx.ID, "SessionID": tx.SessionID, "PrincipalID": tx.PrincipalID, "SpaceID": tx.SpaceID, "DomainID": tx.DomainID, "Mode": string(tx.Mode), "State": string(tx.State), "BaseRevision": tx.BaseRevision}}
	payload, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	raw, err := graph.ExecuteLocalRaftGraphRead(ctx, spaceID, payload)
	if err != nil {
		return 0, err
	}
	var out struct {
		Revision int64 `json:"revision"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, err
	}
	return out.Revision, nil
}

func syntheticLogseqUUID(parts ...any) string {
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, fmt.Sprint(part))
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("mycel/synthetic-logseq/"+strings.Join(values, "/"))).String()
}

func intEnv(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
