package client

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	graphsvc "github.com/myceldb/mycel/internal/graph/service"
	hybridsearch "github.com/myceldb/mycel/internal/search/hybrid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
)

func TestSemanticSearchHydratesHitsAtCurrentGraphRevision(t *testing.T) {
	ctx := context.Background()
	graphs := newSearchHydrationGraphModule(t, ctx)
	spaceID := uuid.New()
	domainID := uuid.New()

	writeTx := searchHydrationGraphTx(spaceID.String(), domainID.String(), 0)
	node, err := graphs.CreateNode(ctx, writeTx, graphsvc.NodeInput{Content: "semantic hit", Props: map[string]any{"kind": "note"}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	commit, err := graphs.CommitTransactionGraph(ctx, writeTx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	if commit.CommittedRevision <= 0 {
		t.Fatalf("CommittedRevision = %d, want > 0", commit.CommittedRevision)
	}

	service := &SemanticService{graphs: graphs}
	nodes, warnings := service.loadSearchNodes(ctx, uuid.NewString(), spaceID, domainID, []semanticsearch.SearchResult{{NodeID: node.ID, RecordID: domainsemantic.AdvancedEmbeddingRecordID(uuid.New())}})
	if len(warnings) != 0 {
		t.Fatalf("loadSearchNodes() warnings = %v, want none", warnings)
	}
	got := nodes[node.ID]
	if got == nil {
		t.Fatalf("loadSearchNodes() did not hydrate node %s", node.ID)
	}
	if text := got.GetPayload().GetFields()["text"].GetStringValue(); text != "semantic hit" {
		t.Fatalf("hydrated node payload text = %q, want %q", text, "semantic hit")
	}
}

func TestHybridSearchHydratesHitsAtCurrentGraphRevision(t *testing.T) {
	ctx := context.Background()
	graphs := newSearchHydrationGraphModule(t, ctx)
	spaceID := uuid.New()
	domainID := uuid.New()

	writeTx := searchHydrationGraphTx(spaceID.String(), domainID.String(), 0)
	node, err := graphs.CreateNode(ctx, writeTx, graphsvc.NodeInput{Content: "hybrid hit"})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := graphs.CommitTransactionGraph(ctx, writeTx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}

	service := &SearchService{graphs: graphs}
	nodes, warnings := service.loadHybridNodes(ctx, uuid.NewString(), spaceID, domainID, []hybridsearch.Result{{NodeID: node.ID.String(), Score: 1}})
	if len(warnings) != 0 {
		t.Fatalf("loadHybridNodes() warnings = %v, want none", warnings)
	}
	got, ok := nodes[node.ID.String()]
	if !ok {
		t.Fatalf("loadHybridNodes() did not hydrate node %s", node.ID)
	}
	if got.Content != "hybrid hit" {
		t.Fatalf("hydrated node content = %q, want %q", got.Content, "hybrid hit")
	}
}

func newSearchHydrationGraphModule(t *testing.T, ctx context.Context) *graphsvc.Module {
	t.Helper()
	m := graphsvc.NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	return m
}

func searchHydrationGraphTx(spaceID string, domainID string, baseRevision int64) daemonsession.GraphTransaction {
	now := time.Now().UTC()
	return daemonsession.GraphTransaction{ID: uuid.NewString(), SessionID: uuid.NewString(), PrincipalID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, Mode: daemonsession.TransactionModeReadWrite, State: daemonsession.TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(time.Hour)}
}
