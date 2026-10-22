package client

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	graphsvc "github.com/myceldb/mycel/internal/graph/service"
	hybridsearch "github.com/myceldb/mycel/internal/search/hybrid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSemanticSearchCancellationDiagnosticUsesBoundedStageWhileRequestRemainsBlocked(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	trace, _ := beginSemanticSearchCancellationTrace(ctx, logger, "authorize-domain", "semantic search request canceled")
	trace.setStage("hydrate-graph-results")
	cancel()
	<-trace.done
	got := logs.String()
	if !strings.Contains(got, "semantic search request canceled") || !strings.Contains(got, "stage=hydrate-graph-results") || !strings.Contains(got, "failure_class=canceled") {
		t.Fatalf("cancellation diagnostic = %q", got)
	}
	for _, forbidden := range []string{"space_id", "domain_id", "query", "principal"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("cancellation diagnostic contains forbidden field %q: %s", forbidden, got)
		}
	}
}

func TestSemanticCandidateBoundFailsClosedForTruncatedFilteredScan(t *testing.T) {
	err := semanticCandidateBoundError(true, semanticsearch.Result{CandidateScanTruncated: true}, 0)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("candidate bound error code = %s, want %s", status.Code(err), codes.ResourceExhausted)
	}
	if err := semanticCandidateBoundError(false, semanticsearch.Result{CandidateScanTruncated: true}, 0); err != nil {
		t.Fatalf("unfiltered scan should not consume the hard-filter truncation signal: %v", err)
	}
}

func TestSemanticResultProvenanceDoesNotRelabelConfiguredRouteAsActual(t *testing.T) {
	item := semanticsearch.SearchResult{ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), ModelEndpointCapabilityID: domainsemantic.ModelEndpointCapabilityID(uuid.New()), IntelligenceProfileID: domainsemantic.IntelligenceProfileID(uuid.New()), SourceGraphRevision: 7, SemanticGeneration: 2, SemanticConfigurationHash: "sha256:configuration", RecordID: domainsemantic.AdvancedEmbeddingRecordID(uuid.New()), Dimensions: 8, VectorSpaceKey: "fixture/8", CreatedAt: time.Now().UTC()}
	got := mapSemanticResultProvenance(item)
	if got.GetConfiguredModelEndpointId() == "" || got.GetConfiguredModelId() == "" || got.GetConfiguredCapabilityId() == "" {
		t.Fatalf("configured route provenance missing: %+v", got)
	}
	if got.GetActualRouteKnown() || got.GetActualModelEndpointId() != "" || got.GetActualModelId() != "" || got.GetActualCapabilityId() != "" {
		t.Fatalf("configured route was relabeled as provider-observed: %+v", got)
	}
}

func TestSemanticSearchHydratesHitsAtCurrentGraphRevision(t *testing.T) {
	ctx := context.Background()
	graphs := newSearchHydrationGraphModule(t, ctx)
	spaceID := uuid.New()
	domainID := uuid.New()

	writeTx := searchHydrationGraphTx(spaceID.String(), domainID.String(), 0)
	node, err := graphs.CreateNode(ctx, writeTx, graphmodel.NodeInput{Content: "semantic hit", Props: map[string]any{"kind": "note"}})
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
	node, err := graphs.CreateNode(ctx, writeTx, graphmodel.NodeInput{Content: "hybrid hit"})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := graphs.CommitTransactionGraph(ctx, writeTx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}

	service := &SearchService{graphs: graphs}
	nodes, warnings, err := service.loadHybridNodes(ctx, uuid.NewString(), spaceID, domainID, []hybridsearch.Result{{NodeID: node.ID.String(), Score: 1}})
	if err != nil {
		t.Fatalf("loadHybridNodes() error = %v", err)
	}
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

func searchHydrationGraphTx(spaceID string, domainID string, baseRevision int64) sessionmodel.GraphTransaction {
	now := time.Now().UTC()
	return sessionmodel.GraphTransaction{ID: uuid.NewString(), SessionID: uuid.NewString(), PrincipalID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(time.Hour)}
}
