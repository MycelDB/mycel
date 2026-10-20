package client

import (
	"context"
	"testing"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHybridOptionsFromProtoDefaultsAndNormalizes(t *testing.T) {
	opts, err := hybridOptionsFromProto(nil, 60)
	if err != nil {
		t.Fatalf("hybridOptionsFromProto(nil) returned error: %v", err)
	}
	if opts.Weights.Lexical != 0.5 || opts.Weights.Semantic != 0.5 {
		t.Fatalf("default weights = %#v, want 0.5/0.5", opts.Weights)
	}
	if opts.RankConstant != 60 {
		t.Fatalf("default rank constant = %v, want 60", opts.RankConstant)
	}
	opts, err = hybridOptionsFromProto(&clientv1.HybridSearchOptions{LexicalWeight: 2, SemanticWeight: 1, RequireBoth: true}, 42)
	if err != nil {
		t.Fatalf("hybridOptionsFromProto weighted returned error: %v", err)
	}
	if opts.Weights.Lexical < 0.66 || opts.Weights.Lexical > 0.67 || opts.Weights.Semantic < 0.33 || opts.Weights.Semantic > 0.34 || !opts.RequireBoth || opts.RankConstant != 42 {
		t.Fatalf("weighted opts = %#v", opts)
	}
}

func TestHybridOptionsFromProtoRejectsInvalidWeights(t *testing.T) {
	if _, err := hybridOptionsFromProto(&clientv1.HybridSearchOptions{LexicalWeight: -1, SemanticWeight: 1}, 60); err == nil {
		t.Fatalf("hybridOptionsFromProto negative weights returned nil error")
	}
}

func TestSearchFiltersFromProto(t *testing.T) {
	filters, err := searchFiltersFromProto(&clientv1.SearchFilters{NodeLabels: []string{"Note"}, NodeIds: []string{"node-a"}, Properties: []*clientv1.PropertyFilter{{Path: "tags", Operator: clientv1.FilterOperator_FILTER_OPERATOR_CONTAINS, Values: []string{"k3s"}}}})
	if err != nil {
		t.Fatalf("searchFiltersFromProto returned error: %v", err)
	}
	if len(filters.NodeLabels) != 1 || len(filters.NodeIDs) != 1 || len(filters.Properties) != 1 {
		t.Fatalf("filters = %#v", filters)
	}
}

func TestSearchFiltersFromProtoRejectsUnsupportedOperator(t *testing.T) {
	_, err := searchFiltersFromProto(&clientv1.SearchFilters{Properties: []*clientv1.PropertyFilter{{Path: "tags", Operator: clientv1.FilterOperator_FILTER_OPERATOR_UNSPECIFIED}}})
	if err == nil {
		t.Fatalf("searchFiltersFromProto unsupported operator returned nil error")
	}
	if _, err := searchFiltersFromProto(&clientv1.SearchFilters{Properties: []*clientv1.PropertyFilter{{Path: "", Operator: clientv1.FilterOperator_FILTER_OPERATOR_EXISTS}}}); err == nil {
		t.Fatal("searchFiltersFromProto empty property path returned nil error")
	}
	if _, err := searchFiltersFromProto(&clientv1.SearchFilters{Properties: []*clientv1.PropertyFilter{{Path: "status", Operator: clientv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{"open", "closed"}}}}); err == nil {
		t.Fatal("searchFiltersFromProto invalid EQUALS values returned nil error")
	}
}

func TestSemanticHybridCandidatesAppliesMinScoreBeforeFusion(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	highNodeID := graph.NodeID(uuid.New())
	weakNodeID := graph.NodeID(uuid.New())
	semantic := &fakeHybridSemanticManager{
		rules:   []domainsemantic.SemanticGenerationRule{{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "notes", Enabled: true, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch), Enabled: true}}}},
		results: []semanticsearch.SearchResult{{NodeID: highNodeID, Score: 0.80}, {NodeID: weakNodeID, Score: 0.11}},
	}
	service := &SearchService{
		semantic: semantic,
		spaces:   fakeHybridSpaceManager{domain: graph.Domain{ID: domainID, SpaceID: spaceID, SemanticMode: graph.DomainSemanticModeNormal}},
	}
	minScore := 0.5
	candidates, warnings, err := service.semanticHybridCandidates(ctx, principalUser{PrincipalID: uuid.NewString()}, spaceID, domainID, "team recovery", 10, &clientv1.SemanticSearchOptions{MinScore: &minScore}, false)
	if err != nil {
		t.Fatalf("semanticHybridCandidates() error = %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("semanticHybridCandidates() warnings = %v, want none", warnings)
	}
	if semantic.lastInput.MinScore != minScore {
		t.Fatalf("semantic SearchInput MinScore = %v, want %v", semantic.lastInput.MinScore, minScore)
	}
	if semantic.lastInput.Text != "team recovery" {
		t.Fatalf("semantic SearchInput Text = %q, want lexical-query fallback", semantic.lastInput.Text)
	}
	if len(candidates) != 1 || candidates[0].NodeID != highNodeID.String() {
		t.Fatalf("candidates = %#v, want only high-scoring semantic candidate %s", candidates, highNodeID)
	}
}

func TestSemanticHybridCandidatesUsesSemanticQueryOverride(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	semantic := &fakeHybridSemanticManager{
		rules: []domainsemantic.SemanticGenerationRule{{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "notes", Enabled: true, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch), Enabled: true}}}},
	}
	service := &SearchService{
		semantic: semantic,
		spaces:   fakeHybridSpaceManager{domain: graph.Domain{ID: domainID, SpaceID: spaceID, SemanticMode: graph.DomainSemanticModeNormal}},
	}
	override := "how can a team recover calmly"
	_, _, err := service.semanticHybridCandidates(ctx, principalUser{PrincipalID: uuid.NewString()}, spaceID, domainID, `"team" OR "recovery"`, 10, &clientv1.SemanticSearchOptions{Query: &override}, false)
	if err != nil {
		t.Fatalf("semanticHybridCandidates() error = %v", err)
	}
	if semantic.lastInput.Text != override {
		t.Fatalf("semantic SearchInput Text = %q, want override %q", semantic.lastInput.Text, override)
	}
}

func TestIsNoSemanticSearchAvailable(t *testing.T) {
	if !isNoSemanticSearchAvailable(status.Error(codes.FailedPrecondition, "no enabled semantic search rule is available for the domain")) {
		t.Fatalf("expected no semantic search availability detection")
	}
	if isNoSemanticSearchAvailable(status.Error(codes.Internal, "no enabled semantic search rule is available for the domain")) {
		t.Fatalf("internal error should not be treated as safe semantic unavailability")
	}
}

type fakeHybridSemanticManager struct {
	daemonsemantic.Manager
	rules     []domainsemantic.SemanticGenerationRule
	results   []semanticsearch.SearchResult
	lastInput daemonsemantic.SearchInput
}

func (m *fakeHybridSemanticManager) ListRules(context.Context, domainspace.SpaceID, graph.DomainID) ([]domainsemantic.SemanticGenerationRule, error) {
	return append([]domainsemantic.SemanticGenerationRule(nil), m.rules...), nil
}

func (m *fakeHybridSemanticManager) Search(_ context.Context, in daemonsemantic.SearchInput) (semanticsearch.Result, error) {
	m.lastInput = in
	out := semanticsearch.Result{}
	for _, result := range m.results {
		if result.Score < in.MinScore {
			continue
		}
		out.Results = append(out.Results, result)
	}
	return out, nil
}

type fakeHybridSpaceManager struct {
	daemonspace.Manager
	domain graph.Domain
}

func (m fakeHybridSpaceManager) GetVisibleDomain(context.Context, string, string, string, string) (graph.Domain, error) {
	return m.domain, nil
}
