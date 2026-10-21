package service

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daemonconfig "github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestSemanticProjectionLeaseFailsClosedAcrossLeadershipTerms(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}, CreatedAt: now, UpdatedAt: now}
	if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildEpoch: "epoch-1", RebuildState: "completed"}}}); err != nil {
		t.Fatal(err)
	}
	var term atomic.Uint64
	term.Store(7)
	module.raftLocalNode = 1
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) {
		return 1, term.Load(), true, nil
	}
	leasedCtx, err := module.bindSemanticOperationLeadership(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := module.saveProjectionLease(leasedCtx, spaceID, ruleID, "search", 2, "epoch-1", 5); err != nil {
		t.Fatal(err)
	}
	search := SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search"}
	if err := module.validateLocalProjectionForSearch(ctx, search); err != nil {
		t.Fatalf("current-term projection rejected: %v", err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildEpoch: "epoch-1", RebuildState: "completed", FreshThrough: 6}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Search(ctx, search); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("stale-frontier search error=%v, want fail-closed readiness error", err)
	}
	if err := module.saveProjectionLease(leasedCtx, spaceID, ruleID, "search", 2, "epoch-1", 6); err != nil {
		t.Fatal(err)
	}
	term.Store(8)
	if _, err := module.Search(ctx, search); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("stale-term search error=%v, want fail-closed readiness error", err)
	}
}

func TestSemanticProjectionLeaseRequiresLocalBaselineOnPromotedNode(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 1}}}); err != nil {
		t.Fatal(err)
	}
	module.raftLocalNode = 2
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 2, 9, true, nil }
	search := SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search"}
	if _, err := module.Search(ctx, search); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("empty promoted-node search error=%v, want fail-closed readiness error", err)
	}
}
