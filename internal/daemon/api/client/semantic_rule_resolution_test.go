package client

import (
	"context"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type semanticRuleResolutionManager struct {
	daemonsemantic.Manager
	rules []domainsemantic.SemanticGenerationRule
}

func (m semanticRuleResolutionManager) ListRules(context.Context, domainspace.SpaceID, graph.DomainID) ([]domainsemantic.SemanticGenerationRule, error) {
	return m.rules, nil
}

func TestEffectiveSemanticRuleForClientReadDisablesPendingTransition(t *testing.T) {
	rule := domainsemantic.SemanticGenerationRule{Enabled: true, ProviderBudgetTransition: domainsemantic.SemanticProviderBudgetTransitionPrepare}
	if effectiveSemanticRuleForClientRead(rule).Enabled {
		t.Fatal("pending rule was advertised as enabled")
	}
	if !rule.Enabled {
		t.Fatal("effective read mutated stored desired state")
	}
}

func TestResolveSearchRulesRejectsExplicitDisabledRuleAsFailedPrecondition(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	service := &SemanticService{semantic: semanticRuleResolutionManager{rules: []domainsemantic.SemanticGenerationRule{{
		ID:       ruleID,
		SpaceID:  spaceID,
		DomainID: domainID,
		Enabled:  false,
	}}}}

	_, err := service.resolveSearchRules(context.Background(), spaceID, domainID, ruleID.String())
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resolveSearchRules() code=%s, want %s (err=%v)", status.Code(err), codes.FailedPrecondition, err)
	}
}
