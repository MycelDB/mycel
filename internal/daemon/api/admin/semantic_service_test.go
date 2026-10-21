package admin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type staleSemanticRuleResponseManager struct {
	storesemantic.SpaceManager
	stale domainsemantic.SemanticGenerationRule
}

func (m staleSemanticRuleResponseManager) UpsertSemanticRule(ctx context.Context, rule domainsemantic.SemanticGenerationRule) (domainsemantic.SemanticGenerationRule, error) {
	if _, err := m.SpaceManager.UpsertSemanticRule(ctx, rule); err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	return m.stale, nil
}

type semanticRuleResponseManager struct {
	daemonsemantic.Manager
	space  storesemantic.SpaceManager
	global storesemantic.GlobalManager
}

func (m semanticRuleResponseManager) BeginMutation(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func (m semanticRuleResponseManager) SpaceManager(context.Context, domainspace.SpaceID) (storesemantic.SpaceManager, error) {
	return m.space, nil
}

func (m semanticRuleResponseManager) GlobalManager() storesemantic.GlobalManager { return m.global }

func TestSetSemanticRuleEnabledReturnsCommittedRequestedState(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	space := storesemantic.NewSpaceManager()
	if err := space.Init(ctx, t.TempDir(), spaceID); err != nil {
		t.Fatal(err)
	}
	created, err := space.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{
		SpaceID:  spaceID,
		DomainID: domainID,
		Key:      "rule",
		Enabled:  true,
		Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}},
		Embeddings: []domainsemantic.SemanticEmbeddingBinding{{
			Key: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch),
			IntelligenceProfile: "profile", VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	global := storesemantic.NewGlobalManager()
	if err := global.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	stale := created
	stale.UpdatedAt = time.Now().UTC()
	manager := semanticRuleResponseManager{space: staleSemanticRuleResponseManager{SpaceManager: space, stale: stale}, global: global}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: "operator"})
	response, err := service.SetSemanticRuleEnabled(requestCtx, &adminv1.SetSemanticRuleEnabledRequest{
		SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), Enabled: false,
	})
	if err != nil {
		t.Fatalf("SetSemanticRuleEnabled() error = %v", err)
	}
	if response.GetRule() == nil || response.GetRule().GetEnabled() {
		t.Fatalf("SetSemanticRuleEnabled() returned stale rule: %+v", response.GetRule())
	}
	rules, err := space.ListSemanticRules(ctx)
	if err != nil || len(rules) != 1 || rules[0].Enabled {
		t.Fatalf("persisted rules=%+v err=%v", rules, err)
	}
}
