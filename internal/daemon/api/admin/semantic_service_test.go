package admin

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type semanticRuleResponseManager struct {
	daemonsemantic.Manager
	space     storesemantic.SpaceManager
	global    storesemantic.GlobalManager
	beforeSet func() error
	lastSet   daemonsemantic.SemanticRuleEnabledInput
}

func (m *semanticRuleResponseManager) BeginMutation(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func (m *semanticRuleResponseManager) SpaceManager(context.Context, domainspace.SpaceID) (storesemantic.SpaceManager, error) {
	return m.space, nil
}

func (m *semanticRuleResponseManager) GlobalManager() storesemantic.GlobalManager { return m.global }

func (m *semanticRuleResponseManager) SetSemanticRuleEnabled(ctx context.Context, in daemonsemantic.SemanticRuleEnabledInput) error {
	m.lastSet = in
	if m.beforeSet != nil {
		if err := m.beforeSet(); err != nil {
			return err
		}
	}
	setter := m.space.(storesemantic.SemanticRuleEnabledManager)
	_, err := setter.SetSemanticRuleEnabled(ctx, in.Rule.ID, in.ExpectedUpdatedAt, in.Enabled, in.UpdatedAt)
	return err
}

func newSemanticRuleServiceTest(t *testing.T) (context.Context, domainspace.SpaceID, storesemantic.SpaceManager, domainsemantic.SemanticGenerationRule, storesemantic.GlobalManager) {
	t.Helper()
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	space := storesemantic.NewSpaceManager()
	if err := space.Init(ctx, t.TempDir(), spaceID); err != nil {
		t.Fatal(err)
	}
	created, err := space.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{
		SpaceID:  spaceID,
		DomainID: graph.DomainID(uuid.New()),
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
	return ctx, spaceID, space, created, global
}

func TestSetSemanticRuleEnabledReturnsExactConditionalMutation(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	manager := &semanticRuleResponseManager{space: space, global: global}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: "operator"})
	response, err := service.SetSemanticRuleEnabled(requestCtx, &adminv1.SetSemanticRuleEnabledRequest{
		SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), Enabled: false,
	})
	if err != nil {
		t.Fatalf("SetSemanticRuleEnabled() error = %v", err)
	}
	rules, err := space.ListSemanticRules(ctx)
	if err != nil || len(rules) != 1 {
		t.Fatalf("persisted rules=%+v err=%v", rules, err)
	}
	persisted := rules[0]
	if response.GetRule() == nil || response.GetRule().GetEnabled() || persisted.Enabled {
		t.Fatalf("response=%+v persisted=%+v", response.GetRule(), persisted)
	}
	if manager.lastSet.Rule.ID != created.ID || !manager.lastSet.ExpectedUpdatedAt.Equal(created.UpdatedAt) || !manager.lastSet.UpdatedAt.Equal(persisted.UpdatedAt) {
		t.Fatalf("conditional mutation=%+v created=%+v persisted=%+v", manager.lastSet, created, persisted)
	}
	if response.GetRule().GetUpdatedAt() != timeValueString(persisted.UpdatedAt) {
		t.Fatalf("response updated_at=%q persisted=%q", response.GetRule().GetUpdatedAt(), timeValueString(persisted.UpdatedAt))
	}
}

func TestSetSemanticRuleEnabledMapsConcurrentUpdateConflict(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	manager := &semanticRuleResponseManager{space: space, global: global, beforeSet: func() error {
		return fmt.Errorf("%w: concurrent semantic rule update", storesemantic.ErrConflict)
	}}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: "operator"})
	_, err := service.SetSemanticRuleEnabled(requestCtx, &adminv1.SetSemanticRuleEnabledRequest{
		SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), Enabled: false,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SetSemanticRuleEnabled() code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
}

func TestSetSemanticRuleEnabledRejectsConcurrentIdentityReplacement(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	manager := &semanticRuleResponseManager{space: space, global: global}
	manager.beforeSet = func() error {
		if err := space.DeleteSemanticRule(ctx, created.ID, false); err != nil {
			return err
		}
		_, err := space.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{
			SpaceID: spaceID, DomainID: created.DomainID, Key: created.Key, Enabled: true,
			Selector: created.Selector, Source: created.Source, Embeddings: created.Embeddings,
		})
		return err
	}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: "operator"})
	_, err := service.SetSemanticRuleEnabled(requestCtx, &adminv1.SetSemanticRuleEnabledRequest{
		SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), Enabled: false,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("SetSemanticRuleEnabled() code=%v err=%v; want NotFound", status.Code(err), err)
	}
	rules, listErr := space.ListSemanticRules(ctx)
	if listErr != nil || len(rules) != 1 || rules[0].ID == created.ID || !rules[0].Enabled {
		t.Fatalf("replacement rule=%+v err=%v", rules, listErr)
	}
}
