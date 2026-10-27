package admin

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type semanticRuleResponseManager struct {
	daemonsemantic.Manager
	space        storesemantic.SpaceManager
	global       storesemantic.GlobalManager
	beforeSet    func() error
	preflightErr error
	prepareErr   error
	purgeErr     error
	purgeCalls   int
	lastSet      daemonsemantic.SemanticRuleEnabledInput
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
	desired := in.Rule
	desired.Enabled = in.Enabled
	desired.ProviderBudgetTransition = in.ProviderBudgetTransition
	desired.ProviderBudgetTransitionID = in.ProviderBudgetTransitionID
	desired.ProviderBudgetTransitionScopeIDs = append([]string(nil), in.ProviderBudgetTransitionScopeIDs...)
	desired.ProviderBudgetTransitionRequiresRebuild = in.ProviderBudgetTransitionRequiresRebuild
	desired.UpdatedAt = in.UpdatedAt
	_, err := setter.SetSemanticRuleEnabled(ctx, in.Rule, desired)
	return err
}

func (m *semanticRuleResponseManager) ConditionalUpsertSemanticRule(ctx context.Context, expected, desired domainsemantic.SemanticGenerationRule) error {
	_, err := m.space.(storesemantic.SemanticRuleEnabledManager).SetSemanticRuleEnabled(ctx, expected, desired)
	return err
}

func (m *semanticRuleResponseManager) ConditionalDeleteSemanticRule(ctx context.Context, expected domainsemantic.SemanticGenerationRule, purge bool) error {
	return m.space.(storesemantic.SemanticRuleConditionalDeleteManager).DeleteSemanticRuleConditional(ctx, expected, purge)
}

func (m *semanticRuleResponseManager) PreflightSemanticProviderBudgets(context.Context, domainsemantic.SemanticGenerationRule) error {
	return m.preflightErr
}

func (m *semanticRuleResponseManager) PrepareSemanticProviderBudgets(context.Context, domainsemantic.SemanticGenerationRule) error {
	return m.prepareErr
}

func (m *semanticRuleResponseManager) CurrentSemanticProviderBudgetScopeIDs(context.Context, domainsemantic.SemanticGenerationRule) ([]domaininference.BudgetScopeID, error) {
	return []domaininference.BudgetScopeID{"scope-a"}, nil
}

func (m *semanticRuleResponseManager) CompleteSemanticProviderBudgets(context.Context, domainsemantic.SemanticGenerationRule, bool) error {
	return nil
}

func (m *semanticRuleResponseManager) PurgeVectorIndex(context.Context, domainspace.SpaceID, domainsemantic.SemanticIndexID) error {
	m.purgeCalls++
	return m.purgeErr
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

func TestMapAdminSemanticErrorMapsDisplayNameConflictToAlreadyExists(t *testing.T) {
	err := storesemantic.NewSemanticRuleDisplayNameConflict("duplicate", []uuid.UUID{uuid.New()})
	mapped := mapAdminSemanticError(err, "create semantic rule")
	if status.Code(mapped) != codes.AlreadyExists {
		t.Fatalf("mapAdminSemanticError() code=%v error=%v, want AlreadyExists", status.Code(mapped), mapped)
	}
}

func TestSemanticProviderBudgetConfigurationChangeIncludesAuthorizationInputs(t *testing.T) {
	base := domainsemantic.SemanticGenerationRule{DomainID: graph.DomainID(uuid.New()), OwnerPrincipalID: "owner-a", Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Enabled: true, ProviderBudget: &domainsemantic.SemanticProviderBudgetPolicy{CorrelationID: "run"}}}}
	changedDomain := base
	changedDomain.DomainID = graph.DomainID(uuid.New())
	if !semanticProviderBudgetConfigurationChanged(base, changedDomain) {
		t.Fatal("domain change did not require provider budget replacement")
	}
	changedOwner := base
	changedOwner.OwnerPrincipalID = "owner-b"
	if !semanticProviderBudgetConfigurationChanged(base, changedOwner) {
		t.Fatal("owner change did not require provider budget replacement")
	}
}

func TestFreshSemanticRuleIDDoesNotReuseIncarnationIdentity(t *testing.T) {
	first, second := freshSemanticRuleID(), freshSemanticRuleID()
	if first == uuid.Nil || second == uuid.Nil || first == second {
		t.Fatalf("fresh rule identities first=%s second=%s", first, second)
	}
}

func TestPendingSemanticRuleIsEffectivelyDisabledForRead(t *testing.T) {
	rule := domainsemantic.SemanticGenerationRule{Enabled: true, ProviderBudgetTransition: domainsemantic.SemanticProviderBudgetTransitionPrepare}
	if semanticRuleEffectiveForRead(rule).Enabled {
		t.Fatal("pending rule was advertised as enabled")
	}
	if !rule.Enabled {
		t.Fatal("effective read mutated stored desired state")
	}
	ctx, spaceID, space, stored, global := newSemanticRuleServiceTest(t)
	stored.ProviderBudgetTransition = domainsemantic.SemanticProviderBudgetTransitionPrepare
	stored, err := space.UpsertSemanticRule(ctx, stored)
	if err != nil {
		t.Fatal(err)
	}
	service := NewAdminSemanticService(&semanticRuleResponseManager{space: space, global: global}, nil, &scopedSemanticAuthorizer{})
	lookedUp, err := service.lookupRule(ctx, spaceID, stored.ID)
	if err != nil || lookedUp.ProviderBudgetTransition == "" || semanticRuleEffectiveForRead(lookedUp).Enabled {
		t.Fatalf("pending read lookup=%+v error=%v", lookedUp, err)
	}
	if _, err := service.getRule(ctx, spaceID, stored.ID); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mutation lookup error=%v", err)
	}
}

func TestDeleteSemanticRulePurgeFailureRetainsDurablePurgeIntent(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	created.Embeddings[0].ProviderBudget = &domainsemantic.SemanticProviderBudgetPolicy{CorrelationID: "run-1", MaxRequests: 1, MaxConcurrentProviderCalls: 1, BatchingMode: domainsemantic.SemanticProviderBatchingSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 1, MaxInputTokens: 1, RequestTimeout: time.Second, TotalTimeout: time.Minute, Currency: "USD", PriceDefinitionVersion: "price-v1", InputMicrosPerMillionTokens: 1, MaxEstimatedCostMicros: 1}
	created, err := space.UpsertSemanticRule(ctx, created)
	if err != nil {
		t.Fatal(err)
	}
	manager := &semanticRuleResponseManager{space: space, global: global, purgeErr: errors.New("purge unavailable")}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: uuid.NewString()})
	_, err = service.DeleteSemanticRule(requestCtx, &adminv1.DeleteSemanticRuleRequest{SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), PurgeVectors: true})
	if err == nil || manager.purgeCalls != 1 {
		t.Fatalf("DeleteSemanticRule() error=%v purgeCalls=%d", err, manager.purgeCalls)
	}
	rules, listErr := space.ListSemanticRules(ctx)
	if listErr != nil || len(rules) != 1 || rules[0].ProviderBudgetTransition != domainsemantic.SemanticProviderBudgetTransitionDelete || !rules[0].ProviderBudgetTransitionPurgeVectors {
		t.Fatalf("purge failure lost durable delete journal: rules=%+v error=%v", rules, listErr)
	}
}

func TestCreateSemanticRuleRejectsFractionalMillisecondBudgetBeforeStaging(t *testing.T) {
	ctx, _, space, created, global := newSemanticRuleServiceTest(t)
	candidate := created
	candidate.ID = uuid.Nil
	candidate.Key = "fractional-timeout"
	candidate.Embeddings[0].ProviderBudget = &domainsemantic.SemanticProviderBudgetPolicy{CorrelationID: "run-1", MaxRequests: 1, MaxConcurrentProviderCalls: 1, BatchingMode: domainsemantic.SemanticProviderBatchingSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 100, MaxInputTokens: 100, RequestTimeout: 1500 * time.Microsecond, TotalTimeout: time.Minute, Currency: "USD", PriceDefinitionVersion: "price-v1", InputMicrosPerMillionTokens: 1, MaxEstimatedCostMicros: 1}
	service := NewAdminSemanticService(&semanticRuleResponseManager{space: space, global: global}, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: uuid.NewString()})
	_, err := service.CreateSemanticRule(requestCtx, &adminv1.CreateSemanticRuleRequest{Rule: semanticRuleToProto(candidate)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateSemanticRule() error=%v code=%v", err, status.Code(err))
	}
	rules, listErr := space.ListSemanticRules(ctx)
	if listErr != nil || len(rules) != 1 || rules[0].ID != created.ID {
		t.Fatalf("fractional timeout staged a rule: rules=%+v error=%v", rules, listErr)
	}
}

func TestCreateSemanticRulePreflightFailureDoesNotStageRule(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	candidate := created
	candidate.ID = uuid.Nil
	candidate.Key = "preflight-rejected"
	candidate.Embeddings[0].ProviderBudget = &domainsemantic.SemanticProviderBudgetPolicy{CorrelationID: "run-1", MaxRequests: 1, MaxRetryAttempts: 0, MaxRetriesPerRequest: 0, MaxConcurrentProviderCalls: 1, BatchingMode: domainsemantic.SemanticProviderBatchingSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 100, MaxInputTokens: 100, RequestTimeout: time.Second, TotalTimeout: time.Minute, Currency: "USD", PriceDefinitionVersion: "price-v1", InputMicrosPerMillionTokens: 1, MaxEstimatedCostMicros: 1}
	manager := &semanticRuleResponseManager{space: space, global: global, preflightErr: errors.New("deterministic invalid route")}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: uuid.NewString()})
	_, err := service.CreateSemanticRule(requestCtx, &adminv1.CreateSemanticRuleRequest{Rule: semanticRuleToProto(candidate)})
	if err == nil {
		t.Fatal("expected preflight rejection")
	}
	rules, listErr := space.ListSemanticRules(ctx)
	if listErr != nil || len(rules) != 1 || rules[0].ID != created.ID {
		t.Fatalf("preflight failure staged a rule: rules=%+v error=%v", rules, listErr)
	}
	_ = spaceID
}

func TestUpdateSemanticRuleRejectsEnabledStateChange(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	manager := &semanticRuleResponseManager{space: space, global: global}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: uuid.NewString()})
	protoRule := semanticRuleToProto(created)
	protoRule.Enabled = false
	_, err := service.UpdateSemanticRule(requestCtx, &adminv1.UpdateSemanticRuleRequest{SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), Rule: protoRule})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateSemanticRule() code=%v error=%v, want InvalidArgument", status.Code(err), err)
	}
}

func TestSetSemanticRuleEnabledPreservesJournalAfterPartialPreparationFailure(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	created.Enabled = false
	created.Embeddings[0].ProviderBudget = &domainsemantic.SemanticProviderBudgetPolicy{CorrelationID: "run-1", MaxRequests: 1, MaxRetryAttempts: 0, MaxRetriesPerRequest: 0, MaxConcurrentProviderCalls: 1, BatchingMode: domainsemantic.SemanticProviderBatchingSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 100, MaxInputTokens: 100, RequestTimeout: time.Second, TotalTimeout: time.Minute, Currency: "USD", PriceDefinitionVersion: "price-v1", InputMicrosPerMillionTokens: 1, MaxEstimatedCostMicros: 1}
	created.UpdatedAt = created.UpdatedAt.Add(time.Second)
	created, err := space.UpsertSemanticRule(ctx, created)
	if err != nil {
		t.Fatal(err)
	}
	manager := &semanticRuleResponseManager{space: space, global: global, prepareErr: errors.New("injected partial preparation")}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: uuid.NewString()})
	_, err = service.SetSemanticRuleEnabled(requestCtx, &adminv1.SetSemanticRuleEnabledRequest{SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), Enabled: true})
	if err == nil {
		t.Fatal("expected preparation failure")
	}
	rules, listErr := space.ListSemanticRules(ctx)
	if listErr != nil || len(rules) != 1 {
		t.Fatalf("rules=%+v error=%v", rules, listErr)
	}
	if !rules[0].Enabled || rules[0].ProviderBudgetTransition != domainsemantic.SemanticProviderBudgetTransitionPrepare || rules[0].ProviderBudgetTransitionID == "" {
		t.Fatalf("partial preparation lost durable journal: %+v", rules[0])
	}
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

func TestSetSemanticRuleEnabledRejectsSameTimestampConcurrentUpdate(t *testing.T) {
	ctx, spaceID, space, created, global := newSemanticRuleServiceTest(t)
	manager := &semanticRuleResponseManager{space: space, global: global, beforeSet: func() error {
		concurrent := created
		concurrent.DisplayName = "concurrent"
		concurrent.UpdatedAt = created.UpdatedAt
		_, err := space.UpsertSemanticRule(ctx, concurrent)
		return err
	}}
	service := NewAdminSemanticService(manager, nil, &scopedSemanticAuthorizer{})
	requestCtx := auth.ContextWithPrincipal(ctx, auth.Principal{PrincipalID: "operator"})
	_, err := service.SetSemanticRuleEnabled(requestCtx, &adminv1.SetSemanticRuleEnabledRequest{
		SpaceId: spaceID.String(), SemanticRuleId: created.ID.String(), Enabled: false,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SetSemanticRuleEnabled() code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
	rules, listErr := space.ListSemanticRules(ctx)
	if listErr != nil || len(rules) != 1 || rules[0].DisplayName != "concurrent" || !rules[0].Enabled {
		t.Fatalf("concurrent rule=%+v err=%v", rules, listErr)
	}
}

func TestSemanticTriggerPolicyTypedAndLegacyCompatibility(t *testing.T) {
	typed := []adminv1.SemanticTriggerEventType{
		adminv1.SemanticTriggerEventType_SEMANTIC_TRIGGER_EVENT_TYPE_EDGE_DELETED,
		adminv1.SemanticTriggerEventType_SEMANTIC_TRIGGER_EVENT_TYPE_NODE_UPDATED,
	}
	got, err := triggerFromProto(&adminv1.SemanticTriggerPolicy{
		Events:     []string{"node_updated", "edge.deleted", "node.updated"},
		EventTypes: typed,
		Labels:     []string{"Note"},
		Debounce:   "30s",
	})
	if err != nil {
		t.Fatalf("triggerFromProto() error = %v", err)
	}
	want := []string{domainsemantic.SemanticTriggerEventNodeUpdated, domainsemantic.SemanticTriggerEventEdgeDeleted}
	if !reflect.DeepEqual(got.Events, want) || !reflect.DeepEqual(got.Labels, []string{"Note"}) || got.Debounce != 30*time.Second {
		t.Fatalf("triggerFromProto() = %+v, want events=%v labels=[Note] debounce=30s", got, want)
	}

	projected := triggerToProto(got)
	if !reflect.DeepEqual(projected.GetEvents(), want) {
		t.Fatalf("triggerToProto() legacy events = %v, want %v", projected.GetEvents(), want)
	}
	wantTyped := []adminv1.SemanticTriggerEventType{
		adminv1.SemanticTriggerEventType_SEMANTIC_TRIGGER_EVENT_TYPE_NODE_UPDATED,
		adminv1.SemanticTriggerEventType_SEMANTIC_TRIGGER_EVENT_TYPE_EDGE_DELETED,
	}
	if !reflect.DeepEqual(projected.GetEventTypes(), wantTyped) {
		t.Fatalf("triggerToProto() event types = %v, want %v", projected.GetEventTypes(), wantTyped)
	}
}

func TestSemanticTriggerPolicyRejectsConflictingOrUnknownTypedEvents(t *testing.T) {
	tests := []struct {
		name string
		in   *adminv1.SemanticTriggerPolicy
	}{
		{
			name: "conflicting representations",
			in: &adminv1.SemanticTriggerPolicy{
				Events:     []string{"node.created"},
				EventTypes: []adminv1.SemanticTriggerEventType{adminv1.SemanticTriggerEventType_SEMANTIC_TRIGGER_EVENT_TYPE_NODE_UPDATED},
			},
		},
		{
			name: "unsupported legacy event",
			in: &adminv1.SemanticTriggerPolicy{
				Events: []string{"future.changed"},
			},
		},
		{
			name: "unspecified",
			in: &adminv1.SemanticTriggerPolicy{
				EventTypes: []adminv1.SemanticTriggerEventType{adminv1.SemanticTriggerEventType_SEMANTIC_TRIGGER_EVENT_TYPE_UNSPECIFIED},
			},
		},
		{
			name: "unknown numeric value",
			in: &adminv1.SemanticTriggerPolicy{
				EventTypes: []adminv1.SemanticTriggerEventType{adminv1.SemanticTriggerEventType(99)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := triggerFromProto(tt.in)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("triggerFromProto() code = %v, err = %v; want InvalidArgument", status.Code(err), err)
			}
		})
	}
}

func TestSemanticTriggerPolicyDefaultsBothRepresentationsToChanged(t *testing.T) {
	got, err := triggerFromProto(&adminv1.SemanticTriggerPolicy{})
	if err != nil {
		t.Fatalf("triggerFromProto() error = %v", err)
	}
	if !reflect.DeepEqual(got.Events, []string{domainsemantic.DefaultSemanticTriggerEventChanged}) {
		t.Fatalf("triggerFromProto() events = %v, want [changed]", got.Events)
	}
	projected := triggerToProto(got)
	if !reflect.DeepEqual(projected.GetEvents(), []string{"changed"}) || !reflect.DeepEqual(projected.GetEventTypes(), []adminv1.SemanticTriggerEventType{adminv1.SemanticTriggerEventType_SEMANTIC_TRIGGER_EVENT_TYPE_CHANGED}) {
		t.Fatalf("triggerToProto() = %+v, want dual changed representations", projected)
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
