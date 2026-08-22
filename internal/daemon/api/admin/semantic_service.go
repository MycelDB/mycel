package admin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	identity "github.com/myceldb/mycel/internal/identity/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const adminSemanticMaxPageSize = 500

type AdminSemanticService struct {
	adminv1.UnimplementedAdminSemanticServiceServer
	semantic   daemonsemantic.Manager
	spaces     daemonspace.Manager
	authorizer OperatorAuthorizer
}

func NewAdminSemanticService(semantic daemonsemantic.Manager, spaces daemonspace.Manager, authorizer OperatorAuthorizer) *AdminSemanticService {
	return &AdminSemanticService{semantic: semantic, spaces: spaces, authorizer: authorizer}
}

func (s *AdminSemanticService) ListSemanticRules(ctx context.Context, req *adminv1.ListSemanticRulesRequest) (*adminv1.ListSemanticRulesResponse, error) {
	spaceID, domainID, err := s.validateSpaceDomain(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if _, err := s.requireSemanticManage(ctx, semanticScope(spaceID, domainID)); err != nil {
		return nil, err
	}
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "open semantic space manager")
	}
	rules, err := spaceMgr.ListSemanticRules(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "list semantic rules")
	}
	stateByRule, searchByBinding, stores, err := s.ruleSummaryMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "load semantic rule metadata")
	}
	items := make([]*clientv1.SemanticGenerationRuleSummary, 0, len(rules))
	for _, rule := range rules {
		if rule.SpaceID != spaceID || (domainID != uuid.Nil && rule.DomainID != domainID) || (!req.GetIncludeDisabled() && !rule.Enabled) {
			continue
		}
		items = append(items, clientapi.MapSemanticRuleSummaryProto(rule, stateByRule[rule.ID], searchByBinding, stores))
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].GetKey() < items[j].GetKey() })
	page, next, err := paginateAdminSemantic(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.ListSemanticRulesResponse{Rules: page, NextPageToken: next}, nil
}

func (s *AdminSemanticService) GetSemanticRule(ctx context.Context, req *adminv1.GetSemanticRuleRequest) (*adminv1.GetSemanticRuleResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := parseSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	if _, err := s.spaces.GetSpace(ctx, spaceID.String()); err != nil {
		return nil, mapSpaceError(err, "get space")
	}
	rule, err := s.getRule(ctx, spaceID, ruleID)
	if err != nil {
		return nil, err
	}
	if _, err := s.requireSemanticManage(ctx, semanticScope(spaceID, rule.DomainID)); err != nil {
		return nil, err
	}
	stateByRule, searchByBinding, stores, err := s.ruleSummaryMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "load semantic rule metadata")
	}
	return &adminv1.GetSemanticRuleResponse{Rule: semanticRuleToProto(rule), Summary: clientapi.MapSemanticRuleSummaryProto(rule, stateByRule[rule.ID], searchByBinding, stores)}, nil
}

func (s *AdminSemanticService) ValidateSemanticRule(ctx context.Context, req *adminv1.ValidateSemanticRuleRequest) (*adminv1.ValidateSemanticRuleResponse, error) {
	rule, err := semanticRuleFromProto(req.GetRule())
	if err != nil {
		return nil, err
	}
	if _, err := s.requireSemanticManage(ctx, semanticScope(rule.SpaceID, rule.DomainID)); err != nil {
		return nil, err
	}
	validation := domainsemantic.ValidateSemanticGenerationRule(rule)
	return &adminv1.ValidateSemanticRuleResponse{Valid: validation.Valid, Diagnostics: validationDiagnosticsToProto(validation.Diagnostics), NormalizedRule: semanticRuleToProto(validation.Rule)}, nil
}

func (s *AdminSemanticService) CreateSemanticRule(ctx context.Context, req *adminv1.CreateSemanticRuleRequest) (*adminv1.CreateSemanticRuleResponse, error) {
	rule, err := semanticRuleFromProto(req.GetRule())
	if err != nil {
		return nil, err
	}
	if rule.ID != uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "semantic_rule_id must be omitted on create")
	}
	if _, _, err := s.validateSpaceDomain(ctx, rule.SpaceID.String(), rule.DomainID.String()); err != nil {
		return nil, err
	}
	if _, err := s.requireSemanticManage(ctx, semanticScope(rule.SpaceID, rule.DomainID)); err != nil {
		return nil, err
	}
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "begin semantic mutation")
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, rule.SpaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "open semantic space manager")
	}
	created, err := spaceMgr.UpsertSemanticRule(ctx, rule)
	if err != nil {
		return nil, mapAdminSemanticError(err, "create semantic rule")
	}
	stateByRule, searchByBinding, stores, err := s.ruleSummaryMetadata(ctx, created.SpaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "load semantic rule metadata")
	}
	return &adminv1.CreateSemanticRuleResponse{Rule: semanticRuleToProto(created), Summary: clientapi.MapSemanticRuleSummaryProto(created, stateByRule[created.ID], searchByBinding, stores)}, nil
}

func (s *AdminSemanticService) UpdateSemanticRule(ctx context.Context, req *adminv1.UpdateSemanticRuleRequest) (*adminv1.UpdateSemanticRuleResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := parseSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	existing, err := s.getRule(ctx, spaceID, ruleID)
	if err != nil {
		return nil, err
	}
	if _, err := s.requireSemanticManage(ctx, semanticScope(spaceID, existing.DomainID)); err != nil {
		return nil, err
	}
	rule, err := semanticRuleFromProto(req.GetRule())
	if err != nil {
		return nil, err
	}
	if rule.ID != uuid.Nil && rule.ID != ruleID {
		return nil, status.Error(codes.InvalidArgument, "rule.semantic_rule_id must match request semantic_rule_id")
	}
	rule.ID = ruleID
	if rule.SpaceID == uuid.Nil {
		rule.SpaceID = spaceID
	}
	if rule.SpaceID != spaceID {
		return nil, status.Error(codes.InvalidArgument, "rule.space_id must match request space_id")
	}
	if rule.DomainID == uuid.Nil {
		rule.DomainID = existing.DomainID
	}
	rule.CreatedAt = existing.CreatedAt
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "begin semantic mutation")
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "open semantic space manager")
	}
	updated, err := spaceMgr.UpsertSemanticRule(ctx, rule)
	if err != nil {
		return nil, mapAdminSemanticError(err, "update semantic rule")
	}
	stateByRule, searchByBinding, stores, err := s.ruleSummaryMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "load semantic rule metadata")
	}
	return &adminv1.UpdateSemanticRuleResponse{Rule: semanticRuleToProto(updated), Summary: clientapi.MapSemanticRuleSummaryProto(updated, stateByRule[updated.ID], searchByBinding, stores)}, nil
}

func (s *AdminSemanticService) SetSemanticRuleEnabled(ctx context.Context, req *adminv1.SetSemanticRuleEnabledRequest) (*adminv1.SetSemanticRuleEnabledResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := parseSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	rule, err := s.getRule(ctx, spaceID, ruleID)
	if err != nil {
		return nil, err
	}
	if _, err := s.requireSemanticManage(ctx, semanticScope(spaceID, rule.DomainID)); err != nil {
		return nil, err
	}
	rule.Enabled = req.GetEnabled()
	rule.UpdatedAt = time.Now().UTC()
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "begin semantic mutation")
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "open semantic space manager")
	}
	updated, err := spaceMgr.UpsertSemanticRule(ctx, rule)
	if err != nil {
		return nil, mapAdminSemanticError(err, "set semantic rule enabled")
	}
	stateByRule, searchByBinding, stores, err := s.ruleSummaryMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "load semantic rule metadata")
	}
	return &adminv1.SetSemanticRuleEnabledResponse{Rule: semanticRuleToProto(updated), Summary: clientapi.MapSemanticRuleSummaryProto(updated, stateByRule[updated.ID], searchByBinding, stores)}, nil
}

func (s *AdminSemanticService) DeleteSemanticRule(ctx context.Context, req *adminv1.DeleteSemanticRuleRequest) (*adminv1.DeleteSemanticRuleResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := parseSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	rule, err := s.getRule(ctx, spaceID, ruleID)
	if err != nil {
		return nil, err
	}
	if _, err := s.requireSemanticManage(ctx, semanticScope(spaceID, rule.DomainID)); err != nil {
		return nil, err
	}
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "begin semantic mutation")
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "open semantic space manager")
	}
	decisionRefs := 0
	if decisions, err := spaceMgr.ListPolicyDecisions(ctx); err == nil {
		for _, decision := range decisions {
			if decision.Scope.SemanticRuleID == ruleID || decision.Scope.SemanticIndexID == domainsemantic.SemanticIndexID(ruleID) {
				decisionRefs++
			}
		}
	}
	if err := spaceMgr.DeleteSemanticRule(ctx, ruleID, req.GetPurgeVectors()); err != nil {
		return nil, mapAdminSemanticError(err, "delete semantic rule")
	}
	vectorsPurged := false
	if req.GetPurgeVectors() {
		if err := s.semantic.PurgeVectorIndex(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID)); err != nil {
			return nil, mapAdminSemanticError(err, "purge semantic rule vectors")
		}
		vectorsPurged = true
	}
	return &adminv1.DeleteSemanticRuleResponse{SemanticRuleId: ruleID.String(), VectorsPurged: vectorsPurged, WorkItemsDeleted: 0, PolicyDecisionsDeleted: int32(decisionRefs)}, nil
}

func (s *AdminSemanticService) getRule(ctx context.Context, spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID) (domainsemantic.SemanticGenerationRule, error) {
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, mapAdminSemanticError(err, "open semantic space manager")
	}
	rules, err := spaceMgr.ListSemanticRules(ctx)
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, mapAdminSemanticError(err, "list semantic rules")
	}
	for _, rule := range rules {
		if rule.ID == ruleID {
			return rule, nil
		}
	}
	return domainsemantic.SemanticGenerationRule{}, status.Error(codes.NotFound, "semantic rule not found")
}

func (s *AdminSemanticService) validateSpaceDomain(ctx context.Context, spaceIDText string, domainIDText string) (domainspace.SpaceID, graph.DomainID, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](spaceIDText, "space_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	domainID, err := optionalSemanticUUID[graph.DomainID](domainIDText, "domain_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if _, err := s.spaces.GetSpace(ctx, spaceID.String()); err != nil {
		return uuid.Nil, uuid.Nil, mapSpaceError(err, "get space")
	}
	return spaceID, domainID, nil
}

func (s *AdminSemanticService) ruleSummaryMetadata(ctx context.Context, spaceID domainspace.SpaceID) (map[domainsemantic.SemanticRuleID]domainsemantic.SemanticRuleState, map[string]domainsemantic.SemanticSearchIndexState, map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend, error) {
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, nil, nil, err
	}
	ruleStates, err := spaceMgr.ListSemanticRuleStates(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	stateByRule := map[domainsemantic.SemanticRuleID]domainsemantic.SemanticRuleState{}
	for _, state := range ruleStates {
		stateByRule[state.SemanticRuleID] = state
	}
	searchStates, err := spaceMgr.ListSearchIndexStates(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	searchByBinding := map[string]domainsemantic.SemanticSearchIndexState{}
	for _, state := range searchStates {
		searchByBinding[state.SemanticRuleID.String()+"/"+strings.ToLower(strings.TrimSpace(state.EmbeddingBindingKey))] = state
	}
	stores, err := s.semantic.GlobalManager().ListVectorStores(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	storeByID := map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend{}
	for _, store := range stores {
		storeByID[store.ID] = store
	}
	return stateByRule, searchByBinding, storeByID, nil
}

func (s *AdminSemanticService) requireSemanticManage(ctx context.Context, scope principalservice.AccessScope) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	if scoped, ok := s.authorizer.(ScopedOperatorAuthorizer); ok {
		if err := scoped.Authorize(ctx, principal.PrincipalID, commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String(), scope); err != nil {
			return daemonauth.Principal{}, err
		}
		return principal, nil
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String())
	if err != nil {
		return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required semantic capability")
	}
	return principal, nil
}

func semanticScope(spaceID domainspace.SpaceID, domainID graph.DomainID) principalservice.AccessScope {
	if domainID != uuid.Nil {
		return principalservice.AccessScope{Type: "domain", SpaceID: spaceID.String(), DomainID: domainID.String()}
	}
	if spaceID != uuid.Nil {
		return principalservice.AccessScope{Type: "space", SpaceID: spaceID.String()}
	}
	return principalservice.AccessScope{Type: "system"}
}

func semanticRuleFromProto(in *adminv1.SemanticGenerationRule) (domainsemantic.SemanticGenerationRule, error) {
	if in == nil {
		return domainsemantic.SemanticGenerationRule{}, status.Error(codes.InvalidArgument, "rule is required")
	}
	spaceID, err := optionalSemanticUUID[domainspace.SpaceID](in.GetSpaceId(), "space_id")
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	domainID, err := optionalSemanticUUID[graph.DomainID](in.GetDomainId(), "domain_id")
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	ruleID, err := optionalSemanticUUID[domainsemantic.SemanticRuleID](in.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	ownerID := identity.PrincipalID(strings.TrimSpace(in.GetOwnerPrincipalId()))
	createdByID := identity.PrincipalID(strings.TrimSpace(in.GetCreatedByPrincipalId()))
	createdAt, err := optionalTime(in.GetCreatedAt(), "created_at")
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	updatedAt, err := optionalTime(in.GetUpdatedAt(), "updated_at")
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	trigger, err := triggerFromProto(in.GetTrigger())
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	selector, err := selectorFromProto(in.GetSelector())
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	source, err := sourceAssemblyFromProto(in.GetSource())
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	bindings, err := embeddingsFromProto(in.GetEmbeddings())
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	maintenance, err := maintenanceFromProto(in.GetMaintenance())
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	return domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: in.GetKey(), DisplayName: in.GetDisplayName(), Description: in.GetDescription(), Enabled: in.GetEnabled(), Trigger: trigger, Selector: selector, Source: source, Embeddings: bindings, Maintenance: maintenance, Storage: storageFromProto(in.GetStorage()), OwnerPrincipalID: ownerID, CreatedByPrincipalID: createdByID, CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
}

func semanticRuleToProto(rule domainsemantic.SemanticGenerationRule) *adminv1.SemanticGenerationRule {
	return &adminv1.SemanticGenerationRule{SemanticRuleId: rule.ID.String(), SpaceId: rule.SpaceID.String(), DomainId: rule.DomainID.String(), Key: rule.Key, DisplayName: rule.DisplayName, Description: rule.Description, Enabled: rule.Enabled, Trigger: triggerToProto(rule.Trigger), Selector: selectorToProto(rule.Selector), Source: sourceAssemblyToProto(rule.Source), Embeddings: embeddingsToProto(rule.Embeddings), Maintenance: maintenanceToProto(rule.Maintenance), Storage: storageToProto(rule.Storage), OwnerPrincipalId: strings.TrimSpace(rule.OwnerPrincipalID.String()), CreatedByPrincipalId: strings.TrimSpace(rule.CreatedByPrincipalID.String()), CreatedAt: timeValueString(rule.CreatedAt), UpdatedAt: timeValueString(rule.UpdatedAt)}
}

func triggerFromProto(in *adminv1.SemanticTriggerPolicy) (domainsemantic.SemanticTriggerPolicy, error) {
	if in == nil {
		return domainsemantic.SemanticTriggerPolicy{}, nil
	}
	debounce, err := optionalDuration(in.GetDebounce(), "trigger.debounce")
	if err != nil {
		return domainsemantic.SemanticTriggerPolicy{}, err
	}
	return domainsemantic.SemanticTriggerPolicy{Events: append([]string(nil), in.GetEvents()...), Labels: append([]string(nil), in.GetLabels()...), Debounce: debounce}, nil
}

func triggerToProto(in domainsemantic.SemanticTriggerPolicy) *adminv1.SemanticTriggerPolicy {
	return &adminv1.SemanticTriggerPolicy{Events: append([]string(nil), in.Events...), Labels: append([]string(nil), in.Labels...), Debounce: durationString(in.Debounce)}
}

func selectorFromProto(in *adminv1.SemanticTargetSelector) (domainsemantic.SemanticTargetSelector, error) {
	if in == nil {
		return domainsemantic.SemanticTargetSelector{}, nil
	}
	nodeIDs := []graph.NodeID{}
	for _, raw := range in.GetNodeIds() {
		id, err := parseSemanticUUID[graph.NodeID](raw, "selector.node_ids")
		if err != nil {
			return domainsemantic.SemanticTargetSelector{}, err
		}
		nodeIDs = append(nodeIDs, id)
	}
	return domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorMode(strings.TrimSpace(in.GetMode())), Labels: append([]string(nil), in.GetLabels()...), GQL: in.GetGql(), TargetAlias: in.GetTargetAlias(), MaxResults: int(in.GetMaxResults()), NodeIDs: nodeIDs}, nil
}

func selectorToProto(in domainsemantic.SemanticTargetSelector) *adminv1.SemanticTargetSelector {
	nodeIDs := make([]string, 0, len(in.NodeIDs))
	for _, id := range in.NodeIDs {
		nodeIDs = append(nodeIDs, id.String())
	}
	return &adminv1.SemanticTargetSelector{Mode: string(in.Mode), Labels: append([]string(nil), in.Labels...), Gql: in.GQL, TargetAlias: in.TargetAlias, MaxResults: int32(in.MaxResults), NodeIds: nodeIDs}
}

func sourceAssemblyFromProto(in *adminv1.SemanticSourceAssemblyPolicy) (domainsemantic.SemanticSourceAssemblyPolicy, error) {
	if in == nil {
		return domainsemantic.SemanticSourceAssemblyPolicy{}, nil
	}
	out := domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceAssemblyMode(strings.TrimSpace(in.GetMode())), IncludeProperties: append([]string(nil), in.GetIncludeProperties()...), ExcludeProperties: append([]string(nil), in.GetExcludeProperties()...), MinimumTextLength: int(in.GetMinimumTextLength()), ContextGQL: in.GetContextGql()}
	if in.MaxDepth != nil {
		v := int(in.GetMaxDepth())
		out.MaxDepth = &v
	}
	return out, nil
}

func sourceAssemblyToProto(in domainsemantic.SemanticSourceAssemblyPolicy) *adminv1.SemanticSourceAssemblyPolicy {
	out := &adminv1.SemanticSourceAssemblyPolicy{Mode: string(in.Mode), IncludeProperties: append([]string(nil), in.IncludeProperties...), ExcludeProperties: append([]string(nil), in.ExcludeProperties...), MinimumTextLength: int32(in.MinimumTextLength), ContextGql: in.ContextGQL}
	if in.MaxDepth != nil {
		v := int32(*in.MaxDepth)
		out.MaxDepth = &v
	}
	return out
}

func embeddingsFromProto(in []*adminv1.SemanticEmbeddingBinding) ([]domainsemantic.SemanticEmbeddingBinding, error) {
	out := make([]domainsemantic.SemanticEmbeddingBinding, 0, len(in))
	for i, item := range in {
		profileID, err := optionalSemanticUUID[domainsemantic.IntelligenceProfileID](item.GetIntelligenceProfileId(), fmt.Sprintf("embeddings[%d].intelligence_profile_id", i))
		if err != nil {
			return nil, err
		}
		storeID, err := optionalSemanticUUID[domainsemantic.VectorStoreID](item.GetVectorStoreId(), fmt.Sprintf("embeddings[%d].vector_store_id", i))
		if err != nil {
			return nil, err
		}
		metadata := map[string]any(nil)
		if item.GetMetadata() != nil {
			metadata = item.GetMetadata().AsMap()
		}
		out = append(out, domainsemantic.SemanticEmbeddingBinding{Key: item.GetKey(), Purpose: item.GetPurpose(), IntelligenceProfile: item.GetIntelligenceProfile(), IntelligenceProfileID: profileID, VectorStore: item.GetVectorStore(), VectorStoreID: storeID, Enabled: item.GetEnabled(), Metadata: metadata})
	}
	return out, nil
}

func embeddingsToProto(in []domainsemantic.SemanticEmbeddingBinding) []*adminv1.SemanticEmbeddingBinding {
	out := make([]*adminv1.SemanticEmbeddingBinding, 0, len(in))
	for _, item := range in {
		metadata, _ := structpb.NewStruct(item.Metadata)
		out = append(out, &adminv1.SemanticEmbeddingBinding{Key: item.Key, Purpose: item.Purpose, IntelligenceProfile: item.IntelligenceProfile, IntelligenceProfileId: uuidText(item.IntelligenceProfileID), VectorStore: item.VectorStore, VectorStoreId: uuidText(item.VectorStoreID), Enabled: item.Enabled, Metadata: metadata})
	}
	return out
}

func maintenanceFromProto(in *adminv1.SemanticMaintenancePolicy) (domainsemantic.SemanticMaintenancePolicy, error) {
	if in == nil {
		return domainsemantic.SemanticMaintenancePolicy{}, nil
	}
	cooldown, err := optionalDuration(in.GetDirtyCooldown(), "maintenance.dirty_cooldown")
	if err != nil {
		return domainsemantic.SemanticMaintenancePolicy{}, err
	}
	return domainsemantic.SemanticMaintenancePolicy{DirtyCooldown: cooldown, MaxBatchSize: int(in.GetMaxBatchSize()), WorkerConcurrency: int(in.GetWorkerConcurrency())}, nil
}

func maintenanceToProto(in domainsemantic.SemanticMaintenancePolicy) *adminv1.SemanticMaintenancePolicy {
	return &adminv1.SemanticMaintenancePolicy{DirtyCooldown: durationString(in.DirtyCooldown), MaxBatchSize: int32(in.MaxBatchSize), WorkerConcurrency: int32(in.WorkerConcurrency)}
}

func storageFromProto(in *adminv1.SemanticStoragePolicy) domainsemantic.SemanticStoragePolicy {
	if in == nil {
		return domainsemantic.SemanticStoragePolicy{}
	}
	return domainsemantic.SemanticStoragePolicy{Searchable: in.GetSearchable(), PhysicalIndex: in.GetPhysicalIndex()}
}

func storageToProto(in domainsemantic.SemanticStoragePolicy) *adminv1.SemanticStoragePolicy {
	return &adminv1.SemanticStoragePolicy{Searchable: in.Searchable, PhysicalIndex: in.PhysicalIndex}
}

func validationDiagnosticsToProto(in []domainsemantic.ValidationDiagnostic) []*adminv1.SemanticRuleValidationDiagnostic {
	out := make([]*adminv1.SemanticRuleValidationDiagnostic, 0, len(in))
	for _, item := range in {
		out = append(out, &adminv1.SemanticRuleValidationDiagnostic{Severity: string(item.Severity), Path: item.Path, Message: item.Message})
	}
	return out
}

func parseSemanticUUID[T ~[16]byte](raw string, name string) (T, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		var zero T
		return zero, status.Errorf(codes.InvalidArgument, "%s must be a UUID", name)
	}
	return T(id), nil
}

func optionalSemanticUUID[T ~[16]byte](raw string, name string) (T, error) {
	if strings.TrimSpace(raw) == "" {
		var zero T
		return zero, nil
	}
	return parseSemanticUUID[T](raw, name)
}

func optionalDuration(raw string, name string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "%s must be a duration", name)
	}
	return value, nil
}

func optionalTime(raw string, name string) (time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, nil
	}
	value, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}, status.Errorf(codes.InvalidArgument, "%s must be RFC3339", name)
	}
	return value, nil
}

func paginateAdminSemantic[T any](items []T, pageSize int, pageToken string) ([]T, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > adminSemanticMaxPageSize {
		pageSize = adminSemanticMaxPageSize
	}
	if start >= len(items) {
		return []T{}, "", nil
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}

func mapAdminSemanticError(err error, action string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return status.Error(codes.NotFound, msg)
	}
	if strings.Contains(msg, "required") || strings.Contains(msg, "invalid") {
		return status.Error(codes.InvalidArgument, msg)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}

func uuidText(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func timeValueString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func durationString(value time.Duration) string {
	if value == 0 {
		return ""
	}
	return value.String()
}
