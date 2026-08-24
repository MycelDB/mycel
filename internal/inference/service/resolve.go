package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
)

type ResolveRequest struct {
	SpaceID               string
	DomainID              string
	SemanticRuleID        string
	EmbeddingBindingKey   string
	SemanticIndexID       string
	NodeID                string
	Operation             domaininference.Operation
	UsageMode             domaininference.UsageMode
	ProfileRef            string
	ProfileID             domaininference.ProfileID
	CapabilityRef         string
	CapabilityID          domaininference.CapabilityID
	EndpointRef           string
	EndpointID            domaininference.EndpointID
	ModelRef              string
	ModelID               domaininference.ModelID
	RequiredFeatures      []string
	Parameters            domaininference.Parameters
	ActorPrincipalID      string
	OnBehalfOfPrincipalID string
	Metadata              map[string]any
}

type ResolveResult struct {
	Allowed         bool
	Decision        domaininference.PolicyDecision
	Profile         domaininference.Profile
	Capability      domaininference.Capability
	Endpoint        domaininference.Endpoint
	Model           domaininference.Model
	Credential      domaininference.Credential
	Secret          domaininference.Secret
	CredentialGrant domaininference.CredentialGrant
}

func (m *Module) Resolve(ctx context.Context, req ResolveRequest) (ResolveResult, error) {
	resolver := resolver{manager: m}
	return resolver.resolve(ctx, req)
}

type resolver struct {
	manager Manager
}

type capabilityCandidate struct {
	capability domaininference.Capability
	endpoint   domaininference.Endpoint
	model      domaininference.Model
}

type grantCandidate struct {
	grant      domaininference.CredentialGrant
	credential domaininference.Credential
	secret     domaininference.Secret
	score      int
}

type policyEvaluation struct {
	allowed          bool
	matchedPolicyIDs []string
	reason           string
}

func (r resolver) resolve(ctx context.Context, req ResolveRequest) (ResolveResult, error) {
	if r.manager == nil {
		return ResolveResult{}, fmt.Errorf("inference resolver requires a manager")
	}
	req.SpaceID = strings.TrimSpace(req.SpaceID)
	if req.SpaceID == "" {
		return ResolveResult{}, fmt.Errorf("space_id is required")
	}
	if req.Operation == "" {
		return ResolveResult{}, fmt.Errorf("operation is required")
	}
	if req.UsageMode == "" {
		req.UsageMode = domaininference.UsageModeInteractive
	}
	spaceMgr, err := r.manager.SpaceManager(ctx, req.SpaceID)
	if err != nil {
		return ResolveResult{}, err
	}
	if err := r.validatePrincipalContext(ctx, req); err != nil {
		return r.deny(ctx, spaceMgr, req, ResolveResult{}, nil, err.Error())
	}
	profile, err := r.resolveProfile(ctx, spaceMgr, req)
	if err != nil {
		return r.deny(ctx, spaceMgr, req, ResolveResult{}, nil, err.Error())
	}
	partial := ResolveResult{Profile: profile}
	candidates, err := r.capabilityCandidates(ctx, req, profile)
	if err != nil {
		return r.deny(ctx, spaceMgr, req, partial, nil, err.Error())
	}
	if len(candidates) == 0 {
		return r.deny(ctx, spaceMgr, req, partial, nil, "no enabled inference capability matches request")
	}
	policies, err := spaceMgr.ListPolicies(ctx)
	if err != nil {
		return ResolveResult{}, err
	}
	var firstDeny ResolveResult
	var firstDenyReason string
	var firstPolicyIDs []string
	for _, candidate := range candidates {
		current := partial
		current.Capability = candidate.capability
		current.Endpoint = candidate.endpoint
		current.Model = candidate.model
		grants, err := r.matchingGrants(ctx, spaceMgr, req, profile, candidate)
		if err != nil {
			return ResolveResult{}, err
		}
		if len(grants) == 0 {
			if firstDenyReason == "" {
				firstDeny = current
				firstDenyReason = "no active credential grant matches request"
			}
			continue
		}
		for _, grant := range grants {
			current.CredentialGrant = grant.grant
			current.Credential = grant.credential
			current.Secret = grant.secret
			eval := r.evaluatePolicies(req, profile, candidate, policies)
			if eval.allowed {
				return r.allow(ctx, spaceMgr, req, current, eval.matchedPolicyIDs, eval.reason)
			}
			if firstDenyReason == "" {
				firstDeny = current
				firstDenyReason = eval.reason
				firstPolicyIDs = eval.matchedPolicyIDs
			}
		}
	}
	if firstDenyReason == "" {
		firstDenyReason = "inference request denied"
	}
	return r.deny(ctx, spaceMgr, req, firstDeny, firstPolicyIDs, firstDenyReason)
}

func (r resolver) resolveProfile(ctx context.Context, spaceMgr inferencestorage.SpaceManager, req ResolveRequest) (domaininference.Profile, error) {
	profiles, err := spaceMgr.ListProfiles(ctx)
	if err != nil {
		return domaininference.Profile{}, err
	}
	for _, profile := range profiles {
		if !profile.Enabled {
			continue
		}
		if req.ProfileID != uuid.Nil && profile.ID != req.ProfileID {
			continue
		}
		if req.ProfileID == uuid.Nil && strings.TrimSpace(req.ProfileRef) != "" && !refMatches(profile.ID, profile.Key, req.ProfileRef) {
			continue
		}
		if req.ProfileID == uuid.Nil && strings.TrimSpace(req.ProfileRef) == "" {
			continue
		}
		if profile.Operation != req.Operation {
			return domaininference.Profile{}, fmt.Errorf("inference profile operation %q does not match request operation %q", profile.Operation, req.Operation)
		}
		if len(profile.DomainIDs) > 0 && req.DomainID != "" && !stringListContains(profile.DomainIDs, req.DomainID) {
			return domaininference.Profile{}, fmt.Errorf("inference profile does not allow domain %q", req.DomainID)
		}
		return profile, nil
	}
	return domaininference.Profile{}, fmt.Errorf("inference profile not found")
}

func (r resolver) capabilityCandidates(ctx context.Context, req ResolveRequest, profile domaininference.Profile) ([]capabilityCandidate, error) {
	global := r.manager.GlobalManager()
	capabilities, err := global.ListCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	endpoints, err := global.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	models, err := global.ListModels(ctx)
	if err != nil {
		return nil, err
	}
	endpointByID := map[uuid.UUID]domaininference.Endpoint{}
	for _, endpoint := range endpoints {
		endpointByID[endpoint.ID] = endpoint
	}
	modelByID := map[uuid.UUID]domaininference.Model{}
	for _, model := range models {
		modelByID[model.ID] = model
	}
	out := []capabilityCandidate{}
	for _, capability := range capabilities {
		endpoint, ok := endpointByID[capability.EndpointID]
		if !ok {
			continue
		}
		model, ok := modelByID[capability.ModelID]
		if !ok {
			continue
		}
		candidate := capabilityCandidate{capability: capability, endpoint: endpoint, model: model}
		if !candidateEnabled(candidate) || !candidateMatchesOperation(candidate, req.Operation) || !candidateMatchesRefs(candidate, req, profile) || !candidateMatchesPrivacy(candidate, profile.PrivacyRequirement) || !candidateMatchesFeatures(candidate, append(profile.RequiredFeatures, req.RequiredFeatures...)) || !candidateMatchesParameters(candidate, req.Parameters, profile.DefaultParameters) {
			continue
		}
		out = append(out, candidate)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].endpoint.Key != out[j].endpoint.Key {
			return out[i].endpoint.Key < out[j].endpoint.Key
		}
		return out[i].model.Key < out[j].model.Key
	})
	return out, nil
}

func (r resolver) matchingGrants(ctx context.Context, spaceMgr inferencestorage.SpaceManager, req ResolveRequest, profile domaininference.Profile, candidate capabilityCandidate) ([]grantCandidate, error) {
	grants, err := spaceMgr.ListCredentialGrants(ctx)
	if err != nil {
		return nil, err
	}
	credentials, err := r.manager.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return nil, err
	}
	secrets, err := r.manager.GlobalManager().ListSecrets(ctx)
	if err != nil {
		return nil, err
	}
	credentialByID := map[uuid.UUID]domaininference.Credential{}
	for _, credential := range credentials {
		credentialByID[credential.ID] = credential
	}
	secretByID := map[uuid.UUID]domaininference.Secret{}
	for _, secret := range secrets {
		secretByID[secret.ID] = secret
	}
	now := time.Now().UTC()
	out := []grantCandidate{}
	for _, grant := range grants {
		credential, ok := credentialByID[grant.CredentialID]
		if !ok {
			continue
		}
		secret := domaininference.Secret{}
		if credential.AuthType != domaininference.CredentialAuthNone {
			var ok bool
			secret, ok = secretByID[credential.SecretID]
			if !ok {
				continue
			}
		}
		if !grantMatches(grant, credential, req, profile, candidate, now) {
			continue
		}
		out = append(out, grantCandidate{grant: grant, credential: credential, secret: secret, score: grantSpecificity(grant, req)})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		if out[i].grant.Priority != out[j].grant.Priority {
			return out[i].grant.Priority > out[j].grant.Priority
		}
		return out[i].grant.CreatedAt.Before(out[j].grant.CreatedAt)
	})
	return out, nil
}

func (r resolver) evaluatePolicies(req ResolveRequest, profile domaininference.Profile, candidate capabilityCandidate, policies []domaininference.Policy) policyEvaluation {
	now := time.Now().UTC()
	matched := []domaininference.Policy{}
	for _, policy := range policies {
		if policyMatches(policy, req, profile, now) {
			matched = append(matched, policy)
		}
	}
	if len(matched) == 0 {
		return policyEvaluation{reason: "no matching inference policy"}
	}
	sort.SliceStable(matched, func(i, j int) bool {
		if matched[i].Priority != matched[j].Priority {
			return matched[i].Priority > matched[j].Priority
		}
		return scopeSpecificity(matched[i].Scope, req) > scopeSpecificity(matched[j].Scope, req)
	})
	ids := make([]string, 0, len(matched))
	hasAllow := false
	var allowedPrivacy []domaininference.PrivacyClass
	requireLocal := false
	disallowThirdParty := false
	maxInput := 0
	maxOutput := 0
	for _, policy := range matched {
		ids = append(ids, policy.ID.String())
		if policy.NoInference || policy.Action == domaininference.PolicyActionDeny {
			return policyEvaluation{matchedPolicyIDs: ids, reason: firstNonEmpty(policy.Reason, "inference denied by policy")}
		}
		if policy.Action == domaininference.PolicyActionAllow || policy.Action == domaininference.PolicyActionRestrict {
			hasAllow = true
		}
		if len(policy.AllowedPrivacyClasses) > 0 {
			allowedPrivacy = append(allowedPrivacy, policy.AllowedPrivacyClasses...)
		}
		if policy.RequireLocalEndpoint {
			requireLocal = true
		}
		if policy.DisallowThirdParty {
			disallowThirdParty = true
		}
		if policy.MaxInputTokens > 0 && (maxInput == 0 || policy.MaxInputTokens < maxInput) {
			maxInput = policy.MaxInputTokens
		}
		if policy.MaxOutputTokens > 0 && (maxOutput == 0 || policy.MaxOutputTokens < maxOutput) {
			maxOutput = policy.MaxOutputTokens
		}
	}
	if !hasAllow {
		return policyEvaluation{matchedPolicyIDs: ids, reason: "no allow inference policy matched request"}
	}
	if len(allowedPrivacy) > 0 && !privacyClassAllowed(allowedPrivacy, candidate.endpoint.PrivacyClass) {
		return policyEvaluation{matchedPolicyIDs: ids, reason: "endpoint privacy class is not allowed by policy"}
	}
	if requireLocal && candidate.endpoint.NetworkClass != domaininference.NetworkClassLocal {
		return policyEvaluation{matchedPolicyIDs: ids, reason: "policy requires local endpoint"}
	}
	if disallowThirdParty && candidate.endpoint.PrivacyClass == domaininference.PrivacyClassThirdParty {
		return policyEvaluation{matchedPolicyIDs: ids, reason: "policy disallows third-party endpoint"}
	}
	params := mergeParameters(profile.DefaultParameters, req.Parameters)
	if maxInput > 0 && params.MaxInputTokens > maxInput {
		return policyEvaluation{matchedPolicyIDs: ids, reason: "request exceeds policy max input tokens"}
	}
	if maxOutput > 0 && params.MaxOutputTokens > maxOutput {
		return policyEvaluation{matchedPolicyIDs: ids, reason: "request exceeds policy max output tokens"}
	}
	return policyEvaluation{allowed: true, matchedPolicyIDs: ids, reason: "inference allowed"}
}

func (r resolver) allow(ctx context.Context, spaceMgr inferencestorage.SpaceManager, req ResolveRequest, result ResolveResult, policyIDs []string, reason string) (ResolveResult, error) {
	decision, err := spaceMgr.UpsertPolicyDecision(ctx, decisionFromResolve(req, result, domaininference.PolicyDecisionAllowed, policyIDs, reason))
	if err != nil {
		return ResolveResult{}, err
	}
	result.Allowed = true
	result.Decision = decision
	return result, nil
}

func (r resolver) deny(ctx context.Context, spaceMgr inferencestorage.SpaceManager, req ResolveRequest, result ResolveResult, policyIDs []string, reason string) (ResolveResult, error) {
	decision, err := spaceMgr.UpsertPolicyDecision(ctx, decisionFromResolve(req, result, domaininference.PolicyDecisionDenied, policyIDs, reason))
	if err != nil {
		return ResolveResult{}, err
	}
	result.Allowed = false
	result.Decision = decision
	return result, nil
}

func decisionFromResolve(req ResolveRequest, result ResolveResult, action domaininference.PolicyDecisionAction, policyIDs []string, reason string) domaininference.PolicyDecision {
	return domaininference.PolicyDecision{SpaceID: req.SpaceID, DomainID: req.DomainID, NodeID: req.NodeID, SemanticRuleID: req.SemanticRuleID, EmbeddingBindingKey: req.EmbeddingBindingKey, Operation: req.Operation, UsageMode: req.UsageMode, ProfileID: result.Profile.ID, CapabilityID: result.Capability.ID, EndpointID: result.Endpoint.ID, ModelID: result.Model.ID, CredentialID: result.Credential.ID, CredentialGrantID: result.CredentialGrant.ID, ActorPrincipalID: req.ActorPrincipalID, OnBehalfOfPrincipalID: req.OnBehalfOfPrincipalID, Action: action, MatchedPolicyIDs: append([]string(nil), policyIDs...), Reason: reason, Metadata: copyMap(req.Metadata)}
}

func candidateEnabled(candidate capabilityCandidate) bool {
	return candidate.capability.Enabled && candidate.endpoint.Enabled && candidate.model.Enabled
}

func candidateMatchesOperation(candidate capabilityCandidate, operation domaininference.Operation) bool {
	if candidate.capability.Operation != operation || !modelSupportsOperation(candidate.model, operation) {
		return false
	}
	return len(candidate.endpoint.Operations) == 0 || operationListContains(candidate.endpoint.Operations, operation)
}

func modelSupportsOperation(model domaininference.Model, operation domaininference.Operation) bool {
	switch operation {
	case domaininference.OperationEmbeddings:
		return model.Kind == domaininference.ModelKindEmbedding
	case domaininference.OperationRerank:
		return model.Kind == domaininference.ModelKindReranker
	case domaininference.OperationChat, domaininference.OperationSummarize, domaininference.OperationClassify:
		return model.Kind == domaininference.ModelKindGenerative
	case domaininference.OperationImageAnalysis:
		return model.Kind == domaininference.ModelKindGenerative && modalityListContains(model.InputModalities, "image") && (modalityListContains(model.OutputModalities, "text") || modalityListContains(model.OutputModalities, "json"))
	default:
		return false
	}
}

func modalityListContains(values []string, value string) bool {
	for _, item := range values {
		if strings.EqualFold(strings.TrimSpace(item), value) {
			return true
		}
	}
	return false
}

func candidateMatchesRefs(candidate capabilityCandidate, req ResolveRequest, profile domaininference.Profile) bool {
	return refsAllow(profile.CapabilityRefs, candidate.capability.ID, candidate.capability.Key) && refsAllow(profile.EndpointRefs, candidate.endpoint.ID, candidate.endpoint.Key) && refsAllow(profile.ModelRefs, candidate.model.ID, candidate.model.Key) && refRequestMatches(req.CapabilityID, req.CapabilityRef, candidate.capability.ID, candidate.capability.Key) && refRequestMatches(req.EndpointID, req.EndpointRef, candidate.endpoint.ID, candidate.endpoint.Key) && refRequestMatches(req.ModelID, req.ModelRef, candidate.model.ID, candidate.model.Key)
}

func candidateMatchesPrivacy(candidate capabilityCandidate, requirement domaininference.PrivacyRequirement) bool {
	if len(requirement.AllowedPrivacyClasses) > 0 && !privacyClassAllowed(requirement.AllowedPrivacyClasses, candidate.endpoint.PrivacyClass) {
		return false
	}
	if requirement.RequireLocalEndpoint && candidate.endpoint.NetworkClass != domaininference.NetworkClassLocal {
		return false
	}
	if requirement.DisallowThirdParty && candidate.endpoint.PrivacyClass == domaininference.PrivacyClassThirdParty {
		return false
	}
	return true
}

func candidateMatchesFeatures(candidate capabilityCandidate, features []string) bool {
	for _, feature := range features {
		switch strings.ToLower(strings.TrimSpace(feature)) {
		case "", "text":
			continue
		case "json", "json_mode", "json-mode":
			if !candidate.capability.SupportsJSONMode {
				return false
			}
		case "tools", "tool_calls", "tool-calls":
			if !candidate.capability.SupportsToolCalls {
				return false
			}
		case "embeddings", "embedding":
			if candidate.capability.Operation != domaininference.OperationEmbeddings {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func candidateMatchesParameters(candidate capabilityCandidate, requested domaininference.Parameters, defaults domaininference.Parameters) bool {
	params := mergeParameters(defaults, requested)
	if candidate.capability.MaxInputTokens > 0 && params.MaxInputTokens > candidate.capability.MaxInputTokens {
		return false
	}
	if candidate.capability.MaxOutputTokens > 0 && params.MaxOutputTokens > candidate.capability.MaxOutputTokens {
		return false
	}
	return true
}

func grantMatches(grant domaininference.CredentialGrant, credential domaininference.Credential, req ResolveRequest, profile domaininference.Profile, candidate capabilityCandidate, now time.Time) bool {
	if grant.SpaceID != "" && grant.SpaceID != req.SpaceID {
		return false
	}
	if grant.State != "" && grant.State != domaininference.GrantStateActive {
		return false
	}
	if !grant.ExpiresAt.IsZero() && !grant.ExpiresAt.After(now) {
		return false
	}
	if credential.Status != domaininference.CredentialStatusActive || credential.EndpointID != candidate.endpoint.ID {
		return false
	}
	if !credentialOwnerMatches(grant, credential, req) {
		return false
	}
	if !scopeMatches(grant.Scope, req) || !operationRefsMatch(grant.Operations, req.Operation) || !usageModesMatch(grant.UsageModes, req.UsageMode) || !principalListMatches(grant.GranteePrincipals, req.ActorPrincipalID) || !onBehalfPrincipalMatches(grant.AllowOnBehalfOfPrincipals, req.ActorPrincipalID, req.OnBehalfOfPrincipalID) {
		return false
	}
	if !refsAllow(grant.ProfileRefs, profile.ID, profile.Key) || !refsAllow(grant.CapabilityRefs, candidate.capability.ID, candidate.capability.Key) || !refsAllow(grant.EndpointRefs, candidate.endpoint.ID, candidate.endpoint.Key) || !refsAllow(grant.ModelRefs, candidate.model.ID, candidate.model.Key) {
		return false
	}
	return true
}

func credentialOwnerMatches(grant domaininference.CredentialGrant, credential domaininference.Credential, req ResolveRequest) bool {
	switch credential.OwnerType {
	case domaininference.CredentialOwnerSystem:
		return true
	case domaininference.CredentialOwnerSpace:
		return strings.TrimSpace(credential.OwnerID) == "" || credential.OwnerID == req.SpaceID
	case domaininference.CredentialOwnerPrincipal:
		owner := strings.TrimSpace(credential.OwnerID)
		if owner == "" {
			return false
		}
		if owner == strings.TrimSpace(req.ActorPrincipalID) {
			return true
		}
		if owner == strings.TrimSpace(req.OnBehalfOfPrincipalID) {
			return principalListExplicitlyAllows(grant.AllowOnBehalfOfPrincipals, owner)
		}
		return false
	default:
		return false
	}
}

func principalListExplicitlyAllows(values []string, principalID string) bool {
	principalID = strings.TrimSpace(principalID)
	if principalID == "" {
		return false
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "*" || value == principalID {
			return true
		}
	}
	return false
}

func onBehalfPrincipalMatches(values []string, actorPrincipalID string, onBehalfPrincipalID string) bool {
	actorPrincipalID = strings.TrimSpace(actorPrincipalID)
	onBehalfPrincipalID = strings.TrimSpace(onBehalfPrincipalID)
	if onBehalfPrincipalID == "" || strings.EqualFold(actorPrincipalID, onBehalfPrincipalID) {
		return true
	}
	return principalListExplicitlyAllows(values, onBehalfPrincipalID)
}

func policyMatches(policy domaininference.Policy, req ResolveRequest, profile domaininference.Profile, now time.Time) bool {
	if policy.SpaceID != "" && policy.SpaceID != req.SpaceID {
		return false
	}
	if policy.State != "" && policy.State != domaininference.PolicyStateActive {
		return false
	}
	if !policy.ExpiresAt.IsZero() && !policy.ExpiresAt.After(now) {
		return false
	}
	return scopeMatches(policy.Scope, req) && operationRefsMatch(policy.Operations, req.Operation) && refsAllow(policy.ProfileRefs, profile.ID, profile.Key)
}

func scopeMatches(scope domaininference.Scope, req ResolveRequest) bool {
	if scope.SpaceID != "" && scope.SpaceID != req.SpaceID {
		return false
	}
	if scope.DomainID != "" && scope.DomainID != req.DomainID {
		return false
	}
	if scope.SemanticRuleID != "" && scope.SemanticRuleID != req.SemanticRuleID {
		return false
	}
	if scope.EmbeddingBindingKey != "" && scope.EmbeddingBindingKey != req.EmbeddingBindingKey {
		return false
	}
	if scope.SemanticIndexID != "" && scope.SemanticIndexID != req.SemanticIndexID {
		return false
	}
	if scope.NodeID != "" && scope.NodeID != req.NodeID {
		return false
	}
	return true
}

func scopeSpecificity(scope domaininference.Scope, req ResolveRequest) int {
	score := 0
	if scope.SpaceID != "" {
		score++
	}
	if scope.DomainID != "" {
		score += 2
	}
	if scope.SemanticRuleID != "" {
		score += 4
	}
	if scope.EmbeddingBindingKey != "" {
		score += 4
	}
	if scope.SemanticIndexID != "" {
		score += 4
	}
	if scope.NodeID != "" {
		score += 8
	}
	return score
}

func grantSpecificity(grant domaininference.CredentialGrant, req ResolveRequest) int {
	return scopeSpecificity(grant.Scope, req) + len(grant.ProfileRefs) + len(grant.CapabilityRefs) + len(grant.EndpointRefs) + len(grant.ModelRefs) + len(grant.UsageModes) + len(grant.GranteePrincipals) + len(grant.AllowOnBehalfOfPrincipals)
}

func operationRefsMatch(values []domaininference.Operation, value domaininference.Operation) bool {
	return len(values) == 0 || operationListContains(values, value)
}

func operationListContains(values []domaininference.Operation, value domaininference.Operation) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func usageModesMatch(values []domaininference.UsageMode, value domaininference.UsageMode) bool {
	if len(values) == 0 {
		return true
	}
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func principalListMatches(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	return stringListContains(values, value)
}

func refsAllow(refs []string, id uuid.UUID, key string) bool {
	if len(refs) == 0 {
		return true
	}
	for _, ref := range refs {
		if refMatches(id, key, ref) {
			return true
		}
	}
	return false
}

func refRequestMatches(requestID uuid.UUID, requestRef string, id uuid.UUID, key string) bool {
	if requestID != uuid.Nil && requestID != id {
		return false
	}
	if strings.TrimSpace(requestRef) != "" && !refMatches(id, key, requestRef) {
		return false
	}
	return true
}

func refMatches(id uuid.UUID, key, ref string) bool {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false
	}
	if parsed, err := uuid.Parse(ref); err == nil && parsed == id {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(key), ref)
}

func stringListContains(values []string, value string) bool {
	for _, item := range values {
		if strings.EqualFold(strings.TrimSpace(item), strings.TrimSpace(value)) {
			return true
		}
	}
	return false
}

func privacyClassAllowed(values []domaininference.PrivacyClass, value domaininference.PrivacyClass) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func mergeParameters(defaults, requested domaininference.Parameters) domaininference.Parameters {
	out := defaults
	if requested.Temperature != nil {
		out.Temperature = requested.Temperature
	}
	if requested.MaxInputTokens != 0 {
		out.MaxInputTokens = requested.MaxInputTokens
	}
	if requested.MaxOutputTokens != 0 {
		out.MaxOutputTokens = requested.MaxOutputTokens
	}
	if requested.ResponseFormat != "" {
		out.ResponseFormat = requested.ResponseFormat
	}
	if requested.Metadata != nil {
		out.Metadata = copyMap(requested.Metadata)
	}
	return out
}

func copyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (r resolver) validatePrincipalContext(ctx context.Context, req ResolveRequest) error {
	checkerProvider, ok := r.manager.(interface{ principalStatusChecker() PrincipalStatusChecker })
	if !ok {
		return nil
	}
	checker := checkerProvider.principalStatusChecker()
	if checker == nil {
		return nil
	}
	for _, principal := range []struct {
		label string
		id    string
	}{
		{label: "actor principal", id: req.ActorPrincipalID},
		{label: "on-behalf-of principal", id: req.OnBehalfOfPrincipalID},
	} {
		principalID := strings.TrimSpace(principal.id)
		if principalID == "" {
			continue
		}
		active, err := checker.IsPrincipalActive(ctx, principalID)
		if err != nil {
			return fmt.Errorf("%s %q is not active", principal.label, principalID)
		}
		if !active {
			return fmt.Errorf("%s %q is disabled", principal.label, principalID)
		}
	}
	return nil
}
