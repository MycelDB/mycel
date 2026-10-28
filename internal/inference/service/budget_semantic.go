package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
)

func (m *Module) ValidateSemanticBudgetScope(ctx context.Context, req inferenceruntime.SemanticBudgetScopeRequest) error {
	_, _, _, err := m.preflightSemanticBudgetScope(ctx, req, time.Now().UTC())
	return err
}

func (m *Module) EnsureSemanticBudgetScope(ctx context.Context, req inferenceruntime.SemanticBudgetScopeRequest) (domaininference.BudgetScope, error) {
	now := time.Now().UTC()
	limits, _, route, err := m.preflightSemanticBudgetScope(ctx, req, now)
	if err != nil {
		return domaininference.BudgetScope{}, err
	}
	current, currentErr := m.CurrentSemanticBudgetScope(ctx, req.SpaceID, req.SemanticRuleID, req.EmbeddingBindingKey)
	hadCurrent := currentErr == nil
	if hadCurrent {
		if current.CorrelationID == req.CorrelationID && current.Route == route && semanticBudgetLimitsEqual(current.Limits, limits) {
			return current, nil
		}
		if _, err := m.CompleteBudgetScope(ctx, current.ID, now); err != nil {
			return domaininference.BudgetScope{}, err
		}
	}
	if !hadCurrent && !errors.Is(currentErr, inferencestorage.ErrBudgetScopeNotFound) {
		return domaininference.BudgetScope{}, currentErr
	}
	scope := domaininference.BudgetScope{
		ID:                  domaininference.BudgetScopeID(uuid.NewString()),
		SpaceID:             req.SpaceID,
		SemanticRuleID:      req.SemanticRuleID,
		EmbeddingBindingKey: strings.ToLower(strings.TrimSpace(req.EmbeddingBindingKey)),
		CorrelationID:       req.CorrelationID,
		State:               domaininference.BudgetScopeActive,
		Route:               route,
		Limits:              limits,
		CreatedAt:           now,
		UpdatedAt:           now,
		ExpiresAt:           limits.Deadline,
	}
	created, err := m.CreateBudgetScope(ctx, scope)
	if err != nil && hadCurrent {
		if _, rollbackErr := m.ReactivateBudgetScope(context.WithoutCancel(ctx), current.ID, time.Now().UTC()); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("reactivate previous semantic provider budget: %w", rollbackErr))
		}
	}
	return created, err
}

func (m *Module) preflightSemanticBudgetScope(ctx context.Context, req inferenceruntime.SemanticBudgetScopeRequest, now time.Time) (domaininference.BudgetLimits, inferenceruntime.ResolveResult, domaininference.BudgetRoute, error) {
	for kind, value := range map[string]string{
		"space":             req.SpaceID,
		"semantic_rule":     req.SemanticRuleID,
		"embedding_binding": req.EmbeddingBindingKey,
		"correlation":       req.CorrelationID,
	} {
		if err := validateBudgetIdentity(kind, strings.TrimSpace(value)); err != nil {
			return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, err
		}
	}
	if req.TotalTimeout <= 0 {
		return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, fmt.Errorf("semantic budget total timeout must be positive")
	}
	limits := req.Limits
	limits.Deadline = now.Add(req.TotalTimeout)
	resolved, err := m.Resolve(ctx, inferenceruntime.ResolveRequest{
		SpaceID: req.SpaceID, DomainID: req.DomainID,
		SemanticRuleID: req.SemanticRuleID, EmbeddingBindingKey: req.EmbeddingBindingKey,
		SemanticIndexID: req.SemanticRuleID, Operation: domaininference.OperationEmbeddings,
		UsageMode: domaininference.UsageModeSemantic, ProfileRef: req.ProfileRef, ProfileID: req.ProfileID,
		ActorPrincipalID: req.ActorPrincipalID,
	})
	if err != nil {
		return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, err
	}
	if !resolved.Allowed {
		return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, inferenceruntime.ErrDenied
	}
	connector := m.connector(resolved.Endpoint.ConnectorType)
	if connector == nil {
		return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, fmt.Errorf("connector %q is not registered", resolved.Endpoint.ConnectorType)
	}
	descriptor := connectors.ConnectorBatchingDescriptor(connector)
	if err := validateResolvedBudgetBatching(limits, resolved.Capability, descriptor, 1); err != nil {
		return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, err
	}
	route := domaininference.BudgetRoute{EndpointID: resolved.Endpoint.ID, ModelID: resolved.Model.ID, CapabilityID: resolved.Capability.ID, Operation: domaininference.OperationEmbeddings}
	candidate := domaininference.BudgetScope{ID: domaininference.BudgetScopeID(uuid.NewString()), SpaceID: req.SpaceID, SemanticRuleID: req.SemanticRuleID, EmbeddingBindingKey: strings.ToLower(strings.TrimSpace(req.EmbeddingBindingKey)), CorrelationID: req.CorrelationID, State: domaininference.BudgetScopeActive, Route: route, Limits: limits, CreatedAt: now, UpdatedAt: now, ExpiresAt: limits.Deadline, RetentionUntil: limits.Deadline.Add(maxBudgetRetention)}
	if err := validateBudgetScope(candidate, now); err != nil {
		return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, err
	}
	if _, err := invocationInputTokenReservation(inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{Operation: domaininference.OperationEmbeddings}}, resolved, limits); err != nil {
		return domaininference.BudgetLimits{}, inferenceruntime.ResolveResult{}, domaininference.BudgetRoute{}, err
	}
	return limits, resolved, route, nil
}

func semanticBudgetLimitsEqual(left, right domaininference.BudgetLimits) bool {
	left.Deadline = time.Time{}
	right.Deadline = time.Time{}
	return left == right
}

func (m *Module) CurrentSemanticBudgetScope(ctx context.Context, spaceID, semanticRuleID, embeddingBindingKey string) (domaininference.BudgetScope, error) {
	bindingKey := strings.ToLower(strings.TrimSpace(embeddingBindingKey))
	scopes, err := m.UsageLedger().ListBudgetScopes(ctx)
	if err != nil {
		return domaininference.BudgetScope{}, err
	}
	var current domaininference.BudgetScope
	for _, scope := range scopes {
		if scope.SpaceID != strings.TrimSpace(spaceID) || scope.SemanticRuleID != strings.TrimSpace(semanticRuleID) || scope.EmbeddingBindingKey != bindingKey {
			continue
		}
		resolved, err := m.GetBudgetScope(ctx, scope.ID)
		if err != nil {
			return domaininference.BudgetScope{}, err
		}
		if resolved.State != domaininference.BudgetScopeActive {
			continue
		}
		if current.ID != "" {
			return domaininference.BudgetScope{}, ErrBudgetPersistence
		}
		current = resolved
	}
	if current.ID == "" {
		return domaininference.BudgetScope{}, inferencestorage.ErrBudgetScopeNotFound
	}
	return current, nil
}
