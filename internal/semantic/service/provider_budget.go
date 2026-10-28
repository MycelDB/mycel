package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const providerBudgetTransitionRetryInterval = 5 * time.Second

type semanticBudgetManager interface {
	ValidateSemanticBudgetScope(context.Context, inferenceruntime.SemanticBudgetScopeRequest) error
	EnsureSemanticBudgetScope(context.Context, inferenceruntime.SemanticBudgetScopeRequest) (domaininference.BudgetScope, error)
	CurrentSemanticBudgetScope(context.Context, string, string, string) (domaininference.BudgetScope, error)
	GetBudgetScope(context.Context, domaininference.BudgetScopeID) (domaininference.BudgetScope, error)
	CompleteBudgetScope(context.Context, domaininference.BudgetScopeID, time.Time) (domaininference.BudgetScope, error)
	ReactivateBudgetScope(context.Context, domaininference.BudgetScopeID, time.Time) (domaininference.BudgetScope, error)
}

func (m *Module) startProviderBudgetTransitionReconciler(parent context.Context) {
	m.mu.Lock()
	if m.budgetTransitionCancel != nil {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.budgetTransitionCancel = cancel
	m.mu.Unlock()
	m.maintenanceWG.Add(1)
	go func() {
		defer m.maintenanceWG.Done()
		ticker := time.NewTicker(providerBudgetTransitionRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if err := m.reconcileProviderBudgetTransitions(ctx); err != nil && !errors.Is(err, context.Canceled) && m.logger != nil {
				m.logger.Warn("semantic provider budget transition reconciliation pending")
			}
		}
	}()
}

func (m *Module) reconcileProviderBudgetTransitions(ctx context.Context) error {
	spaces, err := m.listBaseSpaceManagers(ctx)
	if err != nil {
		return err
	}
	var reconcileErr error
	for _, space := range spaces {
		manager, openErr := m.SpaceManager(ctx, space.SpaceID)
		if openErr != nil {
			reconcileErr = errors.Join(reconcileErr, openErr)
			continue
		}
		rules, listErr := manager.ListSemanticRules(ctx)
		if listErr != nil {
			reconcileErr = errors.Join(reconcileErr, listErr)
			continue
		}
		for _, rule := range rules {
			if rule.ProviderBudgetTransition == "" {
				continue
			}
			if err := m.reconcileProviderBudgetTransition(ctx, manager, rule); err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
			}
		}
	}
	return reconcileErr
}

func (m *Module) reconcileProviderBudgetTransition(ctx context.Context, manager storesemantic.SpaceManager, rule domainsemantic.SemanticGenerationRule) error {
	budgetManager, ok := m.inferenceInvoker.(semanticBudgetManager)
	if !ok && len(rule.ProviderBudgetTransitionScopeIDs) != 0 {
		return fmt.Errorf("semantic provider budget enforcement is unavailable")
	}
	for _, rawID := range rule.ProviderBudgetTransitionScopeIDs {
		id := domaininference.BudgetScopeID(rawID)
		// Recovery rolls forward even when a pre-crash reservation became
		// ambiguous. Completing the old scope blocks new attempts while retaining
		// every conservative charge and immutable attempt event.
		if _, err := budgetManager.CompleteBudgetScope(ctx, id, time.Now().UTC()); err != nil && !errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
			return err
		}
	}
	switch rule.ProviderBudgetTransition {
	case domainsemantic.SemanticProviderBudgetTransitionPrepare:
		if rule.Enabled && semanticRuleHasProviderBudgetModel(rule) {
			if err := m.PrepareSemanticProviderBudgets(ctx, rule); err != nil {
				return err
			}
		}
		if rule.ProviderBudgetTransitionRequiresRebuild {
			if err := m.MarkRuleConfigurationChanged(ctx, rule.SpaceID, rule); err != nil {
				return err
			}
		}
	case domainsemantic.SemanticProviderBudgetTransitionComplete:
	case domainsemantic.SemanticProviderBudgetTransitionReliability:
		if err := m.MarkRuleConfigurationChanged(ctx, rule.SpaceID, rule); err != nil {
			return err
		}
	case domainsemantic.SemanticProviderBudgetTransitionDelete:
		if rule.ProviderBudgetTransitionPurgeVectors {
			if err := m.PurgeVectorIndex(ctx, rule.SpaceID, domainsemantic.SemanticIndexID(rule.ID)); err != nil {
				return err
			}
		}
		return m.ConditionalDeleteSemanticRule(ctx, rule, rule.ProviderBudgetTransitionPurgeVectors)
	default:
		return fmt.Errorf("unsupported semantic provider budget transition")
	}
	expected := rule
	rule.ProviderBudgetTransition = ""
	rule.ProviderBudgetTransitionID = ""
	rule.ProviderBudgetTransitionScopeIDs = nil
	rule.ProviderBudgetTransitionRequiresRebuild = false
	rule.ProviderBudgetTransitionPurgeVectors = false
	rule.UpdatedAt = rule.UpdatedAt.Add(time.Nanosecond)
	return m.ConditionalUpsertSemanticRule(ctx, expected, rule)
}

func semanticRuleHasProviderBudgetModel(rule domainsemantic.SemanticGenerationRule) bool {
	for _, binding := range rule.Embeddings {
		if binding.ProviderBudget != nil {
			return true
		}
	}
	return false
}

func (m *Module) PreflightSemanticProviderBudgets(ctx context.Context, rule domainsemantic.SemanticGenerationRule) error {
	manager, requests, err := m.semanticProviderBudgetRequests(rule)
	if err != nil {
		return err
	}
	for _, request := range requests {
		if err := manager.ValidateSemanticBudgetScope(ctx, request); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) PrepareSemanticProviderBudgets(ctx context.Context, rule domainsemantic.SemanticGenerationRule) error {
	manager, requests, err := m.semanticProviderBudgetRequests(rule)
	if err != nil {
		return err
	}
	created := make([]domaininference.BudgetScopeID, 0, len(requests))
	for _, request := range requests {
		scope, err := manager.EnsureSemanticBudgetScope(ctx, request)
		if err != nil {
			cleanupErr := error(nil)
			for _, id := range created {
				if _, completeErr := manager.CompleteBudgetScope(context.WithoutCancel(ctx), id, time.Now().UTC()); completeErr != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("complete prepared semantic provider budget %s: %w", id, completeErr))
				}
			}
			return errors.Join(err, cleanupErr)
		}
		created = append(created, scope.ID)
	}
	return nil
}

func (m *Module) semanticProviderBudgetRequests(rule domainsemantic.SemanticGenerationRule) (semanticBudgetManager, []inferenceruntime.SemanticBudgetScopeRequest, error) {
	if rule.ID.String() == "00000000-0000-0000-0000-000000000000" {
		return nil, nil, fmt.Errorf("semantic rule id is required before provider budget preparation")
	}
	manager, ok := m.inferenceInvoker.(semanticBudgetManager)
	if !ok {
		for _, binding := range rule.Embeddings {
			if binding.ProviderBudget != nil {
				return nil, nil, fmt.Errorf("semantic provider budget enforcement is unavailable")
			}
		}
		return nil, nil, nil
	}
	requests := make([]inferenceruntime.SemanticBudgetScopeRequest, 0, len(rule.Embeddings))
	for _, binding := range rule.Embeddings {
		if binding.ProviderBudget == nil || !binding.Enabled {
			continue
		}
		budget := binding.ProviderBudget
		if budget.MaxRetryAttempts > math.MaxInt64-budget.MaxRequests {
			return nil, nil, fmt.Errorf("semantic provider budget attempt limit overflows")
		}
		requestMillis := budget.RequestTimeout.Milliseconds()
		totalMillis := budget.TotalTimeout.Milliseconds()
		if requestMillis <= 0 || totalMillis <= 0 {
			return nil, nil, fmt.Errorf("semantic provider budget timeout is below millisecond precision")
		}
		requests = append(requests, inferenceruntime.SemanticBudgetScopeRequest{
			SpaceID: rule.SpaceID.String(), DomainID: rule.DomainID.String(), SemanticRuleID: rule.ID.String(), EmbeddingBindingKey: binding.Key, CorrelationID: budget.CorrelationID,
			ProfileRef: binding.IntelligenceProfile, ProfileID: domaininference.ProfileID(binding.IntelligenceProfileID), ActorPrincipalID: rule.OwnerPrincipalID.String(), TotalTimeout: budget.TotalTimeout,
			Limits: domaininference.BudgetLimits{MaxRequests: budget.MaxRequests, MaxAttempts: budget.MaxRequests + budget.MaxRetryAttempts, MaxRetryAttempts: budget.MaxRetryAttempts, MaxRetriesPerRequest: budget.MaxRetriesPerRequest, MaxConcurrentProviderCalls: budget.MaxConcurrentProviderCalls, BatchingMode: domaininference.BatchingMode(budget.BatchingMode), MaxBatchItems: budget.MaxBatchItems, MaxInputTokensPerRequest: budget.MaxInputTokensPerRequest, MaxInputTokens: budget.MaxInputTokens, MaxProviderMillisPerRequest: requestMillis, MaxProviderMillis: totalMillis, Currency: budget.Currency, PriceDefinitionVersion: budget.PriceDefinitionVersion, InputMicrosPerMillionTokens: budget.InputMicrosPerMillionTokens, MaxEstimatedCostMicros: budget.MaxEstimatedCostMicros},
		})
	}
	return manager, requests, nil
}

func (m *Module) CurrentSemanticProviderBudgetScopeIDs(ctx context.Context, rule domainsemantic.SemanticGenerationRule) ([]domaininference.BudgetScopeID, error) {
	manager, ok := m.inferenceInvoker.(semanticBudgetManager)
	if !ok {
		return nil, fmt.Errorf("semantic provider budget enforcement is unavailable")
	}
	ids := make([]domaininference.BudgetScopeID, 0, len(rule.Embeddings))
	for _, binding := range rule.Embeddings {
		if binding.ProviderBudget == nil {
			continue
		}
		scope, err := manager.CurrentSemanticBudgetScope(ctx, rule.SpaceID.String(), rule.ID.String(), binding.Key)
		if errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, scope.ID)
	}
	return ids, nil
}

func (m *Module) ReactivateSemanticProviderBudgetScopes(ctx context.Context, ids []domaininference.BudgetScopeID) error {
	manager, ok := m.inferenceInvoker.(semanticBudgetManager)
	if !ok {
		return fmt.Errorf("semantic provider budget enforcement is unavailable")
	}
	var reactivationErr error
	for _, id := range ids {
		if err := reactivateSemanticProviderBudgetScope(ctx, manager, id); err != nil {
			reactivationErr = errors.Join(reactivationErr, fmt.Errorf("reactivate semantic provider budget %s: %w", id, err))
		}
	}
	return reactivationErr
}

func reactivateSemanticProviderBudgetScope(ctx context.Context, manager semanticBudgetManager, id domaininference.BudgetScopeID) error {
	scope, err := manager.GetBudgetScope(ctx, id)
	if err != nil {
		return err
	}
	if scope.State == domaininference.BudgetScopeActive {
		return nil
	}
	if scope.State != domaininference.BudgetScopeCompleted {
		return fmt.Errorf("semantic provider budget scope cannot be reactivated from state %s", scope.State)
	}
	_, err = manager.ReactivateBudgetScope(ctx, id, time.Now().UTC())
	return err
}

func (m *Module) CompleteSemanticProviderBudgets(ctx context.Context, rule domainsemantic.SemanticGenerationRule, requireQuiescent bool) error {
	manager, ok := m.inferenceInvoker.(semanticBudgetManager)
	if !ok {
		for _, binding := range rule.Embeddings {
			if binding.ProviderBudget != nil {
				return fmt.Errorf("semantic provider budget enforcement is unavailable")
			}
		}
		return nil
	}
	scopes := make([]domaininference.BudgetScope, 0, len(rule.Embeddings))
	for _, binding := range rule.Embeddings {
		if binding.ProviderBudget == nil {
			continue
		}
		scope, err := manager.CurrentSemanticBudgetScope(ctx, rule.SpaceID.String(), rule.ID.String(), binding.Key)
		if errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if requireQuiescent && (scope.Usage.ActiveProviderCalls != 0 || scope.Usage.AmbiguousAttempts != 0) {
			return status.Error(codes.FailedPrecondition, "semantic provider budget scope has reserved or ambiguous attempts")
		}
		scopes = append(scopes, scope)
	}
	for _, scope := range scopes {
		if _, err := manager.CompleteBudgetScope(ctx, scope.ID, time.Now().UTC()); err != nil {
			for _, rollbackScope := range scopes {
				if rollbackErr := reactivateSemanticProviderBudgetScope(context.WithoutCancel(ctx), manager, rollbackScope.ID); rollbackErr != nil {
					err = errors.Join(err, fmt.Errorf("reactivate semantic provider budget %s after partial completion: %w", rollbackScope.ID, rollbackErr))
				}
			}
			return err
		}
	}
	return nil
}
