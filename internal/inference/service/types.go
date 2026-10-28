package service

import (
	"context"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
)

const ModuleName = "inference"

type AuthorizationRevisionSource interface {
	GlobalAuthorizationRevision(ctx context.Context) (uint64, error)
	SpaceAuthorizationRevision(ctx context.Context, spaceID string) (uint64, error)
}

type PrincipalStatusChecker interface {
	IsPrincipalActive(ctx context.Context, principalID string) (bool, error)
}

type Manager interface {
	GlobalManager() inferencestorage.GlobalManager
	SpaceManager(ctx context.Context, spaceID string) (inferencestorage.SpaceManager, error)
	UsageLedger() inferencestorage.UsageLedger
	RequireLocalWriteAllowed() error
	RequireBudgetWriteAllowed() error
	EnsureLinearizableBudgetRead(ctx context.Context) error
	Resolve(ctx context.Context, req inferenceruntime.ResolveRequest) (inferenceruntime.ResolveResult, error)
	Invoke(ctx context.Context, req inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error)
	ValidateSemanticBudgetScope(ctx context.Context, req inferenceruntime.SemanticBudgetScopeRequest) error
	EnsureSemanticBudgetScope(ctx context.Context, req inferenceruntime.SemanticBudgetScopeRequest) (domaininference.BudgetScope, error)
	CurrentSemanticBudgetScope(ctx context.Context, spaceID, semanticRuleID, embeddingBindingKey string) (domaininference.BudgetScope, error)
	GetBudgetScope(ctx context.Context, id domaininference.BudgetScopeID) (domaininference.BudgetScope, error)
	ListBudgetScopes(ctx context.Context) ([]domaininference.BudgetScope, error)
}
