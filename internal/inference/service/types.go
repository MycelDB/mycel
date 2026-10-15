package service

import (
	"context"

	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
)

const ModuleName = "inference"

type PrincipalStatusChecker interface {
	IsPrincipalActive(ctx context.Context, principalID string) (bool, error)
}

type Manager interface {
	GlobalManager() inferencestorage.GlobalManager
	SpaceManager(ctx context.Context, spaceID string) (inferencestorage.SpaceManager, error)
	UsageLedger() inferencestorage.UsageLedger
	RequireLocalWriteAllowed() error
	Resolve(ctx context.Context, req inferenceruntime.ResolveRequest) (inferenceruntime.ResolveResult, error)
	Invoke(ctx context.Context, req inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error)
}
