package service

import (
	"context"

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
	Resolve(ctx context.Context, req ResolveRequest) (ResolveResult, error)
	Invoke(ctx context.Context, req InvokeRequest) (InvokeResponse, error)
}
