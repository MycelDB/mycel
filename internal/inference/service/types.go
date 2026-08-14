package service

import (
	"context"

	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
)

const ModuleName = "inference"

type Manager interface {
	GlobalManager() inferencestorage.GlobalManager
	SpaceManager(ctx context.Context, spaceID string) (inferencestorage.SpaceManager, error)
	UsageLedger() inferencestorage.UsageLedger
	Resolve(ctx context.Context, req ResolveRequest) (ResolveResult, error)
}
