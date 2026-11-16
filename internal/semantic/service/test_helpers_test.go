package service

import (
	"context"
	"testing"

	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func seedTestIntelligenceProfile(t *testing.T, ctx context.Context, manager storesemantic.SpaceManager, spaceID domainspace.SpaceID) domainsemantic.IntelligenceProfile {
	return seedTestIntelligenceProfileWithConfig(t, ctx, manager, domainsemantic.IntelligenceProfile{
		SpaceID:   spaceID,
		Key:       "profile",
		Operation: domainsemantic.OperationEmbeddings,
		Enabled:   true,
	})
}

func seedTestIntelligenceProfileWithConfig(t *testing.T, ctx context.Context, manager storesemantic.SpaceManager, profile domainsemantic.IntelligenceProfile) domainsemantic.IntelligenceProfile {
	t.Helper()
	stored, err := manager.UpsertIntelligenceProfile(ctx, profile)
	if err != nil {
		t.Fatalf("seed intelligence profile: %v", err)
	}
	return stored
}
