package service

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestModuleExplicitOnlyDomainIsDirectlyAddressableButNotListed(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	ownerID := uuid.New().String()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "space", OwnerUserID: uuid.MustParse(ownerID)})
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := m.CreateDomain(ctx, ownerID, CreateDomainInput{SpaceID: sp.SpaceID.String(), Key: "explicit", Name: "Explicit", DiscoveryMode: graph.DomainDiscoveryModeExplicitOnly})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := m.ListVisibleDomains(ctx, ownerID, sp.SpaceID.String(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, domain := range listed {
		if domain.ID == explicit.ID {
			t.Fatalf("explicit-only domain was listed: %#v", listed)
		}
	}
	got, err := m.GetVisibleDomain(ctx, ownerID, sp.SpaceID.String(), explicit.ID.String(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != explicit.ID || got.DiscoveryMode != graph.DomainDiscoveryModeExplicitOnly {
		t.Fatalf("direct get = %#v, want %#v", got, explicit)
	}
}

func TestModuleUpdateDomainPolicyRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	ownerID := uuid.New().String()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "space", OwnerUserID: uuid.MustParse(ownerID)})
	if err != nil {
		t.Fatal(err)
	}
	domain, err := m.CreateDomain(ctx, ownerID, CreateDomainInput{SpaceID: sp.SpaceID.String(), Key: "mode", Name: "Mode"})
	if err != nil {
		t.Fatal(err)
	}
	discoveryMode := graph.DomainDiscoveryModeExplicitOnly
	searchMode := graph.DomainSearchModeExplicitOnly
	semanticMode := graph.DomainSemanticModeExplicitOnly
	readOnly := true
	updated, err := m.UpdateDomain(ctx, ownerID, UpdateDomainInput{SpaceID: sp.SpaceID.String(), DomainID: domain.ID.String(), DiscoveryMode: &discoveryMode, SearchMode: &searchMode, SemanticMode: &semanticMode, ReadOnly: &readOnly})
	if err != nil {
		t.Fatal(err)
	}
	if updated.DiscoveryMode != graph.DomainDiscoveryModeExplicitOnly || updated.SearchMode != graph.DomainSearchModeExplicitOnly || updated.SemanticMode != graph.DomainSemanticModeExplicitOnly || !updated.ReadOnly {
		t.Fatalf("updated policy = %+v", updated)
	}
}
