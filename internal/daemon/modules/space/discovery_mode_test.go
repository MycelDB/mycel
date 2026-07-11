package space

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/graph/model"
)

func TestModuleDirectOnlyDomainIsDirectlyAddressableButNotListed(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	ownerID := uuid.New().String()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "space", OwnerUserID: uuid.MustParse(ownerID)})
	if err != nil {
		t.Fatal(err)
	}
	direct, err := m.CreateDomain(ctx, ownerID, CreateDomainInput{SpaceID: sp.SpaceID.String(), Key: "direct", Name: "Direct", DiscoveryMode: graph.DomainDiscoveryModeDirectOnly})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := m.ListVisibleDomains(ctx, ownerID, sp.SpaceID.String(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, domain := range listed {
		if domain.ID == direct.ID {
			t.Fatalf("direct_only domain was listed: %#v", listed)
		}
	}
	got, err := m.GetVisibleDomain(ctx, ownerID, sp.SpaceID.String(), direct.ID.String(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != direct.ID || got.DiscoveryMode != graph.DomainDiscoveryModeDirectOnly {
		t.Fatalf("direct get = %#v, want %#v", got, direct)
	}
}

func TestModuleUpdateDiscoveryModeRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
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
	mode := graph.DomainDiscoveryModeDirectOnly
	updated, err := m.UpdateDomain(ctx, ownerID, UpdateDomainInput{SpaceID: sp.SpaceID.String(), DomainID: domain.ID.String(), DiscoveryMode: &mode})
	if err != nil {
		t.Fatal(err)
	}
	if updated.DiscoveryMode != graph.DomainDiscoveryModeDirectOnly {
		t.Fatalf("updated discovery mode = %q, want direct_only", updated.DiscoveryMode)
	}
}
