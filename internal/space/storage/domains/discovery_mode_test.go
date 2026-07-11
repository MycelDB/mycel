package domains

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestManagerDiscoveryModeDefaultsAndRoundTrips(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	if err := m.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	spaceID := domainspace.SpaceID(uuid.New())

	normal, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "normal"})
	if err != nil {
		t.Fatal(err)
	}
	if normal.DiscoveryMode != graph.DomainDiscoveryModeNormal {
		t.Fatalf("default discovery mode = %q, want normal", normal.DiscoveryMode)
	}

	direct, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "direct", DiscoveryMode: graph.DomainDiscoveryModeDirectOnly})
	if err != nil {
		t.Fatal(err)
	}
	if direct.DiscoveryMode != graph.DomainDiscoveryModeDirectOnly {
		t.Fatalf("discovery mode = %q, want direct_only", direct.DiscoveryMode)
	}

	got, err := m.GetByID(ctx, direct.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DiscoveryMode != graph.DomainDiscoveryModeDirectOnly {
		t.Fatalf("round-trip discovery mode = %q, want direct_only", got.DiscoveryMode)
	}
}

func TestManagerDiscoveryModeRejectsInvalid(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	if err := m.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	_, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "bad", DiscoveryMode: graph.DomainDiscoveryMode("private")})
	if err == nil {
		t.Fatal("expected invalid discovery mode error")
	}
	domain, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "good"})
	if err != nil {
		t.Fatal(err)
	}
	invalid := graph.DomainDiscoveryMode("private")
	_, err = m.Update(ctx, UpdateInput{DomainID: domain.ID, DiscoveryMode: &invalid})
	if err == nil {
		t.Fatal("expected invalid discovery mode update error")
	}
}

func TestManagerDiscoveryModeNormalizesMissingStoredValue(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	now := time.Now().UTC()
	raw := map[string]any{"domains": []map[string]any{{
		"id": domainID, "space_id": spaceID, "key": "legacy", "name": "Legacy", "default": false, "created_at": now, "updated_at": now,
	}}}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, domainsStoreFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	m := NewManager()
	if err := m.Init(ctx, dir); err != nil {
		t.Fatal(err)
	}
	got, err := m.GetByID(ctx, domainID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DiscoveryMode != graph.DomainDiscoveryModeNormal {
		t.Fatalf("legacy discovery mode = %q, want normal", got.DiscoveryMode)
	}
}
