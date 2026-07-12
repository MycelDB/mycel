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

func TestManagerDomainPolicyDefaultsAndRoundTrips(t *testing.T) {
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
	if normal.DiscoveryMode != graph.DomainDiscoveryModeNormal || normal.SearchMode != graph.DomainSearchModeNormal || normal.SemanticMode != graph.DomainSemanticModeNormal || normal.ReadOnly {
		t.Fatalf("default domain policy = %+v, want normal/search normal/semantic normal/writable", normal)
	}

	explicit, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "manual", DiscoveryMode: graph.DomainDiscoveryModeExplicitOnly, SearchMode: graph.DomainSearchModeExplicitOnly, SemanticMode: graph.DomainSemanticModeExplicitOnly, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if explicit.DiscoveryMode != graph.DomainDiscoveryModeExplicitOnly || explicit.SearchMode != graph.DomainSearchModeExplicitOnly || explicit.SemanticMode != graph.DomainSemanticModeExplicitOnly || !explicit.ReadOnly {
		t.Fatalf("explicit domain policy = %+v", explicit)
	}

	got, err := m.GetByID(ctx, explicit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DiscoveryMode != graph.DomainDiscoveryModeExplicitOnly || got.SearchMode != graph.DomainSearchModeExplicitOnly || got.SemanticMode != graph.DomainSemanticModeExplicitOnly || !got.ReadOnly {
		t.Fatalf("round-trip domain policy = %+v", got)
	}
}

func TestManagerDomainPolicyRejectsInvalid(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	if err := m.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	if _, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "bad", DiscoveryMode: graph.DomainDiscoveryMode("private")}); err == nil {
		t.Fatal("expected invalid discovery mode error")
	}
	if _, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "bad-search", SearchMode: graph.DomainSearchMode("private")}); err == nil {
		t.Fatal("expected invalid search mode error")
	}
	if _, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "bad-semantic", SemanticMode: graph.DomainSemanticMode("private")}); err == nil {
		t.Fatal("expected invalid semantic mode error")
	}
	domain, err := m.Create(ctx, CreateInput{SpaceID: spaceID, Key: "good"})
	if err != nil {
		t.Fatal(err)
	}
	invalidDiscovery := graph.DomainDiscoveryMode("private")
	if _, err = m.Update(ctx, UpdateInput{DomainID: domain.ID, DiscoveryMode: &invalidDiscovery}); err == nil {
		t.Fatal("expected invalid discovery mode update error")
	}
	invalidSearch := graph.DomainSearchMode("private")
	if _, err = m.Update(ctx, UpdateInput{DomainID: domain.ID, SearchMode: &invalidSearch}); err == nil {
		t.Fatal("expected invalid search mode update error")
	}
	invalidSemantic := graph.DomainSemanticMode("private")
	if _, err = m.Update(ctx, UpdateInput{DomainID: domain.ID, SemanticMode: &invalidSemantic}); err == nil {
		t.Fatal("expected invalid semantic mode update error")
	}
}

func TestManagerDomainPolicyNormalizesMissingStoredValues(t *testing.T) {
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
	if got.DiscoveryMode != graph.DomainDiscoveryModeNormal || got.SearchMode != graph.DomainSearchModeNormal || got.SemanticMode != graph.DomainSemanticModeNormal || got.ReadOnly {
		t.Fatalf("legacy policy = %+v, want default normal policy", got)
	}
}
