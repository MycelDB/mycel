package domains

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestManagerEnsureDefaultAndListBySpace(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	if err := m.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domain, err := m.EnsureDefault(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if domain.ID == uuid.Nil || domain.SpaceID != spaceID || domain.Key != graph.DefaultDomainKey || !domain.Default {
		t.Fatalf("unexpected default domain: %#v", domain)
	}
	again, err := m.EnsureDefault(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != domain.ID {
		t.Fatalf("expected idempotent default domain, got %s and %s", domain.ID, again.ID)
	}
	domains, err := m.ListBySpace(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if len(domains) != 1 || domains[0].ID != domain.ID {
		t.Fatalf("unexpected domains: %#v", domains)
	}
}
