package domains

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
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

func TestManagerEmbeddingPolicyRoundTrip(t *testing.T) {
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
	policy, err := m.SetEmbeddingPolicy(ctx, domainembedding.DomainEmbeddingPolicy{SpaceID: spaceID, DomainID: domain.ID, Enabled: true, SourceMode: domainembedding.SourceModeSubtree, TargetTemplateKeys: []string{"logseq.journal"}, RefreshMode: domainembedding.DomainEmbeddingRefreshDirty})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.GetEmbeddingPolicy(ctx, spaceID, domain.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Enabled || got.SourceMode != domainembedding.SourceModeSubtree || got.RefreshMode != domainembedding.DomainEmbeddingRefreshDirty || len(got.TargetTemplateKeys) != 1 || got.TargetTemplateKeys[0] != policy.TargetTemplateKeys[0] {
		t.Fatalf("unexpected policy: %#v", got)
	}
}
