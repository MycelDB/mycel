package service

import (
	"testing"

	"github.com/google/uuid"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestSemanticProjectionIdentityIsStableAndBindingScoped(t *testing.T) {
	rule := domainsemantic.SemanticGenerationRule{
		ID:       domainsemantic.SemanticRuleID(uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
		SpaceID:  domainspace.SpaceID(uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")),
		DomainID: graphmodel.DomainID(uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")),
	}
	first := domainsemantic.SemanticEmbeddingBinding{Key: " Search "}
	second := domainsemantic.SemanticEmbeddingBinding{Key: "other"}
	id1, scope1, key1, err := semanticProjectionIdentity(rule, first)
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	id2, scope2, key2, err := semanticProjectionIdentity(rule, first)
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if id1 != id2 || scope1 != scope2 || key1 != key2 {
		t.Fatalf("identity changed across calls: %q/%q vs %q/%q", id1, key1, id2, key2)
	}
	id3, _, _, err := semanticProjectionIdentity(rule, second)
	if err != nil {
		t.Fatalf("scoped identity: %v", err)
	}
	if id1 == id3 {
		t.Fatalf("binding identities collided: %q", id1)
	}
	if key1 != rule.ID.String()+"/search" {
		t.Fatalf("unexpected canonical key %q", key1)
	}
}
