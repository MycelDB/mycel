package service

import (
	"testing"

	"github.com/google/uuid"
)

func TestGraphQueryIndexProjectionIdentityUsesStableName(t *testing.T) {
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	first, scope, key, err := graphQueryIndexProjectionIdentity(spaceID, domainID, "title")
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	second, _, secondKey, err := graphQueryIndexProjectionIdentity(spaceID, domainID, "title")
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if first != second || key != secondKey || key != "query-index:title" {
		t.Fatalf("identity changed: %q/%q/%q", first, second, key)
	}
	other, _, _, err := graphQueryIndexProjectionIdentity(spaceID, domainID, "other")
	if err != nil {
		t.Fatalf("other identity: %v", err)
	}
	if first == other {
		t.Fatal("different index names collided")
	}
	if scope.SpaceID != spaceID || scope.DomainID != domainID {
		t.Fatalf("scope = %+v", scope)
	}
	base := GraphPersistentQueryIndexStatus{SchemaHash: "schema-1", DefinitionFingerprint: "definition-1", KeyEncodingVersion: 1}
	changed := base
	changed.SchemaHash = "schema-2"
	if graphQueryIndexConfigurationHash(base) == graphQueryIndexConfigurationHash(changed) {
		t.Fatal("schema changes did not change query-index configuration hash")
	}
}

func TestGraphCheckpointProjectionIdentityIsStableAndScoped(t *testing.T) {
	spaceID := uuid.NewString()
	firstDomain := uuid.NewString()
	secondDomain := uuid.NewString()
	first, firstScope, err := graphCheckpointProjectionIdentity(spaceID, firstDomain)
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	second, secondScope, err := graphCheckpointProjectionIdentity(spaceID, firstDomain)
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if first != second || firstScope != secondScope {
		t.Fatalf("identity changed across calls: %q/%+v vs %q/%+v", first, firstScope, second, secondScope)
	}
	other, _, err := graphCheckpointProjectionIdentity(spaceID, secondDomain)
	if err != nil {
		t.Fatalf("second domain identity: %v", err)
	}
	if first == other {
		t.Fatalf("domain identities collided: %q", first)
	}
	if got := firstScope; got.SpaceID != spaceID || got.DomainID != firstDomain {
		t.Fatalf("scope = %+v", got)
	}
}
