package service

import (
	"testing"

	"github.com/google/uuid"
)

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
