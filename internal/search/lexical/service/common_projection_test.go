package service

import (
	"testing"

	"github.com/google/uuid"
)

func TestLexicalProjectionIdentityIsStableAndDomainScoped(t *testing.T) {
	spaceID := uuid.NewString()
	firstDomain := uuid.NewString()
	secondDomain := uuid.NewString()
	first, scope, err := lexicalProjectionIdentity(spaceID, firstDomain)
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	second, secondScope, err := lexicalProjectionIdentity(spaceID, firstDomain)
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if first != second || scope != secondScope {
		t.Fatalf("identity changed across calls: %q/%+v vs %q/%+v", first, scope, second, secondScope)
	}
	other, _, err := lexicalProjectionIdentity(spaceID, secondDomain)
	if err != nil {
		t.Fatalf("second domain identity: %v", err)
	}
	if first == other {
		t.Fatalf("domain identities collided: %q", first)
	}
}
