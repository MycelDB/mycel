package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

func TestAuthorizationRaftAssertionUsesAuthoritativeSourceNotDerivedProjection(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	module.WithAuthorizationRevisionSource(fixedAuthorizationRevisionSource{global: 41, space: 42})
	payload, err := json.Marshal(globalAuthorizationAssertion{ExpectedRevision: 41})
	if err != nil {
		t.Fatal(err)
	}
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceGlobalAuthorizationAssert, payload, "authoritative-assert")
	fixture.credential.Status = "revoked"
	if _, err := module.GlobalManager().UpsertCredential(ctx, fixture.credential); err != nil {
		t.Fatal(err)
	}
	if err := (AuthorizationRaftStateMachine{Module: module}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("derived projection drift changed authoritative assertion: %v", err)
	}
}

func TestAuthorizationRaftAssertionOrdersCredentialRevocation(t *testing.T) {
	ctx := context.Background()
	module, _ := newResolverFixture(t, ctx)
	source := &fixedAuthorizationRevisionSource{global: 51, space: 52}
	module.WithAuthorizationRevisionSource(source)
	payload, err := json.Marshal(globalAuthorizationAssertion{ExpectedRevision: 51})
	if err != nil {
		t.Fatal(err)
	}
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceGlobalAuthorizationAssert, payload, "assert-before")
	sm := AuthorizationRaftStateMachine{Module: module}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("current assertion: %v", err)
	}
	source.global = 53
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err == nil {
		t.Fatal("stale global authorization assertion succeeded after credential revocation")
	}
}
