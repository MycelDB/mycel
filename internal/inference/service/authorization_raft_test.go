package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	model "github.com/myceldb/mycel/internal/inference/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestInferenceBudgetRaftApplyAndSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	source, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "raft-budget-scope", now)
	scope.ScopeSequence = 17
	payload, err := json.Marshal(inferenceMutationRecord{Kind: "budget_scope.upsert", Payload: rawInference(scope)})
	if err != nil {
		t.Fatal(err)
	}
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceBudget, payload, "budget-upsert")
	sm := AuthorizationRaftStateMachine{Module: source}
	if !sm.SupportsRaftCommandRecord(consensus.CommandScopeSystem, recordTypeInferenceBudget) {
		t.Fatal("budget raft record is not advertised")
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("apply budget raft command: %v", err)
	}
	persisted, err := source.GetBudgetScope(ctx, scope.ID)
	if err != nil || persisted.ScopeSequence != 17 {
		t.Fatalf("replicated budget scope=%+v error=%v", persisted, err)
	}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot inference budgets: %v", err)
	}
	restored, _ := newResolverFixture(t, ctx)
	restoredSM := AuthorizationRaftStateMachine{Module: restored}
	if err := restoredSM.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("restore inference budget snapshot: %v", err)
	}
	restoredScope, err := restored.GetBudgetScope(ctx, scope.ID)
	if err != nil || restoredScope.ScopeSequence != 17 {
		t.Fatalf("restored budget scope=%+v error=%v", restoredScope, err)
	}
	highWater, err := restored.UsageLedger().BudgetScopeHighWater(ctx)
	if err != nil || highWater != 17 {
		t.Fatalf("restored budget high-water=%d error=%v", highWater, err)
	}
}

func TestInferenceBudgetRaftLinearizesScopeCreationHighWaterAndBinding(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	first := newTestBudgetScope(fixture, "raft-create-first", now)
	first.SpaceID, first.SemanticRuleID, first.EmbeddingBindingKey = "space-a", "rule-a", "search"
	second := first
	second.ID = "raft-create-second"
	command := func(id string, scope model.BudgetScope) consensus.RaftCommand {
		payload, err := json.Marshal(inferenceMutationRecord{Kind: "budget_scope.upsert", Payload: rawInference(scope), BudgetScopeDigest: budgetScopeDigest(scope), AllocateBudgetScopeSequence: true, EnforceActiveSemanticBinding: true})
		if err != nil {
			t.Fatal(err)
		}
		return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceBudget, payload, id)
	}
	sm := AuthorizationRaftStateMachine{Module: module}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, command("create-first", first)); err != nil {
		t.Fatalf("apply first creation: %v", err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, command("create-second", second)); status.Code(err) != codes.FailedPrecondition || consensus.IsFatalApplyError(err) {
		t.Fatalf("concurrent creation error=%v code=%v fatal=%t", err, status.Code(err), consensus.IsFatalApplyError(err))
	}
	persisted, err := module.GetBudgetScope(ctx, first.ID)
	if err != nil || persisted.ScopeSequence != 1 {
		t.Fatalf("created scope=%+v error=%v", persisted, err)
	}
	if _, err := module.GetBudgetScope(ctx, second.ID); err == nil {
		t.Fatal("conflicting active scope was persisted")
	}
	third := second
	third.ID = "raft-create-third"
	third.EmbeddingBindingKey = "rerank"
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, command("create-third", third)); err != nil {
		t.Fatalf("apply unrelated concurrent creation: %v", err)
	}
	persistedThird, err := module.GetBudgetScope(ctx, third.ID)
	if err != nil || persistedThird.ScopeSequence != 2 {
		t.Fatalf("unrelated scope=%+v error=%v", persistedThird, err)
	}
}

func TestInferenceBudgetRaftMigratesLegacyZeroSequenceDuringReplay(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	created := newTestBudgetScope(fixture, "legacy-zero-sequence", time.Now().UTC())
	reserved := created
	reserved.UpdatedAt = created.UpdatedAt.Add(time.Second)
	reserved.Usage.Attempts = 1
	reserved.AttemptEvents = []model.BudgetAttemptEvent{{Sequence: 1, AttemptVersion: 1, Attempt: model.BudgetAttempt{ID: "attempt-1", State: model.BudgetAttemptReserved}, RecordedAt: reserved.UpdatedAt}}
	completed := reserved
	completed.UpdatedAt = reserved.UpdatedAt.Add(time.Second)
	completed.AttemptEvents = append(append([]model.BudgetAttemptEvent(nil), reserved.AttemptEvents...), model.BudgetAttemptEvent{Sequence: 2, AttemptVersion: 2, Attempt: model.BudgetAttempt{ID: "attempt-1", State: model.BudgetAttemptSucceeded}, RecordedAt: completed.UpdatedAt})
	command := func(scope model.BudgetScope, index uint64) consensus.RaftCommand {
		payload, err := json.Marshal(inferenceMutationRecord{Kind: "budget_scope.upsert", Payload: rawInference(scope)})
		if err != nil {
			t.Fatal(err)
		}
		return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceBudget, payload, fmt.Sprintf("legacy-zero-sequence-%d", index))
	}
	sm := AuthorizationRaftStateMachine{Module: module}
	for index, scope := range []model.BudgetScope{created, reserved, completed} {
		if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: uint64(index + 1), RaftTerm: 1}, command(scope, uint64(index+1))); err != nil {
			t.Fatalf("apply legacy mutation %d: %v", index, err)
		}
	}
	// Replayed historical records must be idempotent and must not regress the terminal state.
	for index, scope := range []model.BudgetScope{created, reserved, completed} {
		if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: uint64(index + 1), RaftTerm: 1}, command(scope, uint64(index+1))); err != nil {
			t.Fatalf("replay legacy mutation %d: %v", index, err)
		}
	}
	persisted, err := module.GetBudgetScope(ctx, created.ID)
	if err != nil || persisted.ScopeSequence != 1 || len(persisted.AttemptEvents) != 2 || persisted.AttemptEvents[1].Attempt.State != model.BudgetAttemptSucceeded {
		t.Fatalf("legacy scope=%+v error=%v", persisted, err)
	}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, _ := newResolverFixture(t, ctx)
	if err := (AuthorizationRaftStateMachine{Module: restored}).RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("restore migrated legacy scope: %v", err)
	}
}

func TestInferenceBudgetRaftPersistenceFailureIsFatal(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	scope := newTestBudgetScope(fixture, "raft-persistence-failure", time.Now().UTC())
	module.mu.Lock()
	module.usageBase = failingBudgetLedger{UsageLedger: module.usageBase}
	module.mu.Unlock()
	payload, err := json.Marshal(inferenceMutationRecord{Kind: "budget_scope.upsert", Payload: rawInference(scope)})
	if err != nil {
		t.Fatal(err)
	}
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceBudget, payload, "fatal-budget-write")
	err = (AuthorizationRaftStateMachine{Module: module}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd)
	if !consensus.IsFatalApplyError(err) {
		t.Fatalf("persistence apply error=%v, want fatal", err)
	}
}

func TestInferenceBudgetRaftRejectsStaleConcurrentScopeMutation(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	initial := newTestBudgetScope(fixture, "raft-budget-cas", now)
	initial.ScopeSequence = 1
	if _, err := module.UsageLedger().UpsertBudgetScope(ctx, initial); err != nil {
		t.Fatalf("seed budget scope: %v", err)
	}
	first := initial
	first.Usage.Requests = 1
	first.UpdatedAt = now.Add(time.Second)
	second := initial
	second.Usage.Requests = 2
	second.UpdatedAt = now.Add(2 * time.Second)
	previousDigest := budgetScopeDigest(initial)
	command := func(id string, next model.BudgetScope) consensus.RaftCommand {
		payload, err := json.Marshal(inferenceMutationRecord{Kind: "budget_scope.upsert", Payload: rawInference(next), BudgetScopeCAS: true, PreviousBudgetScopeDigest: previousDigest, BudgetScopeDigest: budgetScopeDigest(next)})
		if err != nil {
			t.Fatal(err)
		}
		return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceBudget, payload, id)
	}
	sm := AuthorizationRaftStateMachine{Module: module}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, command("first", first)); err != nil {
		t.Fatalf("apply first mutation: %v", err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, command("second", second)); status.Code(err) != codes.FailedPrecondition || consensus.IsFatalApplyError(err) {
		t.Fatalf("stale mutation error=%v code=%v fatal=%t", err, status.Code(err), consensus.IsFatalApplyError(err))
	}
	persisted, err := module.GetBudgetScope(ctx, initial.ID)
	if err != nil || !persisted.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("stale mutation overwrote scope=%+v error=%v", persisted, err)
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
