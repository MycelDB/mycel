package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConcurrentBudgetReservationsCannotOversubscribe(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-concurrent", now)
	scope.Limits.MaxRequests = 5
	scope.Limits.MaxAttempts = 5
	scope.Limits.MaxRetryAttempts = 5
	scope.Limits.MaxRetriesPerRequest = 5
	scope.Limits.MaxConcurrentProviderCalls = 5
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	var admitted atomic.Int64
	var denied atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, _, err := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: domaininference.BudgetAttemptID(fmt.Sprintf("attempt-%d", index)), RequestID: fmt.Sprintf("request-%d", index), Route: scope.Route, BatchItems: 1, InputTokens: 1, ProviderMillis: 1000, Now: now})
			if err == nil {
				admitted.Add(1)
				return
			}
			if !errors.Is(err, ErrBudgetDenied) {
				t.Errorf("ReserveProviderAttempt() error=%v", err)
				return
			}
			denied.Add(1)
		}(i)
	}
	wg.Wait()
	if admitted.Load() != 5 || denied.Load() != 95 {
		t.Fatalf("admitted=%d denied=%d", admitted.Load(), denied.Load())
	}
	persisted, err := module.GetBudgetScope(ctx, scope.ID)
	if err != nil {
		t.Fatalf("GetBudgetScope() error=%v", err)
	}
	if persisted.Usage.Attempts != 5 || persisted.Usage.BatchItems != 5 || persisted.Usage.ActiveProviderCalls != 5 || len(persisted.Attempts) != 5 {
		t.Fatalf("persisted scope=%+v", persisted)
	}
}

func TestBudgetScopeReactivationForMutationRollback(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	scope := newTestBudgetScope(fixture, "scope-rollback", time.Now().UTC())
	created, err := module.CreateBudgetScope(ctx, scope)
	if err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	completed, err := module.CompleteBudgetScope(ctx, created.ID, time.Now().UTC())
	if err != nil || completed.State != domaininference.BudgetScopeCompleted {
		t.Fatalf("CompleteBudgetScope() scope=%#v error=%v", completed, err)
	}
	repeatedCompletion, err := module.CompleteBudgetScope(ctx, created.ID, completed.UpdatedAt.Add(time.Second))
	if err != nil || !repeatedCompletion.UpdatedAt.Equal(completed.UpdatedAt) {
		t.Fatalf("repeated CompleteBudgetScope() scope=%#v error=%v", repeatedCompletion, err)
	}
	reactivated, err := module.ReactivateBudgetScope(ctx, created.ID, time.Now().UTC())
	if err != nil || reactivated.State != domaininference.BudgetScopeActive {
		t.Fatalf("ReactivateBudgetScope() scope=%#v error=%v", reactivated, err)
	}
	repeatedReactivation, err := module.ReactivateBudgetScope(ctx, created.ID, reactivated.UpdatedAt.Add(time.Second))
	if err != nil || !repeatedReactivation.UpdatedAt.Equal(reactivated.UpdatedAt) {
		t.Fatalf("repeated ReactivateBudgetScope() scope=%#v error=%v", repeatedReactivation, err)
	}
}

func TestBudgetAttemptEventsAreAppendOnlyAndVersioned(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-events", now)
	created, err := module.CreateBudgetScope(ctx, scope)
	if err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	if created.ScopeSequence == 0 {
		t.Fatal("scope sequence was not allocated")
	}
	_, attempt, err := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-events", RequestID: "request-events", Route: scope.Route, BatchItems: 1, InputTokens: 10, ProviderMillis: 100, Now: now})
	if err != nil {
		t.Fatalf("ReserveProviderAttempt() error=%v", err)
	}
	if _, err := module.CompleteProviderAttempt(ctx, domaininference.BudgetCompletionRequest{ScopeID: scope.ID, AttemptID: attempt.ID, State: domaininference.BudgetAttemptSucceeded, InputTokens: 5, ProviderMillis: 50, UsageKnown: true, OutcomeKnown: true, ProviderDurationKnown: true, Outcome: "succeeded", CompletedAt: now.Add(50 * time.Millisecond)}); err != nil {
		t.Fatalf("CompleteProviderAttempt() error=%v", err)
	}
	persisted, err := module.GetBudgetScope(ctx, scope.ID)
	if err != nil {
		t.Fatalf("GetBudgetScope() error=%v", err)
	}
	if len(persisted.AttemptEvents) != 2 || persisted.AttemptEvents[0].Sequence != 1 || persisted.AttemptEvents[0].AttemptVersion != 1 || persisted.AttemptEvents[0].Attempt.State != domaininference.BudgetAttemptReserved || persisted.AttemptEvents[1].Sequence != 2 || persisted.AttemptEvents[1].AttemptVersion != 2 || persisted.AttemptEvents[1].Attempt.State != domaininference.BudgetAttemptSucceeded {
		t.Fatalf("attempt events=%+v", persisted.AttemptEvents)
	}
	if persisted.AttemptEvents[0].Attempt.ChargedInputTokens != 10 {
		t.Fatalf("reserved event was mutated: %+v", persisted.AttemptEvents[0])
	}
}

func TestBudgetScopeRejectsReachableReservationCostOverflow(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	scope := newTestBudgetScope(fixture, "scope-cost-overflow", time.Now().UTC())
	maxInt := int64(^uint64(0) >> 1)
	scope.Limits.MaxInputTokensPerRequest = maxInt
	scope.Limits.MaxInputTokens = maxInt
	scope.Limits.Currency = "USD"
	scope.Limits.PriceDefinitionVersion = "test-v1"
	scope.Limits.InputMicrosPerMillionTokens = maxInt
	scope.Limits.MaxEstimatedCostMicros = maxInt
	if _, err := module.CreateBudgetScope(ctx, scope); err == nil || !strings.Contains(err.Error(), "cost bounds overflow") {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	if scopes, err := module.ListBudgetScopes(ctx); err != nil || len(scopes) != 0 {
		t.Fatalf("overflow validation persisted scopes=%+v error=%v", scopes, err)
	}
}

func TestBudgetChargedCostOverflowExhaustsAndSaturates(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-overflow", now)
	scope.Limits.Currency = "USD"
	scope.Limits.PriceDefinitionVersion = "test-v1"
	scope.Limits.InputMicrosPerMillionTokens = 1
	scope.Limits.OutputMicrosPerMillionTokens = 1
	scope.Limits.MaxEstimatedCostMicros = 100
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	_, attempt, err := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-overflow", RequestID: "request-overflow", Route: scope.Route, BatchItems: 1, InputTokens: 10, ProviderMillis: 100, Now: now})
	if err != nil {
		t.Fatalf("ReserveProviderAttempt() error=%v", err)
	}
	maxInt := int64(^uint64(0) >> 1)
	persisted, err := module.CompleteProviderAttempt(ctx, domaininference.BudgetCompletionRequest{ScopeID: scope.ID, AttemptID: attempt.ID, State: domaininference.BudgetAttemptSucceeded, InputTokens: 10, ProviderMillis: 50, CostMicros: maxInt, CostSaturated: true, UsageKnown: true, OutcomeKnown: true, ProviderDurationKnown: true, Outcome: "succeeded", CompletedAt: now.Add(50 * time.Millisecond)})
	if !errors.Is(err, ErrBudgetAccounting) {
		t.Fatalf("CompleteProviderAttempt() error=%v", err)
	}
	if persisted.State != domaininference.BudgetScopeExhausted || !persisted.Usage.AccountingOverflow || persisted.Usage.EstimatedCostMicros != maxInt || len(persisted.AttemptEvents) != 2 || !persisted.AttemptEvents[1].Attempt.ChargedCostSaturated {
		t.Fatalf("persisted overflow scope=%+v", persisted)
	}
}

func TestBudgetDuplicateAttemptAndRetryAccountingFailClosed(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-retry", now)
	scope.Limits.MaxRetryAttempts = 1
	scope.Limits.MaxRetriesPerRequest = 1
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	request := domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-1", RequestID: "request-1", Route: scope.Route, BatchItems: 1, InputTokens: 1, ProviderMillis: 1000, Now: now}
	if _, _, err := module.ReserveProviderAttempt(ctx, request); err != nil {
		t.Fatalf("first reserve error=%v", err)
	}
	if _, _, err := module.ReserveProviderAttempt(ctx, request); !errors.Is(err, ErrBudgetAttemptDuplicate) || !errors.Is(err, ErrBudgetDenied) {
		t.Fatalf("duplicate reserve error=%v", err)
	}
	request.AttemptID = "attempt-2"
	// Caller/work-claim retry metadata is intentionally ignored. Durable attempts
	// for the stable request ID define the retry index.
	if _, attempt, err := module.ReserveProviderAttempt(ctx, request); err != nil || !attempt.Retry || attempt.RetryIndex != 1 {
		t.Fatalf("derived retry attempt=%+v error=%v", attempt, err)
	}
	request.AttemptID = "attempt-3"
	if _, _, err := module.ReserveProviderAttempt(ctx, request); budgetDenialReason(err) != "retry_limit_exhausted" {
		t.Fatalf("second retry error=%v reason=%s", err, budgetDenialReason(err))
	}
	request.AttemptID = "attempt-4"
	request.RequestID = "new-request"
	request.Retry = true
	request.RetryIndex = 99
	if _, attempt, err := module.ReserveProviderAttempt(ctx, request); err != nil || attempt.Retry || attempt.RetryIndex != 0 {
		t.Fatalf("derived first attempt=%+v error=%v", attempt, err)
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	if persisted.Usage.Requests != 2 || persisted.Usage.Attempts != 3 || persisted.Usage.RetryAttempts != 1 {
		t.Fatalf("usage=%+v", persisted.Usage)
	}
}

func TestReactivateBudgetScopeRejectsAnotherActiveSemanticBinding(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	first := newTestBudgetScope(fixture, "scope-reactivate-first", now)
	first.SpaceID, first.SemanticRuleID, first.EmbeddingBindingKey, first.CorrelationID = "space-a", "rule-a", "search", "correlation-a"
	created, err := module.CreateBudgetScope(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := module.CompleteBudgetScope(ctx, created.ID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "scope-reactivate-second"
	second.CreatedAt, second.UpdatedAt = now.Add(2*time.Second), now.Add(2*time.Second)
	second.ExpiresAt, second.Limits.Deadline = now.Add(time.Hour), now.Add(time.Hour)
	if _, err := module.CreateBudgetScope(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := module.ReactivateBudgetScope(ctx, created.ID, now.Add(3*time.Second)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ReactivateBudgetScope() error=%v code=%v", err, status.Code(err))
	}
}

func TestBudgetCompletionRefundsKnownConservativeReservation(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-complete", now)
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	_, _, err := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-1", RequestID: "request-1", Route: scope.Route, BatchItems: 1, InputTokens: 10, OutputTokens: 20, ProviderMillis: 1000, Now: now})
	if err != nil {
		t.Fatalf("reserve error=%v", err)
	}
	completed, err := module.CompleteProviderAttempt(ctx, domaininference.BudgetCompletionRequest{ScopeID: scope.ID, AttemptID: "attempt-1", State: domaininference.BudgetAttemptSucceeded, InputTokens: 4, OutputTokens: 6, ProviderMillis: 25, UsageKnown: true, OutcomeKnown: true, ProviderDurationKnown: true, Outcome: "succeeded", CompletedAt: now.Add(25 * time.Millisecond)})
	if err != nil {
		t.Fatalf("complete error=%v", err)
	}
	if completed.Usage.InputTokens != 4 || completed.Usage.OutputTokens != 6 || completed.Usage.ProviderMillis != 25 || completed.Usage.ActiveProviderCalls != 0 {
		t.Fatalf("usage=%+v", completed.Usage)
	}
}

func TestBudgetUnknownOutcomeRetainsReservationAcrossLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-ambiguous", now)
	scope.Limits.MaxConcurrentProviderCalls = 1
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	if _, _, err := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-1", RequestID: "request-1", Route: scope.Route, BatchItems: 1, InputTokens: 10, OutputTokens: 20, ProviderMillis: 10, Now: now}); err != nil {
		t.Fatalf("reserve error=%v", err)
	}
	completed, err := module.CompleteProviderAttempt(ctx, domaininference.BudgetCompletionRequest{ScopeID: scope.ID, AttemptID: "attempt-1", State: domaininference.BudgetAttemptSucceeded, InputTokens: 1, OutputTokens: 1, ProviderMillis: 1, UsageKnown: true, OutcomeKnown: false, ProviderDurationKnown: true, Outcome: "credential-secret", CompletedAt: now.Add(time.Millisecond)})
	if err != nil {
		t.Fatalf("CompleteProviderAttempt() error=%v", err)
	}
	usage := calculateBudgetUsage(completed.Attempts, now.Add(time.Second))
	if completed.Attempts[0].UsageKnown || completed.Attempts[0].ProviderDurationKnown || completed.Attempts[0].Outcome != "connector_error" || usage.ActiveProviderCalls != 1 || usage.AmbiguousAttempts != 1 || usage.InputTokens != 10 || usage.OutputTokens != 20 || usage.ProviderMillis != 10 {
		t.Fatalf("attempt=%+v usage=%+v", completed.Attempts[0], usage)
	}
	if _, _, err := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-after-ambiguity", RequestID: "request-after-ambiguity", Route: scope.Route, BatchItems: 1, InputTokens: 1, OutputTokens: 1, ProviderMillis: 10, Now: now.Add(time.Second)}); budgetDenialReason(err) != "concurrency_limit_exhausted" {
		t.Fatalf("ambiguous concurrency reservation error=%v", err)
	}
}

func TestBudgetCostAndBatchDenialsOccurBeforeMutation(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-cost", now)
	scope.Limits.MaxEstimatedCostMicros = 1
	scope.Limits.Currency = "USD"
	scope.Limits.PriceDefinitionVersion = "test-v1"
	scope.Limits.InputMicrosPerMillionTokens = 1_000_000
	scope.Limits.OutputMicrosPerMillionTokens = 1_000_000
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	_, _, err := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-cost", RequestID: "request-cost", Route: scope.Route, BatchItems: 1, InputTokens: 2, ProviderMillis: 10, Now: now})
	if budgetDenialReason(err) != "cost_limit_exhausted" {
		t.Fatalf("cost denial=%v reason=%s", err, budgetDenialReason(err))
	}
	_, _, err = module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-batch", RequestID: "request-batch", Route: scope.Route, BatchItems: 2, ProviderMillis: 10, Now: now})
	if budgetDenialReason(err) != "batch_limit_exceeded" {
		t.Fatalf("batch denial=%v reason=%s", err, budgetDenialReason(err))
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	if len(persisted.Attempts) != 0 {
		t.Fatalf("denied attempts mutated scope: %+v", persisted.Attempts)
	}
}

func TestBudgetCostLimitsRequireAuthoritativeRoutePrices(t *testing.T) {
	ctx := context.Background()
	_, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	tests := []struct {
		name        string
		operation   domaininference.Operation
		inputPrice  int64
		outputPrice int64
		wantError   string
	}{
		{name: "missing input price", operation: domaininference.OperationChat, outputPrice: 1_000_000, wantError: "positive input token price"},
		{name: "missing output price", operation: domaininference.OperationChat, inputPrice: 1_000_000, wantError: "positive output token price"},
		{name: "embedding has no output price", operation: domaininference.OperationEmbeddings, inputPrice: 1_000_000},
		{name: "chat has complete prices", operation: domaininference.OperationChat, inputPrice: 1_000_000, outputPrice: 1_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := newTestBudgetScope(fixture, "scope-pricing", now)
			scope.Route.Operation = tt.operation
			scope.Limits.MaxEstimatedCostMicros = 1_000
			scope.Limits.Currency = "USD"
			scope.Limits.PriceDefinitionVersion = "test-v1"
			scope.Limits.InputMicrosPerMillionTokens = tt.inputPrice
			scope.Limits.OutputMicrosPerMillionTokens = tt.outputPrice
			err := validateBudgetScope(scope, now)
			if tt.wantError == "" && err != nil {
				t.Fatalf("validateBudgetScope() error=%v", err)
			}
			if tt.wantError != "" && (err == nil || !strings.Contains(err.Error(), tt.wantError)) {
				t.Fatalf("validateBudgetScope() error=%v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestBudgetedInvokeReservesBeforeConnectorAndRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-invoke", now)
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	fake := &connectors.FakeConnector{Text: "ok"}
	resolver := &countingSecretResolver{value: "secret"}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	module.SetSecretResolver(resolver)
	request := inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-invoke", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-invoke", BatchItems: 1}
	if _, err := module.Invoke(ctx, request); err != nil {
		t.Fatalf("Invoke() error=%v", err)
	}
	if _, err := module.Invoke(ctx, request); !errors.Is(err, ErrBudgetAttemptDuplicate) {
		t.Fatalf("duplicate Invoke() error=%v", err)
	}
	_, calls := fake.Calls()
	if calls != 1 || resolver.calls != 1 {
		t.Fatalf("connector calls=%d resolver calls=%d, want 1 each", calls, resolver.calls)
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	if len(persisted.Attempts) != 1 || persisted.Attempts[0].State != domaininference.BudgetAttemptSucceeded || persisted.Attempts[0].UsageKnown || persisted.Usage.InputTokens != 100 || persisted.Usage.OutputTokens != 8 {
		t.Fatalf("attempts=%+v usage=%+v", persisted.Attempts, persisted.Usage)
	}
	events, _ := module.UsageLedger().ListUsageEvents(ctx)
	if len(events) != 2 || events[0].BudgetScopeID != scope.ID || events[0].BudgetAttemptID != request.BudgetAttemptID || events[1].ErrorCode != "budget_denied" || events[1].ErrorMessage != "duplicate_attempt" {
		t.Fatalf("events=%+v", events)
	}
}

func TestBudgetedInvokeLocalWriteGateDeniesBeforeProviderAccess(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-write-gate", now)
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	fake := &connectors.FakeConnector{Text: "must-not-run"}
	resolver := &countingSecretResolver{value: "secret"}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	module.SetSecretResolver(resolver)
	module.mu.Lock()
	module.writeAllowed = func() error { return errors.New("clustered local write rejected") }
	module.mu.Unlock()
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-write-gate", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-write-gate", BatchItems: 1})
	if err == nil || !strings.Contains(err.Error(), "clustered local write rejected") {
		t.Fatalf("Invoke() error=%v, want local write rejection", err)
	}
	_, calls := fake.Calls()
	if calls != 0 || resolver.calls != 0 {
		t.Fatalf("connector calls=%d resolver calls=%d, want zero", calls, resolver.calls)
	}
}

func TestBudgetedInvokeAmbiguousOutcomeRetainsAuthoritativeLookingUsageReservation(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-ambiguous-usage", now)
	scope.Limits.MaxConcurrentProviderCalls = 2
	scope.Limits.MaxEstimatedCostMicros = 1_000
	scope.Limits.Currency = "USD"
	scope.Limits.PriceDefinitionVersion = "test-v1"
	scope.Limits.InputMicrosPerMillionTokens = 1_000_000
	scope.Limits.OutputMicrosPerMillionTokens = 1_000_000
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, ambiguousUsageConnector{})
	module.SetSecretResolver(&countingSecretResolver{value: "secret"})
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-ambiguous-usage", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-ambiguous-usage", BatchItems: 1})
	if err == nil {
		t.Fatal("Invoke() unexpectedly succeeded")
	}
	persisted, getErr := module.GetBudgetScope(ctx, scope.ID)
	if getErr != nil {
		t.Fatalf("GetBudgetScope() error=%v", getErr)
	}
	attempt := persisted.Attempts[0]
	if attempt.UsageKnown || attempt.OutcomeKnown || persisted.Usage.ActiveProviderCalls != 1 || persisted.Usage.InputTokens != 100 || persisted.Usage.OutputTokens != 8 || persisted.Usage.EstimatedCostMicros != 108 {
		t.Fatalf("attempt=%+v usage=%+v", attempt, persisted.Usage)
	}
}

func TestBudgetedInvokePropagatesStrictestPerRequestOutputCeiling(t *testing.T) {
	tests := []struct {
		name      string
		requested int
		want      int64
	}{
		{name: "no request ceiling", want: 7},
		{name: "looser request ceiling", requested: 8, want: 7},
		{name: "stricter request ceiling", requested: 5, want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			module, fixture := newResolverFixture(t, ctx)
			now := time.Now().UTC()
			scope := newTestBudgetScope(fixture, "scope-output-ceiling", now)
			scope.Limits.MaxOutputTokens = 0
			scope.Limits.MaxOutputTokensPerRequest = 7
			if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
				t.Fatalf("CreateBudgetScope() error=%v", err)
			}
			connector := &capturingBudgetConnector{}
			module.SetConnector(domaininference.ConnectorOpenAICompatible, connector)
			module.SetSecretResolver(&countingSecretResolver{value: "secret"})
			_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: tt.requested}}, Input: "hello", RequestID: "request-output-ceiling", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-output-ceiling", BatchItems: 1})
			if err != nil {
				t.Fatalf("Invoke() error=%v", err)
			}
			persisted, getErr := module.GetBudgetScope(ctx, scope.ID)
			if getErr != nil {
				t.Fatalf("GetBudgetScope() error=%v", getErr)
			}
			if connector.calls.Load() != 1 || connector.maxOutput.Load() != tt.want || persisted.Attempts[0].ReservedOutputTokens != tt.want {
				t.Fatalf("connector calls=%d max_output_tokens=%d attempt=%+v", connector.calls.Load(), connector.maxOutput.Load(), persisted.Attempts[0])
			}
		})
	}
}

func TestBudgetedInvokeDeadlineDuringCredentialResolutionSkipsProvider(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-secret-deadline", now)
	scope.Limits.MaxProviderMillisPerRequest = 20
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	fake := &connectors.FakeConnector{Text: "must-not-run"}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	module.SetSecretResolver(blockingSecretResolver{})
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-secret-deadline", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-secret-deadline", BatchItems: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Invoke() error=%v", err)
	}
	_, calls := fake.Calls()
	if calls != 0 {
		t.Fatalf("connector calls=%d", calls)
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	if persisted.Attempts[0].State != domaininference.BudgetAttemptFailed || !persisted.Attempts[0].UsageKnown || !persisted.Attempts[0].OutcomeKnown || persisted.Usage.ActiveProviderCalls != 0 {
		t.Fatalf("scope=%+v", persisted)
	}
}

func TestBudgetedInvokeCancellationRetainsAmbiguousReservation(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-cancel", now)
	scope.Limits.MaxProviderMillisPerRequest = 200
	scope.Limits.MaxConcurrentProviderCalls = 1
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	blocking := &blockingBudgetConnector{}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, blocking)
	module.SetSecretResolver(&countingSecretResolver{value: "secret"})
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-cancel", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-cancel", BatchItems: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Invoke() error=%v", err)
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	if blocking.calls.Load() != 1 || persisted.Usage.AmbiguousAttempts != 1 || persisted.Usage.ActiveProviderCalls != 1 || persisted.Usage.ProviderMillis != 200 || persisted.Attempts[0].State != domaininference.BudgetAttemptCanceled {
		t.Fatalf("calls=%d scope=%+v", blocking.calls.Load(), persisted)
	}
	_, _, reserveErr := module.ReserveProviderAttempt(ctx, domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-after-cancel", RequestID: "request-after-cancel", Route: scope.Route, BatchItems: 1, InputTokens: 1, OutputTokens: 1, ProviderMillis: 10, Now: time.Now().UTC()})
	if budgetDenialReason(reserveErr) != "concurrency_limit_exhausted" {
		t.Fatalf("ambiguous cancellation admitted another provider call: %v", reserveErr)
	}
}

func TestBudgetUsageKnownRequiresTrustedCompleteComponents(t *testing.T) {
	estimated := domaininference.Usage{InputTokens: 1, TotalTokens: 1, TokenCountSource: "estimated"}
	if budgetUsageKnown(estimated, domaininference.ConnectorOpenAICompatible, domaininference.OperationChat) {
		t.Fatal("hosted estimate was trusted")
	}
	if !budgetUsageKnown(estimated, domaininference.ConnectorFake, domaininference.OperationChat) {
		t.Fatal("deterministic fake usage was not trusted")
	}
	tests := []struct {
		name      string
		usage     domaininference.Usage
		operation domaininference.Operation
		want      bool
	}{
		{name: "complete chat", usage: domaininference.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5, TokenCountSource: "provider_reported"}, operation: domaininference.OperationChat, want: true},
		{name: "chat total only", usage: domaininference.Usage{TotalTokens: 5, TokenCountSource: "provider_reported"}, operation: domaininference.OperationChat},
		{name: "chat inconsistent", usage: domaininference.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 6, TokenCountSource: "provider_reported"}, operation: domaininference.OperationChat},
		{name: "complete embedding", usage: domaininference.Usage{InputTokens: 3, TotalTokens: 3, TokenCountSource: "provider_reported"}, operation: domaininference.OperationEmbeddings, want: true},
		{name: "embedding total only", usage: domaininference.Usage{TotalTokens: 3, TokenCountSource: "provider_reported"}, operation: domaininference.OperationEmbeddings},
		{name: "embedding inconsistent", usage: domaininference.Usage{InputTokens: 2, TotalTokens: 3, TokenCountSource: "provider_reported"}, operation: domaininference.OperationEmbeddings},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := budgetUsageKnown(tt.usage, domaininference.ConnectorOpenAICompatible, tt.operation); got != tt.want {
				t.Fatalf("budgetUsageKnown()=%t, want %t", got, tt.want)
			}
		})
	}
}

func TestBudgetedInvokeReservationOverrunPersistsUsageEvidence(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-overrun", now)
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, overrunUsageConnector{})
	module.SetSecretResolver(&countingSecretResolver{value: "secret"})
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-overrun", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-overrun", BatchItems: 1})
	if !errors.Is(err, ErrBudgetAccounting) {
		t.Fatalf("Invoke() error=%v", err)
	}
	events, listErr := module.UsageLedger().ListUsageEvents(ctx)
	if listErr != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusFailed || events[0].ErrorCode != "reservation_exceeded" || events[0].ErrorMessage != "reservation_exceeded" {
		t.Fatalf("events=%+v err=%v", events, listErr)
	}
}

func TestBudgetedInvokePersistsOnlySanitizedFailureDiagnostics(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-sanitized-failure", now)
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, &connectors.FakeConnector{Err: connectors.NewFakeError("credential-secret", false, "raw provider body SECRET")})
	module.SetSecretResolver(&countingSecretResolver{value: "secret"})
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "prompt-secret", Metadata: map[string]any{"authorization": "credential-secret", "prompt": "prompt-secret"}, RequestID: "request-sanitized", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-sanitized", BatchItems: 1})
	if err == nil {
		t.Fatal("Invoke() expected error")
	}
	events, listErr := module.UsageLedger().ListUsageEvents(ctx)
	if listErr != nil || len(events) != 1 {
		t.Fatalf("events=%+v err=%v", events, listErr)
	}
	if events[0].ErrorCode != "provider_error" || events[0].ErrorMessage != "provider_error" || len(events[0].Metadata) != 0 {
		t.Fatalf("unsafe usage event=%+v", events[0])
	}
	persisted, getErr := module.GetBudgetScope(ctx, scope.ID)
	if getErr != nil || len(persisted.Attempts) != 1 || persisted.Attempts[0].Outcome != "connector_error" {
		t.Fatalf("unsafe budget attempt=%+v err=%v", persisted.Attempts, getErr)
	}
}

func TestBudgetedInvokeUnavailableUsageRetainsTokenAndCostReservations(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-unavailable-usage", now)
	scope.Limits.Currency = "USD"
	scope.Limits.PriceDefinitionVersion = "test-v1"
	scope.Limits.InputMicrosPerMillionTokens = 1_000_000
	scope.Limits.OutputMicrosPerMillionTokens = 1_000_000
	scope.Limits.MaxEstimatedCostMicros = 1_000
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, unavailableUsageConnector{})
	module.SetSecretResolver(&countingSecretResolver{value: "secret"})
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-unavailable", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-unavailable", BatchItems: 1})
	if err != nil {
		t.Fatalf("Invoke() error=%v", err)
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	attempt := persisted.Attempts[0]
	if attempt.UsageKnown || !attempt.OutcomeKnown || !attempt.ProviderDurationKnown || persisted.Usage.InputTokens != 100 || persisted.Usage.OutputTokens != 8 || persisted.Usage.EstimatedCostMicros != 108 {
		t.Fatalf("scope=%+v", persisted)
	}
}

func TestPinnedEmbeddingTokenizerEnforcesPerRequestCeiling(t *testing.T) {
	resolved := inferenceruntime.ResolveResult{Endpoint: domaininference.Endpoint{ConnectorType: domaininference.ConnectorOpenAICompatible, BaseURL: "https://api.openai.com/v1"}, Model: domaininference.Model{ProviderModelName: "text-embedding-3-small"}}
	tokens, err := invocationInputTokenReservation(inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{Operation: domaininference.OperationEmbeddings}, Input: "hello"}, resolved, domaininference.BudgetLimits{MaxInputTokensPerRequest: 20, MaxInputTokens: 200})
	if err != nil || tokens != 1 {
		t.Fatalf("token reservation=%d error=%v", tokens, err)
	}
	_, err = invocationInputTokenReservation(inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{Operation: domaininference.OperationEmbeddings}, Input: strings.Repeat(" x", 21)}, resolved, domaininference.BudgetLimits{MaxInputTokensPerRequest: 20, MaxInputTokens: 200})
	if budgetDenialReason(err) != "input_tokens_per_request_exceeded" {
		t.Fatalf("oversized input error=%v reason=%s", err, budgetDenialReason(err))
	}
	resolved.Capability.ProviderModelOverride = "unknown-embedding-model"
	if _, err := invocationInputTokenReservation(inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{Operation: domaininference.OperationEmbeddings}, Input: "hello"}, resolved, domaininference.BudgetLimits{MaxInputTokensPerRequest: 20, MaxInputTokens: 200}); budgetDenialReason(err) != "input_token_reservation_unavailable" {
		t.Fatalf("unknown override tokenizer error=%v", err)
	}
	resolved.Model.ProviderModelName = "unknown-base-model"
	resolved.Capability.ProviderModelOverride = "text-embedding-3-small"
	if tokens, err := invocationInputTokenReservation(inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{Operation: domaininference.OperationEmbeddings}, Input: "hello"}, resolved, domaininference.BudgetLimits{MaxInputTokensPerRequest: 20, MaxInputTokens: 200}); err != nil || tokens != 1 {
		t.Fatalf("known override token reservation=%d error=%v", tokens, err)
	}
	resolved.Capability.ProviderModelOverride = ""
	if _, err := invocationInputTokenReservation(inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{Operation: domaininference.OperationEmbeddings}, Input: "hello"}, resolved, domaininference.BudgetLimits{MaxInputTokensPerRequest: 20, MaxInputTokens: 200}); budgetDenialReason(err) != "input_token_reservation_unavailable" {
		t.Fatalf("unknown base tokenizer error=%v", err)
	}
}

func TestBudgetedInvokeRejectsUnsupportedBatchBeforeCredentialsAndProvider(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	fixture.capability.BatchingMode = domaininference.BatchingModeBatch
	fixture.capability.MaxBatchItems = 2
	if _, err := module.GlobalManager().UpsertCapability(ctx, fixture.capability); err != nil {
		t.Fatalf("UpsertCapability() error=%v", err)
	}
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-batch", now)
	scope.Limits.BatchingMode = domaininference.BatchingModeBatch
	scope.Limits.MaxBatchItems = 2
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	fake := &connectors.FakeConnector{Text: "must-not-run"}
	resolver := &countingSecretResolver{value: "must-not-read"}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	module.SetSecretResolver(resolver)
	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 8}}, Input: "hello", RequestID: "request-batch", BudgetScopeID: scope.ID, BudgetAttemptID: "attempt-batch", BatchItems: 2})
	if budgetDenialReason(err) != "connector_batching_unsupported" {
		t.Fatalf("Invoke() error=%v reason=%s", err, budgetDenialReason(err))
	}
	_, calls := fake.Calls()
	if calls != 0 || resolver.calls != 0 {
		t.Fatalf("connector calls=%d resolver calls=%d", calls, resolver.calls)
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	if len(persisted.Attempts) != 0 {
		t.Fatalf("denied batch persisted attempts=%+v", persisted.Attempts)
	}
}

type blockingSecretResolver struct{}

func (blockingSecretResolver) ResolveSecret(ctx context.Context, _ domaininference.Secret) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

type failingBudgetLedger struct{ inferencestorage.UsageLedger }

func (failingBudgetLedger) UpsertBudgetScope(context.Context, domaininference.BudgetScope) (domaininference.BudgetScope, error) {
	return domaininference.BudgetScope{}, errors.New("injected persistence failure")
}

type ambiguousUsageConnector struct{}

func (ambiguousUsageConnector) Embed(context.Context, connectors.EmbeddingRequest) (connectors.EmbeddingResponse, error) {
	return connectors.EmbeddingResponse{}, errors.New("unexpected embedding invocation")
}

func (ambiguousUsageConnector) Chat(context.Context, connectors.ChatRequest) (connectors.ChatResponse, error) {
	return connectors.ChatResponse{Usage: domaininference.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2, TokenCountSource: "provider_reported"}}, connectors.ConnectorError{Code: "network_error", Retryable: true, Err: errors.New("ambiguous provider outcome")}
}

type capturingBudgetConnector struct {
	calls     atomic.Int64
	maxOutput atomic.Int64
}

func (c *capturingBudgetConnector) Embed(context.Context, connectors.EmbeddingRequest) (connectors.EmbeddingResponse, error) {
	return connectors.EmbeddingResponse{}, errors.New("unexpected embedding invocation")
}

func (c *capturingBudgetConnector) Chat(_ context.Context, req connectors.ChatRequest) (connectors.ChatResponse, error) {
	c.calls.Add(1)
	c.maxOutput.Store(int64(req.Parameters.MaxOutputTokens))
	return connectors.ChatResponse{Text: "ok", Usage: domaininference.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2, TokenCountSource: "provider_reported"}}, nil
}

type overrunUsageConnector struct{}

func (overrunUsageConnector) Embed(context.Context, connectors.EmbeddingRequest) (connectors.EmbeddingResponse, error) {
	return connectors.EmbeddingResponse{Vector: []float64{1}, Usage: domaininference.Usage{InputTokens: 101, TotalTokens: 101, TokenCountSource: "provider_reported"}}, nil
}

func (overrunUsageConnector) Chat(context.Context, connectors.ChatRequest) (connectors.ChatResponse, error) {
	return connectors.ChatResponse{Text: "ok", Usage: domaininference.Usage{InputTokens: 101, OutputTokens: 1, TotalTokens: 102, TokenCountSource: "provider_reported"}}, nil
}

type unavailableUsageConnector struct{}

func (unavailableUsageConnector) Embed(context.Context, connectors.EmbeddingRequest) (connectors.EmbeddingResponse, error) {
	return connectors.EmbeddingResponse{Vector: []float64{1}, Usage: domaininference.Usage{TokenCountSource: "unavailable"}}, nil
}

func (unavailableUsageConnector) Chat(context.Context, connectors.ChatRequest) (connectors.ChatResponse, error) {
	return connectors.ChatResponse{Text: "ok", Usage: domaininference.Usage{TokenCountSource: "unavailable"}}, nil
}

type blockingBudgetConnector struct{ calls atomic.Int64 }

func (c *blockingBudgetConnector) Embed(ctx context.Context, _ connectors.EmbeddingRequest) (connectors.EmbeddingResponse, error) {
	c.calls.Add(1)
	<-ctx.Done()
	return connectors.EmbeddingResponse{}, ctx.Err()
}

func (c *blockingBudgetConnector) Chat(ctx context.Context, _ connectors.ChatRequest) (connectors.ChatResponse, error) {
	c.calls.Add(1)
	<-ctx.Done()
	return connectors.ChatResponse{}, ctx.Err()
}

func newTestBudgetScope(fixture resolverFixture, id string, now time.Time) domaininference.BudgetScope {
	return domaininference.BudgetScope{ID: domaininference.BudgetScopeID(id), State: domaininference.BudgetScopeActive, Route: domaininference.BudgetRoute{EndpointID: fixture.endpoint.ID, ModelID: fixture.model.ID, CapabilityID: fixture.capability.ID, Operation: fixture.capability.Operation}, Limits: domaininference.BudgetLimits{MaxRequests: 10, MaxAttempts: 20, MaxRetryAttempts: 10, MaxRetriesPerRequest: 10, MaxConcurrentProviderCalls: 10, BatchingMode: domaininference.BatchingModeSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 100, MaxInputTokens: 1000, MaxOutputTokens: 1000, MaxProviderMillisPerRequest: 1000, MaxProviderMillis: 10_000, Deadline: now.Add(time.Hour)}, CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(time.Hour), RetentionUntil: now.Add(time.Hour + 24*time.Hour)}
}

func TestBudgetPersistenceFailurePoisonsFurtherProviderReservations(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	scope := newTestBudgetScope(fixture, "scope-persistence-failure", now)
	if _, err := module.CreateBudgetScope(ctx, scope); err != nil {
		t.Fatalf("CreateBudgetScope() error=%v", err)
	}
	module.mu.Lock()
	base := module.usage
	module.usage = failingBudgetLedger{UsageLedger: base}
	module.mu.Unlock()
	request := domaininference.BudgetReservationRequest{ScopeID: scope.ID, AttemptID: "attempt-1", RequestID: "request-1", Route: scope.Route, BatchItems: 1, InputTokens: 1, OutputTokens: 1, ProviderMillis: 10, Now: now}
	if _, _, err := module.ReserveProviderAttempt(ctx, request); !errors.Is(err, ErrBudgetPersistence) {
		t.Fatalf("first reservation error=%v", err)
	}
	module.mu.Lock()
	module.usage = base
	module.mu.Unlock()
	request.AttemptID = "attempt-2"
	request.RequestID = "request-2"
	if _, _, err := module.ReserveProviderAttempt(ctx, request); !errors.Is(err, ErrBudgetPersistence) {
		t.Fatalf("reservation after persistence ambiguity error=%v", err)
	}
	persisted, _ := module.GetBudgetScope(ctx, scope.ID)
	if len(persisted.Attempts) != 0 {
		t.Fatalf("failed persistence mutated base ledger: %+v", persisted.Attempts)
	}
}

func TestBudgetScopePruneDeletesTerminalAndNaturallyExpiredScopes(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	terminal := newTestBudgetScope(fixture, "scope-terminal", now)
	terminal.RetentionUntil = terminal.ExpiresAt.Add(time.Hour)
	active := newTestBudgetScope(fixture, "scope-active", now)
	active.RetentionUntil = active.ExpiresAt.Add(time.Hour)
	if _, err := module.CreateBudgetScope(ctx, terminal); err != nil {
		t.Fatalf("create terminal scope: %v", err)
	}
	if _, err := module.CreateBudgetScope(ctx, active); err != nil {
		t.Fatalf("create active scope: %v", err)
	}
	if _, err := module.CompleteBudgetScope(ctx, terminal.ID, now.Add(time.Minute)); err != nil {
		t.Fatalf("complete terminal scope: %v", err)
	}
	deleted, err := module.PruneBudgetScopes(ctx, now.Add(3*time.Hour), 10)
	if err != nil || deleted != 2 {
		t.Fatalf("PruneBudgetScopes() deleted=%d err=%v", deleted, err)
	}
	if _, err := module.GetBudgetScope(ctx, terminal.ID); !errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
		t.Fatalf("terminal scope error=%v", err)
	}
	if _, err := module.GetBudgetScope(ctx, active.ID); !errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
		t.Fatalf("naturally expired scope error=%v", err)
	}
}

func TestCreateBudgetScopePrunesRetentionEligibleScopes(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	now := time.Now().UTC()
	expired := newTestBudgetScope(fixture, "scope-retention-expired", now.Add(-4*time.Hour))
	expired.ScopeSequence = 1
	expired.State = domaininference.BudgetScopeCompleted
	expired.ExpiresAt = now.Add(-3 * time.Hour)
	expired.Limits.Deadline = expired.ExpiresAt
	expired.RetentionUntil = now.Add(-time.Hour)
	if _, err := module.UsageLedger().UpsertBudgetScope(ctx, expired); err != nil {
		t.Fatalf("seed retention-eligible scope: %v", err)
	}
	fresh := newTestBudgetScope(fixture, "scope-fresh-after-prune", now)
	if _, err := module.CreateBudgetScope(ctx, fresh); err != nil {
		t.Fatalf("create scope after automatic prune: %v", err)
	}
	if _, err := module.GetBudgetScope(ctx, expired.ID); !errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
		t.Fatalf("retention-eligible scope error=%v", err)
	}
}

func TestBudgetScopeRouteMustBeComplete(t *testing.T) {
	now := time.Now().UTC()
	scope := domaininference.BudgetScope{ID: "scope-invalid", State: domaininference.BudgetScopeActive, Route: domaininference.BudgetRoute{EndpointID: uuid.New(), Operation: domaininference.OperationChat}, Limits: domaininference.BudgetLimits{MaxRequests: 1, MaxAttempts: 1, MaxConcurrentProviderCalls: 1, BatchingMode: domaininference.BatchingModeSingle, MaxBatchItems: 1, MaxProviderMillisPerRequest: 1, MaxProviderMillis: 1, Deadline: now.Add(time.Minute)}, ExpiresAt: now.Add(time.Minute), RetentionUntil: now.Add(time.Hour)}
	if err := validateBudgetScope(scope, now); err == nil {
		t.Fatal("validateBudgetScope() error=nil")
	}
}
