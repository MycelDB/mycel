package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
)

func TestProcedureInferenceAttemptLedgerIsMonotonicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	now := time.Now().UTC()
	attempt := automation.ProcedureInferenceAttempt{
		ID: "attempt-a", RecordVersion: 1, SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()),
		ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1,
		InvocationID: "inv", RunID: "run", RunAttemptNumber: 1, ProviderAttemptNumber: 1,
		State: automation.InferenceAttemptStateRequested, RequestedAt: now,
	}
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatalf("put requested: %v", err)
	}
	authorized := attempt
	authorized.RecordVersion = 2
	authorized.State = automation.InferenceAttemptStateAuthorized
	authorized.AuthorizedAt = now.Add(time.Second)
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, authorized); err != nil {
		t.Fatalf("put authorized: %v", err)
	}
	started := authorized
	started.RecordVersion = 3
	started.State = automation.InferenceAttemptStateStarted
	started.StartedAt = now.Add(2 * time.Second)
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, started); err != nil {
		t.Fatalf("put started: %v", err)
	}
	completed := started
	completed.RecordVersion = 4
	completed.State = automation.InferenceAttemptStateSucceeded
	completed.OutcomeCategory = "succeeded"
	completed.CompletedAt = now.Add(3 * time.Second)
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, completed); err != nil {
		t.Fatalf("put completed: %v", err)
	}
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, completed); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}

	ledger, err := store.GetProcedureInferenceLedger(ctx, attempt.SpaceID)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.HighWaterSequence != 4 || ledger.EarliestRetainedSequence != 1 || len(ledger.Attempts) != 1 || len(ledger.Changes) != 4 {
		t.Fatalf("unexpected ledger: %+v", ledger)
	}
	for i, change := range ledger.Changes {
		if change.Sequence != uint64(i+1) || change.EventID == "" || change.SchemaVersion != automation.InferenceAttemptChangeSchemaV1 {
			t.Fatalf("unexpected change %d: %+v", i, change)
		}
	}
}

func TestBeginProcedureInferenceAttemptRefusesExistingStartedAttempt(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	now := time.Now().UTC()
	mgr.now = func() time.Time { return now }
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, EventID: "event", ActorPrincipalID: automationActor}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, AttemptNumber: 1, ActorPrincipalID: automationActor}

	first, err := mgr.beginProcedureInferenceAttempt(ctx, inv, &run, domaininference.OperationChat, "profile")
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	first, err = mgr.authorizeProcedureInferenceAttempt(ctx, first, inferenceruntime.ResolveResult{Allowed: true})
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	first, err = mgr.startProcedureInferenceAttempt(ctx, first)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.State != automation.InferenceAttemptStateStarted {
		t.Fatalf("first state = %q", first.State)
	}
	secondRun := run
	secondRun.InferenceAttemptIDs = nil
	second, err := mgr.beginProcedureInferenceAttempt(ctx, inv, &secondRun, domaininference.OperationChat, "profile")
	if !errors.Is(err, ErrInferenceAttemptAlreadyStarted) {
		t.Fatalf("second begin error = %v", err)
	}
	if second.ID != first.ID || len(secondRun.InferenceAttemptIDs) != 1 || secondRun.InferenceAttemptIDs[0] != first.ID {
		t.Fatalf("existing attempt was not reused: second=%+v run=%+v", second, secondRun)
	}
	ledger, err := store.GetProcedureInferenceLedger(ctx, inv.SpaceID)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.HighWaterSequence != 3 || len(ledger.Changes) != 3 {
		t.Fatalf("duplicate begin changed ledger: %+v", ledger)
	}
}

func TestLocalInvocationClaimHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()), Status: "pending"}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, claimed, err := mgr.claimInvocation(ctx, inv)
			results <- claimed
			errs <- err
		}()
	}
	close(start)
	winners := 0
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if <-results {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("claim winners=%d, want 1", winners)
	}
}

func TestStandaloneRestartReclaimsExpiredInvocation(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := storage.NewFileStore(root)
	domainID := graph.DomainID(uuid.New())
	now := time.Now().UTC()
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: domainID, Status: "running", ClaimVersion: 1, ClaimToken: "old", ClaimExpiresAt: now.Add(-time.Second)}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	restarted := NewManager(storage.NewFileStore(root))
	restarted.now = func() time.Time { return now }
	pending, err := restarted.pendingInvocations(ctx, domainID, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != inv.ID {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	claimed, ok, err := restarted.claimInvocation(ctx, pending[0])
	if err != nil || !ok || claimed.ClaimVersion != 2 || claimed.ClaimToken == "old" {
		t.Fatalf("claimed=%+v ok=%v err=%v", claimed, ok, err)
	}
}

func TestStandaloneRestartAdvancesKnownPreProviderTerminalAttempt(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := storage.NewFileStore(root)
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, Status: "running", ClaimVersion: 2, ClaimToken: "reclaimed"}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1}
	attempt, err := mgr.beginProcedureInferenceAttempt(ctx, inv, &run, domaininference.OperationChat, "profile")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.completeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.InvokeResponse{Status: domaininference.UsageStatusFailed}, errors.New("routing unavailable")); err != nil {
		t.Fatal(err)
	}

	restarted := NewManager(storage.NewFileStore(root))
	stop, err := restarted.reconcilePreProviderTerminalAttempt(ctx, &inv, automation.Definition{Safety: automation.Safety{MaxAttempts: 3}})
	if err != nil || stop {
		t.Fatalf("reconcile stop=%v err=%v", stop, err)
	}
	if inv.AttemptCount != 1 || automationRunID(inv, inv.AttemptCount+1) == run.ID {
		t.Fatalf("logical attempt did not advance: inv=%+v run=%+v", inv, run)
	}
	persisted, err := restarted.store.GetInvocation(ctx, domainID, inv.ID)
	if err != nil || persisted.AttemptCount != 1 || persisted.Status != "running" {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
}

func TestStandaloneRestartSuppressesStartedProviderAttempt(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := storage.NewFileStore(root)
	mgr := NewManager(store)
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()), BindingID: "binding", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: inv.DomainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1}
	attempt, err := mgr.beginProcedureInferenceAttempt(ctx, inv, &run, domaininference.OperationChat, "profile")
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = mgr.authorizeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.ResolveResult{Allowed: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.startProcedureInferenceAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	restarted := NewManager(storage.NewFileStore(root))
	secondRun := run
	secondRun.InferenceAttemptIDs = nil
	if _, err := restarted.beginProcedureInferenceAttempt(ctx, inv, &secondRun, domaininference.OperationChat, "profile"); !errors.Is(err, ErrInferenceAttemptAlreadyStarted) {
		t.Fatalf("restart begin error=%v", err)
	}
}

func TestProcedureInferenceTransitionCapacityReservations(t *testing.T) {
	tests := []struct {
		name     string
		changes  int
		incoming string
		other    []string
		want     bool
	}{
		{name: "requested exact fit", changes: maxProcedureInferenceChangesPerSpace - 4, incoming: automation.InferenceAttemptStateRequested, want: true},
		{name: "requested one over", changes: maxProcedureInferenceChangesPerSpace - 3, incoming: automation.InferenceAttemptStateRequested, want: false},
		{name: "authorized exact fit", changes: maxProcedureInferenceChangesPerSpace - 3, incoming: automation.InferenceAttemptStateAuthorized, want: true},
		{name: "started exact fit", changes: maxProcedureInferenceChangesPerSpace - 2, incoming: automation.InferenceAttemptStateStarted, want: true},
		{name: "terminal exact fit", changes: maxProcedureInferenceChangesPerSpace - 1, incoming: automation.InferenceAttemptStateSucceeded, want: true},
		{name: "other active reservation", changes: maxProcedureInferenceChangesPerSpace - 3, incoming: automation.InferenceAttemptStateSucceeded, other: []string{automation.InferenceAttemptStateRequested}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := procedureInferenceTransitionFits(tt.changes, tt.incoming, tt.other); got != tt.want {
				t.Fatalf("procedureInferenceTransitionFits()=%v want %v", got, tt.want)
			}
		})
	}
}

func TestProcedureInferenceAttemptLedgerSerializesConcurrentSpacesState(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := uuid.NewString()
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			<-start
			attempt := automation.ProcedureInferenceAttempt{ID: fmt.Sprintf("attempt-%d", i), RecordVersion: 1, SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, InvocationID: fmt.Sprintf("inv-%d", i), RunID: fmt.Sprintf("run-%d", i), RunAttemptNumber: 1, ProviderAttemptNumber: 1, State: automation.InferenceAttemptStateRequested, RequestedAt: time.Now().UTC()}
			errs <- mgr.putProcedureInferenceAttemptRuntime(ctx, attempt)
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	ledger, err := store.GetProcedureInferenceLedger(ctx, spaceID)
	if err != nil || ledger.HighWaterSequence != 2 || len(ledger.Attempts) != 2 || len(ledger.Changes) != 2 {
		t.Fatalf("concurrent ledger update lost state: %+v err=%v", ledger, err)
	}
}

func TestRetryInvocationRejectsProviderStartedAttempt(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()), BindingID: "binding", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, Status: "failed"}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: inv.DomainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1}
	attempt, err := mgr.beginProcedureInferenceAttempt(ctx, inv, &run, domaininference.OperationChat, "profile")
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = mgr.authorizeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.ResolveResult{Allowed: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mgr.startProcedureInferenceAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.RetryInvocation(ctx, inv.DomainID, inv.ID); !errors.Is(err, ErrInferenceAttemptAlreadyStarted) {
		t.Fatalf("RetryInvocation() error=%v", err)
	}
	persisted, err := store.GetInvocation(ctx, inv.DomainID, inv.ID)
	if err != nil || persisted.Status != "failed" {
		t.Fatalf("retry changed invocation: %+v err=%v", persisted, err)
	}
}

func TestCompleteProcedureInferenceAttemptPersistsAfterRequestCancellation(t *testing.T) {
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	now := time.Now().UTC()
	mgr.now = func() time.Time { return now }
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()), BindingID: "binding", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, EventID: "event"}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: inv.DomainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1}
	attempt, err := mgr.beginProcedureInferenceAttempt(context.Background(), inv, &run, domaininference.OperationChat, "profile")
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = mgr.authorizeProcedureInferenceAttempt(context.Background(), attempt, inferenceruntime.ResolveResult{Allowed: true})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = mgr.startProcedureInferenceAttempt(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if state, err := mgr.completeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.InvokeResponse{Status: domaininference.UsageStatusSucceeded}, nil); err != nil || state != automation.InferenceAttemptStateSucceeded {
		t.Fatalf("complete state=%q err=%v", state, err)
	}
	persisted, err := mgr.getProcedureInferenceAttempt(context.Background(), inv.SpaceID, attempt.ID)
	if err != nil || persisted.State != automation.InferenceAttemptStateSucceeded {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
}

func TestAutomationRunIDIsDeterministicPerLogicalAttempt(t *testing.T) {
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New())}
	first := automationRunID(inv, 1)
	if first == "" || first != automationRunID(inv, 1) {
		t.Fatal("run ID is not deterministic")
	}
	if first == automationRunID(inv, 2) {
		t.Fatal("different logical run attempts must have different IDs")
	}
}
