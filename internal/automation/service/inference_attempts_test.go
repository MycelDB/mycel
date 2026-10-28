package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/consensus"
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
	metrics := mgr.Metrics()
	if metrics.ProcedureInferenceRequested != 1 || metrics.ProcedureInferenceAuthorized != 1 || metrics.ProcedureInferenceStarted != 1 || metrics.ProcedureInferenceTerminal != 1 || metrics.ProcedureInferenceAmbiguous != 0 {
		t.Fatalf("unexpected lifecycle metrics: %+v", metrics)
	}
	if metrics.ProcedureInferenceAttemptCapacity != maxProcedureInferenceAttemptsPerSpace || metrics.ProcedureInferenceChangeCapacity != maxProcedureInferenceChangesPerSpace || metrics.ProcedureInferenceHistoryByteCapacity != maxProcedureInferenceHistoryBytes || metrics.ProcedureInferenceRetentionAgeSeconds != 30*24*60*60 || metrics.ProcedureInferenceAttemptByteCapacity != automation.ProcedureInferenceAttemptMaxBytes || metrics.ProcedureInferenceEvidenceStringByteCapacity != automation.ProcedureInferenceEvidenceStringMaxBytes {
		t.Fatalf("unexpected capacity diagnostics: %+v", metrics)
	}
	diagnostics, err := mgr.ProcedureInferenceDiagnostics(ctx)
	if err != nil || diagnostics.Spaces != 1 || diagnostics.RetainedAttempts != 1 || diagnostics.RetainedChanges != 4 || diagnostics.RetainedStateBytes == 0 || diagnostics.RetainedHistoryBytes == 0 {
		t.Fatalf("unexpected durable diagnostics: %+v err=%v", diagnostics, err)
	}
}

type procedureInferenceNoListStore struct {
	storage.Store
	listCalls int
}

func (s *procedureInferenceNoListStore) ListProcedureInferenceLedgers(context.Context) ([]automation.ProcedureInferenceLedger, error) {
	s.listCalls++
	return nil, errors.New("full-ledger scan forbidden")
}

func TestProcedureInferenceApplyDiagnosticsDoNotScanAllLedgers(t *testing.T) {
	ctx := context.Background()
	store := &procedureInferenceNoListStore{Store: storage.NewFileStore(t.TempDir())}
	var output bytes.Buffer
	mgr := NewManager(store).WithLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	old := time.Unix(1_700_000_000, 0).UTC()
	mgr.now = func() time.Time { return old }
	attempt := automation.ProcedureInferenceAttempt{
		ID: "attempt-no-scan", RecordVersion: 1, SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()),
		ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1,
		InvocationID: "inv", RunID: "run", RunAttemptNumber: 1, ProviderAttemptNumber: 1,
		State: automation.InferenceAttemptStateRequested, RequestedAt: old,
	}
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatalf("put old requested attempt: %v", err)
	}
	mgr.now = func() time.Time { return old.Add(procedureInferenceRetentionAge + time.Hour) }
	authorized := attempt
	authorized.RecordVersion = 2
	authorized.State = automation.InferenceAttemptStateAuthorized
	authorized.AuthorizedAt = mgr.now()
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, authorized); err != nil {
		t.Fatalf("put authorized attempt with pruning: %v", err)
	}
	if store.listCalls != 0 {
		t.Fatalf("apply diagnostics performed %d full-ledger scans", store.listCalls)
	}
	if !strings.Contains(output.String(), `"reason":"retention_gap_advance"`) {
		t.Fatalf("missing constant-time pruning diagnostic: %s", output.String())
	}
}

func TestProcedureInferenceCapacityDiagnosticsAreOperatorVisibleAndSanitized(t *testing.T) {
	ctx := context.Background()
	var output bytes.Buffer
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	mgr.logProcedureInferenceFixedBounds()
	mgr.recordProcedureInferenceStateByteCapacityRejection(ctx)
	logged := output.String()
	for _, want := range []string{
		`"msg":"procedure_inference_capacity_bounds"`,
		`"retention_age_seconds":2592000`,
		`"attempt_byte_capacity":65536`,
		`"evidence_string_byte_capacity":1024`,
		`"msg":"procedure_inference_capacity_diagnostics"`,
		`"reason":"state_byte_capacity_rejection"`,
		`"state_byte_capacity_rejections":1`,
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("operator diagnostic log missing %s: %s", want, logged)
		}
	}
	for _, forbidden := range []string{"space_id", "attempt_id", "principal_id", "provider_id"} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("operator diagnostic log contains forbidden identifier field %q: %s", forbidden, logged)
		}
	}
}

func TestProcedureInferenceMetricsCountAmbiguousAsTerminalSubset(t *testing.T) {
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	mgr.recordProcedureInferenceTransition(automation.InferenceAttemptStateAmbiguous)
	metrics := mgr.Metrics()
	if metrics.ProcedureInferenceTerminal != 1 || metrics.ProcedureInferenceAmbiguous != 1 {
		t.Fatalf("ambiguous metrics=%+v", metrics)
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
	persistedRun, err := store.GetRun(ctx, domainID, run.ID)
	if err != nil || len(persistedRun.InferenceAttemptIDs) != 1 || persistedRun.InferenceAttemptIDs[0] != first.ID {
		t.Fatalf("pre-terminal run reference=%+v err=%v", persistedRun, err)
	}
	first, err = mgr.authorizeProcedureInferenceAttempt(ctx, first, inferenceruntime.ResolveResult{Allowed: true, Decision: domaininference.PolicyDecision{AuthorizationRevision: 1}})
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

func TestProcessPendingStaleSnapshotCannotOverwriteActiveStandaloneClaim(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewFileStore(t.TempDir())
	store := &blockingPendingSnapshotStore{Store: baseStore, captured: make(chan struct{}), release: make(chan struct{})}
	mgr := NewManager(store)
	inv := automation.Invocation{ID: "inv-stale", SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()), AutomationID: "procedure", AutomationVersion: 1, Status: "pending"}
	if err := baseStore.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	result := make(chan struct {
		processed int
		err       error
	}, 1)
	go func() {
		processed, err := mgr.ProcessPending(ctx, inv.DomainID, 10)
		result <- struct {
			processed int
			err       error
		}{processed: processed, err: err}
	}()
	<-store.captured

	claimed, ok, err := mgr.claimInvocation(ctx, inv)
	if err != nil || !ok {
		t.Fatalf("claim=%+v ok=%v err=%v", claimed, ok, err)
	}
	ledger := automation.ProcedureInferenceLedger{SpaceID: inv.SpaceID, Attempts: []automation.ProcedureInferenceAttempt{{ID: "attempt", InvocationID: inv.ID, SpaceID: inv.SpaceID, State: automation.InferenceAttemptStateStarted, RunAttemptNumber: 1}}}
	if err := baseStore.PutProcedureInferenceLedger(ctx, ledger); err != nil {
		t.Fatal(err)
	}
	close(store.release)
	got := <-result
	if got.err != nil || got.processed != 0 {
		t.Fatalf("stale ProcessPending() processed=%d err=%v", got.processed, got.err)
	}
	current, err := baseStore.GetInvocation(ctx, inv.DomainID, inv.ID)
	if err != nil || current.Status != "running" || current.ClaimToken != claimed.ClaimToken || current.ClaimVersion != claimed.ClaimVersion {
		t.Fatalf("active claim overwritten: current=%+v claimed=%+v err=%v", current, claimed, err)
	}
}

type blockingPendingSnapshotStore struct {
	storage.Store
	captured chan struct{}
	release  chan struct{}
	blocked  bool
}

func (s *blockingPendingSnapshotStore) ListInvocations(ctx context.Context, domainID graph.DomainID, filter storage.InvocationFilter) ([]automation.Invocation, error) {
	items, err := s.Store.ListInvocations(ctx, domainID, filter)
	if err == nil && filter.Status == "pending" && !s.blocked {
		s.blocked = true
		close(s.captured)
		<-s.release
	}
	return items, err
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

func TestStandaloneRestartReconcilesTerminalRunBeforeInvocationFinalization(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := storage.NewFileStore(root)
	domainID := graph.DomainID(uuid.New())
	now := time.Now().UTC()
	inv := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: domainID, Status: "running", ClaimVersion: 1, ClaimToken: "old", ClaimExpiresAt: now.Add(-time.Second)}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, AttemptNumber: 1, Status: "succeeded", RenderedInputHash: "input-hash", CompletedAt: now, InferenceAttemptIDs: []string{"attempt-1"}}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatal(err)
	}

	restarted := NewManager(storage.NewFileStore(root))
	reconciled, err := restarted.reconcileTerminalRunFinalization(ctx, &inv, nil)
	if err != nil || !reconciled {
		t.Fatalf("reconciled=%v err=%v", reconciled, err)
	}
	persisted, err := restarted.store.GetInvocation(ctx, domainID, inv.ID)
	if err != nil || persisted.Status != "succeeded" || persisted.AttemptCount != 1 || persisted.InputHash != run.RenderedInputHash {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
	persistedRun, err := restarted.store.GetRun(ctx, domainID, run.ID)
	if err != nil || len(persistedRun.InferenceAttemptIDs) != 1 || persistedRun.InferenceAttemptIDs[0] != "attempt-1" {
		t.Fatalf("run=%+v err=%v", persistedRun, err)
	}

	skippedInvocation := automation.Invocation{ID: "skipped-inv", SpaceID: inv.SpaceID, DomainID: domainID, Status: "running"}
	if err := restarted.store.PutInvocation(ctx, skippedInvocation); err != nil {
		t.Fatal(err)
	}
	skippedRun := automation.Run{ID: automationRunID(skippedInvocation, 1), DomainID: domainID, InvocationID: skippedInvocation.ID, AttemptNumber: 1, Status: "skipped", Error: skipReasonCoalesced, CompletedAt: now}
	if err := restarted.store.PutRun(ctx, skippedRun); err != nil {
		t.Fatal(err)
	}
	reconciled, err = restarted.reconcileTerminalRunFinalization(ctx, &skippedInvocation, nil)
	if err != nil || !reconciled || skippedInvocation.AttemptCount != 0 {
		t.Fatalf("skipped reconciliation changed attempt count: inv=%+v reconciled=%v err=%v", skippedInvocation, reconciled, err)
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
	attempt, err = mgr.authorizeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.ResolveResult{Allowed: true, Decision: domaininference.PolicyDecision{AuthorizationRevision: 1}})
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

func TestProcedureInferenceRetentionPrunesOnlyOldTerminalStateAndAdvancesWatermarks(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	ledger := automation.ProcedureInferenceLedger{SpaceID: uuid.NewString(), HighWaterSequence: retainedProcedureInferenceChanges + 1, EarliestRetainedSequence: 1, EarliestRetainedStateSequence: 1}
	for i := 0; i < retainedProcedureInferenceChanges+1; i++ {
		ledger.Changes = append(ledger.Changes, automation.ProcedureInferenceAttemptChange{Sequence: uint64(i + 1)})
	}
	for i := 0; i < retainedProcedureInferenceAttempts+2; i++ {
		state := automation.InferenceAttemptStateSucceeded
		if i == retainedProcedureInferenceAttempts+1 {
			state = automation.InferenceAttemptStateAmbiguous
		}
		ledger.Attempts = append(ledger.Attempts, automation.ProcedureInferenceAttempt{ID: fmt.Sprintf("attempt-%05d", i), State: state, CompletedAt: base.Add(time.Duration(i) * time.Second)})
	}
	protected := ledger.Attempts[0].ID
	pruneProcedureInferenceLedger(&ledger, protected, time.Time{}, nil)
	if len(ledger.Changes) != retainedProcedureInferenceChanges || ledger.EarliestRetainedSequence != 2 {
		t.Fatalf("retained changes=%d earliest=%d", len(ledger.Changes), ledger.EarliestRetainedSequence)
	}
	if len(ledger.Attempts) != retainedProcedureInferenceAttempts || ledger.EarliestRetainedStateSequence != ledger.HighWaterSequence+1 {
		t.Fatalf("retained attempts=%d state watermark=%d", len(ledger.Attempts), ledger.EarliestRetainedStateSequence)
	}
	seen := map[string]bool{}
	for _, attempt := range ledger.Attempts {
		seen[attempt.ID] = true
	}
	if !seen[protected] || !seen[fmt.Sprintf("attempt-%05d", retainedProcedureInferenceAttempts+1)] {
		t.Fatalf("protected or ambiguous attempt was pruned")
	}
}

func TestProcedureInferenceChangeOnlyRetentionInvalidatesPreRetainedSnapshot(t *testing.T) {
	ledger := automation.ProcedureInferenceLedger{HighWaterSequence: 3, EarliestRetainedSequence: 1, EarliestRetainedStateSequence: 1, Attempts: []automation.ProcedureInferenceAttempt{{ID: "active", State: automation.InferenceAttemptStateStarted}}, Changes: make([]automation.ProcedureInferenceAttemptChange, retainedProcedureInferenceChanges+1)}
	for i := range ledger.Changes {
		ledger.Changes[i].Sequence = uint64(i + 1)
	}
	stats := pruneProcedureInferenceLedger(&ledger, "", time.Time{}, nil)
	if stats.Changes != 1 || stats.HistoryBytes == 0 || !stats.GapAdvanced {
		t.Fatalf("prune diagnostics=%+v", stats)
	}
	if ledger.EarliestRetainedSequence != 2 || ledger.EarliestRetainedStateSequence != 2 {
		t.Fatalf("retention watermarks history=%d state=%d", ledger.EarliestRetainedSequence, ledger.EarliestRetainedStateSequence)
	}
}

func TestProcedureInferenceRetentionEnforcesByteTargetsBelowCountThreshold(t *testing.T) {
	large := strings.Repeat("x", 5<<20)
	ledger := automation.ProcedureInferenceLedger{HighWaterSequence: 10, EarliestRetainedSequence: 1, EarliestRetainedStateSequence: 1}
	for i := 0; i < 10; i++ {
		attempt := automation.ProcedureInferenceAttempt{ID: fmt.Sprintf("attempt-%02d", i), ProcedureID: large, State: automation.InferenceAttemptStateSucceeded, CompletedAt: time.Unix(int64(i+1), 0).UTC()}
		ledger.Attempts = append(ledger.Attempts, attempt)
		ledger.Changes = append(ledger.Changes, automation.ProcedureInferenceAttemptChange{Sequence: uint64(i + 1), Attempt: attempt})
	}
	stats := pruneProcedureInferenceLedger(&ledger, "", time.Time{}, nil)
	stateBytes, historyBytes := procedureInferenceLedgerBytes(ledger)
	if stats.Attempts == 0 || stats.Changes == 0 || stats.StateBytes == 0 || stats.HistoryBytes == 0 || !stats.GapAdvanced {
		t.Fatalf("byte retention did not report pressure: %+v", stats)
	}
	if stateBytes > retainedProcedureInferenceStateBytes || historyBytes > retainedProcedureInferenceHistoryBytes {
		t.Fatalf("byte retention state=%d history=%d", stateBytes, historyBytes)
	}
}

func TestProcedureInferenceRetentionPrunesAgedTerminalButKeepsAmbiguous(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	old := now.Add(-procedureInferenceRetentionAge - time.Hour)
	ledger := automation.ProcedureInferenceLedger{HighWaterSequence: 4, EarliestRetainedSequence: 1, EarliestRetainedStateSequence: 1, Attempts: []automation.ProcedureInferenceAttempt{{ID: "old", State: automation.InferenceAttemptStateSucceeded, CompletedAt: old}, {ID: "ambiguous", State: automation.InferenceAttemptStateAmbiguous, CompletedAt: old}, {ID: "recent", State: automation.InferenceAttemptStateSucceeded, CompletedAt: now}}, Changes: []automation.ProcedureInferenceAttemptChange{{Sequence: 1, CommittedAt: old}, {Sequence: 2, CommittedAt: now}}}
	pruneProcedureInferenceLedger(&ledger, "", now, nil)
	if len(ledger.Attempts) != 2 || ledger.Attempts[0].ID != "ambiguous" || ledger.Attempts[1].ID != "recent" || ledger.EarliestRetainedSequence != 2 || ledger.EarliestRetainedStateSequence != ledger.HighWaterSequence+1 {
		t.Fatalf("retained ledger=%+v", ledger)
	}
}

func TestProcedureInferenceRetentionDoesNotPruneRunReferencedAttempt(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	old := now.Add(-procedureInferenceRetentionAge - time.Hour)
	ledger := automation.ProcedureInferenceLedger{HighWaterSequence: 3, Attempts: []automation.ProcedureInferenceAttempt{{ID: "referenced", State: automation.InferenceAttemptStateSucceeded, CompletedAt: old}, {ID: "unreferenced", State: automation.InferenceAttemptStateSucceeded, CompletedAt: old}, {ID: "recent", State: automation.InferenceAttemptStateSucceeded, CompletedAt: now}}}
	pruneProcedureInferenceLedger(&ledger, "", now, map[string]struct{}{"referenced": {}})
	if len(ledger.Attempts) != 2 || ledger.Attempts[0].ID != "referenced" || ledger.Attempts[1].ID != "recent" {
		t.Fatalf("retained attempts=%+v", ledger.Attempts)
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

func TestProcedureInferenceProviderStartRejectsChangedAuthorizationRevisionAtRaftApply(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	revisions := &outputFenceRevisionInference{revision: 2}
	mgr := NewManager(store).WithInferenceInvoker(revisions)
	mgr.raftGroups = &consensus.MultiGroup{}
	mgr.raftPartitionCount = 1
	attempt := automation.ProcedureInferenceAttempt{ID: "attempt", RecordVersion: 1, SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()), ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, InvocationID: "inv", RunID: "run", RunAttemptNumber: 1, ProviderAttemptNumber: 1, State: automation.InferenceAttemptStateRequested, RequestedAt: time.Unix(100, 0).UTC()}
	if err := mgr.applyProcedureInferenceAttemptTransition(ctx, attempt, attempt.RequestedAt); err != nil {
		t.Fatal(err)
	}
	attempt.RecordVersion = 2
	attempt.State = automation.InferenceAttemptStateAuthorized
	attempt.AuthorizedAt = time.Unix(101, 0).UTC()
	attempt.ConfigurationRevision = 1
	attempt.AuthorizationRevision = 1
	attempt.PolicyDecisionID = uuid.NewString()
	if err := mgr.applyProcedureInferenceAttemptTransition(ctx, attempt, attempt.AuthorizedAt); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSpaceAuthorizationRevision(ctx, storage.SpaceAuthorizationRevision{SpaceID: attempt.SpaceID, Revision: 2}); err != nil {
		t.Fatal(err)
	}
	attempt.RecordVersion = 3
	attempt.State = automation.InferenceAttemptStateStarted
	attempt.StartedAt = time.Unix(102, 0).UTC()
	if err := mgr.applyProcedureInferenceAttemptTransition(ctx, attempt, attempt.StartedAt); !errors.Is(err, ErrProcedureAuthorizationRevoked) {
		t.Fatalf("provider start error=%v", err)
	}
}

func TestProcedureInferenceChangeCommittedAtIsDeterministicAcrossApply(t *testing.T) {
	ctx := context.Background()
	spaceID := uuid.NewString()
	attempt := automation.ProcedureInferenceAttempt{ID: "attempt", RecordVersion: 1, SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, InvocationID: "inv", RunID: "run", RunAttemptNumber: 1, ProviderAttemptNumber: 1, State: automation.InferenceAttemptStateRequested, RequestedAt: time.Unix(100, 0).UTC()}
	committedAt := time.Unix(200, 123).UTC()
	for _, root := range []string{t.TempDir(), t.TempDir()} {
		mgr := NewManager(storage.NewFileStore(root))
		if err := mgr.applyProcedureInferenceAttemptTransition(ctx, attempt, committedAt); err != nil {
			t.Fatal(err)
		}
		ledger, err := mgr.store.GetProcedureInferenceLedger(ctx, spaceID)
		if err != nil || len(ledger.Changes) != 1 || !ledger.Changes[0].CommittedAt.Equal(committedAt) {
			t.Fatalf("ledger=%+v err=%v", ledger, err)
		}
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
	attempt, err = mgr.authorizeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.ResolveResult{Allowed: true, Decision: domaininference.PolicyDecision{AuthorizationRevision: 1}})
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
	attempt, err = mgr.authorizeProcedureInferenceAttempt(context.Background(), attempt, inferenceruntime.ResolveResult{Allowed: true, Decision: domaininference.PolicyDecision{AuthorizationRevision: 1}})
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
