package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReconcileProviderStartedPublicationFinalizesPublishedOutputWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv", SpaceID: spaceID, DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "procedure", ProcedureVersion: 1, Status: "retryable", ClaimVersion: 1, ClaimToken: "old-claim", AttemptCount: 0, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1, ActorPrincipalID: automationActor}
	outputKey := automationOutputIdempotencyKey(inv, run.ID)
	graphs := &automationE2EGraph{
		node:              graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID},
		idempotentCommits: map[string]graphchange.CommitResult{automationActor + "\x00" + outputKey: {IdempotencyKey: outputKey, CommittedRevision: 1}},
	}
	mgr := NewManager(store).WithGraphRuntime(automationE2ESessions{spaceID: spaceID, domainID: domainID.String()}, graphs)
	if err := mgr.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatal(err)
	}
	attempt := persistSucceededPublicationAttempt(t, ctx, mgr, inv, run, "publication_pending")
	retentions := 0
	acknowledgements := 0
	graphs.idempotencyRetentions = &retentions
	graphs.idempotencyAcknowledgements = &acknowledgements
	graphs.retainIdempotency = func(_ context.Context, _, _, _, key, commitID string) error {
		persisted := requireProcedureInferenceAttempt(t, ctx, store, spaceID, attempt.ID)
		if persisted.PublicationDisposition != "publication_pending" {
			t.Fatalf("legacy receipt promoted after terminal disposition: %q", persisted.PublicationDisposition)
		}
		if key != outputKey || commitID != "commit-"+outputKey {
			t.Fatalf("retained key=%q commit=%q", key, commitID)
		}
		return nil
	}
	graphs.acknowledgeIdempotency = func(_ context.Context, _, _, _, key, commitID string) error {
		persisted := requireProcedureInferenceAttempt(t, ctx, store, spaceID, attempt.ID)
		if persisted.PublicationDisposition != "published" {
			t.Fatalf("receipt acknowledged before terminal disposition: %q", persisted.PublicationDisposition)
		}
		if key != outputKey || commitID != "commit-"+outputKey {
			t.Fatalf("acknowledged key=%q commit=%q", key, commitID)
		}
		return nil
	}

	reconciled, err := mgr.reconcileProviderStartedPublication(ctx, &inv)
	if err != nil || !reconciled {
		t.Fatalf("reconcileProviderStartedPublication() reconciled=%v err=%v", reconciled, err)
	}
	persistedAttempt := requireProcedureInferenceAttempt(t, ctx, store, spaceID, attempt.ID)
	if persistedAttempt.PublicationDisposition != "published" || persistedAttempt.PublicationReceiptCommitID != "commit-"+outputKey {
		t.Fatalf("publication disposition=%q receipt=%q", persistedAttempt.PublicationDisposition, persistedAttempt.PublicationReceiptCommitID)
	}
	persistedRun, err := store.GetRun(ctx, domainID, run.ID)
	if err != nil || persistedRun.Status != "succeeded" || len(persistedRun.InferenceAttemptIDs) != 1 || persistedRun.InferenceAttemptIDs[0] != attempt.ID {
		t.Fatalf("persisted run=%+v err=%v", persistedRun, err)
	}
	persistedInvocation, err := store.GetInvocation(ctx, domainID, inv.ID)
	if err != nil || persistedInvocation.Status != "succeeded" || persistedInvocation.AttemptCount != 1 {
		t.Fatalf("persisted invocation=%+v err=%v", persistedInvocation, err)
	}
	if retentions != 1 || acknowledgements != 1 {
		t.Fatalf("receipt retentions=%d acknowledgements=%d want=1,1", retentions, acknowledgements)
	}
}

func TestAutomationDomainsIncludeInvocationOnlyDomain(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-only", SpaceID: uuid.NewString(), DomainID: domainID, Status: "retryable", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(store)
	domains, err := mgr.ListAutomationDomains(ctx)
	if err != nil || len(domains) != 1 || domains[0] != domainID {
		t.Fatalf("ListAutomationDomains()=%v err=%v", domains, err)
	}
}

func TestClaimPublicationRecoveryDoesNotPreemptCurrentProcessClaim(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	now := time.Now().UTC()
	mgr := NewManager(store)
	inv := automation.Invocation{ID: "inv-active", SpaceID: uuid.NewString(), DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "procedure", ProcedureVersion: 1, Status: "running", ClaimVersion: 2, ClaimToken: "active", ClaimProcessID: mgr.processID, ClaimExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
	if err := mgr.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatal(err)
	}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, AttemptNumber: 1, ActorPrincipalID: automationActor}
	persistSucceededPublicationAttempt(t, ctx, mgr, inv, run, "publication_pending")
	pending, err := mgr.pendingInvocations(ctx, domainID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pendingInvocations()=%+v err=%v, want current publisher excluded", pending, err)
	}
	claimed, ok, err := mgr.claimPublicationRecovery(ctx, inv)
	if err != nil || ok || claimed.ClaimToken != inv.ClaimToken || claimed.ClaimVersion != inv.ClaimVersion {
		t.Fatalf("claimPublicationRecovery() claimed=%v invocation=%+v err=%v", ok, claimed, err)
	}
}

func TestClaimPublicationRecoveryTakesOverUnexpiredLocalClaim(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	now := time.Now().UTC()
	inv := automation.Invocation{
		ID:             "inv-local-restart",
		SpaceID:        uuid.NewString(),
		DomainID:       domainID,
		Status:         "running",
		ClaimVersion:   4,
		ClaimToken:     "interrupted-worker",
		ClaimProcessID: "obsolete-process",
		ClaimExpiresAt: now.Add(automationClaimLease),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	mgr := NewManager(store)
	if err := mgr.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatal(err)
	}

	claimed, ok, err := mgr.claimPublicationRecovery(ctx, inv)
	if err != nil || !ok {
		t.Fatalf("claimPublicationRecovery() claimed=%v err=%v", ok, err)
	}
	if claimed.ClaimVersion != inv.ClaimVersion+1 || claimed.ClaimToken == inv.ClaimToken || claimed.Status != "running" {
		t.Fatalf("claim was not replaced: %+v", claimed)
	}
}

func TestPublicationReceiptSurvivesLaterEntityMetadataReplacement(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-receipt", SpaceID: spaceID, DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "procedure", ProcedureVersion: 1, Status: "retryable", ClaimVersion: 1, ClaimToken: "old", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1, ActorPrincipalID: automationActor}
	key := automationOutputIdempotencyKey(inv, run.ID)
	graphs := &automationE2EGraph{
		// Current entity metadata belongs to a later publication and no longer
		// carries this run's key. The graph-owned receipt remains authoritative.
		node:              graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Meta: map[string]any{"automation": map[string]any{"output_idempotency_key": "later-output"}}},
		idempotentCommits: map[string]graphchange.CommitResult{automationActor + "\x00" + key: {IdempotencyKey: key, CommittedRevision: 1}},
	}
	mgr := NewManager(store).WithGraphRuntime(automationE2ESessions{spaceID: spaceID, domainID: domainID.String()}, graphs)
	if err := mgr.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatal(err)
	}
	attempt := persistSucceededPublicationAttempt(t, ctx, mgr, inv, run, "publication_pending")
	if reconciled, err := mgr.reconcileProviderStartedPublication(ctx, &inv); err != nil || !reconciled {
		t.Fatalf("reconcileProviderStartedPublication() reconciled=%v err=%v", reconciled, err)
	}
	if got := requireProcedureInferenceAttempt(t, ctx, store, spaceID, attempt.ID).PublicationDisposition; got != "published" {
		t.Fatalf("publication disposition=%q", got)
	}
}

func TestReconcileProviderStartedPublicationPreservesMatchingTerminalRun(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	now := time.Now().UTC()
	inv := automation.Invocation{ID: "inv-terminal-run", SpaceID: spaceID, DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "procedure", ProcedureVersion: 1, Status: "running", ClaimVersion: 1, ClaimToken: "old-claim", ClaimProcessID: "obsolete-process", ClaimExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, AttemptNumber: 1, Status: "succeeded", ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, ActorPrincipalID: automationActor, StartedAt: now, CompletedAt: now}
	mgr := NewManager(store).WithGraphRuntime(automationE2ESessions{spaceID: spaceID, domainID: domainID.String()}, &automationE2EGraph{})
	if err := mgr.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatal(err)
	}
	if err := mgr.putRunRuntime(ctx, spaceID, run); err != nil {
		t.Fatal(err)
	}
	persistSucceededPublicationAttempt(t, ctx, mgr, inv, run, "published")
	if reconciled, err := mgr.reconcileProviderStartedPublication(ctx, &inv); err != nil || !reconciled {
		t.Fatalf("reconcileProviderStartedPublication() reconciled=%v err=%v", reconciled, err)
	}
	persistedRun, err := store.GetRun(ctx, domainID, run.ID)
	if err != nil || persistedRun.ClaimToken != run.ClaimToken || persistedRun.ClaimVersion != run.ClaimVersion || persistedRun.Status != "succeeded" {
		t.Fatalf("terminal run was rewritten: %+v err=%v", persistedRun, err)
	}
	persistedInvocation, err := store.GetInvocation(ctx, domainID, inv.ID)
	if err != nil || persistedInvocation.Status != "succeeded" {
		t.Fatalf("invocation was not finalized: %+v err=%v", persistedInvocation, err)
	}
}

func TestReconcileProviderStartedPublicationFinalizesAbsentOutputAsFailed(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv", SpaceID: spaceID, DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "procedure", ProcedureVersion: 1, Status: "failed", ClaimVersion: 1, ClaimToken: "old-claim", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1, ActorPrincipalID: automationActor}
	graphs := &automationE2EGraph{node: graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID}}
	mgr := NewManager(store).WithGraphRuntime(automationE2ESessions{spaceID: spaceID, domainID: domainID.String()}, graphs)
	if err := mgr.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatal(err)
	}
	attempt := persistSucceededPublicationAttempt(t, ctx, mgr, inv, run, "publication_pending")

	reconciled, err := mgr.reconcileProviderStartedPublication(ctx, &inv)
	if err != nil || !reconciled {
		t.Fatalf("reconcileProviderStartedPublication() reconciled=%v err=%v", reconciled, err)
	}
	persistedAttempt := requireProcedureInferenceAttempt(t, ctx, store, spaceID, attempt.ID)
	if persistedAttempt.PublicationDisposition != "publication_failed" {
		t.Fatalf("publication disposition=%q", persistedAttempt.PublicationDisposition)
	}
	persistedRun, err := store.GetRun(ctx, domainID, run.ID)
	if err != nil || persistedRun.Status != "failed" || persistedRun.Error != publicationRecoveryFailure {
		t.Fatalf("persisted run=%+v err=%v", persistedRun, err)
	}
}

func TestPublicationDiagnosticsUseClosedCategories(t *testing.T) {
	var output bytes.Buffer
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	run := automation.Run{
		ID: "run", InferenceAttemptIDs: []string{"attempt"}, ClaimOwnerNodeID: 2, ClaimVersion: 3,
		ClaimToken: "secret-claim-token", OutputIdempotencyKey: "secret-output-key", ActorPrincipalID: "secret-principal",
		AuthorizationRevision: 4, GlobalAuthorizationRevision: 5, SpaceAuthorizationRevision: 6,
	}
	raw := errors.New("secret provider body")
	mgr.logPublicationStageAtRevision(run, "graph_commit", "failed", publicationFailureCategory(raw), 7)
	logged := output.String()
	for _, required := range []string{
		`"category":"operation_failed"`, `"claim_owner_node_id":2`, `"claim_version":3`,
		`"authorization_revision":4`, `"global_authorization_revision":5`, `"space_authorization_revision":6`,
		`"output_key_hash":"` + publicationOutputKeyHash(run.OutputIdempotencyKey) + `"`, `"graph_revision":7`,
	} {
		if !strings.Contains(logged, required) {
			t.Fatalf("publication diagnostic missing %s: %s", required, logged)
		}
	}
	for _, prohibited := range []string{raw.Error(), run.ClaimToken, run.OutputIdempotencyKey, run.ActorPrincipalID, "provider", "principal"} {
		if strings.Contains(logged, prohibited) {
			t.Fatalf("publication diagnostic leaked %q: %s", prohibited, logged)
		}
	}
}

func TestDispositionPersistenceDiagnosticsCoverEveryPostProviderBranch(t *testing.T) {
	run := automation.Run{ID: "run", DomainID: graph.DomainID(uuid.New()), InferenceAttemptIDs: []string{"attempt"}}
	for _, disposition := range []string{
		"not_published_pre_provider_failure",
		"not_published_provider_failure",
		"not_published_provider_ambiguous",
		"suppressed_procedure_control",
		"rejected_invalid_output",
		"publication_precondition_failed",
		"publication_pending",
		"suppressed_authorization_or_control",
		"not_changed_or_duplicate",
		"published",
	} {
		t.Run(disposition, func(t *testing.T) {
			var output bytes.Buffer
			mgr := NewManager(storage.NewFileStore(t.TempDir())).WithLogger(slog.New(slog.NewJSONHandler(&output, nil)))
			if err := mgr.recordRunPublicationDispositionWithDiagnostics(context.Background(), run, disposition); !errors.Is(err, ErrAutomationNotFound) {
				t.Fatalf("missing attempt disposition error=%v", err)
			}
			logged := output.String()
			for _, required := range []string{`"stage":"` + publicationDispositionStage(disposition) + `"`, `"result":"started"`, `"result":"failed"`, `"category":"not_found"`} {
				if !strings.Contains(logged, required) {
					t.Fatalf("disposition diagnostic missing %s: %s", required, logged)
				}
			}
			if strings.Contains(logged, "secret") {
				t.Fatalf("disposition diagnostic leaked unsafe text: %s", logged)
			}
		})
	}

	t.Run("completed", func(t *testing.T) {
		var output bytes.Buffer
		store := storage.NewFileStore(t.TempDir())
		attempt := automation.ProcedureInferenceAttempt{ID: "attempt", RecordVersion: 1, SpaceID: "space", DomainID: run.DomainID, ProcedureID: "procedure", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, InvocationID: "invocation", RunID: run.ID, RunAttemptNumber: 1, ProviderAttemptNumber: 1, State: automation.InferenceAttemptStateSucceeded, RequestedAt: time.Now().UTC()}
		if err := store.PutProcedureInferenceLedger(context.Background(), automation.ProcedureInferenceLedger{SpaceID: "space", Attempts: []automation.ProcedureInferenceAttempt{attempt}}); err != nil {
			t.Fatal(err)
		}
		mgr := NewManager(store).WithLogger(slog.New(slog.NewJSONHandler(&output, nil)))
		if err := mgr.recordRunPublicationDispositionWithDiagnostics(context.Background(), run, "publication_pending"); err != nil {
			t.Fatal(err)
		}
		logged := output.String()
		for _, required := range []string{`"stage":"disposition_publication_pending"`, `"result":"started"`, `"result":"completed"`, `"category":"none"`} {
			if !strings.Contains(logged, required) {
				t.Fatalf("completion diagnostic missing %s: %s", required, logged)
			}
		}
	})
}

func TestRecoveryPersistenceStagesObserveSanitizedFailures(t *testing.T) {
	for _, stage := range []string{"recovery_disposition_persistence", "recovery_receipt_backfill_persistence", "recovery_run_persistence", "recovery_invocation_persistence"} {
		t.Run(stage, func(t *testing.T) {
			var output bytes.Buffer
			mgr := NewManager(storage.NewFileStore(t.TempDir())).WithLogger(slog.New(slog.NewJSONHandler(&output, nil)))
			rawErr := status.Error(codes.Unavailable, "secret infrastructure detail")
			err := mgr.runPublicationPersistenceStage(automation.Run{ID: "run", InferenceAttemptIDs: []string{"attempt"}}, stage, func() error { return rawErr })
			if !errors.Is(err, rawErr) {
				t.Fatalf("stage error=%v", err)
			}
			logged := output.String()
			for _, required := range []string{`"stage":"` + stage + `"`, `"result":"started"`, `"result":"failed"`, `"category":"unavailable"`} {
				if !strings.Contains(logged, required) {
					t.Fatalf("diagnostic missing %s: %s", required, logged)
				}
			}
			if strings.Contains(logged, "secret infrastructure detail") {
				t.Fatalf("raw persistence error leaked: %s", logged)
			}
		})
	}
}

func TestInferenceFailurePublicationDispositionPreservesProviderBoundary(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected string
	}{
		{name: "pre provider", err: errors.New("routing unavailable"), expected: "not_published_pre_provider_failure"},
		{name: "provider started", err: markInferenceGenerationError(errors.New("provider failed"), true, false), expected: "not_published_provider_failure"},
		{name: "ambiguous", err: markInferenceGenerationError(ErrInferenceAttemptAlreadyStarted, true, true), expected: "not_published_provider_ambiguous"},
		{name: "terminal persistence ambiguous", err: markInferenceGenerationError(errors.New("persistence unavailable"), true, true), expected: "not_published_provider_ambiguous"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if actual := inferenceFailurePublicationDisposition(tc.err); actual != tc.expected {
				t.Fatalf("disposition=%q, want %q", actual, tc.expected)
			}
		})
	}
}

func TestCommittedSpaceAuthorizationRevisionPreservesReadFailures(t *testing.T) {
	readErr := status.Error(codes.Unavailable, "storage unavailable")
	if err := validateCommittedSpaceAuthorizationRevision(0, readErr, 7); !errors.Is(err, readErr) {
		t.Fatalf("read failure=%v, want original %v", err, readErr)
	}
	if err := validateCommittedSpaceAuthorizationRevision(6, nil, 7); !errors.Is(err, ErrProcedureAuthorizationRevoked) {
		t.Fatalf("mismatch error=%v", err)
	}
	if err := validateCommittedSpaceAuthorizationRevision(7, nil, 7); err != nil {
		t.Fatalf("matching revision error=%v", err)
	}
}

type failingAuthorizationResolver struct{ err error }

func (f failingAuthorizationResolver) Invoke(context.Context, inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error) {
	return inferenceruntime.InvokeResponse{}, f.err
}

func (f failingAuthorizationResolver) Resolve(context.Context, inferenceruntime.ResolveRequest) (inferenceruntime.ResolveResult, error) {
	return inferenceruntime.ResolveResult{}, f.err
}

func TestAuthorizationRevalidationPreservesResolverInfrastructureFailure(t *testing.T) {
	infrastructureErr := status.Error(codes.Unavailable, "projection unavailable")
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithInferenceInvoker(failingAuthorizationResolver{err: infrastructureErr})
	domainID := graph.DomainID(uuid.New())
	run := automation.Run{ID: "run"}
	err := mgr.revalidateProcedureAuthorization(context.Background(), automation.Definition{DomainID: domainID}, automation.Invocation{ID: "invocation", SpaceID: uuid.NewString(), DomainID: domainID, ProcedureID: "procedure", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1}, &run)
	if !errors.Is(err, infrastructureErr) || errors.Is(err, ErrProcedureAuthorizationRevoked) {
		t.Fatalf("revalidation error=%v, want preserved infrastructure failure", err)
	}
	if category := publicationFailureCategory(err); category != "unavailable" {
		t.Fatalf("category=%q, want unavailable", category)
	}
}

func TestGlobalAuthorizationAssertionErrorsPreserveInfrastructureCategories(t *testing.T) {
	cases := []struct {
		err      error
		expected error
		category string
	}{
		{err: status.Error(codes.FailedPrecondition, "revision changed"), expected: ErrProcedureAuthorizationRevoked, category: "authorization_revoked"},
		{err: status.Error(codes.Unavailable, "no leader"), expected: nil, category: "unavailable"},
		{err: context.DeadlineExceeded, expected: nil, category: "deadline_exceeded"},
		{err: context.Canceled, expected: nil, category: "canceled"},
	}
	for _, tc := range cases {
		mapped := mapGlobalAuthorizationAssertionError(tc.err)
		if tc.expected != nil && !errors.Is(mapped, tc.expected) {
			t.Fatalf("mapped error=%v, want %v", mapped, tc.expected)
		}
		if tc.expected == nil && !errors.Is(mapped, tc.err) {
			t.Fatalf("mapped error=%v, want original %v", mapped, tc.err)
		}
		if actual := publicationFailureCategory(mapped); actual != tc.category {
			t.Fatalf("category=%q, want %q", actual, tc.category)
		}
	}
}

func TestPublicationFailureCategoriesAreClosed(t *testing.T) {
	cases := []struct {
		err      error
		expected string
	}{
		{err: nil, expected: "none"},
		{err: context.DeadlineExceeded, expected: "deadline_exceeded"},
		{err: context.Canceled, expected: "canceled"},
		{err: ErrProcedureDisabled, expected: "procedure_disabled"},
		{err: ErrProcedureAuthorizationRevoked, expected: "authorization_revoked"},
		{err: graphchange.ErrConflict, expected: "graph_conflict"},
		{err: status.Error(codes.FailedPrecondition, "unsafe detail"), expected: "failed_precondition"},
		{err: status.Error(codes.Unavailable, "unsafe detail"), expected: "unavailable"},
		{err: status.Error(codes.PermissionDenied, "unsafe detail"), expected: "permission_denied"},
		{err: errors.New("unsafe detail"), expected: "operation_failed"},
	}
	for _, tc := range cases {
		if actual := publicationFailureCategory(tc.err); actual != tc.expected {
			t.Fatalf("publicationFailureCategory(%v)=%q, want %q", tc.err, actual, tc.expected)
		}
	}
}

func persistSucceededPublicationAttempt(t *testing.T, ctx context.Context, mgr *AutomationManager, inv automation.Invocation, run automation.Run, disposition string) automation.ProcedureInferenceAttempt {
	t.Helper()
	now := time.Now().UTC()
	attempt := newRequestedProcedureInferenceAttempt(inv, run, "chat", "profile", now)
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.RecordVersion++
	attempt.State = automation.InferenceAttemptStateAuthorized
	attempt.AuthorizedAt = now
	attempt.AuthorizationRevision = 1
	attempt.GlobalAuthorizationRevision = 1
	attempt.SpaceAuthorizationRevision = 1
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.RecordVersion++
	attempt.State = automation.InferenceAttemptStateStarted
	attempt.StartedAt = now
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.RecordVersion++
	attempt.State = automation.InferenceAttemptStateSucceeded
	attempt.OutcomeCategory = "succeeded"
	attempt.CompletedAt = now
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.RecordVersion++
	attempt.PublicationDisposition = disposition
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	return attempt
}

func requireProcedureInferenceAttempt(t *testing.T, ctx context.Context, store storage.Store, spaceID, attemptID string) automation.ProcedureInferenceAttempt {
	t.Helper()
	ledger, err := store.GetProcedureInferenceLedger(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range ledger.Attempts {
		if attempt.ID == attemptID {
			return attempt
		}
	}
	t.Fatalf("attempt %s not found", attemptID)
	return automation.ProcedureInferenceAttempt{}
}
