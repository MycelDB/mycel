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
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestReconcileProviderStartedPublicationFinalizesPublishedOutputWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv", SpaceID: spaceID, DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "procedure", ProcedureVersion: 1, Status: "retryable", ClaimVersion: 1, ClaimToken: "old-claim", AttemptCount: 0, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	run := automation.Run{ID: automationRunID(inv, 1), DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1, ActorPrincipalID: automationActor}
	outputKey := automationOutputIdempotencyKey(inv, run.ID)
	graphs := &automationE2EGraph{node: graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Meta: map[string]any{"automation": map[string]any{"output_idempotency_key": outputKey}}}}
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
	if persistedAttempt.PublicationDisposition != "published" {
		t.Fatalf("publication disposition=%q", persistedAttempt.PublicationDisposition)
	}
	persistedRun, err := store.GetRun(ctx, domainID, run.ID)
	if err != nil || persistedRun.Status != "succeeded" || len(persistedRun.InferenceAttemptIDs) != 1 || persistedRun.InferenceAttemptIDs[0] != attempt.ID {
		t.Fatalf("persisted run=%+v err=%v", persistedRun, err)
	}
	persistedInvocation, err := store.GetInvocation(ctx, domainID, inv.ID)
	if err != nil || persistedInvocation.Status != "succeeded" || persistedInvocation.AttemptCount != 1 {
		t.Fatalf("persisted invocation=%+v err=%v", persistedInvocation, err)
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
	run := automation.Run{ID: "run", InferenceAttemptIDs: []string{"attempt"}}
	raw := errors.New("secret provider body")
	mgr.logPublicationStage(run, "graph_commit", "failed", publicationFailureCategory(raw))
	logged := output.String()
	if !strings.Contains(logged, `"category":"operation_failed"`) || strings.Contains(logged, raw.Error()) {
		t.Fatalf("unsafe publication diagnostic: %s", logged)
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
