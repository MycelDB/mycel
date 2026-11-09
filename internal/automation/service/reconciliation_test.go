package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/render"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestStartReconciliationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "target", Mode: automation.InputModeTemplate, Template: "{{target.payload.text}}"}}
	binding := automation.Binding{ID: "binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{DomainID: domainID, SpaceID: "space"}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}}, Reconciliation: &automation.ReconciliationConfig{Target: "target", Discovery: automation.ReconciliationDiscovery{GQL: "MATCH (target:Page) RETURN target FETCH FIRST 10 ROWS ONLY"}}}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	if err := store.PutBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(store)
	first, err := manager.StartReconciliation(ctx, "principal", "space", domainID, binding.ID, "key", 10, 1)
	if err != nil {
		t.Fatalf("StartReconciliation() error = %v", err)
	}
	second, err := manager.StartReconciliation(ctx, "principal", "space", domainID, binding.ID, "key", 10, 1)
	if err != nil {
		t.Fatalf("idempotent StartReconciliation() error = %v", err)
	}
	if first.ID != second.ID || second.Status != automation.ReconciliationStatusQueued {
		t.Fatalf("idempotency result = %+v then %+v", first, second)
	}
	if first.RequestedByPrincipalID != "principal" {
		t.Fatalf("requested-by principal = %q, want persisted caller", first.RequestedByPrincipalID)
	}
}

func TestCancelReconciliationWaitsForActiveInvocations(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	binding := reconciliationTestBinding(domainID, "space")
	procedure := reconciliationTestProcedure(domainID)
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	if err := store.PutBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(store)
	operation, err := manager.StartReconciliation(ctx, "principal", "space", domainID, binding.ID, "key", 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	invocation := automation.Invocation{ID: "active", DomainID: domainID, SpaceID: "space", EventID: reconciliationEventPrefix + operation.ID + ":node", Status: "running"}
	if err := store.PutInvocation(ctx, invocation); err != nil {
		t.Fatal(err)
	}
	canceled, err := manager.CancelReconciliation(ctx, "space", operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !canceled.CancelRequested || canceled.Status != automation.ReconciliationStatusRunning {
		t.Fatalf("cancel response = %+v", canceled)
	}
	if _, err := manager.ProcessReconciliations(ctx, domainID, 1); err != nil {
		t.Fatal(err)
	}
	waiting, err := manager.GetReconciliation(ctx, "space", operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Status != automation.ReconciliationStatusRunning || !waiting.CompletedAt.IsZero() {
		t.Fatalf("operation completed while invocation was active: %+v", waiting)
	}
	invocation.Status = "succeeded"
	if err := store.PutInvocation(ctx, invocation); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ProcessReconciliations(ctx, domainID, 1); err != nil {
		t.Fatal(err)
	}
	finished, err := manager.GetReconciliation(ctx, "space", operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != automation.ReconciliationStatusCanceled || finished.CompletedCount != 1 {
		t.Fatalf("finished operation = %+v", finished)
	}
}

func TestReconciliationFreshInputSkipsBeforeInference(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	nodeID := uuid.New()
	node := graph.Node{ID: nodeID, DomainID: domainID, Payload: map[string]any{"text": "same"}}
	def := automation.Definition{ID: "binding", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Mode: automation.InputModeTemplate, Template: "{{changed.payload.text}}"}, Prompt: "unused"}.Normalize()
	invocation := automation.Invocation{ID: "reconciliation-invocation", DomainID: domainID, SpaceID: "space", BindingID: "binding", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, ChangedElementID: nodeID.String(), EventID: reconciliationEventPrefix + "operation:" + nodeID.String(), Status: "running"}
	rendered, err := render.Render(def.Input, render.Context{Changed: node})
	if err != nil {
		t.Fatal(err)
	}
	node.Meta = map[string]any{"automation": map[string]any{"input_hash": generationInputFingerprint(def, invocation, rendered.Hash, "")}}
	manager := NewManager(store).WithGraphRuntime(automationE2ESessions{spaceID: "space", domainID: domainID.String()}, &automationE2EGraph{node: node})
	run, err := manager.executeInvocation(ctx, def, invocation)
	if err != nil {
		t.Fatalf("executeInvocation() error = %v", err)
	}
	if run.Status != "skipped" || run.Error != reconciliationFreshReason {
		t.Fatalf("fresh target run = %+v", run)
	}
}

func TestReconciliationProgressCountsFreshSkipsAndFailures(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	operation := automation.ReconciliationOperation{ID: "operation", SpaceID: "space", DomainID: domainID, Status: automation.ReconciliationStatusRunning, DiscoveryComplete: true}
	if err := store.PutReconciliationOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	invocations := []automation.Invocation{
		{ID: "fresh", DomainID: domainID, EventID: reconciliationEventPrefix + operation.ID + ":fresh", Status: "skipped", SkipReason: reconciliationFreshReason},
		{ID: "failed", DomainID: domainID, EventID: reconciliationEventPrefix + operation.ID + ":failed", Status: "failed"},
	}
	for _, invocation := range invocations {
		if err := store.PutInvocation(ctx, invocation); err != nil {
			t.Fatal(err)
		}
	}
	manager := NewManager(store)
	if err := manager.refreshReconciliationProgress(ctx, operation); err != nil {
		t.Fatal(err)
	}
	updated, err := manager.GetReconciliation(ctx, "space", operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != automation.ReconciliationStatusCompletedFailures || updated.CompletedCount != 1 || updated.FreshSkippedCount != 1 || updated.FailedCount != 1 || updated.FailureCode != "RECONCILIATION_TARGET_FAILURES" {
		t.Fatalf("progress = %+v", updated)
	}
}

func reconciliationTestProcedure(domainID graph.DomainID) automation.Procedure {
	return automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "target", Mode: automation.InputModeTemplate, Template: "{{target.payload.text}}"}}
}

func reconciliationTestBinding(domainID graph.DomainID, spaceID string) automation.Binding {
	return automation.Binding{ID: "binding", Version: 1, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, Status: automation.StatusEnabled, Scope: automation.BindingScope{DomainID: domainID, SpaceID: spaceID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}}, Reconciliation: &automation.ReconciliationConfig{Target: "target", Discovery: automation.ReconciliationDiscovery{GQL: "MATCH (target:Page) RETURN target FETCH FIRST 10 ROWS ONLY"}}}
}

func TestProcessReconciliationQueuesBoundedTargets(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "target", Mode: automation.InputModeTemplate, Template: "{{target.payload.text}}"}}
	binding := automation.Binding{ID: "binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{DomainID: domainID, SpaceID: spaceID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}}, Reconciliation: &automation.ReconciliationConfig{Target: "target", Discovery: automation.ReconciliationDiscovery{GQL: "MATCH (target:Page) RETURN target FETCH FIRST 1 ROWS ONLY"}}}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	if err := store.PutBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	firstID := uuid.New()
	secondID := uuid.New()
	graphs := &automationE2EGraph{nodes: map[string]graph.Node{
		firstID.String():  {ID: firstID, DomainID: domainID, Labels: []string{"Page"}},
		secondID.String(): {ID: secondID, DomainID: domainID, Labels: []string{"Page"}},
	}}
	manager := NewManager(store).WithGraphRuntime(automationE2ESessions{spaceID: spaceID, domainID: domainID.String()}, graphs)
	operation, err := manager.StartReconciliation(ctx, "principal", spaceID, domainID, binding.ID, "key", 10, 1)
	if err != nil {
		t.Fatalf("StartReconciliation() error = %v", err)
	}
	if _, err := manager.ProcessReconciliations(ctx, domainID, 1); err != nil {
		t.Fatalf("ProcessReconciliations() error = %v", err)
	}
	invocations, err := store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(invocations) != 1 || invocations[0].EventID == "" {
		t.Fatalf("invocations = %+v, want one bounded reconciliation invocation", invocations)
	}
	updated, err := manager.GetReconciliation(ctx, spaceID, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != automation.ReconciliationStatusRunning || !updated.DiscoveryComplete || updated.ScheduledCount != 1 {
		t.Fatalf("updated operation = %+v", updated)
	}
	updated.DiscoveryComplete = false
	updated.Cursor = ""
	if err := store.PutReconciliationOperation(ctx, updated); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ProcessReconciliations(ctx, domainID, 1); err != nil {
		t.Fatalf("replayed ProcessReconciliations() error = %v", err)
	}
	invocations, err = store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(invocations) != 1 {
		t.Fatalf("replayed invocations = %+v, want one idempotent invocation", invocations)
	}
	replayed, err := manager.GetReconciliation(ctx, spaceID, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ScheduledCount != 1 {
		t.Fatalf("replayed operation = %+v, want scheduled_count unchanged", replayed)
	}
}
