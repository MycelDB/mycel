package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestFileStoreProcedureAndBindingRoundTrip(t *testing.T) {
	store := NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	ctx := context.Background()

	procedure := automation.Procedure{ID: "proc", DomainID: domainID, Version: 1, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"properties.summary": "$result.text"}}}}}}
	binding := automation.Binding{ID: "binding", DomainID: domainID, ProcedureID: procedure.ID, Version: 1, Status: automation.StatusEnabled, Scope: automation.BindingScope{DomainID: domainID}, Trigger: automation.BindingTrigger{Events: []string{automation.EventNodeUpdated}}, Runtime: automation.RuntimeContext{ActorPrincipalID: "automation", OwnerPrincipalID: "user", OnBehalfOfPrincipalID: "user"}}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatalf("put procedure: %v", err)
	}
	if err := store.PutBinding(ctx, binding); err != nil {
		t.Fatalf("put binding: %v", err)
	}
	gotProcedure, err := store.GetProcedure(ctx, domainID, procedure.ID)
	if err != nil || gotProcedure.ID != procedure.ID {
		t.Fatalf("get procedure = %+v err=%v", gotProcedure, err)
	}
	gotBinding, err := store.GetBinding(ctx, domainID, binding.ID)
	if err != nil || gotBinding.ID != binding.ID || gotBinding.Runtime.OnBehalfOfPrincipalID != "user" {
		t.Fatalf("get binding = %+v err=%v", gotBinding, err)
	}
	procedures, err := store.ListProcedures(ctx, domainID)
	if err != nil || len(procedures) != 1 || procedures[0].ID != procedure.ID {
		t.Fatalf("list procedures = %+v err=%v", procedures, err)
	}
	bindings, err := store.ListBindings(ctx, domainID)
	if err != nil || len(bindings) != 1 || bindings[0].ID != binding.ID {
		t.Fatalf("list bindings = %+v err=%v", bindings, err)
	}
	bindingDomains, err := store.ListBindingDomains(ctx)
	if err != nil || len(bindingDomains) != 1 || bindingDomains[0] != domainID {
		t.Fatalf("list binding domains = %+v err=%v", bindingDomains, err)
	}
}

func TestFileStorePutInvocationDeduplicatesStableIDAcrossCreatedAtDays(t *testing.T) {
	store := NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	ctx := context.Background()
	inv := automation.Invocation{ID: "inv-stable", DomainID: domainID, Status: "pending", CreatedAt: time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatalf("put first invocation: %v", err)
	}
	inv.Status = "running"
	inv.CreatedAt = inv.CreatedAt.Add(24 * time.Hour)
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatalf("put moved invocation: %v", err)
	}
	items, err := store.ListInvocations(ctx, domainID, InvocationFilter{})
	if err != nil || len(items) != 1 || items[0].Status != "running" {
		t.Fatalf("ListInvocations() = %+v err=%v, want one updated invocation", items, err)
	}
}

func TestFileStoreGetRunAcceptsInvocationID(t *testing.T) {
	store := NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	invocationID := uuid.NewString()
	oldRunID := uuid.NewString()
	latestRunID := uuid.NewString()
	ctx := context.Background()

	oldRun := automation.Run{ID: oldRunID, DomainID: domainID, InvocationID: invocationID, AttemptNumber: 1, Status: "failed", StartedAt: time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)}
	latestRun := automation.Run{ID: latestRunID, DomainID: domainID, InvocationID: invocationID, AttemptNumber: 2, Status: "succeeded", StartedAt: time.Date(2026, 8, 18, 11, 0, 0, 0, time.UTC)}
	for _, run := range []automation.Run{oldRun, latestRun} {
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("put run %q: %v", run.ID, err)
		}
	}

	byRunID, err := store.GetRun(ctx, domainID, oldRunID)
	if err != nil {
		t.Fatalf("get by run id: %v", err)
	}
	if byRunID.ID != oldRunID {
		t.Fatalf("get by run id returned %q, want %q", byRunID.ID, oldRunID)
	}

	byInvocationID, err := store.GetRun(ctx, domainID, invocationID)
	if err != nil {
		t.Fatalf("get by invocation id: %v", err)
	}
	if byInvocationID.ID != latestRunID {
		t.Fatalf("get by invocation id returned %q, want latest run %q", byInvocationID.ID, latestRunID)
	}
}
