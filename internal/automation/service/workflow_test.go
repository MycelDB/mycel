package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestStartWorkflowInstanceCreatesInitialStepRuns(t *testing.T) {
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	def := automation.Definition{ID: "wf", Version: 1, Workflow: &automation.Workflow{Steps: []automation.WorkflowStep{{ID: "first", Kind: automation.WorkflowStepLLM}, {ID: "second", Kind: automation.WorkflowStepAction, DependsOn: []string{"first"}}}}}
	inv := automation.Invocation{ID: "inv", DomainID: domainID, AutomationID: "wf", AutomationVersion: 1, ChangedElementID: "node"}
	inst, err := mgr.startWorkflowInstance(context.Background(), def, inv)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != "pending" || inst.AutomationID != "wf" {
		t.Fatalf("instance = %+v", inst)
	}
	runs, err := store.ListWorkflowStepRuns(context.Background(), domainID, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].StepID != "first" {
		t.Fatalf("runs = %+v", runs)
	}
}

func TestRunnableWorkflowSteps(t *testing.T) {
	wf := automation.Workflow{Steps: []automation.WorkflowStep{{ID: "a"}, {ID: "b", DependsOn: []string{"a"}}, {ID: "c", DependsOn: []string{"b"}}}}
	got := runnableWorkflowSteps(wf, map[string]bool{"a": true})
	if len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("got %+v", got)
	}
	_ = time.Now()
}
