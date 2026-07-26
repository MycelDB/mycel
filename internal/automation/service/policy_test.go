package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestPolicyRequiresApproval(t *testing.T) {
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	domainID := graph.DomainID(uuid.New())
	if _, err := mgr.PutPolicy(context.Background(), automation.Policy{DomainID: domainID, RequireApproval: true}); err != nil {
		t.Fatal(err)
	}
	def := automation.Definition{ID: "wf", DomainID: domainID, Status: automation.StatusEnabled, Trigger: automation.Trigger{Events: []string{automation.EventNodeCreated}}, Workflow: &automation.Workflow{Steps: []automation.WorkflowStep{{ID: "act", Kind: automation.WorkflowStepAction}}}}
	if err := mgr.enforcePolicy(context.Background(), def); err == nil {
		t.Fatal("expected approval policy error")
	}
	def.Workflow.Steps[0].Approval = "required"
	if err := mgr.enforcePolicy(context.Background(), def); err != nil {
		t.Fatal(err)
	}
}
