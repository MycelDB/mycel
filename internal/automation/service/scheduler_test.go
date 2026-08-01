package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProcessScheduledFailsClosedWhenWritesDisallowed(t *testing.T) {
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithWriteAllowed(func() error {
		return status.Error(codes.Unavailable, "local writes disabled")
	})
	_, err := mgr.ProcessScheduled(context.Background(), graph.DomainID(uuid.New()), 10)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ProcessScheduled() error = %v, want Unavailable", err)
	}
}

func TestProcessScheduledCreatesInvocation(t *testing.T) {
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	def := automation.Definition{ID: "scheduled", DomainID: domainID, Version: 1, Status: automation.StatusEnabled, Trigger: automation.Trigger{Schedule: &automation.ScheduleTrigger{Interval: "1h"}}, Workflow: &automation.Workflow{Steps: []automation.WorkflowStep{{ID: "step", Kind: automation.WorkflowStepTool, Tool: "debug.echo"}}}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := store.PutDefinition(context.Background(), def); err != nil {
		t.Fatal(err)
	}
	count, err := mgr.ProcessScheduled(context.Background(), domainID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count=%d", count)
	}
	items, err := store.ListInvocations(context.Background(), domainID, storage.InvocationFilter{Status: "pending"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].EventType != "schedule" {
		t.Fatalf("items=%+v", items)
	}
}
