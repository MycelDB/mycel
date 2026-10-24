package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestProcedureUpdateRejectsConflictingSameVersion(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	base := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Name: "base"}
	if err := store.PutProcedure(ctx, base); err != nil {
		t.Fatal(err)
	}
	first := base
	first.Version = 2
	first.Name = "first"
	second := base
	second.Version = 2
	second.Name = "second"
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, candidate := range []automation.Procedure{first, second} {
		candidate := candidate
		go func() {
			<-start
			errs <- mgr.applyAutomationMutation(ctx, automationMutationRecord{Kind: "procedure.update", DomainID: domainID, ID: candidate.ID, Payload: rawAutomation(candidate)})
		}()
	}
	close(start)
	succeeded := 0
	for i := 0; i < 2; i++ {
		if <-errs == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful conflicting procedure updates=%d, want 1", succeeded)
	}
	got, err := store.GetProcedure(ctx, domainID, base.ID)
	if err != nil || (got.Name != "first" && got.Name != "second") || got.Version != 2 {
		t.Fatalf("procedure overwritten: %+v err=%v", got, err)
	}
}

func TestBindingUpdateRejectsConflictingSameVersion(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	base := automation.Binding{ID: "binding", Version: 1, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, Name: "base"}
	if err := store.PutBinding(ctx, base); err != nil {
		t.Fatal(err)
	}
	first := base
	first.Version = 2
	first.Name = "first"
	second := base
	second.Version = 2
	second.Name = "second"
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, candidate := range []automation.Binding{first, second} {
		candidate := candidate
		go func() {
			<-start
			errs <- mgr.applyAutomationMutation(ctx, automationMutationRecord{Kind: "binding.update", DomainID: domainID, ID: candidate.ID, Payload: rawAutomation(candidate)})
		}()
	}
	close(start)
	succeeded := 0
	for i := 0; i < 2; i++ {
		if <-errs == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful conflicting binding updates=%d, want 1", succeeded)
	}
	got, err := store.GetBinding(ctx, domainID, base.ID)
	if err != nil || (got.Name != "first" && got.Name != "second") || got.Version != 2 {
		t.Fatalf("binding overwritten: %+v err=%v", got, err)
	}
}
