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

func TestRecordSuccessfulInputHashForDuplicateOutputSkip(t *testing.T) {
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv", DomainID: domainID, SpaceID: uuid.NewString(), AutomationID: "a", AutomationVersion: 1, ChangedElementID: "node-1", InputHash: "hash"}
	def := automation.Definition{ID: "a", Version: 1}
	if err := mgr.recordSuccessfulInputHash(context.Background(), def, inv, automation.Run{ID: "run-1", Status: "skipped", Error: skipReasonDuplicateOutput}); err != nil {
		t.Fatal(err)
	}
	dup, err := mgr.hasSuccessfulInputHash(context.Background(), inv, "hash", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if !dup {
		t.Fatal("expected duplicate-output skip to repair successful-input index")
	}
}

func TestHasSuccessfulInputHash(t *testing.T) {
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	now := time.Now().UTC()
	prior := automation.Invocation{ID: "prior", DomainID: domainID, AutomationID: "a", AutomationVersion: 2, ChangedElementID: "node-1", InputHash: "hash", Status: "succeeded", CreatedAt: now}
	if err := store.PutInvocation(context.Background(), prior); err != nil {
		t.Fatal(err)
	}
	def := automation.Definition{ID: "a", Version: 2}
	if err := mgr.recordSuccessfulInputHash(context.Background(), def, prior, automation.Run{ID: "run-1", Status: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	dup, err := mgr.hasSuccessfulInputHash(context.Background(), automation.Invocation{ID: "current", DomainID: domainID, AutomationID: "a", AutomationVersion: 2, ChangedElementID: "node-1"}, "hash", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if !dup {
		t.Fatal("expected duplicate")
	}
	dup, err = mgr.hasSuccessfulInputHash(context.Background(), automation.Invocation{ID: "current", DomainID: domainID, AutomationID: "a", AutomationVersion: 3, ChangedElementID: "node-1"}, "hash", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if dup {
		t.Fatal("did not expect duplicate across versions")
	}
}
