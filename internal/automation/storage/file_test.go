package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

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
