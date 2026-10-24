package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type outputFenceRevisionInference struct{ revision uint64 }

func (f *outputFenceRevisionInference) Invoke(context.Context, inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error) {
	return inferenceruntime.InvokeResponse{}, nil
}

func (f *outputFenceRevisionInference) CurrentAuthorizationRevisions(context.Context, string) (uint64, uint64, error) {
	return 1, f.revision, nil
}

func (f *outputFenceRevisionInference) AssertGlobalAuthorizationRevision(context.Context, uint64) error {
	return nil
}

func TestValidateAutomationOutputFenceAcceptsCurrentClaim(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID, ProcedureID: "proc", Status: "running", ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "current-token", ClaimExpiresAt: time.Now().Add(time.Minute)}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatalf("PutInvocation() error = %v", err)
	}
	validation := graph.AutomationOutputFenceValidation{SpaceID: spaceID, DomainID: domainID, EntityKind: "node", EntityID: uuid.NewString(), InvocationID: inv.ID, ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, OutputIdempotencyKey: "output-key", ProcedureID: "proc", AuthorizationRevision: 1}
	if err := mgr.ValidateAutomationOutputFence(ctx, validation); err != nil {
		t.Fatalf("ValidateAutomationOutputFence() error = %v", err)
	}
}

func TestValidateAutomationOutputFenceRejectsChangedAuthorizationRevisionInCluster(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	revisions := &outputFenceRevisionInference{revision: 12}
	mgr := NewManager(store).WithInferenceInvoker(revisions)
	mgr.raftGroups = &consensus.MultiGroup{}
	mgr.raftPartitionCount = 1
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID, ProcedureID: "proc", Status: "running", ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "current-token"}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	if err := store.PutProcedure(ctx, automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSpaceAuthorizationRevision(ctx, storage.SpaceAuthorizationRevision{SpaceID: spaceID, Revision: 12}); err != nil {
		t.Fatal(err)
	}
	validation := graph.AutomationOutputFenceValidation{SpaceID: spaceID, DomainID: domainID, InvocationID: inv.ID, ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "current-token", OutputIdempotencyKey: "output-key", ProcedureID: "proc", AuthorizationRevision: 11, GlobalAuthorizationRevision: 1, SpaceAuthorizationRevision: 11}
	if err := mgr.ValidateAutomationOutputFence(ctx, validation); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("authorization revision fence error=%v", err)
	}
	validation.AuthorizationRevision = 12
	validation.SpaceAuthorizationRevision = 12
	if err := mgr.ValidateAutomationOutputFence(ctx, validation); err != nil {
		t.Fatalf("matching authorization revision error=%v", err)
	}
}

func TestValidateAutomationOutputFenceRejectsStaleClaim(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID, Status: "running", ClaimOwnerNodeID: 2, ClaimVersion: 8, ClaimToken: "new-token", ClaimExpiresAt: time.Now().Add(time.Minute)}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatalf("PutInvocation() error = %v", err)
	}
	validation := graph.AutomationOutputFenceValidation{SpaceID: spaceID, DomainID: domainID, EntityKind: "node", EntityID: uuid.NewString(), InvocationID: inv.ID, ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "old-token", OutputIdempotencyKey: "output-key"}
	if err := mgr.ValidateAutomationOutputFence(ctx, validation); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ValidateAutomationOutputFence() error = %v, code=%v; want FailedPrecondition", err, status.Code(err))
	}
}

func TestValidateAutomationOutputFenceRejectsTerminalInvocation(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID, Status: "succeeded", ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "current-token"}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatalf("PutInvocation() error = %v", err)
	}
	validation := graph.AutomationOutputFenceValidation{SpaceID: spaceID, DomainID: domainID, EntityKind: "node", EntityID: uuid.NewString(), InvocationID: inv.ID, ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "current-token", OutputIdempotencyKey: "output-key"}
	if err := mgr.ValidateAutomationOutputFence(ctx, validation); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ValidateAutomationOutputFence() error = %v, code=%v; want FailedPrecondition", err, status.Code(err))
	}
}
