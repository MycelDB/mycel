package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateAutomationOutputFenceAcceptsCurrentClaim(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID, Status: "running", ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "current-token", ClaimExpiresAt: time.Now().Add(time.Minute)}
	if err := store.PutInvocation(ctx, inv); err != nil {
		t.Fatalf("PutInvocation() error = %v", err)
	}
	validation := graphservice.AutomationOutputFenceValidation{SpaceID: spaceID, DomainID: domainID, EntityKind: "node", EntityID: uuid.NewString(), InvocationID: inv.ID, ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, OutputIdempotencyKey: "output-key"}
	if err := mgr.ValidateAutomationOutputFence(ctx, validation); err != nil {
		t.Fatalf("ValidateAutomationOutputFence() error = %v", err)
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
	validation := graphservice.AutomationOutputFenceValidation{SpaceID: spaceID, DomainID: domainID, EntityKind: "node", EntityID: uuid.NewString(), InvocationID: inv.ID, ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "old-token", OutputIdempotencyKey: "output-key"}
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
	validation := graphservice.AutomationOutputFenceValidation{SpaceID: spaceID, DomainID: domainID, EntityKind: "node", EntityID: uuid.NewString(), InvocationID: inv.ID, ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "current-token", OutputIdempotencyKey: "output-key"}
	if err := mgr.ValidateAutomationOutputFence(ctx, validation); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ValidateAutomationOutputFence() error = %v, code=%v; want FailedPrecondition", err, status.Code(err))
	}
}
