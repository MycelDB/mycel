package service

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ValidateAutomationOutputFence lets the graph Raft state machine validate that
// an automation-generated output commit is still fenced by the current
// Raft-owned invocation claim. This method intentionally avoids wall-clock lease
// checks so graph state-machine apply remains deterministic across replicas;
// workers check and renew lease expiry before proposing graph output commits.
func (m *AutomationManager) ValidateAutomationOutputFence(ctx context.Context, validation graphservice.AutomationOutputFenceValidation) error {
	if m == nil || m.store == nil {
		return status.Error(codes.FailedPrecondition, "automation output fencing state is not configured")
	}
	if strings.TrimSpace(validation.InvocationID) == "" {
		return status.Error(codes.FailedPrecondition, "automation output fence is missing invocation_id")
	}
	if validation.DomainID == uuid.Nil {
		return status.Error(codes.FailedPrecondition, "automation output fence is missing domain_id")
	}
	current, err := m.store.GetInvocation(ctx, validation.DomainID, validation.InvocationID)
	if err != nil {
		if err == storage.ErrNotFound {
			return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: invocation not found", validation.InvocationID)
		}
		return mapStoreError(err)
	}
	if strings.TrimSpace(validation.SpaceID) != "" && strings.TrimSpace(current.SpaceID) != strings.TrimSpace(validation.SpaceID) {
		return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: space mismatch", validation.InvocationID)
	}
	if current.Status != "running" {
		return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: status %q is not running", validation.InvocationID, current.Status)
	}
	if current.ClaimOwnerNodeID == 0 || current.ClaimVersion == 0 || strings.TrimSpace(current.ClaimToken) == "" {
		return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: current claim is incomplete", validation.InvocationID)
	}
	if current.ClaimOwnerNodeID != validation.ClaimOwnerNodeID || current.ClaimVersion != validation.ClaimVersion || strings.TrimSpace(current.ClaimToken) != strings.TrimSpace(validation.ClaimToken) {
		return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: stale claim", validation.InvocationID)
	}
	if strings.TrimSpace(validation.OutputIdempotencyKey) == "" {
		return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: output idempotency key is required", validation.InvocationID)
	}
	return nil
}
