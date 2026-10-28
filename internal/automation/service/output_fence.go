package service

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ValidateAutomationOutputFence lets the graph Raft state machine validate that
// an automation-generated output commit is still fenced by the current
// Raft-owned invocation claim. This method intentionally avoids wall-clock lease
// checks so graph state-machine apply remains deterministic across replicas;
// workers check and renew lease expiry before proposing graph output commits.
func (m *AutomationManager) ApplySpaceAuthorizationRevision(ctx context.Context, value string, revision uint64) error {
	spaceID := strings.TrimSpace(value)
	if spaceID == "" || revision == 0 {
		return status.Error(codes.FailedPrecondition, "space authorization revision is incomplete")
	}
	return mapStoreError(m.store.PutSpaceAuthorizationRevision(ctx, storage.SpaceAuthorizationRevision{SpaceID: spaceID, Revision: revision}))
}

func (m *AutomationManager) currentCommittedSpaceAuthorizationRevision(ctx context.Context, spaceID string) (uint64, error) {
	revision, err := m.store.GetSpaceAuthorizationRevision(ctx, spaceID)
	if err != nil {
		return 0, mapStoreError(err)
	}
	return revision.Revision, nil
}

func (m *AutomationManager) ValidateAutomationOutputFence(ctx context.Context, validation graph.AutomationOutputFenceValidation) error {
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
	if strings.TrimSpace(validation.ProcedureID) != strings.TrimSpace(current.ProcedureID) || validation.AuthorizationRevision == 0 {
		return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: procedure authorization evidence is incomplete", validation.InvocationID)
	}
	if err := m.procedureFenceAllows(ctx, current.SpaceID, current.DomainID, current.ProcedureID); err != nil {
		if errors.Is(err, ErrProcedureDisabled) {
			return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: procedure is disabled", validation.InvocationID)
		}
		return err
	}
	if m.raftGroups != nil {
		if validation.GlobalAuthorizationRevision == 0 {
			return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: global authorization assertion is missing", validation.InvocationID)
		}
		revision, err := m.currentCommittedSpaceAuthorizationRevision(ctx, current.SpaceID)
		if err := validateCommittedSpaceAuthorizationRevision(revision, err, validation.SpaceAuthorizationRevision); err != nil {
			if errors.Is(err, ErrProcedureAuthorizationRevoked) {
				return status.Errorf(codes.FailedPrecondition, "automation output fence rejected for invocation %s: authorization revision changed", validation.InvocationID)
			}
			return err
		}
	}
	return nil
}

func validateCommittedSpaceAuthorizationRevision(current uint64, readErr error, expected uint64) error {
	if readErr != nil {
		return readErr
	}
	if current == 0 || expected == 0 || current != expected {
		return ErrProcedureAuthorizationRevoked
	}
	return nil
}
