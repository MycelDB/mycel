package service

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingAutomationFenceValidator struct {
	calls []AutomationOutputFenceValidation
	err   error
}

func (v *recordingAutomationFenceValidator) ValidateAutomationOutputFence(ctx context.Context, validation AutomationOutputFenceValidation) error {
	v.calls = append(v.calls, validation)
	return v.err
}

func TestGraphRaftApplyValidatesAutomationOutputFence(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	validator := &recordingAutomationFenceValidator{err: status.Error(codes.FailedPrecondition, "stale automation claim")}
	m.SetAutomationOutputFenceValidator(validator)
	spaceID := uuid.NewString()
	domainID := uuid.New()
	nodeID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Content: "stale output", Meta: map[string]any{"automation": map[string]any{"automation_id": "page-summary", "binding_id": "binding-a", "run_id": "run-a", "invocation_id": "inv-a", "claim_owner_node_id": uint64(2), "claim_version": uint64(7), "claim_token": "old-token", "output_idempotency_key": "output-key"}}}}, OperationCount: 1}
	cmd, err := m.buildGraphCommitRaftCommand(record, 64, "automation-output-stale")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ApplyCommand() error = %v, code=%v; want FailedPrecondition", err, status.Code(err))
	}
	if len(validator.calls) != 1 {
		t.Fatalf("validator calls=%d want 1", len(validator.calls))
	}
	call := validator.calls[0]
	if call.SpaceID != spaceID || call.DomainID != domainID || call.EntityKind != "node" || call.EntityID != nodeID.String() || call.InvocationID != "inv-a" || call.ClaimOwnerNodeID != 2 || call.ClaimVersion != 7 || call.ClaimToken != "old-token" || call.OutputIdempotencyKey != "output-key" {
		t.Fatalf("unexpected validation call: %#v", call)
	}
	if rev, err := m.CurrentRevision(ctx, spaceID); err != nil || rev != 0 {
		t.Fatalf("CurrentRevision() = %d, %v; want 0, nil", rev, err)
	}
}

func TestGraphRaftApplyFailsClosedForIncompleteAutomationOutputFence(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutEdges: []domaingraph.Edge{{ID: uuid.New(), DomainID: domainID, FromID: uuid.New(), ToID: uuid.New(), Meta: map[string]any{"automation": map[string]any{"invocation_id": "inv-a", "output_idempotency_key": "output-key"}}}}, OperationCount: 1}
	cmd, err := m.buildGraphCommitRaftCommand(record, 64, "automation-output-incomplete")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	err = (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ApplyCommand() error = %v, code=%v; want FailedPrecondition", err, status.Code(err))
	}
	if rev, err := m.CurrentRevision(ctx, spaceID); err != nil || rev != 0 {
		t.Fatalf("CurrentRevision() = %d, %v; want 0, nil", rev, err)
	}
}
