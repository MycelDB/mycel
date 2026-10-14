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

type automationFenceTestHarness struct {
	ctx       context.Context
	module    *Module
	validator *recordingAutomationFenceValidator
}

func newAutomationFenceTestHarness(t *testing.T, validatorErr error) automationFenceTestHarness {
	t.Helper()
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	validator := &recordingAutomationFenceValidator{err: validatorErr}
	m.SetAutomationOutputFenceValidator(validator)
	return automationFenceTestHarness{ctx: ctx, module: m, validator: validator}
}

func (h automationFenceTestHarness) applyRecord(t *testing.T, record graphCommitRecord, reason string) error {
	t.Helper()
	cmd, err := h.module.buildGraphCommitRaftCommand(record, 64, reason)
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	return (RaftStateMachine{Module: h.module, PartitionCount: 64}).ApplyCommand(h.ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd)
}

func TestGraphRaftApplyValidatesAutomationOutputFence(t *testing.T) {
	h := newAutomationFenceTestHarness(t, status.Error(codes.FailedPrecondition, "stale automation claim"))
	spaceID := uuid.NewString()
	domainID := uuid.New()
	nodeID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Content: "stale output", Meta: map[string]any{"automation": map[string]any{"automation_id": "page-summary", "binding_id": "binding-a", "run_id": "run-a", "invocation_id": "inv-a", "claim_owner_node_id": uint64(2), "claim_version": uint64(7), "claim_token": "old-token", "output_idempotency_key": "output-key"}}}}, AutomationFenceNodeIDs: []domaingraph.NodeID{nodeID}, OperationCount: 1}
	if err := h.applyRecord(t, record, "automation-output-stale"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ApplyCommand() error = %v, code=%v; want FailedPrecondition", err, status.Code(err))
	}
	if len(h.validator.calls) != 1 {
		t.Fatalf("validator calls=%d want 1", len(h.validator.calls))
	}
	call := h.validator.calls[0]
	if call.SpaceID != spaceID || call.DomainID != domainID || call.EntityKind != "node" || call.EntityID != nodeID.String() || call.InvocationID != "inv-a" || call.ClaimOwnerNodeID != 2 || call.ClaimVersion != 7 || call.ClaimToken != "old-token" || call.OutputIdempotencyKey != "output-key" {
		t.Fatalf("unexpected validation call: %#v", call)
	}
	if rev, err := h.module.CurrentRevision(h.ctx, spaceID); err != nil || rev != 0 {
		t.Fatalf("CurrentRevision() = %d, %v; want 0, nil", rev, err)
	}
}

func TestGraphRaftApplyIgnoresPersistedAutomationMetaWithoutExplicitFence(t *testing.T) {
	h := newAutomationFenceTestHarness(t, status.Error(codes.FailedPrecondition, "stale automation claim"))
	spaceID := uuid.NewString()
	domainID := uuid.New()
	nodeID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Content: "ordinary update preserving old meta", Meta: map[string]any{"automation": map[string]any{"invocation_id": "old-invocation", "claim_owner_node_id": uint64(2), "claim_version": uint64(7), "claim_token": "old-token", "output_idempotency_key": "old-output"}}}}, OperationCount: 1}
	if err := h.applyRecord(t, record, "ordinary-update-with-stale-automation-meta"); err != nil {
		t.Fatalf("ApplyCommand() error = %v, want stale meta ignored without explicit fence", err)
	}
	if len(h.validator.calls) != 0 {
		t.Fatalf("validator calls=%d want 0", len(h.validator.calls))
	}
	store, err := h.module.store(h.ctx, spaceID, domainID.String())
	if err != nil {
		t.Fatalf("store() error = %v", err)
	}
	got, err := store.GetNode(h.ctx, nodeID)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if got.ID != nodeID || got.Meta["automation"] == nil {
		t.Fatalf("stored node=%#v, want ordinary write with preserved durable meta", got)
	}
}

func TestGraphRaftApplyFailsClosedForIncompleteAutomationOutputFence(t *testing.T) {
	h := newAutomationFenceTestHarness(t, nil)
	spaceID := uuid.NewString()
	domainID := uuid.New()
	edgeID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutEdges: []domaingraph.Edge{{ID: edgeID, DomainID: domainID, FromID: uuid.New(), ToID: uuid.New(), Meta: map[string]any{"automation": map[string]any{"invocation_id": "inv-a", "output_idempotency_key": "output-key"}}}}, AutomationFenceEdgeIDs: []domaingraph.EdgeID{edgeID}, OperationCount: 1}
	if err := h.applyRecord(t, record, "automation-output-incomplete"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ApplyCommand() error = %v, code=%v; want FailedPrecondition", err, status.Code(err))
	}
	if rev, err := h.module.CurrentRevision(h.ctx, spaceID); err != nil || rev != 0 {
		t.Fatalf("CurrentRevision() = %d, %v; want 0, nil", rev, err)
	}
}
