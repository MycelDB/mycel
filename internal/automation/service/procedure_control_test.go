package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
)

func TestProcedureControlPreservesExecutionVersionAndFencesStarts(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	procedure := automation.Procedure{ID: "proc", Version: 7, DomainID: domainID, Status: automation.StatusEnabled, Prompt: "immutable prompt"}
	binding := automation.Binding{ID: "binding", Version: 3, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: spaceID, DomainID: domainID}}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	if err := store.PutBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}

	disabled, barrier, err := mgr.SetProcedureStatusAs(ctx, spaceID, domainID, procedure.ID, automation.StatusDisabled, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if disabled.Version != 7 || disabled.Prompt != procedure.Prompt || disabled.ControlEpoch != 1 || disabled.Status != automation.StatusDisabled {
		t.Fatalf("disabled procedure=%+v", disabled)
	}
	if !barrier.Completed || barrier.ControlEpoch != 1 || !reflect.DeepEqual(barrier.AffectedPartitions, []uint32{0}) || !reflect.DeepEqual(barrier.AcknowledgedPartitions, []uint32{0}) {
		t.Fatalf("disable barrier=%+v", barrier)
	}
	invocation := automation.Invocation{DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version}
	if err := mgr.requireProcedureEnabledRuntime(ctx, invocation); !errors.Is(err, ErrProcedureDisabled) {
		t.Fatalf("disabled start fence error=%v", err)
	}
	fence, err := store.GetProcedureControlFence(ctx, 0, domainID, procedure.ID)
	if err != nil || fence.ControlEpoch != 1 || fence.TargetStatus != automation.StatusDisabled {
		t.Fatalf("persisted disable fence=%+v err=%v", fence, err)
	}
	if err := store.PutSpaceAuthorizationRevision(ctx, storage.SpaceAuthorizationRevision{SpaceID: spaceID, Revision: 9}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := mgr.snapshotAutomationPartition(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	restoredStore := storage.NewFileStore(t.TempDir())
	restored := NewManager(restoredStore)
	if err := restored.restoreAutomationPartition(ctx, snapshot, 0, 1); err != nil {
		t.Fatal(err)
	}
	if restoredFence, err := restoredStore.GetProcedureControlFence(ctx, 0, domainID, procedure.ID); err != nil || restoredFence.ControlEpoch != 1 {
		t.Fatalf("restored fence=%+v err=%v", restoredFence, err)
	}
	if restoredBarrier, err := restoredStore.GetProcedureControlBarrier(ctx, domainID, procedure.ID); err != nil || !restoredBarrier.Completed || restoredBarrier.ControlEpoch != 1 {
		t.Fatalf("restored barrier=%+v err=%v", restoredBarrier, err)
	}
	if revision, err := restoredStore.GetSpaceAuthorizationRevision(ctx, spaceID); err != nil || revision.Revision != 9 {
		t.Fatalf("restored authorization revision=%+v err=%v", revision, err)
	}

	enabled, barrier, err := mgr.SetProcedureStatusAs(ctx, spaceID, domainID, procedure.ID, automation.StatusEnabled, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if enabled.Version != 7 || enabled.ControlEpoch != 2 || enabled.Status != automation.StatusEnabled || !barrier.Completed || barrier.ControlEpoch != 2 {
		t.Fatalf("enable procedure=%+v barrier=%+v", enabled, barrier)
	}
	if err := mgr.requireProcedureEnabledRuntime(ctx, invocation); err != nil {
		t.Fatalf("enabled start fence: %v", err)
	}
}

func TestProcedureControlResumesDurableIncompleteBarrier(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	committedAt := time.Unix(100, 0).UTC()
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusDisabled, ControlEpoch: 1, ControlledBy: "operator", ControlledAt: committedAt}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	pending := automation.ProcedureControlBarrier{SpaceID: spaceID, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: 1, TargetStatus: automation.StatusDisabled, ControlEpoch: 1, ControllingPrincipalID: "operator", AffectedPartitions: []uint32{0}, CommittedAt: committedAt}
	if err := store.PutProcedureControlBarrier(ctx, pending); err != nil {
		t.Fatal(err)
	}
	controlled, completed, err := mgr.SetProcedureStatusAs(ctx, spaceID, domainID, procedure.ID, automation.StatusDisabled, "different-retry-principal")
	if err != nil {
		t.Fatal(err)
	}
	if controlled.ControlEpoch != 1 || !completed.Completed || completed.ControlEpoch != 1 || completed.ControllingPrincipalID != "operator" || !reflect.DeepEqual(completed.AcknowledgedPartitions, []uint32{0}) {
		t.Fatalf("resumed control=%+v barrier=%+v", controlled, completed)
	}
}

func TestProcedureUpdatePreservesControlStateAndFencedIdentityCannotBeRecreated(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	created, err := mgr.CreateProcedureAs(ctx, domainID, `{"id":"proc","version":1,"status":"enabled","input":{"target":"changed","fields":["payload.text"]},"inference":{"operation":"chat","profile":"profile"},"prompt":"one","output":{"mode":"text","actions":[{"update_node":{"target":"changed","set":{"payload.summary":"$result.text"}}}]}}`, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mgr.SetProcedureStatusAs(ctx, spaceID, domainID, created.ID, automation.StatusDisabled, "operator"); err != nil {
		t.Fatal(err)
	}
	updated, err := mgr.UpdateProcedureAs(ctx, domainID, created.ID, `{"id":"proc","version":99,"status":"enabled","control_epoch":99,"input":{"target":"changed","fields":["payload.text"]},"inference":{"operation":"chat","profile":"profile"},"prompt":"two","output":{"mode":"text","actions":[{"update_node":{"target":"changed","set":{"payload.summary":"$result.text"}}}]}}`, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != automation.StatusDisabled || updated.ControlEpoch != 1 || updated.ControlledBy != "operator" {
		t.Fatalf("updated control state=%+v", updated)
	}
	if err := mgr.DeleteProcedure(ctx, domainID, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CreateProcedureAs(ctx, domainID, `{"id":"proc","version":1,"status":"enabled","input":{"target":"changed","fields":["payload.text"]},"inference":{"operation":"chat","profile":"profile"},"prompt":"new","output":{"mode":"text","actions":[{"update_node":{"target":"changed","set":{"payload.summary":"$result.text"}}}]}}`, "owner"); err == nil {
		t.Fatal("recreated fenced procedure identity")
	}
}

func TestIncompleteControlBarrierRejectsProcedureMutationAndIdentityReuse(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	barrier := automation.ProcedureControlBarrier{DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: 1, TargetStatus: automation.StatusDisabled, ControlEpoch: 1, AffectedPartitions: []uint32{0}}
	if err := store.PutProcedureControlBarrier(ctx, barrier); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpdateProcedureAs(ctx, domainID, procedure.ID, `{"id":"proc","version":1,"status":"enabled","input":{"target":"changed","fields":["payload.text"]},"inference":{"operation":"chat","profile":"profile"},"prompt":"changed","output":{"mode":"text","actions":[{"update_node":{"target":"changed","set":{"payload.summary":"$result.text"}}}]}}`, "owner"); err == nil {
		t.Fatal("updated procedure during incomplete barrier")
	}
	if err := mgr.DeleteProcedure(ctx, domainID, procedure.ID); err == nil {
		t.Fatal("deleted procedure during incomplete barrier")
	}
	if err := store.DeleteProcedure(ctx, domainID, procedure.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CreateProcedureAs(ctx, domainID, `{"id":"proc","version":1,"status":"enabled","input":{"target":"changed","fields":["payload.text"]},"inference":{"operation":"chat","profile":"profile"},"prompt":"new","output":{"mode":"text","actions":[{"update_node":{"target":"changed","set":{"payload.summary":"$result.text"}}}]}}`, "owner"); err == nil {
		t.Fatal("recreated identity with retained incomplete barrier")
	}
}

func TestEnablingFenceRemainsClosedUntilRelease(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	invocation := automation.Invocation{SpaceID: spaceID, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1}
	if err := store.PutProcedureControlFence(ctx, automation.ProcedureControlFence{PartitionID: 0, SpaceID: spaceID, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, TargetStatus: "enabling", ControlEpoch: 2}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.requireProcedureEnabledRuntime(ctx, invocation); !errors.Is(err, ErrProcedureDisabled) {
		t.Fatalf("enabling fence error=%v", err)
	}
}

func TestAppliedDisableFenceAtomicallyRejectsProviderStart(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	invocation := automation.Invocation{ID: "inv", SpaceID: spaceID, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1}
	run := automation.Run{ID: "run", DomainID: domainID, InvocationID: invocation.ID, ProcedureID: invocation.ProcedureID, ProcedureVersion: 1, BindingID: invocation.BindingID, BindingVersion: 1, AttemptNumber: 1}
	attempt := newRequestedProcedureInferenceAttempt(invocation, run, "chat", "profile", mgr.now())
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt, err := mgr.authorizeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.ResolveResult{Allowed: true, Decision: domaininference.PolicyDecision{AuthorizationRevision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutProcedureControlFence(ctx, automation.ProcedureControlFence{PartitionID: 0, SpaceID: spaceID, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, TargetStatus: automation.StatusDisabled, ControlEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.startProcedureInferenceAttempt(ctx, attempt); !errors.Is(err, ErrProcedureDisabled) {
		t.Fatalf("provider start error=%v", err)
	}
}

func TestProcedureControlApplyRejectsImmutableMutation(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	original := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Prompt: "original"}
	if err := store.PutProcedure(ctx, original); err != nil {
		t.Fatal(err)
	}
	mutated := original
	mutated.Status = automation.StatusDisabled
	mutated.Prompt = "changed"
	mutated.ControlEpoch = 1
	if err := mgr.applyAutomationMutation(ctx, automationMutationRecord{Kind: "procedure.control", DomainID: domainID, ID: original.ID, Payload: rawAutomation(mutated)}); err == nil {
		t.Fatal("expected immutable procedure configuration mutation to be rejected")
	}
}
