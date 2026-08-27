package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestGraphProcedureAndBindingWritesUseRaftWhenLocalWritesRejected(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithWriteAllowed(func() error {
		return errors.New("clustered local write rejected: raft executor is not configured for this subsystem")
	})
	mg, stop := startAutomationRaftForTest(t, ctx, mgr, 8)
	defer stop()
	mgr.EnableExperimentalRaft(mg, 8)

	procedure := automation.Procedure{ID: "knot-pkm.page-summary", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	procedureJSON := mustAutomationJSON(t, procedure)
	createdProcedure, err := mgr.CreateProcedureAs(ctx, domainID, procedureJSON, "operator")
	if err != nil {
		if strings.Contains(err.Error(), "raft executor is not configured for this subsystem") {
			t.Fatalf("procedure create used rejected local write path: %v", err)
		}
		t.Fatalf("CreateProcedureAs() error = %v", err)
	}
	if createdProcedure.ID != procedure.ID || createdProcedure.CreatedByPrincipalID != "operator" {
		t.Fatalf("unexpected procedure: %+v", createdProcedure)
	}
	listedProcedures, err := mgr.ListProcedures(ctx, domainID, "")
	if err != nil || len(listedProcedures) != 1 || listedProcedures[0].ID != procedure.ID {
		t.Fatalf("ListProcedures() = %+v err=%v", listedProcedures, err)
	}

	binding := automation.Binding{ID: "user.page-summary", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "operator", OnBehalfOfPrincipalID: "operator", InferenceProfile: "summary"}}
	createdBinding, err := mgr.CreateBindingAs(ctx, domainID, mustAutomationJSON(t, binding), "operator")
	if err != nil {
		if strings.Contains(err.Error(), "raft executor is not configured for this subsystem") {
			t.Fatalf("binding create used rejected local write path: %v", err)
		}
		t.Fatalf("CreateBindingAs() error = %v", err)
	}
	if createdBinding.ID != binding.ID || createdBinding.CreatedByPrincipalID != "operator" {
		t.Fatalf("unexpected binding: %+v", createdBinding)
	}
	listedBindings, err := mgr.ListBindings(ctx, domainID, "")
	if err != nil || len(listedBindings) != 1 || listedBindings[0].ID != binding.ID {
		t.Fatalf("ListBindings() = %+v err=%v", listedBindings, err)
	}
}

func TestAutomationRaftReplayCreateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	mgr.raftPartitionCount = 64
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText}}
	rec := automationMutationRecord{Kind: "procedure.create", DomainID: domainID, ID: procedure.ID, Payload: rawAutomation(procedure)}
	cmd, err := mgr.buildAutomationRaftCommand(rec)
	if err != nil {
		t.Fatalf("buildAutomationRaftCommand() error = %v", err)
	}
	if err := mgr.applyAutomationRaftCommand(ctx, cmd, 64); err != nil {
		t.Fatalf("first apply error = %v", err)
	}
	if err := mgr.applyAutomationRaftCommand(ctx, cmd, 64); err != nil {
		t.Fatalf("replay apply error = %v", err)
	}
	procedures, err := mgr.ListProcedures(ctx, domainID, "")
	if err != nil || len(procedures) != 1 || procedures[0].ID != procedure.ID {
		t.Fatalf("ListProcedures() = %+v err=%v", procedures, err)
	}
}

func TestAutomationRaftReplayStaleGraphReplayCursorIsNoOp(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.New().String()
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	mgr.raftPartitionCount = 64
	existing := storage.GraphReplayCursor{SpaceID: spaceID, DomainID: domainID, Revision: 10, UpdatedAt: "2026-08-27T15:00:00Z"}
	if err := mgr.store.PutGraphReplayCursor(ctx, existing); err != nil {
		t.Fatalf("seed graph replay cursor: %v", err)
	}
	stale := storage.GraphReplayCursor{SpaceID: spaceID, DomainID: domainID, Revision: 7, UpdatedAt: "2026-08-27T14:00:00Z"}
	cmd, err := mgr.buildAutomationRaftCommand(automationMutationRecord{Kind: "graph_replay_cursor.upsert", DomainID: domainID, SpaceID: spaceID, ID: domainID.String(), Payload: rawAutomation(stale)})
	if err != nil {
		t.Fatalf("buildAutomationRaftCommand() error = %v", err)
	}
	if err := mgr.applyAutomationRaftCommand(ctx, cmd, 64); err != nil {
		t.Fatalf("stale cursor replay apply error = %v", err)
	}
	got, err := mgr.store.GetGraphReplayCursor(ctx, spaceID, domainID)
	if err != nil {
		t.Fatalf("GetGraphReplayCursor() error = %v", err)
	}
	if got.Revision != existing.Revision || got.UpdatedAt != existing.UpdatedAt {
		t.Fatalf("cursor changed after stale replay: %+v want %+v", got, existing)
	}
}

func TestAutomationRaftReplayStaleScheduleCheckpointIsNoOp(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.New().String()
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	mgr.raftPartitionCount = 64
	existing := storage.ScheduleCheckpoint{DomainID: domainID, SpaceID: spaceID, AutomationID: "binding-a", LastRunAt: "2026-08-27T15:00:00Z", UpdatedAt: "2026-08-27T15:00:01Z"}
	if err := mgr.store.PutScheduleCheckpoint(ctx, existing); err != nil {
		t.Fatalf("seed schedule checkpoint: %v", err)
	}
	stale := storage.ScheduleCheckpoint{DomainID: domainID, SpaceID: spaceID, AutomationID: existing.AutomationID, LastRunAt: "2026-08-27T14:00:00Z", UpdatedAt: "2026-08-27T14:00:01Z"}
	cmd, err := mgr.buildAutomationRaftCommand(automationMutationRecord{Kind: "schedule_checkpoint.upsert", DomainID: domainID, SpaceID: spaceID, ID: stale.AutomationID, Payload: rawAutomation(stale)})
	if err != nil {
		t.Fatalf("buildAutomationRaftCommand() error = %v", err)
	}
	if err := mgr.applyAutomationRaftCommand(ctx, cmd, 64); err != nil {
		t.Fatalf("stale checkpoint replay apply error = %v", err)
	}
	got, err := mgr.store.GetScheduleCheckpoint(ctx, domainID, existing.AutomationID)
	if err != nil {
		t.Fatalf("GetScheduleCheckpoint() error = %v", err)
	}
	if got.LastRunAt != existing.LastRunAt || got.UpdatedAt != existing.UpdatedAt {
		t.Fatalf("checkpoint changed after stale replay: %+v want %+v", got, existing)
	}
}

func TestAutomationRaftCreateRejectsConflictingExistingContent(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	mgr.raftPartitionCount = 64
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText}}
	if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
		t.Fatalf("seed procedure: %v", err)
	}
	conflict := procedure
	conflict.Prompt = "Different prompt"
	cmd, err := mgr.buildAutomationRaftCommand(automationMutationRecord{Kind: "procedure.create", DomainID: domainID, ID: conflict.ID, Payload: rawAutomation(conflict)})
	if err != nil {
		t.Fatalf("buildAutomationRaftCommand() error = %v", err)
	}
	if err := mgr.applyAutomationRaftCommand(ctx, cmd, 64); err == nil || !strings.Contains(err.Error(), "different content") {
		t.Fatalf("expected conflicting create to fail, got %v", err)
	}
}

func TestAutomationSnapshotRestoreValidatesBeforeDelete(t *testing.T) {
	ctx := context.Background()
	partitionCount := uint32(8)
	domainID := graph.DomainID(uuid.New())
	partitionID := automationPartitionForTest(t, domainID, partitionCount)
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	procedure := automation.Procedure{ID: "kept", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText}}
	if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
		t.Fatalf("seed procedure: %v", err)
	}
	wrongDomain := graph.DomainID(uuid.New())
	for automationPartitionForTest(t, wrongDomain, partitionCount) == partitionID {
		wrongDomain = graph.DomainID(uuid.New())
	}
	bad, err := json.Marshal(automationSnapshot{Version: 1, Procedures: []automation.Procedure{{ID: "wrong", Version: 1, DomainID: wrongDomain}}})
	if err != nil {
		t.Fatalf("marshal bad snapshot: %v", err)
	}
	if err := mgr.restoreAutomationPartition(ctx, bad, partitionID, partitionCount); err == nil {
		t.Fatal("expected restore to reject wrong-partition snapshot")
	}
	procedures, err := mgr.ListProcedures(ctx, domainID, "")
	if err != nil || len(procedures) != 1 || procedures[0].ID != procedure.ID {
		t.Fatalf("restore deleted existing procedure before validation: %+v err=%v", procedures, err)
	}
}

func TestAutomationRaftModeIgnoresLegacyDefinitionsForRuntime(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	mgr.raftPartitionCount = 64
	mgr.raftGroups = &consensus.MultiGroup{}
	legacy := automation.Definition{ID: "legacy", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Trigger: automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText}}
	if err := store.PutDefinition(ctx, legacy.Normalize()); err != nil {
		t.Fatalf("seed legacy definition: %v", err)
	}
	items, err := mgr.listRunnableAutomations(ctx, domainID, automation.StatusEnabled)
	if err != nil {
		t.Fatalf("listRunnableAutomations() error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("raft mode should ignore legacy definitions, got %+v", items)
	}
	if _, err := mgr.resolveInvocationAutomation(ctx, domainID, automation.Invocation{AutomationID: legacy.ID}); !errors.Is(err, ErrAutomationNotFound) {
		t.Fatalf("resolve legacy invocation error = %v, want ErrAutomationNotFound", err)
	}
}

func TestAutomationRaftReplicatesProcedureAndBindingAcrossManagers(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	partitionCount := uint32(8)
	router := consensus.NewLocalMessageRouter()
	managers := map[consensus.NodeID]*AutomationManager{}
	peers := []consensus.NodeID{1, 2, 3}
	for _, id := range peers {
		managers[id] = NewManager(storage.NewFileStore(t.TempDir())).WithWriteAllowed(func() error {
			return errors.New("clustered local write rejected: raft executor is not configured for this subsystem")
		})
	}
	groups := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, id := range peers {
		localID := id
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: localID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(partitionID uint32) consensus.StateMachine {
			return RaftStateMachine{Manager: managers[localID], PartitionID: partitionID, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d): %v", id, err)
		}
		groups[id] = mg
		defer mg.Stop()
		for _, group := range mg.Groups() {
			router.Register(group)
		}
		managers[id].EnableExperimentalRaft(mg, partitionCount)
	}
	stopTick := startRaftTicker(t, groups)
	defer stopTick()
	waitForAutomationRaftLeaders(t, groups, partitionCount)

	procedure := automation.Procedure{ID: "knot-pkm.page-summary", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	if _, err := managers[1].CreateProcedureAs(ctx, domainID, mustAutomationJSON(t, procedure), "operator"); err != nil {
		t.Fatalf("CreateProcedureAs() error = %v", err)
	}
	binding := automation.Binding{ID: "user.page-summary", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "operator", OnBehalfOfPrincipalID: "operator", InferenceProfile: "summary"}}
	if _, err := managers[1].CreateBindingAs(ctx, domainID, mustAutomationJSON(t, binding), "operator"); err != nil {
		t.Fatalf("CreateBindingAs() error = %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, mgr := range managers {
			procedures, _ := mgr.ListProcedures(ctx, domainID, "")
			bindings, _ := mgr.ListBindings(ctx, domainID, "")
			if len(procedures) != 1 || procedures[0].ID != procedure.ID || len(bindings) != 1 || bindings[0].ID != binding.ID {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("automation resources did not replicate to all managers: %v", err)
	}
}

func startAutomationRaftForTest(t *testing.T, ctx context.Context, mgr *AutomationManager, partitionCount uint32) (*consensus.MultiGroup, func()) {
	t.Helper()
	router := consensus.NewLocalMessageRouter()
	mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: partitionCount, Transport: consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, nodeID == 1 })}, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(partitionID uint32) consensus.StateMachine {
		return RaftStateMachine{Manager: mgr, PartitionID: partitionID, PartitionCount: partitionCount}
	}}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup(): %v", err)
	}
	for _, group := range mg.Groups() {
		router.Register(group)
	}
	groups := map[consensus.NodeID]*consensus.MultiGroup{1: mg}
	stopTick := startRaftTicker(t, groups)
	waitForAutomationRaftLeaders(t, groups, partitionCount)
	return mg, func() { stopTick(); mg.Stop() }
}

func startRaftTicker(t *testing.T, groups map[consensus.NodeID]*consensus.MultiGroup) func() {
	t.Helper()
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for _, mg := range groups {
					mg.Tick()
				}
			}
		}
	}()
	return func() { close(stop) }
}

func waitForAutomationRaftLeaders(t *testing.T, groups map[consensus.NodeID]*consensus.MultiGroup, partitionCount uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stable := 0
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		leaders := map[uint32]consensus.NodeID{}
		for _, mg := range groups {
			for _, status := range mg.Status() {
				if status.PartitionID == nil {
					continue
				}
				if status.Leader == 0 {
					stable = 0
					return false
				}
				if current, ok := leaders[*status.PartitionID]; ok && current != status.Leader {
					stable = 0
					return false
				}
				leaders[*status.PartitionID] = status.Leader
			}
		}
		if len(leaders) != int(partitionCount) {
			stable = 0
			return false
		}
		stable++
		return stable >= 3
	}); err != nil {
		t.Fatalf("raft leaders not elected for %d partitions: %v", partitionCount, err)
	}
}

func automationPartitionForTest(t *testing.T, domainID graph.DomainID, partitionCount uint32) uint32 {
	t.Helper()
	for partitionID := uint32(0); partitionID < partitionCount; partitionID++ {
		if automationDomainInPartition(domainID, partitionID, partitionCount) {
			return partitionID
		}
	}
	t.Fatalf("domain %s did not map to any automation partition", domainID)
	return 0
}

func mustAutomationJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal automation json: %v", err)
	}
	return string(b)
}
