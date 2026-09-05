package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/actions"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clustermodel "github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	inferenceconnectors "github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAutomationExecutionRaftReplicatesInvocationClaimRunAndIdempotency(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	leader := cluster.managers[leaderID]
	seedDisabledProcedureBindingOnAll(t, ctx, cluster.managers, domainID, spaceID, "binding-a")

	inv := invocationForRunnable(leader.now, domainID, runnableAutomation{Procedure: automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusDisabled}, Binding: automation.Binding{ID: "binding-a", Version: 1, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, Scope: automation.BindingScope{SpaceID: spaceID.String(), DomainID: domainID}}}, automation.Invocation{ID: graphTriggeredInvocationID(spaceID.String(), domainID, "event-a", "binding-a", "node-a"), SpaceID: spaceID.String(), EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated}, "operator")
	if err := leader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("putInvocationRuntime() error = %v", err)
	}
	cluster.waitForInvocation(t, domainID, inv.ID)

	for id, mgr := range cluster.managers {
		processed, err := mgr.ProcessPending(ctx, domainID, 10)
		if id == leaderID {
			if err != nil || processed != 1 {
				t.Fatalf("leader ProcessPending() processed=%d err=%v", processed, err)
			}
			continue
		}
		if err != nil || processed != 0 {
			t.Fatalf("follower %d ProcessPending() processed=%d err=%v, want no-op", id, processed, err)
		}
	}
	cluster.waitForInvocationStatus(t, domainID, inv.ID, "skipped")
	cluster.waitForRun(t, domainID, inv.ID)
	for id, mgr := range cluster.managers {
		got, err := mgr.store.GetInvocation(ctx, domainID, inv.ID)
		if err != nil {
			t.Fatalf("node %d get invocation: %v", id, err)
		}
		if got.ClaimOwnerNodeID != uint64(leaderID) || got.ClaimToken == "" || got.ClaimVersion == 0 {
			t.Fatalf("node %d missing replicated claim: %+v", id, got)
		}
		run, err := mgr.store.GetRun(ctx, domainID, inv.ID)
		if err != nil {
			t.Fatalf("node %d get run by invocation: %v", id, err)
		}
		if run.InvocationID != inv.ID || run.ClaimToken != got.ClaimToken || run.OutputIdempotencyKey == "" {
			t.Fatalf("node %d missing replicated run fencing: run=%+v inv=%+v", id, run, got)
		}
	}

	index := storage.SuccessfulInputIndex{DomainID: domainID, AutomationID: "binding-a", Version: 1, ChangedElementID: "node-a", InputHash: "hash-a", InvocationID: inv.ID, RunID: inv.ID}
	if err := leader.putSuccessfulInputRuntime(ctx, spaceID.String(), index); err != nil {
		t.Fatalf("putSuccessfulInputRuntime() error = %v", err)
	}
	cluster.waitForSuccessfulInput(t, domainID, index)
}

func TestProcessPendingRaftDebounceWaitDoesNotHoldRunningClaim(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	leader := cluster.managers[leaderID]
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	leader.now = func() time.Time { return base }
	for _, mgr := range cluster.managers {
		mgr.now = leader.now
	}
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText}, Safety: automation.Safety{Debounce: &automation.Debounce{Duration: "30s", CoalesceBy: "changed"}}}
	binding := automation.Binding{ID: "binding-a", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: spaceID.String(), DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "owner", OnBehalfOfPrincipalID: "owner", InferenceProfile: "summary"}}
	for _, mgr := range cluster.managers {
		if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
			t.Fatalf("seed procedure: %v", err)
		}
		if err := mgr.store.PutBinding(ctx, binding); err != nil {
			t.Fatalf("seed binding: %v", err)
		}
	}
	inv := invocationForRunnable(leader.now, domainID, runnableAutomation{Procedure: procedure, Binding: binding}, automation.Invocation{ID: graphTriggeredInvocationID(spaceID.String(), domainID, "event-a", binding.ID, "node-a"), SpaceID: spaceID.String(), EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, CreatedAt: base, UpdatedAt: base}, "owner")
	if err := leader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("putInvocationRuntime() error = %v", err)
	}
	cluster.waitForInvocation(t, domainID, inv.ID)

	processed, err := leader.ProcessPending(ctx, domainID, 10)
	if err != nil || processed != 0 {
		t.Fatalf("early ProcessPending() processed=%d err=%v", processed, err)
	}
	cluster.waitForInvocationStatus(t, domainID, inv.ID, "pending")
	if _, err := leader.store.GetRun(ctx, domainID, inv.ID); err != storage.ErrNotFound {
		t.Fatalf("early run err=%v, want ErrNotFound", err)
	}

	got, err := leader.store.GetInvocation(ctx, domainID, inv.ID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}
	if got.ClaimToken != "" || got.ClaimVersion != 0 || got.ClaimOwnerNodeID != 0 || !got.ClaimExpiresAt.IsZero() {
		t.Fatalf("pre-debounce invocation was claimed: %+v", got)
	}
}

func TestClusteredGraphAutomationOutputRetriesGraphConflict(t *testing.T) {
	ctx := context.Background()
	inference := &conflictInjectingInference{text: "generated summary"}
	cluster := newAutomationGraphExecutionRaftCluster(t, ctx, 8, inference)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	procedure := automation.Procedure{ID: "page-summary", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize this page", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"properties.summary": "$result.text"}}}}}, Safety: automation.Safety{Idempotency: automation.Idempotency{SkipIfOutputUnchanged: true}}}
	binding := automation.Binding{ID: "page-summary-binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: spaceID.String(), DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "owner", OnBehalfOfPrincipalID: "owner", InferenceProfile: "summary"}}
	pageID := graph.NodeID(uuid.New())
	cluster.commitGraphMutation(t, ctx, leaderID, spaceID, domainID, func(tx sessionservice.GraphTransaction) error {
		_, err := cluster.graphs[leaderID].CreateNode(ctx, tx, graphservice.NodeInput{NodeID: pageID.String(), Labels: []string{"Page"}, Payload: map[string]any{"text": "original"}, Properties: map[string]any{}})
		return err
	})
	cluster.enableGraphAutomationSinks()
	binding.CreatedAt = time.Now().UTC()
	for _, mgr := range cluster.managers {
		if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
			t.Fatalf("seed procedure: %v", err)
		}
		if err := mgr.store.PutBinding(ctx, binding); err != nil {
			t.Fatalf("seed binding: %v", err)
		}
	}
	cluster.commitGraphMutation(t, ctx, leaderID, spaceID, domainID, func(tx sessionservice.GraphTransaction) error {
		node, err := cluster.graphs[leaderID].GetNode(ctx, tx, pageID.String())
		if err != nil {
			return err
		}
		node.Payload["text"] = "updated before automation"
		_, err = cluster.graphs[leaderID].UpdateNode(ctx, tx, graphservice.UpdateNodeInput{NodeID: node.ID.String(), Labels: node.Labels, Properties: node.Properties, Payload: node.Payload, Meta: node.Meta, Content: &node.Content, Props: node.Props})
		return err
	})
	cluster.waitForInvocationCount(t, domainID, 1)
	leaderID = cluster.leaderForSpace(t, spaceID)
	inference.onFirstInvoke = func(ctx context.Context) error {
		cluster.commitGraphMutation(t, ctx, leaderID, spaceID, domainID, func(tx sessionservice.GraphTransaction) error {
			node, err := cluster.graphs[leaderID].GetNode(ctx, tx, pageID.String())
			if err != nil {
				return err
			}
			if node.Properties == nil {
				node.Properties = map[string]any{}
			}
			node.Properties["touched_during_inference"] = true
			if node.Meta == nil {
				node.Meta = map[string]any{}
			}
			node.Meta["automation"] = map[string]any{"automation_id": binding.ID, "generated": true}
			_, err = cluster.graphs[leaderID].UpdateNode(ctx, tx, graphservice.UpdateNodeInput{NodeID: node.ID.String(), Labels: node.Labels, Properties: node.Properties, Payload: node.Payload, Meta: node.Meta, Content: &node.Content, Props: node.Props})
			return err
		})
		return nil
	}

	processed, err := cluster.managers[leaderID].ProcessPending(ctx, domainID, 10)
	if err != nil || processed != 1 {
		t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
	}
	cluster.waitForInvocationCount(t, domainID, 1)
	invs, err := cluster.managers[leaderID].store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 1 {
		t.Fatalf("ListInvocations()=%+v err=%v, want one invocation", invs, err)
	}
	inv := invs[0]
	cluster.waitForInvocationStatus(t, domainID, inv.ID, "succeeded")
	cluster.waitForRun(t, domainID, inv.ID)
	for nodeID, mgr := range cluster.managers {
		gotInv, err := mgr.store.GetInvocation(ctx, domainID, inv.ID)
		if err != nil {
			t.Fatalf("node %d missing invocation: %v", nodeID, err)
		}
		if gotInv.Status != "succeeded" || strings.Contains(strings.ToLower(gotInv.SkipReason), "graph conflict") {
			t.Fatalf("node %d terminal invocation includes graph conflict: %+v", nodeID, gotInv)
		}
		run, err := mgr.store.GetRun(ctx, domainID, inv.ID)
		if err != nil {
			t.Fatalf("node %d missing run: %v", nodeID, err)
		}
		if run.Status != "succeeded" || run.OutputHash == "" || run.MutationID == "" || strings.Contains(strings.ToLower(run.Error), "graph conflict") {
			t.Fatalf("node %d terminal run invalid: %+v", nodeID, run)
		}
	}
	readTx := cluster.beginReadOnlyTransaction(t, ctx, leaderID, spaceID, domainID)
	got, err := cluster.graphs[leaderID].GetNode(ctx, readTx, pageID.String())
	if err != nil {
		t.Fatalf("GetNode(final): %v", err)
	}
	if got.Properties["summary"] != "generated summary" || got.Properties["touched_during_inference"] != true {
		t.Fatalf("final page properties=%+v, want summary and concurrent write preserved", got.Properties)
	}
}

func TestAutomationGraphReplayRecoveryEnqueuesMissedEventAndAdvancesCursor(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	seedDisabledProcedureBindingOnAll(t, ctx, cluster.managers, domainID, spaceID, "binding-a")
	for _, mgr := range cluster.managers {
		procedure, err := mgr.store.GetProcedure(ctx, domainID, "proc")
		if err != nil {
			t.Fatal(err)
		}
		procedure.Status = automation.StatusEnabled
		if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
			t.Fatal(err)
		}
	}
	nodeID := graph.NodeID(uuid.New())
	replayer := replayEvents{events: []graphchange.CommittedEvent{{
		ID:            uuid.New(),
		SpaceID:       spaceID,
		DomainID:      domainID,
		Revision:      7,
		GraphRevision: 7,
		CommittedAt:   time.Now().UTC(),
		Origin:        graphchange.OriginMetadata{PrincipalID: "operator"},
		Changes:       []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID.String(), Node: &graph.Node{ID: nodeID, DomainID: domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "hello"}}}},
	}}}
	for _, mgr := range cluster.managers {
		if err := mgr.RecoverGraphChanges(ctx, replayer); err != nil {
			t.Fatalf("RecoverGraphChanges() error = %v", err)
		}
	}
	invID := graphTriggeredInvocationID(spaceID.String(), domainID, replayer.events[0].ID.String(), "binding-a", nodeID.String())
	cluster.waitForInvocation(t, domainID, invID)
	cluster.waitForGraphReplayCursor(t, spaceID.String(), domainID, 7)
	leaderMetrics := cluster.managers[leaderID].Metrics()
	if leaderMetrics.GraphReplayScopes == 0 || leaderMetrics.GraphReplayEvents != 1 || leaderMetrics.GraphReplayInvocationsCreated != 1 || leaderMetrics.GraphReplayCursorAdvances != 1 {
		t.Fatalf("unexpected leader replay metrics: %+v", leaderMetrics)
	}
	for id, mgr := range cluster.managers {
		if id != leaderID && mgr.Metrics().GraphReplayFollowerSkips == 0 {
			t.Fatalf("follower %d did not record replay follower skip: %+v", id, mgr.Metrics())
		}
	}
	for _, mgr := range cluster.managers {
		if err := mgr.RecoverGraphChanges(ctx, replayer); err != nil {
			t.Fatalf("second RecoverGraphChanges() error = %v", err)
		}
	}
	leader := cluster.managers[cluster.leaderForSpace(t, spaceID)]
	invs, err := leader.store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(invs) != 1 {
		t.Fatalf("replay should be idempotent, got %d invocations", len(invs))
	}
}

func TestAutomationGraphReplayRecoveryRecordsSkippedEventMetrics(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	seedEnabledProcedureBindingOnAll(t, ctx, cluster.managers, domainID, spaceID, "binding-a")
	nodeID := graph.NodeID(uuid.New())
	replayer := replayEvents{events: []graphchange.CommittedEvent{{
		ID:            uuid.New(),
		SpaceID:       spaceID,
		DomainID:      domainID,
		Revision:      9,
		GraphRevision: 9,
		CommittedAt:   time.Now().UTC(),
		Origin:        graphchange.OriginMetadata{PrincipalID: "operator"},
		Changes:       []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID.String(), Node: &graph.Node{ID: nodeID, DomainID: domainID, Labels: []string{"Note"}, Payload: map[string]any{"text": "not a page"}}}},
	}}}
	if err := cluster.managers[leaderID].RecoverGraphChanges(ctx, replayer); err != nil {
		t.Fatalf("RecoverGraphChanges() error = %v", err)
	}
	cluster.waitForGraphReplayCursor(t, spaceID.String(), domainID, 9)
	metrics := cluster.managers[leaderID].Metrics()
	if metrics.GraphReplayEvents != 1 || metrics.GraphReplaySkippedEvents != 1 || metrics.GraphReplayInvocationsCreated != 0 || metrics.GraphReplayCursorAdvances != 1 {
		t.Fatalf("unexpected skipped replay metrics: %+v", metrics)
	}
	invs, err := cluster.managers[leaderID].store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(invs) != 0 {
		t.Fatalf("non-matching replay should not create invocations: %+v", invs)
	}
}

func TestAutomationGraphReplayRecoveryAfterPartitionLeaderCrashBeforeEnqueue(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	oldLeaderID := cluster.leaderForSpace(t, spaceID)
	seedEnabledProcedureBindingOnAll(t, ctx, cluster.managers, domainID, spaceID, "binding-a")
	priorCursor := storage.GraphReplayCursor{SpaceID: spaceID.String(), DomainID: domainID, Revision: 10, UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
	if err := cluster.managers[oldLeaderID].putGraphReplayCursorRuntime(ctx, priorCursor); err != nil {
		t.Fatalf("seed graph replay cursor: %v", err)
	}
	cluster.waitForGraphReplayCursor(t, spaceID.String(), domainID, 10)

	notifications := map[consensus.NodeID]*graphnotification.Module{}
	for nodeID := range cluster.managers {
		module := graphnotification.NewModule()
		module.SetRetentionForTest(100, time.Hour)
		if result := module.Init(ctx, automationNotificationHost{dataDir: t.TempDir()}); !result.OK {
			t.Fatalf("notification Init(%d): %v", nodeID, result.Error)
		}
		notifications[nodeID] = module
	}
	nodeID := graph.NodeID(uuid.New())
	event := graphchange.CommittedEvent{
		ID:            uuid.New(),
		SpaceID:       spaceID,
		DomainID:      domainID,
		DomainIDs:     []graph.DomainID{domainID},
		Revision:      11,
		GraphRevision: 11,
		CommittedAt:   time.Now().UTC(),
		Origin:        graphchange.OriginMetadata{PrincipalID: "operator"},
		Changes:       []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID.String(), Node: &graph.Node{ID: nodeID, DomainID: domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "hello"}}}},
	}
	// Simulate the graph Raft command applying everywhere, but the old partition
	// leader crashing before the async automation consumer creates an invocation.
	for nodeID, module := range notifications {
		if err := module.OnGraphCommitted(ctx, event); err != nil {
			t.Fatalf("OnGraphCommitted(%d) error = %v", nodeID, err)
		}
	}
	invID := graphTriggeredInvocationID(spaceID.String(), domainID, event.ID.String(), "binding-a", nodeID.String())
	for nodeID, mgr := range cluster.managers {
		if _, err := mgr.store.GetInvocation(ctx, domainID, invID); err != storage.ErrNotFound {
			t.Fatalf("node %d invocation before recovery err=%v, want ErrNotFound", nodeID, err)
		}
	}

	cluster.crashNode(oldLeaderID)
	newLeaderID := cluster.waitForNewLeaderForSpace(t, spaceID, oldLeaderID)
	newLeader := cluster.managers[newLeaderID]
	if err := newLeader.RecoverGraphChanges(ctx, notifications[newLeaderID]); err != nil {
		t.Fatalf("new leader RecoverGraphChanges() error = %v", err)
	}
	cluster.waitForInvocationOnLiveNodes(t, domainID, invID)
	cluster.waitForGraphReplayCursorOnLiveNodes(t, spaceID.String(), domainID, 11)
	if _, err := cluster.managers[oldLeaderID].store.GetInvocation(ctx, domainID, invID); err != storage.ErrNotFound {
		t.Fatalf("crashed old leader invocation err=%v, want still missing", err)
	}
}

func TestAutomationGraphReplayRecoveryAfterPartitionLeaderCrashDuringRunningInvocation(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	oldLeaderID := cluster.leaderForSpace(t, spaceID)
	oldLeader := cluster.managers[oldLeaderID]
	seedDisabledProcedureBindingOnAll(t, ctx, cluster.managers, domainID, spaceID, "binding-a")
	now := time.Now().UTC()
	inv := automation.Invocation{ID: "inv-running-failover", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "running", ClaimOwnerNodeID: uint64(oldLeaderID), ClaimVersion: 1, ClaimToken: "old-leader-claim", ClaimExpiresAt: now.Add(-time.Minute), CreatedAt: now.Add(-2 * time.Minute), UpdatedAt: now.Add(-2 * time.Minute)}
	if err := oldLeader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("seed running invocation: %v", err)
	}
	cluster.waitForInvocation(t, domainID, inv.ID)

	cluster.crashNode(oldLeaderID)
	newLeaderID := cluster.waitForNewLeaderForSpace(t, spaceID, oldLeaderID)
	newLeader := cluster.managers[newLeaderID]
	processed, err := newLeader.ProcessPending(ctx, domainID, 10)
	if err != nil || processed != 1 {
		t.Fatalf("new leader ProcessPending() processed=%d err=%v", processed, err)
	}
	cluster.waitForInvocationStatusOnLiveNodes(t, domainID, inv.ID, "skipped")
	cluster.waitForRunIDOnLiveNodes(t, domainID, inv.ID)
	for id, mgr := range cluster.managers {
		if cluster.crashed[id] {
			continue
		}
		got, err := mgr.store.GetInvocation(ctx, domainID, inv.ID)
		if err != nil {
			t.Fatalf("node %d get invocation: %v", id, err)
		}
		if got.ClaimOwnerNodeID != uint64(newLeaderID) || got.ClaimVersion <= inv.ClaimVersion || got.ClaimToken == inv.ClaimToken || got.Status != "skipped" {
			t.Fatalf("node %d did not observe reclaimed terminal invocation: %+v", id, got)
		}
		run, err := mgr.store.GetRun(ctx, domainID, inv.ID)
		if err != nil {
			t.Fatalf("node %d get run: %v", id, err)
		}
		if run.ClaimOwnerNodeID != uint64(newLeaderID) || run.ClaimVersion != got.ClaimVersion || run.ClaimToken != got.ClaimToken || run.Status != "skipped" {
			t.Fatalf("node %d run did not carry reclaimed claim: run=%+v inv=%+v", id, run, got)
		}
	}
	metrics := newLeader.Metrics()
	if metrics.ClaimReclaims != 1 || metrics.Skipped != 1 || metrics.Processed != 1 {
		t.Fatalf("unexpected reclaim metrics: %+v", metrics)
	}
}

func TestAutomationGraphReplayRecoveryReportsUnreclaimableRunningClaim(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	leader := cluster.managers[leaderID]
	inv := automation.Invocation{ID: "inv-no-expiry", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "running", ClaimOwnerNodeID: uint64(leaderID), ClaimVersion: 1, ClaimToken: "claim-without-expiry", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := leader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("seed running invocation: %v", err)
	}
	cluster.waitForInvocation(t, domainID, inv.ID)
	processed, err := leader.ProcessPending(ctx, domainID, 10)
	if err != nil || processed != 0 {
		t.Fatalf("ProcessPending() processed=%d err=%v, want stranded no-op", processed, err)
	}
	if metrics := leader.Metrics(); metrics.ClaimAbandoned != 1 {
		t.Fatalf("unreclaimable running claim metric not recorded: %+v", metrics)
	}
}

func TestAutomationGraphReplayRecoveryReportsLosslessGap(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	_ = cluster.leaderForSpace(t, spaceID)
	seedDisabledProcedureBindingOnAll(t, ctx, cluster.managers, domainID, spaceID, "binding-a")
	var gotErr error
	var gotMgr *AutomationManager
	for _, mgr := range cluster.managers {
		if err := mgr.RecoverGraphChanges(ctx, gapReplayer{}); err != nil {
			gotErr = err
			gotMgr = mgr
			break
		}
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "graph change replay gap") {
		t.Fatalf("RecoverGraphChanges() gap error = %v", gotErr)
	}
	if gotMgr == nil {
		t.Fatal("gap metric manager was not captured")
	}
	if gotMgr.Metrics().GraphReplayGaps != 1 {
		t.Fatalf("gap metric not recorded: %+v", gotMgr.Metrics())
	}
}

func TestAutomationRuntimeReadsUseCommittedReplicatedStateAndMutationsFailOnFollowers(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	leader := cluster.managers[leaderID]
	inv := automation.Invocation{ID: "inv-runtime-api", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "failed", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := leader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("seed invocation: %v", err)
	}
	run := automation.Run{ID: "run-runtime-api", DomainID: domainID, InvocationID: inv.ID, Status: "failed", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}
	if err := leader.putRunRuntime(ctx, spaceID.String(), run); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	cluster.waitForInvocation(t, domainID, inv.ID)
	cluster.waitForRunID(t, domainID, run.ID)
	for id, mgr := range cluster.managers {
		items, err := mgr.ListInvocations(ctx, domainID, storage.InvocationFilter{})
		if err != nil || len(items) != 1 || items[0].ID != inv.ID {
			t.Fatalf("node %d ListInvocations() items=%+v err=%v", id, items, err)
		}
		gotRun, err := mgr.GetRun(ctx, domainID, run.ID)
		if err != nil || gotRun.InvocationID != inv.ID {
			t.Fatalf("node %d GetRun() run=%+v err=%v", id, gotRun, err)
		}
	}
	for id, mgr := range cluster.managers {
		if id == leaderID {
			continue
		}
		if _, err := mgr.RetryInvocation(ctx, domainID, inv.ID); status.Code(err) != codes.Unavailable {
			t.Fatalf("follower %d RetryInvocation() error=%v code=%v, want Unavailable", id, err, status.Code(err))
		}
		if _, err := mgr.CancelInvocation(ctx, domainID, inv.ID); status.Code(err) != codes.Unavailable {
			t.Fatalf("follower %d CancelInvocation() error=%v code=%v, want Unavailable", id, err, status.Code(err))
		}
	}
	if retried, err := leader.RetryInvocation(ctx, domainID, inv.ID); err != nil || retried.Status != "pending" {
		t.Fatalf("leader RetryInvocation() inv=%+v err=%v", retried, err)
	}
}

func TestAutomationRuntimeReadsForwardToPartitionLeader(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	stopBackends := cluster.startAutomationRuntimeReadBackends(t)
	defer stopBackends()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	leader := cluster.managers[leaderID]
	var follower *AutomationManager
	var followerID consensus.NodeID
	for id, mgr := range cluster.managers {
		if id != leaderID {
			followerID = id
			follower = mgr
			break
		}
	}
	if follower == nil {
		t.Fatal("expected follower")
	}
	inv := automation.Invocation{ID: "inv-forwarded-runtime-api", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "failed", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := leader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("seed invocation: %v", err)
	}
	run := automation.Run{ID: "run-forwarded-runtime-api", DomainID: domainID, InvocationID: inv.ID, Status: "failed", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}
	if err := leader.putRunRuntime(ctx, spaceID.String(), run); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	_ = follower.store.DeleteInvocation(ctx, domainID, inv.ID)
	_ = follower.store.DeleteRun(ctx, domainID, run.ID)
	items, err := follower.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil || len(items) != 1 || items[0].ID != inv.ID {
		t.Fatalf("follower %d ListInvocations() via forwarding items=%+v err=%v", followerID, items, err)
	}
	gotRun, err := follower.GetRun(ctx, domainID, run.ID)
	if err != nil || gotRun.ID != run.ID || gotRun.InvocationID != inv.ID {
		t.Fatalf("follower %d GetRun() via forwarding run=%+v err=%v", followerID, gotRun, err)
	}
}

func TestAutomationExecutionRaftRejectsFollowerRuntimeMutation(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	var follower *AutomationManager
	for id, mgr := range cluster.managers {
		if id != leaderID {
			follower = mgr
			break
		}
	}
	if follower == nil {
		t.Fatal("expected follower")
	}
	inv := automation.Invocation{ID: uuid.NewString(), DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "pending", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	err := follower.putInvocationRuntime(ctx, inv)
	if err == nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("follower putInvocationRuntime() error = %v, want unavailable", err)
	}
}

func TestAutomationRaftRejectsConflictingSameVersionClaim(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	now := time.Now().UTC()
	base := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "pending", CreatedAt: now, UpdatedAt: now}
	if err := mgr.store.PutInvocation(ctx, base); err != nil {
		t.Fatal(err)
	}
	first := base
	first.Status = "running"
	first.ClaimOwnerNodeID = 1
	first.ClaimVersion = 1
	first.ClaimToken = "claim-one"
	first.UpdatedAt = now.Add(time.Second)
	if err := mgr.applyAutomationMutation(ctx, automationMutationRecord{Kind: "invocation.upsert", DomainID: domainID, SpaceID: spaceID.String(), ID: first.ID, Payload: rawAutomation(first)}); err != nil {
		t.Fatalf("first claim apply error = %v", err)
	}
	second := first
	second.ClaimToken = "claim-two"
	second.UpdatedAt = now.Add(2 * time.Second)
	if err := mgr.applyAutomationMutation(ctx, automationMutationRecord{Kind: "invocation.upsert", DomainID: domainID, SpaceID: spaceID.String(), ID: second.ID, Payload: rawAutomation(second)}); err == nil {
		t.Fatal("expected same-version conflicting claim to be rejected")
	}
	got, err := mgr.store.GetInvocation(ctx, domainID, base.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClaimToken != "claim-one" {
		t.Fatalf("conflicting claim overwrote winner: %+v", got)
	}
}

func TestCancelRunningInvocationRaftFailsClosed(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	leader := cluster.managers[leaderID]
	inv := automation.Invocation{ID: "inv-running", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "running", ClaimOwnerNodeID: uint64(leaderID), ClaimVersion: 1, ClaimToken: "claim", ClaimExpiresAt: time.Now().Add(time.Minute), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := leader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("seed running invocation: %v", err)
	}
	cluster.waitForInvocation(t, domainID, inv.ID)
	if _, err := leader.CancelInvocation(ctx, domainID, inv.ID); err == nil || !strings.Contains(err.Error(), "cannot cancel a running automation invocation") {
		t.Fatalf("CancelInvocation() error = %v, want fail closed", err)
	}
}

func TestAutomationCancelFencesStaleWorkerTerminalUpdate(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	now := time.Now().UTC()
	running := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "running", ClaimOwnerNodeID: 1, ClaimVersion: 1, ClaimToken: "worker-claim", CreatedAt: now, UpdatedAt: now}
	if err := mgr.store.PutInvocation(ctx, running); err != nil {
		t.Fatal(err)
	}
	cancelled := running
	cancelled.Status = "cancelled"
	cancelled.SkipReason = "cancelled"
	cancelled.ClaimOwnerNodeID = 0
	cancelled.ClaimVersion = 2
	cancelled.ClaimToken = ""
	cancelled.UpdatedAt = now.Add(time.Second)
	if err := mgr.applyAutomationMutation(ctx, automationMutationRecord{Kind: "invocation.upsert", DomainID: domainID, SpaceID: spaceID.String(), ID: cancelled.ID, Payload: rawAutomation(cancelled)}); err != nil {
		t.Fatalf("cancel apply error = %v", err)
	}
	staleWorker := running
	staleWorker.Status = "succeeded"
	staleWorker.UpdatedAt = now.Add(2 * time.Second)
	if err := mgr.applyAutomationMutation(ctx, automationMutationRecord{Kind: "invocation.upsert", DomainID: domainID, SpaceID: spaceID.String(), ID: staleWorker.ID, Payload: rawAutomation(staleWorker)}); err == nil {
		t.Fatal("expected stale worker terminal update to be rejected after cancel")
	}
	got, err := mgr.store.GetInvocation(ctx, domainID, running.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "cancelled" || got.ClaimVersion != 2 {
		t.Fatalf("stale worker overwrote cancellation: %+v", got)
	}
}

func TestAutomationOutputIdempotencyKeyDetectsExistingGraphOutput(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: uuid.NewString()}
	keyFirst := automationOutputIdempotencyKey(inv, "run-a")
	keySecond := automationOutputIdempotencyKey(inv, "run-b")
	if keyFirst == "" || keyFirst != keySecond {
		t.Fatalf("output idempotency key must be invocation-stable across runs: %q %q", keyFirst, keySecond)
	}
	graphs := &automationE2EGraph{node: graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Meta: map[string]any{"automation": map[string]any{"output_idempotency_key": keyFirst}}}}
	applied, err := (actions.Engine{Graphs: graphs}).OutputAlreadyApplied(ctx, sessionservice.GraphTransaction{}, keySecond)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("expected existing graph automation output metadata to be detected")
	}
}

func TestAutomationExecutionSnapshotIncludesRuntimeState(t *testing.T) {
	ctx := context.Background()
	partitionCount := uint32(8)
	spaceID := domainspace.SpaceID(uuid.New())
	partitionID := automationSpacePartitionForTest(t, spaceID, partitionCount)
	domainID := graph.DomainID(uuid.New())
	source := NewManager(storage.NewFileStore(t.TempDir()))
	seedDisabledProcedureBindingOnAll(t, ctx, map[consensus.NodeID]*AutomationManager{1: source}, domainID, spaceID, "binding-a")
	inv := automation.Invocation{ID: "inv-a", DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", EventID: "event-a", ChangedElementID: "node-a", ChangedElementKind: "node", EventType: automation.EventNodeUpdated, Status: "succeeded", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := source.store.PutInvocation(ctx, inv); err != nil {
		t.Fatal(err)
	}
	run := automation.Run{ID: "run-a", DomainID: domainID, InvocationID: inv.ID, Status: "succeeded", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(), OutputIdempotencyKey: "output-a"}
	if err := source.store.PutRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	instance := automation.WorkflowInstance{ID: "workflow-a", DomainID: domainID, InvocationID: inv.ID, AutomationID: inv.AutomationID, AutomationVersion: inv.AutomationVersion, Status: "pending", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := source.store.PutWorkflowInstance(ctx, instance); err != nil {
		t.Fatal(err)
	}
	step := automation.WorkflowStepRun{ID: "step-a", DomainID: domainID, InstanceID: instance.ID, StepID: "step", Status: "pending", StartedAt: time.Now().UTC()}
	if err := source.store.PutWorkflowStepRun(ctx, step); err != nil {
		t.Fatal(err)
	}
	index := storage.SuccessfulInputIndex{DomainID: domainID, AutomationID: "binding-a", Version: 1, ChangedElementID: "node-a", InputHash: "hash-a", InvocationID: inv.ID, RunID: run.ID}
	if err := source.store.PutSuccessfulInputIndex(ctx, index); err != nil {
		t.Fatal(err)
	}
	checkpoint := storage.ScheduleCheckpoint{DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", LastRunAt: time.Now().UTC().Format(time.RFC3339), UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
	if err := source.store.PutScheduleCheckpoint(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	cursor := storage.GraphReplayCursor{SpaceID: spaceID.String(), DomainID: domainID, Revision: 42, UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
	if err := source.store.PutGraphReplayCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}

	snap, err := source.snapshotAutomationPartition(ctx, partitionID, partitionCount)
	if err != nil {
		t.Fatalf("snapshotAutomationPartition() error = %v", err)
	}
	target := NewManager(storage.NewFileStore(t.TempDir()))
	if err := target.restoreAutomationPartition(ctx, snap, partitionID, partitionCount); err != nil {
		t.Fatalf("restoreAutomationPartition() error = %v", err)
	}
	if _, err := target.store.GetInvocation(ctx, domainID, inv.ID); err != nil {
		t.Fatalf("restored invocation missing: %v", err)
	}
	if _, err := target.store.GetRun(ctx, domainID, run.ID); err != nil {
		t.Fatalf("restored run missing: %v", err)
	}
	if _, err := target.store.GetWorkflowInstance(ctx, domainID, instance.ID); err != nil {
		t.Fatalf("restored workflow instance missing: %v", err)
	}
	steps, err := target.store.ListWorkflowStepRuns(ctx, domainID, instance.ID)
	if err != nil || len(steps) != 1 || steps[0].ID != step.ID {
		t.Fatalf("restored workflow step missing: steps=%+v err=%v", steps, err)
	}
	if _, err := target.store.GetSuccessfulInputIndex(ctx, domainID, index.AutomationID, index.Version, index.ChangedElementID, index.InputHash); err != nil {
		t.Fatalf("restored successful input missing: %v", err)
	}
	if _, err := target.store.GetScheduleCheckpoint(ctx, domainID, checkpoint.AutomationID); err != nil {
		t.Fatalf("restored schedule checkpoint missing: %v", err)
	}
	gotCursor, err := target.store.GetGraphReplayCursor(ctx, spaceID.String(), domainID)
	if err != nil || gotCursor.Revision != 42 {
		t.Fatalf("restored graph replay cursor missing: cursor=%+v err=%v", gotCursor, err)
	}
}

func TestProcessScheduledRaftEnqueuesOnceAndStartsWorkflowOnLeader(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	fixedNow := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	for _, mgr := range cluster.managers {
		mgr.now = func() time.Time { return fixedNow }
	}
	procedure := automation.Procedure{ID: "scheduled-proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Workflow: &automation.Workflow{Steps: []automation.WorkflowStep{{ID: "step", Kind: automation.WorkflowStepTool, Tool: "debug.echo"}}}}
	binding := automation.Binding{ID: "scheduled-binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: 1, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: spaceID.String(), DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeSchedule, Schedule: &automation.ScheduleTrigger{Interval: "1h"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "owner", OnBehalfOfPrincipalID: "owner"}}
	for _, mgr := range cluster.managers {
		if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
			t.Fatal(err)
		}
		if err := mgr.store.PutBinding(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}
	for id, mgr := range cluster.managers {
		count, err := mgr.ProcessScheduled(ctx, domainID, 10)
		if id == leaderID {
			if err != nil || count != 1 {
				t.Fatalf("leader ProcessScheduled() count=%d err=%v", count, err)
			}
			continue
		}
		if err != nil || count != 0 {
			t.Fatalf("follower %d ProcessScheduled() count=%d err=%v, want no-op", id, count, err)
		}
	}
	cluster.waitForScheduleCheckpoint(t, domainID, binding.ID)
	invs, err := cluster.managers[leaderID].ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil {
		t.Fatalf("ListInvocations() error = %v", err)
	}
	if len(invs) != 1 || invs[0].EventType != "schedule" || invs[0].ChangedElementKind != "schedule" {
		t.Fatalf("unexpected scheduled invocation: %+v", invs)
	}
	if want := scheduledInvocationID(spaceID.String(), domainID, binding.ID, fixedNow); invs[0].ID != want {
		t.Fatalf("scheduled invocation ID = %s, want %s", invs[0].ID, want)
	}
	invID := invs[0].ID
	if count, err := cluster.managers[leaderID].ProcessScheduled(ctx, domainID, 10); err != nil || count != 0 {
		t.Fatalf("second ProcessScheduled() count=%d err=%v, want idempotent no-op", count, err)
	}
	processed, err := cluster.managers[leaderID].ProcessPending(ctx, domainID, 10)
	if err != nil || processed != 1 {
		t.Fatalf("ProcessPending() count=%d err=%v", processed, err)
	}
	cluster.waitForInvocationStatus(t, domainID, invID, "succeeded")
	instID := workflowInstanceID(invs[0])
	stepID := workflowStepRunID(instID, "step")
	for id, mgr := range cluster.managers {
		inst, err := mgr.store.GetWorkflowInstance(ctx, domainID, instID)
		if err != nil {
			t.Fatalf("node %d missing workflow instance: %v", id, err)
		}
		if inst.InvocationID != invID || inst.Status != "pending" {
			t.Fatalf("node %d unexpected workflow instance: %+v", id, inst)
		}
		steps, err := mgr.store.ListWorkflowStepRuns(ctx, domainID, instID)
		if err != nil || len(steps) != 1 || steps[0].ID != stepID || steps[0].Status != "pending" {
			t.Fatalf("node %d unexpected workflow step runs: %+v err=%v", id, steps, err)
		}
	}
}

func TestProcessScheduledRaftFailoverBeforeCheckpointDoesNotDuplicateInvocation(t *testing.T) {
	ctx := context.Background()
	cluster := newAutomationExecutionRaftCluster(t, ctx, 8)
	defer cluster.stop()
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	leaderID := cluster.leaderForSpace(t, spaceID)
	leader := cluster.managers[leaderID]
	fixedNow := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	for _, mgr := range cluster.managers {
		mgr.now = func() time.Time { return fixedNow }
	}
	procedure := automation.Procedure{ID: "scheduled-proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Workflow: &automation.Workflow{Steps: []automation.WorkflowStep{{ID: "step", Kind: automation.WorkflowStepTool, Tool: "debug.echo"}}}}
	binding := automation.Binding{ID: "scheduled-binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: 1, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: spaceID.String(), DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeSchedule, Schedule: &automation.ScheduleTrigger{Interval: "1h"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "owner", OnBehalfOfPrincipalID: "owner"}}
	for _, mgr := range cluster.managers {
		if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
			t.Fatal(err)
		}
		if err := mgr.store.PutBinding(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}

	inv := invocationForRunnable(leader.now, domainID, runnableAutomation{Procedure: procedure, Binding: binding}, automation.Invocation{ID: scheduledInvocationID(spaceID.String(), domainID, binding.ID, fixedNow), SpaceID: spaceID.String(), EventID: "schedule:" + binding.ID + ":" + fixedNow.Format(time.RFC3339), ChangedElementKind: "schedule", EventType: "schedule"}, "")
	if err := leader.putInvocationRuntime(ctx, inv); err != nil {
		t.Fatalf("seed scheduled invocation before checkpoint: %v", err)
	}
	cluster.waitForInvocation(t, domainID, inv.ID)

	cluster.crashNode(leaderID)
	newLeaderID := cluster.waitForNewLeaderForSpace(t, spaceID, leaderID)
	count, err := cluster.managers[newLeaderID].ProcessScheduled(ctx, domainID, 10)
	if err != nil || count != 1 {
		t.Fatalf("new leader ProcessScheduled() count=%d err=%v", count, err)
	}
	cluster.waitForScheduleCheckpointOnLiveNodes(t, domainID, binding.ID)
	for id, mgr := range cluster.managers {
		if cluster.crashed[id] {
			continue
		}
		invs, err := mgr.store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
		if err != nil || len(invs) != 1 || invs[0].ID != inv.ID {
			t.Fatalf("node %d invocations after failover: %+v err=%v", id, invs, err)
		}
	}
}

type automationGraphExecutionRaftCluster struct {
	partitionCount uint32
	managers       map[consensus.NodeID]*AutomationManager
	modules        map[consensus.NodeID]*Module
	sessions       map[consensus.NodeID]*sessionservice.Module
	graphs         map[consensus.NodeID]*graphservice.Module
	notifications  map[consensus.NodeID]*graphnotification.Module
	groups         map[consensus.NodeID]*consensus.MultiGroup
	router         *consensus.LocalMessageRouter
	stop           func()
}

type automationGraphRaftHost struct {
	dataDir string
	nodeID  consensus.NodeID
}

func (h automationGraphRaftHost) DataDir() string { return h.dataDir }
func (h automationGraphRaftHost) Log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
func (h automationGraphRaftHost) LocalRouteIdentity() coreruntime.LocalRouteIdentity {
	return coreruntime.LocalRouteIdentity{RaftMode: true, RaftNodeID: uint64(h.nodeID)}
}
func (h automationGraphRaftHost) RequireLocalWriteAllowed() error {
	return errors.New("clustered local write rejected: raft executor is not configured for this subsystem")
}

type raftRecordSupporter interface {
	SupportsRaftCommandRecord(consensus.CommandScope, wal.RecordType) bool
}

type automationGraphPartitionStateMachine struct {
	graph      graphservice.RaftStateMachine
	automation RaftStateMachine
}

func (s automationGraphPartitionStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if supporter, ok := any(s.graph).(raftRecordSupporter); ok && supporter.SupportsRaftCommandRecord(cmd.Scope, cmd.RecordType) {
		return s.graph.ApplyCommand(ctx, apply, cmd)
	}
	if supporter, ok := any(s.automation).(raftRecordSupporter); ok && supporter.SupportsRaftCommandRecord(cmd.Scope, cmd.RecordType) {
		return s.automation.ApplyCommand(ctx, apply, cmd)
	}
	return fmt.Errorf("unsupported automation graph raft command record type %s", cmd.RecordType)
}

func newAutomationGraphExecutionRaftCluster(t *testing.T, ctx context.Context, partitionCount uint32, inference inferenceservice.Manager) automationGraphExecutionRaftCluster {
	t.Helper()
	router := consensus.NewLocalMessageRouter()
	peers := []consensus.NodeID{1, 2, 3}
	modules := map[consensus.NodeID]*Module{}
	managers := map[consensus.NodeID]*AutomationManager{}
	sessions := map[consensus.NodeID]*sessionservice.Module{}
	graphs := map[consensus.NodeID]*graphservice.Module{}
	notifications := map[consensus.NodeID]*graphnotification.Module{}
	registrations := []graphnotification.Registration{}
	for _, id := range peers {
		host := automationGraphRaftHost{dataDir: t.TempDir(), nodeID: id}
		sessions[id] = sessionservice.NewModule()
		if result := sessions[id].Init(ctx, host); !result.OK {
			t.Fatalf("session Init(%d): %v", id, result.Error)
		}
		graphs[id] = graphservice.NewModule()
		if result := graphs[id].Init(ctx, host); !result.OK {
			t.Fatalf("graph Init(%d): %v", id, result.Error)
		}
		modules[id] = NewModule("").WithGraphRuntime(sessions[id], graphs[id]).WithInferenceManager(inference).WithWorkerConfig(WorkerConfig{Enabled: false})
		if result := modules[id].Init(ctx, host); !result.OK {
			t.Fatalf("automation Init(%d): %v", id, result.Error)
		}
		managers[id] = modules[id].AutomationManager
		graphs[id].SetAutomationOutputFenceValidator(managers[id])
		notifications[id] = graphnotification.NewModule()
		notifications[id].SetRetentionForTest(100, time.Hour)
		if result := notifications[id].Init(ctx, automationNotificationHost{dataDir: t.TempDir()}); !result.OK {
			t.Fatalf("notification Init(%d): %v", id, result.Error)
		}
		reg, err := notifications[id].RegisterConsumer(ctx, graphnotification.ConsumerSpec{ConsumerName: "automation", Filter: graphchange.Filter{EventTypes: []graphchange.ChangeType{graphchange.ChangeTypeNodeUpdated}}, Projection: graphchange.Projection{IncludeRevision: true, IncludeNewNodeSnapshot: true, IncludeOldNodeSnapshot: true}, Lossless: true}, modules[id])
		if err != nil {
			t.Fatalf("notification RegisterConsumer(%d): %v", id, err)
		}
		registrations = append(registrations, reg)
	}
	groups := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, id := range peers {
		localID := id
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: localID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(partitionID uint32) consensus.StateMachine {
			return automationGraphPartitionStateMachine{graph: graphservice.RaftStateMachine{Module: graphs[localID], PartitionID: partitionID, PartitionCount: partitionCount}, automation: RaftStateMachine{Manager: managers[localID], PartitionID: partitionID, PartitionCount: partitionCount}}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d): %v", id, err)
		}
		groups[id] = mg
		for _, group := range mg.Groups() {
			router.Register(group)
		}
		graphs[id].EnableExperimentalRaft(mg, partitionCount)
		managers[id].EnableExperimentalRaft(mg, partitionCount)
	}
	stopTick := startRaftTicker(t, groups)
	waitForAutomationRaftLeaders(t, groups, partitionCount)
	return automationGraphExecutionRaftCluster{partitionCount: partitionCount, managers: managers, modules: modules, sessions: sessions, graphs: graphs, notifications: notifications, groups: groups, router: router, stop: func() {
		for _, reg := range registrations {
			_ = reg.Close()
		}
		stopTick()
		for _, mg := range groups {
			mg.Stop()
		}
	}}
}

func (c automationGraphExecutionRaftCluster) leaderForSpace(t *testing.T, spaceID domainspace.SpaceID) consensus.NodeID {
	t.Helper()
	partitionID := automationSpacePartitionForTest(t, spaceID, c.partitionCount)
	for _, mg := range c.groups {
		group, ok := mg.Group(consensus.PartitionGroupID(partitionID))
		if ok && group.Leader() != 0 {
			return group.Leader()
		}
	}
	t.Fatalf("no leader for space partition %d", partitionID)
	return 0
}

func (c automationGraphExecutionRaftCluster) enableGraphAutomationSinks() {
	for id, graphModule := range c.graphs {
		graphModule.SetRaftApplyChangeSink(c.notifications[id])
	}
}

func (c automationGraphExecutionRaftCluster) beginReadOnlyTransaction(t *testing.T, ctx context.Context, nodeID consensus.NodeID, spaceID domainspace.SpaceID, domainID graph.DomainID) sessionservice.GraphTransaction {
	t.Helper()
	sess, err := c.sessions[nodeID].OpenSession(ctx, sessionservice.OpenSessionInput{PrincipalID: "reader", SpaceID: spaceID.String(), DomainID: domainID.String()})
	if err != nil {
		t.Fatalf("OpenSession(read): %v", err)
	}
	tx, err := c.sessions[nodeID].BeginTransaction(ctx, sessionservice.BeginTransactionInput{PrincipalID: "reader", SessionID: sess.ID, Mode: sessionservice.TransactionModeReadOnly})
	if err != nil {
		t.Fatalf("BeginTransaction(read): %v", err)
	}
	return tx
}

func (c automationGraphExecutionRaftCluster) commitGraphMutation(t *testing.T, ctx context.Context, nodeID consensus.NodeID, spaceID domainspace.SpaceID, domainID graph.DomainID, mutate func(sessionservice.GraphTransaction) error) {
	t.Helper()
	sess, err := c.sessions[nodeID].OpenSession(ctx, sessionservice.OpenSessionInput{PrincipalID: "operator", SpaceID: spaceID.String(), DomainID: domainID.String()})
	if err != nil {
		t.Fatalf("OpenSession(write): %v", err)
	}
	tx, err := c.sessions[nodeID].BeginTransaction(ctx, sessionservice.BeginTransactionInput{PrincipalID: "operator", SessionID: sess.ID, Mode: sessionservice.TransactionModeReadWrite})
	if err != nil {
		t.Fatalf("BeginTransaction(write): %v", err)
	}
	if err := mutate(tx); err != nil {
		c.graphs[nodeID].DiscardTransactionGraph(ctx, tx.ID)
		_, _ = c.sessions[nodeID].RollbackTransaction(ctx, "operator", tx.ID)
		t.Fatalf("mutate graph: %v", err)
	}
	commit, err := c.graphs[nodeID].CommitTransactionGraph(ctx, tx)
	if err != nil {
		c.graphs[nodeID].DiscardTransactionGraph(ctx, tx.ID)
		_, _ = c.sessions[nodeID].RollbackTransaction(ctx, "operator", tx.ID)
		t.Fatalf("CommitTransactionGraph: %v", err)
	}
	if _, err := c.sessions[nodeID].CommitTransactionAtRevision(ctx, "operator", tx.ID, commit.OperationCount, commit.CommittedRevision); err != nil {
		t.Fatalf("CommitTransactionAtRevision: %v", err)
	}
}

func (c automationGraphExecutionRaftCluster) waitForNotificationEventsHandled(t *testing.T, wantPublished uint64) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, notification := range c.notifications {
			if notification.Diagnostics().EventsPublished < wantPublished {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("graph notifications did not publish %d events: %v", wantPublished, err)
	}
}

func (c automationGraphExecutionRaftCluster) waitForInvocationCount(t *testing.T, domainID graph.DomainID, want int) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, mgr := range c.managers {
			items, err := mgr.store.ListInvocations(context.Background(), domainID, storage.InvocationFilter{})
			if err != nil || len(items) != want {
				return false
			}
		}
		return true
	}); err != nil {
		counts := map[consensus.NodeID]int{}
		for id, mgr := range c.managers {
			items, listErr := mgr.store.ListInvocations(context.Background(), domainID, storage.InvocationFilter{})
			if listErr == nil {
				counts[id] = len(items)
			}
		}
		diagnostics := map[consensus.NodeID]graphnotification.Diagnostics{}
		for id, notification := range c.notifications {
			diagnostics[id] = notification.Diagnostics()
		}
		t.Fatalf("invocation count did not reach %d: %v; counts=%+v notification_diagnostics=%+v", want, err, counts, diagnostics)
	}
}

func (c automationGraphExecutionRaftCluster) waitForInvocationStatus(t *testing.T, domainID graph.DomainID, invocationID string, status string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, mgr := range c.managers {
			inv, err := mgr.store.GetInvocation(context.Background(), domainID, invocationID)
			if err != nil || inv.Status != status {
				return false
			}
		}
		return true
	}); err != nil {
		statuses := map[consensus.NodeID]string{}
		errorsByNode := map[consensus.NodeID]string{}
		for id, mgr := range c.managers {
			inv, getErr := mgr.store.GetInvocation(context.Background(), domainID, invocationID)
			if getErr != nil {
				errorsByNode[id] = getErr.Error()
				continue
			}
			statuses[id] = inv.Status + ":" + inv.SkipReason
		}
		t.Fatalf("invocation %s did not reach %s: %v; statuses=%+v errors=%+v", invocationID, status, err, statuses, errorsByNode)
	}
}

func (c automationGraphExecutionRaftCluster) waitForRun(t *testing.T, domainID graph.DomainID, invocationID string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, mgr := range c.managers {
			if _, err := mgr.store.GetRun(context.Background(), domainID, invocationID); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("run %s did not replicate: %v", invocationID, err)
	}
}

type conflictInjectingInference struct {
	mu            sync.Mutex
	text          string
	onFirstInvoke func(context.Context) error
	invocations   int
}

func (f *conflictInjectingInference) GlobalManager() inferencestorage.GlobalManager { return nil }
func (f *conflictInjectingInference) SpaceManager(context.Context, string) (inferencestorage.SpaceManager, error) {
	return nil, nil
}
func (f *conflictInjectingInference) UsageLedger() inferencestorage.UsageLedger { return nil }
func (f *conflictInjectingInference) RequireLocalWriteAllowed() error           { return nil }
func (f *conflictInjectingInference) Resolve(context.Context, inferenceservice.ResolveRequest) (inferenceservice.ResolveResult, error) {
	return inferenceservice.ResolveResult{Allowed: true}, nil
}
func (f *conflictInjectingInference) Invoke(ctx context.Context, req inferenceservice.InvokeRequest) (inferenceservice.InvokeResponse, error) {
	f.mu.Lock()
	f.invocations++
	callback := f.onFirstInvoke
	if f.invocations != 1 {
		callback = nil
	}
	f.mu.Unlock()
	if callback != nil {
		if err := callback(ctx); err != nil {
			return inferenceservice.InvokeResponse{}, err
		}
	}
	return inferenceservice.InvokeResponse{Allowed: true, Text: f.text, Usage: inferenceconnectors.Usage{InputTokens: 4, OutputTokens: 2, TotalTokens: 6, TokenCountSource: "fake"}, Status: domaininference.UsageStatusSucceeded}, nil
}

type automationExecutionRaftCluster struct {
	partitionCount uint32
	managers       map[consensus.NodeID]*AutomationManager
	groups         map[consensus.NodeID]*consensus.MultiGroup
	router         *consensus.LocalMessageRouter
	crashed        map[consensus.NodeID]bool
	stop           func()
}

func newAutomationExecutionRaftCluster(t *testing.T, ctx context.Context, partitionCount uint32) automationExecutionRaftCluster {
	t.Helper()
	router := consensus.NewLocalMessageRouter()
	peers := []consensus.NodeID{1, 2, 3}
	managers := map[consensus.NodeID]*AutomationManager{}
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
		for _, group := range mg.Groups() {
			router.Register(group)
		}
		managers[id].EnableExperimentalRaft(mg, partitionCount)
	}
	stopTick := startRaftTicker(t, groups)
	waitForAutomationRaftLeaders(t, groups, partitionCount)
	return automationExecutionRaftCluster{partitionCount: partitionCount, managers: managers, groups: groups, router: router, crashed: map[consensus.NodeID]bool{}, stop: func() {
		stopTick()
		for _, mg := range groups {
			mg.Stop()
		}
	}}
}

func (c automationExecutionRaftCluster) startAutomationRuntimeReadBackends(t *testing.T) func() {
	t.Helper()
	addrs := make([]string, 3)
	servers := []*grpc.Server{}
	listeners := []net.Listener{}
	for nodeID, mgr := range c.managers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen backend for node %d: %v", nodeID, err)
		}
		server := grpc.NewServer()
		svc := clusterbackend.NewService(clustermodel.NodeIdentity{Version: clustermodel.NodeIdentityVersion, NodeID: "node", ClusterID: "automation-runtime-read-test", ClusterAdmitted: true}, clustermodel.NodeStateClustered, nil)
		svc.AutomationRuntimeReader = mgr
		clusterpb.RegisterClusterBackendServiceServer(server, svc)
		go func() {
			_ = server.Serve(listener)
		}()
		addrs[int(nodeID)-1] = listener.Addr().String()
		servers = append(servers, server)
		listeners = append(listeners, listener)
	}
	for nodeID, mgr := range c.managers {
		mgr.EnableExperimentalRaftNetworking(nodeID, addrs, "")
	}
	return func() {
		for _, server := range servers {
			server.Stop()
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}
}

func (c automationExecutionRaftCluster) leaderForSpace(t *testing.T, spaceID domainspace.SpaceID) consensus.NodeID {
	t.Helper()
	partitionID := automationSpacePartitionForTest(t, spaceID, c.partitionCount)
	for nodeID, mg := range c.groups {
		group, ok := mg.Group(consensus.PartitionGroupID(partitionID))
		if ok && group.Leader() != 0 {
			return group.Leader()
		}
		_ = nodeID
	}
	t.Fatalf("no leader for space partition %d", partitionID)
	return 0
}

func (c automationExecutionRaftCluster) waitForInvocation(t *testing.T, domainID graph.DomainID, invocationID string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, mgr := range c.managers {
			if _, err := mgr.store.GetInvocation(context.Background(), domainID, invocationID); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("invocation %s did not replicate: %v", invocationID, err)
	}
}

func (c automationExecutionRaftCluster) crashNode(nodeID consensus.NodeID) {
	if c.crashed != nil {
		c.crashed[nodeID] = true
	}
	if c.router != nil {
		c.router.UnregisterNode(nodeID)
	}
	if mg := c.groups[nodeID]; mg != nil {
		mg.Stop()
	}
}

func (c automationExecutionRaftCluster) waitForNewLeaderForSpace(t *testing.T, spaceID domainspace.SpaceID, previous consensus.NodeID) consensus.NodeID {
	t.Helper()
	partitionID := automationSpacePartitionForTest(t, spaceID, c.partitionCount)
	var leader consensus.NodeID
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		candidate := consensus.NodeID(0)
		for nodeID, mg := range c.groups {
			if c.crashed[nodeID] {
				continue
			}
			group, ok := mg.Group(consensus.PartitionGroupID(partitionID))
			if !ok {
				return false
			}
			observed := group.Leader()
			if observed == 0 || observed == previous || c.crashed[observed] {
				return false
			}
			if candidate == 0 {
				candidate = observed
				continue
			}
			if observed != candidate {
				return false
			}
		}
		leader = candidate
		return leader != 0
	}); err != nil {
		t.Fatalf("new raft leader not elected for space partition %d after node %d crash: %v", partitionID, previous, err)
	}
	return leader
}

func (c automationExecutionRaftCluster) waitForInvocationOnLiveNodes(t *testing.T, domainID graph.DomainID, invocationID string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for id, mgr := range c.managers {
			if c.crashed[id] {
				continue
			}
			if _, err := mgr.store.GetInvocation(context.Background(), domainID, invocationID); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("invocation %s did not replicate to live nodes: %v", invocationID, err)
	}
}

func (c automationExecutionRaftCluster) waitForGraphReplayCursorOnLiveNodes(t *testing.T, spaceID string, domainID graph.DomainID, revision uint64) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for id, mgr := range c.managers {
			if c.crashed[id] {
				continue
			}
			cursor, err := mgr.store.GetGraphReplayCursor(context.Background(), spaceID, domainID)
			if err != nil || cursor.Revision != revision {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("graph replay cursor did not replicate to live nodes: %v", err)
	}
}

func (c automationExecutionRaftCluster) waitForInvocationStatus(t *testing.T, domainID graph.DomainID, invocationID string, status string) {
	t.Helper()
	c.waitForInvocationStatusMatching(t, domainID, invocationID, status, false)
}

func (c automationExecutionRaftCluster) waitForInvocationStatusOnLiveNodes(t *testing.T, domainID graph.DomainID, invocationID string, status string) {
	t.Helper()
	c.waitForInvocationStatusMatching(t, domainID, invocationID, status, true)
}

func (c automationExecutionRaftCluster) waitForInvocationStatusMatching(t *testing.T, domainID graph.DomainID, invocationID string, status string, liveOnly bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for id, mgr := range c.managers {
			if liveOnly && c.crashed[id] {
				continue
			}
			inv, err := mgr.store.GetInvocation(context.Background(), domainID, invocationID)
			if err != nil || inv.Status != status {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("invocation %s did not reach %s: %v", invocationID, status, err)
	}
}

func (c automationExecutionRaftCluster) waitForRun(t *testing.T, domainID graph.DomainID, invocationID string) {
	t.Helper()
	c.waitForRunID(t, domainID, invocationID)
}

func (c automationExecutionRaftCluster) waitForRunID(t *testing.T, domainID graph.DomainID, runID string) {
	t.Helper()
	c.waitForRunIDMatching(t, domainID, runID, false)
}

func (c automationExecutionRaftCluster) waitForRunIDOnLiveNodes(t *testing.T, domainID graph.DomainID, runID string) {
	t.Helper()
	c.waitForRunIDMatching(t, domainID, runID, true)
}

func (c automationExecutionRaftCluster) waitForRunIDMatching(t *testing.T, domainID graph.DomainID, runID string, liveOnly bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for id, mgr := range c.managers {
			if liveOnly && c.crashed[id] {
				continue
			}
			if _, err := mgr.store.GetRun(context.Background(), domainID, runID); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("run %s did not replicate: %v", runID, err)
	}
}

func (c automationExecutionRaftCluster) waitForSuccessfulInput(t *testing.T, domainID graph.DomainID, index storage.SuccessfulInputIndex) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, mgr := range c.managers {
			if _, err := mgr.store.GetSuccessfulInputIndex(context.Background(), domainID, index.AutomationID, index.Version, index.ChangedElementID, index.InputHash); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("successful input did not replicate: %v", err)
	}
}

func (c automationExecutionRaftCluster) waitForGraphReplayCursor(t *testing.T, spaceID string, domainID graph.DomainID, revision uint64) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, mgr := range c.managers {
			cursor, err := mgr.store.GetGraphReplayCursor(context.Background(), spaceID, domainID)
			if err != nil || cursor.Revision != revision {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("graph replay cursor did not replicate: %v", err)
	}
}

func (c automationExecutionRaftCluster) waitForScheduleCheckpoint(t *testing.T, domainID graph.DomainID, bindingID string) {
	t.Helper()
	c.waitForScheduleCheckpointMatching(t, domainID, bindingID, false)
}

func (c automationExecutionRaftCluster) waitForScheduleCheckpointOnLiveNodes(t *testing.T, domainID graph.DomainID, bindingID string) {
	t.Helper()
	c.waitForScheduleCheckpointMatching(t, domainID, bindingID, true)
}

func (c automationExecutionRaftCluster) waitForScheduleCheckpointMatching(t *testing.T, domainID graph.DomainID, bindingID string, liveOnly bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for id, mgr := range c.managers {
			if liveOnly && c.crashed[id] {
				continue
			}
			if _, err := mgr.store.GetScheduleCheckpoint(context.Background(), domainID, bindingID); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("schedule checkpoint did not replicate: %v", err)
	}
}

type replayEvents struct {
	events []graphchange.CommittedEvent
}

func (r replayEvents) Replay(ctx context.Context, spec graphnotification.ConsumerSpec, consumer graphnotification.Consumer) error {
	for _, event := range r.events {
		if spec.Start.AfterRevision != nil && automationEventRevision(event) <= *spec.Start.AfterRevision {
			continue
		}
		if err := consumer.HandleGraphChange(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

type gapReplayer struct{}

func (gapReplayer) Replay(ctx context.Context, spec graphnotification.ConsumerSpec, consumer graphnotification.Consumer) error {
	return consumer.HandleGraphChangeGap(ctx, graphchange.Gap{SpaceID: spec.Scope.SpaceID, DomainID: spec.Scope.DomainID, RequestedAfterRevision: 1, OldestAvailableRevision: 10, CurrentRevision: 20})
}

func automationSpacePartitionForTest(t *testing.T, spaceID domainspace.SpaceID, partitionCount uint32) uint32 {
	t.Helper()
	partitionID, err := partitioning.PartitionForSpaceID(spaceID, partitionCount)
	if err != nil {
		t.Fatalf("PartitionForSpaceID() error = %v", err)
	}
	return partitionID.Uint32()
}

type automationNotificationHost struct {
	dataDir string
}

func (h automationNotificationHost) DataDir() string { return h.dataDir }
func (h automationNotificationHost) Log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func seedEnabledProcedureBindingOnAll(t *testing.T, ctx context.Context, managers map[consensus.NodeID]*AutomationManager, domainID graph.DomainID, spaceID domainspace.SpaceID, bindingID string) {
	t.Helper()
	seedDisabledProcedureBindingOnAll(t, ctx, managers, domainID, spaceID, bindingID)
	for _, mgr := range managers {
		procedure, err := mgr.store.GetProcedure(ctx, domainID, "proc")
		if err != nil {
			t.Fatalf("seed get procedure: %v", err)
		}
		procedure.Status = automation.StatusEnabled
		if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
			t.Fatalf("seed enable procedure: %v", err)
		}
	}
}

func seedDisabledProcedureBindingOnAll(t *testing.T, ctx context.Context, managers map[consensus.NodeID]*AutomationManager, domainID graph.DomainID, spaceID domainspace.SpaceID, bindingID string) {
	t.Helper()
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusDisabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText}}
	binding := automation.Binding{ID: bindingID, Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: spaceID.String(), DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "owner", OnBehalfOfPrincipalID: "owner", InferenceProfile: "summary"}}
	for _, mgr := range managers {
		if err := mgr.store.PutProcedure(ctx, procedure); err != nil {
			t.Fatalf("seed procedure: %v", err)
		}
		if err := mgr.store.PutBinding(ctx, binding); err != nil {
			t.Fatalf("seed binding: %v", err)
		}
	}
}
