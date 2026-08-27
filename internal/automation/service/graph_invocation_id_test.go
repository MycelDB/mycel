package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestGraphTriggeredInvocationIDIsStableAndScoped(t *testing.T) {
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	eventID := uuid.NewString()
	nodeID := uuid.NewString()

	first := graphTriggeredInvocationID(spaceID, domainID, eventID, "binding-a", nodeID)
	second := graphTriggeredInvocationID(spaceID, domainID, eventID, "binding-a", nodeID)
	if first == "" || first != second {
		t.Fatalf("graphTriggeredInvocationID() not stable: %q %q", first, second)
	}
	if _, err := uuid.Parse(first); err != nil {
		t.Fatalf("graphTriggeredInvocationID() = %q, want UUID string: %v", first, err)
	}
	if got := graphTriggeredInvocationID(spaceID, domainID, eventID, "binding-b", nodeID); got == first {
		t.Fatal("different binding ID should produce a different invocation ID")
	}
	if got := graphTriggeredInvocationID(spaceID, domainID, eventID, "binding-a", uuid.NewString()); got == first {
		t.Fatal("different target node ID should produce a different invocation ID")
	}
}

func TestHandleGraphChangeReplayUsesSameInvocation(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := uuid.NewString()
	seedGraphRunnable(t, ctx, store, domainID, "binding-a")
	eventID := uuid.New()
	node := graph.Node{ID: graph.NodeID(uuid.MustParse(nodeID)), DomainID: domainID, Labels: []string{"Page"}}
	event := graphchange.CommittedEvent{ID: eventID, SpaceID: spaceID, DomainID: domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID, Node: &node}}}

	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("first HandleGraphChange() error = %v", err)
	}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("replay HandleGraphChange() error = %v", err)
	}
	invs, err := store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(invs) != 1 {
		t.Fatalf("replayed graph event should create one invocation, got %+v", invs)
	}
	expectedID := graphTriggeredInvocationID(spaceID.String(), domainID, eventID.String(), "binding-a", nodeID)
	if invs[0].ID != expectedID || invs[0].EventID != eventID.String() || invs[0].BindingID != "binding-a" || invs[0].ChangedElementID != nodeID {
		t.Fatalf("unexpected deterministic invocation: %+v expected_id=%s", invs[0], expectedID)
	}
}

func TestHandleGraphChangeReplayDoesNotOverwriteCompletedInvocation(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := uuid.NewString()
	seedGraphRunnable(t, ctx, store, domainID, "binding-a")
	eventID := uuid.New()
	node := graph.Node{ID: graph.NodeID(uuid.MustParse(nodeID)), DomainID: domainID, Labels: []string{"Page"}}
	event := graphchange.CommittedEvent{ID: eventID, SpaceID: spaceID, DomainID: domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID, Node: &node}}}

	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("first HandleGraphChange() error = %v", err)
	}
	invs, err := store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 1 {
		t.Fatalf("ListInvocations() = %+v err=%v", invs, err)
	}
	completed := invs[0]
	completed.Status = "succeeded"
	completed.UpdatedAt = completed.UpdatedAt.Add(time.Minute)
	if err := store.PutInvocation(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("replay completed HandleGraphChange() error = %v", err)
	}
	got, err := store.GetInvocation(ctx, domainID, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "succeeded" || !got.UpdatedAt.Equal(completed.UpdatedAt) {
		t.Fatalf("replay overwrote completed invocation: %+v", got)
	}
}

func TestHandleGraphChangeRejectsDeterministicInvocationConflict(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := uuid.NewString()
	seedGraphRunnable(t, ctx, store, domainID, "binding-a")
	eventID := uuid.New()
	invocationID := graphTriggeredInvocationID(spaceID.String(), domainID, eventID.String(), "binding-a", nodeID)
	if err := store.PutInvocation(ctx, automation.Invocation{ID: invocationID, DomainID: domainID, SpaceID: spaceID.String(), AutomationID: "binding-a", AutomationVersion: 1, BindingID: "binding-a", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, EventID: "different-event", ChangedElementID: nodeID, ChangedElementKind: "node", EventType: automation.EventNodeUpdated, ActorPrincipalID: automationActor, OwnerPrincipalID: "owner", OnBehalfOfPrincipalID: "owner", AutomationOwnerPrincipalID: "owner", EventOriginPrincipalID: "principal-a", Status: "pending", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	node := graph.Node{ID: graph.NodeID(uuid.MustParse(nodeID)), DomainID: domainID, Labels: []string{"Page"}}
	event := graphchange.CommittedEvent{ID: eventID, SpaceID: spaceID, DomainID: domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID, Node: &node}}}

	err := mgr.HandleGraphChange(ctx, event)
	if err == nil || !strings.Contains(err.Error(), "different trigger metadata") {
		t.Fatalf("HandleGraphChange() error = %v, want deterministic conflict", err)
	}
	got, err := store.GetInvocation(ctx, domainID, invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EventID != "different-event" {
		t.Fatalf("conflict path overwrote existing invocation: %+v", got)
	}
}

func seedGraphRunnable(t *testing.T, ctx context.Context, store storage.Store, domainID graph.DomainID, bindingID string) {
	t.Helper()
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText}}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatalf("seed procedure: %v", err)
	}
	binding := automation.Binding{ID: bindingID, Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "owner", OnBehalfOfPrincipalID: "owner", InferenceProfile: "summary"}}
	if err := store.PutBinding(ctx, binding); err != nil {
		t.Fatalf("seed binding: %v", err)
	}
}
