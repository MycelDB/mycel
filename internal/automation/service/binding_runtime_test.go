package service

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestBindingRuntimePrincipalOverridesOperatorCreatedContext(t *testing.T) {
	ctx := context.Background()
	inference, ids, _ := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	nodeID := uuid.New()
	node := graph.Node{ID: graph.NodeID(nodeID), DomainID: ids.domainID, Labels: []string{"Page"}, Properties: map[string]any{"title": "Hello"}, Payload: map[string]any{"text": "World"}}
	graphs := &automationE2EGraph{node: node}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference)

	procedure := automation.Procedure{ID: "knot-pkm.page-summary", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"properties.title", "payload.text"}}, Inference: automation.InferenceRef{Operation: "chat"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	binding := automation.Binding{ID: "user.page-summary", Version: 1, DomainID: ids.domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: ids.spaceID, DomainID: ids.domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: "automation", OwnerPrincipalID: "principal-a", OnBehalfOfPrincipalID: "principal-a", InferenceProfileID: ids.profileID.String()}}
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	if err := store.PutBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}

	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "operator-admin"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 1 {
		t.Fatalf("invocations = %+v err=%v", invs, err)
	}
	if invs[0].BindingID != binding.ID || invs[0].ProcedureID != procedure.ID || invs[0].ActorPrincipalID != "automation" || invs[0].OnBehalfOfPrincipalID != "principal-a" || invs[0].EventOriginPrincipalID != "operator-admin" {
		t.Fatalf("unexpected invocation runtime: %+v", invs[0])
	}

	processed, err := mgr.ProcessPending(ctx, ids.domainID, 10)
	if err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d", processed)
	}
	if got := graphs.node.Payload["summary"]; got != "result text" {
		t.Fatalf("summary payload = %#v", got)
	}
	run, err := store.GetRun(ctx, ids.domainID, invs[0].ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != "succeeded" || run.BindingID != binding.ID || run.ProcedureID != procedure.ID || run.OnBehalfOfPrincipalID != "principal-a" || run.EventOriginPrincipalID != "operator-admin" || run.CredentialGrantID == "" {
		t.Fatalf("unexpected run: %+v", run)
	}
}

func TestLegacyDefinitionKeepsEventOriginOverride(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	nodeID := uuid.New()
	node := graph.Node{ID: graph.NodeID(nodeID), DomainID: domainID, Labels: []string{"Page"}}
	mgr := NewManager(store)
	def, err := mgr.CreateAutomationAs(ctx, domainID, automationDefinitionJSON("legacy-runtime", ""), "operator-admin")
	if err != nil {
		t.Fatalf("CreateAutomationAs() error = %v", err)
	}
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.New()), DomainID: domainID, Origin: graphchange.OriginMetadata{PrincipalID: "event-user"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
	invs, err := store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 1 {
		t.Fatalf("invocations = %+v err=%v", invs, err)
	}
	if invs[0].BindingID != def.ID || invs[0].ProcedureID != automation.LegacyProcedureID(def.ID) || invs[0].OnBehalfOfPrincipalID != "event-user" || invs[0].OwnerPrincipalID != "operator-admin" {
		t.Fatalf("legacy invocation did not preserve old event-origin override: %+v", invs[0])
	}
}

func TestProcedureBindingGraphContextUpdatesSelectedTarget(t *testing.T) {
	ctx := context.Background()
	inference, ids, _ := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	pageID := uuid.New()
	entryID := uuid.New()
	page := graph.Node{ID: graph.NodeID(pageID), DomainID: ids.domainID, Labels: []string{"Page"}, Properties: map[string]any{"title": "Project"}, Payload: map[string]any{}}
	entry := graph.Node{ID: graph.NodeID(entryID), DomainID: ids.domainID, Labels: []string{"Entry"}, Payload: map[string]any{"text": "Launch notes"}}
	graphs := &automationE2EGraph{nodes: map[string]graph.Node{page.ID.String(): page, entry.ID.String(): entry}, edges: []graph.Edge{{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: page.ID, ToID: entry.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 1}}}}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference)
	procedure := automation.Procedure{ID: "page-summary-procedure", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "page", Mode: automation.InputModeGQLTemplate, Template: "{{page.properties.title}}\n{{#each entries}}{{entry.payload.text}}\n{{/each}}", Context: map[string]automation.ContextQuery{"entries": {GQL: "MATCH (page)-[r:HAS_ENTRY]->(entry:Entry) RETURN entry ORDER BY r.properties.position FETCH FIRST 20 ROWS ONLY", Limit: 20}}}, Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize page", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "page", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	binding := automation.Binding{ID: "entry-trigger-binding", Version: 1, DomainID: ids.domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: ids.spaceID, DomainID: ids.domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Entry"}, Condition: automation.Condition{GQL: "MATCH (page:Page)-[r:HAS_ENTRY]->(changed:Entry) RETURN changed, page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "principal-a", OnBehalfOfPrincipalID: "principal-a"}}
	putProcedureAndBinding(t, ctx, store, procedure, binding)

	emitNodeUpdated(t, ctx, mgr, ids.spaceID, ids.domainID, "principal-a", entry)
	if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 1 {
		t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
	}
	updatedPage := graphs.nodes[page.ID.String()]
	if got := updatedPage.Payload["summary"]; got != "result text" {
		t.Fatalf("page summary payload = %#v", got)
	}
	if _, ok := graphs.nodes[entry.ID.String()].Payload["summary"]; ok {
		t.Fatal("entry trigger should not be the updated output target")
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{Status: "succeeded"})
	if err != nil || len(invs) != 1 {
		t.Fatalf("succeeded invocations: %+v err=%v", invs, err)
	}
	run, err := store.GetRun(ctx, ids.domainID, invs[0].ID)
	if err != nil || run.BindingID != binding.ID || run.ProcedureID != procedure.ID || run.TargetAlias != "page" || run.TargetNodeID != page.ID.String() || len(run.Context) == 0 {
		t.Fatalf("unexpected run diagnostics: %+v err=%v", run, err)
	}
}

func TestSharedProcedureRunsWithDistinctBindingProfilesAndPrincipals(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	secondProfileID, secondGrantID := addAutomationProfileGrant(t, ctx, inference, ids, "summarize-b", "principal-b", domaininference.OperationChat)
	store := storage.NewFileStore(t.TempDir())
	pageID := uuid.New()
	noteID := uuid.New()
	page := graph.Node{ID: graph.NodeID(pageID), DomainID: ids.domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "Page text"}}
	note := graph.Node{ID: graph.NodeID(noteID), DomainID: ids.domainID, Labels: []string{"Note"}, Payload: map[string]any{"text": "Note text"}}
	graphs := &automationE2EGraph{nodes: map[string]graph.Node{page.ID.String(): page, note.ID.String(): note}}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference)
	procedure := automation.Procedure{ID: "shared-summary", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	bindingA := automation.Binding{ID: "page-binding", Version: 1, DomainID: ids.domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: ids.spaceID, DomainID: ids.domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "principal-a", OnBehalfOfPrincipalID: "principal-a", InferenceProfileID: ids.profileID.String()}}
	bindingB := automation.Binding{ID: "note-binding", Version: 1, DomainID: ids.domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: ids.spaceID, DomainID: ids.domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Note"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "principal-b", OnBehalfOfPrincipalID: "principal-b", InferenceProfileID: secondProfileID.String()}}
	putProcedureAndBinding(t, ctx, store, procedure, bindingA, bindingB)

	emitNodeUpdated(t, ctx, mgr, ids.spaceID, ids.domainID, "operator-admin", page)
	emitNodeUpdated(t, ctx, mgr, ids.spaceID, ids.domainID, "operator-admin", note)
	if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 2 {
		t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 2 {
		t.Fatalf("expected two delegated calls, got %d", chatCalls)
	}
	events, err := inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 2 {
		t.Fatalf("usage events = %+v err=%v", events, err)
	}
	seen := map[string]domaininference.ProfileID{}
	for _, event := range events {
		if event.ActorPrincipalID != automationActor || event.Metadata["procedure_id"] != procedure.ID || event.Metadata["binding_id"] == "" || event.CredentialGrantID == uuid.Nil {
			t.Fatalf("usage event missing delegated diagnostics: %+v", event)
		}
		seen[event.OnBehalfOfPrincipalID] = event.ProfileID
	}
	if seen["principal-a"] != domaininference.ProfileID(ids.profileID) || seen["principal-b"] != domaininference.ProfileID(secondProfileID) {
		t.Fatalf("binding profiles did not override/fan out as expected: %+v", seen)
	}
	if !usageEventsIncludeGrant(events, domaininference.CredentialGrantID(ids.grantID)) || !usageEventsIncludeGrant(events, domaininference.CredentialGrantID(secondGrantID)) {
		t.Fatalf("usage events did not include expected grants: %+v", events)
	}
}

func TestBindingDelegatedInferenceFailsClosed(t *testing.T) {
	cases := []struct {
		name       string
		onBehalf   string
		domain     func(automationInferenceIDs) graph.DomainID
		operation  domaininference.Operation
		wantReason string
	}{
		{name: "no_on_behalf_grant", onBehalf: "principal-b", domain: func(ids automationInferenceIDs) graph.DomainID { return ids.domainID }, operation: domaininference.OperationChat, wantReason: "no active credential grant matches request"},
		{name: "wrong_domain", onBehalf: "principal-a", domain: func(ids automationInferenceIDs) graph.DomainID { return graph.DomainID(uuid.New()) }, operation: domaininference.OperationChat, wantReason: "inference profile does not allow domain"},
		{name: "wrong_operation", onBehalf: "principal-a", domain: func(ids automationInferenceIDs) graph.DomainID { return ids.domainID }, operation: domaininference.OperationSummarize, wantReason: "does not match request operation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
			domainID := tc.domain(ids)
			store := storage.NewFileStore(t.TempDir())
			nodeID := uuid.New()
			node := graph.Node{ID: graph.NodeID(nodeID), DomainID: domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "World"}}
			graphs := &automationE2EGraph{node: node}
			sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: domainID.String()}
			mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference)
			procedure := automation.Procedure{ID: "delegated-denial", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: string(tc.operation), ProfileID: ids.profileID.String()}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
			binding := automation.Binding{ID: "delegated-denial-binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: ids.spaceID, DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: tc.onBehalf, OnBehalfOfPrincipalID: tc.onBehalf, InferenceProfileID: ids.profileID.String()}}
			putProcedureAndBinding(t, ctx, store, procedure, binding)
			emitNodeUpdated(t, ctx, mgr, ids.spaceID, domainID, "operator-admin", node)
			if processed, err := mgr.ProcessPending(ctx, domainID, 10); err != nil || processed != 1 {
				t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
			}
			invs, err := store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
			if err != nil || len(invs) != 1 || invs[0].Status != "failed" || !strings.Contains(invs[0].SkipReason, tc.wantReason) {
				t.Fatalf("unexpected failed invocation: %+v err=%v", invs, err)
			}
			run, err := store.GetRun(ctx, domainID, invs[0].ID)
			if err != nil || run.Status != "failed" || run.CredentialGrantID != "" && tc.name == "no_on_behalf_grant" || run.ActorPrincipalID != automationActor || run.OnBehalfOfPrincipalID != tc.onBehalf || !strings.Contains(run.Error, tc.wantReason) {
				t.Fatalf("unexpected failed run: %+v err=%v", run, err)
			}
			_, chatCalls := fake.Calls()
			if chatCalls != 0 {
				t.Fatalf("denied inference should not call connector, got %d", chatCalls)
			}
			events, err := inference.UsageLedger().ListUsageEvents(ctx)
			if err != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusDenied || events[0].Metadata["binding_id"] != binding.ID || events[0].Metadata["procedure_id"] != procedure.ID {
				t.Fatalf("unexpected denied usage event: %+v err=%v", events, err)
			}
		})
	}
}

func TestDisabledBindingOrProcedureDoesNotInvoke(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	domainID := graph.DomainID(uuid.New())
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Page"}}
	mgr := NewManager(store)
	procedure := automation.Procedure{ID: "proc", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "profile"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	disabledBinding := automation.Binding{ID: "disabled-binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusDisabled, Scope: automation.BindingScope{DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "principal-a", OnBehalfOfPrincipalID: "principal-a", InferenceProfile: "profile"}}
	putProcedureAndBinding(t, ctx, store, procedure, disabledBinding)
	emitNodeUpdated(t, ctx, mgr, uuid.NewString(), domainID, "operator-admin", node)
	invs, err := store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 0 {
		t.Fatalf("disabled binding should not enqueue invocations: %+v err=%v", invs, err)
	}

	disabledProcedure := procedure
	disabledProcedure.ID = "disabled-proc"
	disabledProcedure.Status = automation.StatusDisabled
	enabledBinding := disabledBinding
	enabledBinding.ID = "enabled-binding-for-disabled-proc"
	enabledBinding.Status = automation.StatusEnabled
	enabledBinding.ProcedureID = disabledProcedure.ID
	putProcedureAndBinding(t, ctx, store, disabledProcedure, enabledBinding)
	emitNodeUpdated(t, ctx, mgr, uuid.NewString(), domainID, "operator-admin", node)
	invs, err = store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 0 {
		t.Fatalf("disabled procedure should not enqueue invocations: %+v err=%v", invs, err)
	}
	if err := automation.ValidateBinding(enabledBinding, &disabledProcedure); err == nil {
		t.Fatal("enabled binding for disabled procedure should fail validation")
	}
}

func putProcedureAndBinding(t *testing.T, ctx context.Context, store *storage.FileStore, procedure automation.Procedure, bindings ...automation.Binding) {
	t.Helper()
	if err := store.PutProcedure(ctx, procedure); err != nil {
		t.Fatalf("put procedure: %v", err)
	}
	for _, binding := range bindings {
		if err := store.PutBinding(ctx, binding); err != nil {
			t.Fatalf("put binding %s: %v", binding.ID, err)
		}
	}
}

func emitNodeUpdated(t *testing.T, ctx context.Context, mgr *AutomationManager, spaceID string, domainID graph.DomainID, origin string, node graph.Node) {
	t.Helper()
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(spaceID)), DomainID: domainID, Origin: graphchange.OriginMetadata{PrincipalID: origin}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: node.ID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
}

func addAutomationProfileGrant(t *testing.T, ctx context.Context, module *inferenceservice.Module, ids automationInferenceIDs, key string, onBehalf string, operation domaininference.Operation) (uuid.UUID, uuid.UUID) {
	t.Helper()
	profileID := uuid.New()
	grantID := uuid.New()
	policyID := uuid.New()
	spaceMgr, err := module.SpaceManager(ctx, ids.spaceID)
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	if _, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{ID: domaininference.ProfileID(profileID), SpaceID: ids.spaceID, Key: key, Operation: operation, DomainIDs: []string{ids.domainID.String()}, CapabilityRefs: []string{ids.capabilityID.String()}, Enabled: true}); err != nil {
		t.Fatalf("upsert second profile: %v", err)
	}
	if _, err := spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(grantID), SpaceID: ids.spaceID, CredentialID: domaininference.CredentialID(ids.credentialID), Scope: domaininference.Scope{SpaceID: ids.spaceID, DomainID: ids.domainID.String()}, Operations: []domaininference.Operation{operation}, ProfileRefs: []string{profileID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeAutomation}, GranteePrincipals: []string{automationActor}, AllowOnBehalfOfPrincipals: []string{onBehalf}, State: domaininference.GrantStateActive}); err != nil {
		t.Fatalf("upsert second grant: %v", err)
	}
	if _, err := spaceMgr.UpsertPolicy(ctx, domaininference.Policy{ID: domaininference.PolicyID(policyID), SpaceID: ids.spaceID, Scope: domaininference.Scope{SpaceID: ids.spaceID, DomainID: ids.domainID.String()}, Operations: []domaininference.Operation{operation}, ProfileRefs: []string{profileID.String()}, Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert second policy: %v", err)
	}
	return profileID, grantID
}

func usageEventsIncludeGrant(events []domaininference.UsageEvent, grantID domaininference.CredentialGrantID) bool {
	for _, event := range events {
		if event.CredentialGrantID == grantID {
			return true
		}
	}
	return false
}
