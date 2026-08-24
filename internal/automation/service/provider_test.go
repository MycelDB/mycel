package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	inferenceconnectors "github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestGenerateTextRequiresConfiguredInference(t *testing.T) {
	mgr := NewManager(nil)
	run := automation.Run{}
	_, err := mgr.generateWithInference(context.Background(), automation.Definition{Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", Profile: "summarize"}}, automation.Invocation{}, "input", &run)
	if !errors.Is(err, ErrInferenceUnavailable) {
		t.Fatalf("generateWithInference() error = %v, want ErrInferenceUnavailable", err)
	}
	if run.Usage.Status != string(domaininference.UsageStatusFailed) {
		t.Fatalf("usage status = %q", run.Usage.Status)
	}
}

func TestGenerateTextRecordsInferenceUsage(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	mgr := NewManager(nil).WithInferenceManager(inference)
	run := automation.Run{ID: uuid.NewString()}
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, OwnerPrincipalID: "owner-a", Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: ids.spaceID, DomainID: ids.domainID, ChangedElementID: uuid.NewString(), ActorPrincipalID: "automation", OnBehalfOfPrincipalID: "principal-a", AutomationOwnerPrincipalID: "owner-a"}
	text, err := mgr.generateWithInference(ctx, def, inv, "input words", &run)
	if err != nil {
		t.Fatal(err)
	}
	if text != "result text" {
		t.Fatalf("text = %q", text)
	}
	if run.ProviderRequestID != "fake" || run.PolicyDecisionID == "" || run.CredentialID != ids.credentialID.String() || run.CredentialGrantID != ids.grantID.String() || run.ActorPrincipalID != "automation" || run.OnBehalfOfPrincipalID != "principal-a" || run.AutomationOwnerPrincipalID != "owner-a" {
		t.Fatalf("unexpected run inference provenance: %+v", run)
	}
	if run.Usage.InputTokens == 0 || run.Usage.OutputTokens == 0 || run.Usage.TotalTokens == 0 {
		t.Fatalf("unexpected usage: %+v", run.Usage)
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 1 {
		t.Fatalf("expected one fake chat call, got %d", chatCalls)
	}
	events, err := inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 || events[0].AutomationID != def.ID || events[0].AutomationRunID != run.ID || events[0].OnBehalfOfPrincipalID != "principal-a" {
		t.Fatalf("unexpected usage events: %#v err=%v", events, err)
	}
}

func TestGenerateTextRecordsDenialWithoutConnectorCall(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, true)
	mgr := NewManager(nil).WithInferenceManager(inference)
	run := automation.Run{ID: uuid.NewString()}
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: ids.spaceID, DomainID: ids.domainID, ChangedElementID: uuid.NewString(), ActorPrincipalID: "automation"}
	_, err := mgr.generateWithInference(ctx, def, inv, "input words", &run)
	if !errors.Is(err, inferenceservice.ErrDenied) {
		t.Fatalf("generateWithInference() error = %v, want denied", err)
	}
	if !strings.Contains(err.Error(), "inference denied by policy") {
		t.Fatalf("generateWithInference() error = %v, want policy decision reason", err)
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 0 {
		t.Fatalf("denied inference should not call connector, got %d", chatCalls)
	}
	if run.PolicyDecisionID == "" {
		t.Fatalf("denied run should retain policy decision: %+v", run)
	}
	if run.Usage.Status != string(domaininference.UsageStatusDenied) {
		t.Fatalf("denied run usage status = %q, want %q", run.Usage.Status, domaininference.UsageStatusDenied)
	}
	events, err := inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusDenied || events[0].PolicyDecisionID.String() != run.PolicyDecisionID {
		t.Fatalf("unexpected denied usage event: %#v err=%v", events, err)
	}
}

func TestAutomationDefinitionStableAcrossCredentialRotation(t *testing.T) {
	ctx := context.Background()
	inference, ids, _ := newAutomationInferenceRuntime(t, ctx, false)
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	before := def.Inference
	if _, err := inference.GlobalManager().UpsertSecret(ctx, domaininference.Secret{ID: domaininference.SecretID(uuid.New()), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", Kind: "none", SecretVersion: "rotated"}); err != nil {
		t.Fatalf("rotate secret: %v", err)
	}
	if !reflect.DeepEqual(def.Inference, before) {
		t.Fatalf("automation inference ref changed after credential rotation: before=%+v after=%+v", before, def.Inference)
	}
}

func TestGenerateTextEnforcesTokenCeilings(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	mgr := NewManager(nil).WithInferenceManager(inference).WithRunCeilings(1, 1)
	run := automation.Run{ID: uuid.NewString()}
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: ids.spaceID, DomainID: ids.domainID}
	_, err := mgr.generateWithInference(ctx, def, inv, "input words", &run)
	if err == nil {
		t.Fatal("expected token ceiling error")
	}

	def.Inference.Parameters.MaxOutputTokens = 100
	_, err = mgr.generateWithInference(ctx, def, inv, "input", &run)
	if err == nil || !strings.Contains(err.Error(), "exceeded by definition") {
		t.Fatalf("expected pre-call definition ceiling error, got %v", err)
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 1 {
		t.Fatalf("definition ceiling should not make another connector call, got %d", chatCalls)
	}
}

func TestGraphContextAutomationUpdatesConditionTarget(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	journalID := uuid.New()
	entryID := uuid.New()
	entryID2 := uuid.New()
	journal := graph.Node{ID: graph.NodeID(journalID), DomainID: ids.domainID, Labels: []string{"Journal"}, Properties: map[string]any{"date": "2026-08-18"}, Payload: map[string]any{}}
	entry := graph.Node{ID: graph.NodeID(entryID), DomainID: ids.domainID, Labels: []string{"JournalEntry"}, Payload: map[string]any{"text": "Did a thing"}}
	entry2 := graph.Node{ID: graph.NodeID(entryID2), DomainID: ids.domainID, Labels: []string{"JournalEntry"}, Payload: map[string]any{"text": "Did another thing"}}
	edge := graph.Edge{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: journal.ID, ToID: entry.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 1}}
	edge2 := graph.Edge{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: journal.ID, ToID: entry2.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 2}}
	graphs := &automationE2EGraph{nodes: map[string]graph.Node{journal.ID.String(): journal, entry.ID.String(): entry, entry2.ID.String(): entry2}, edges: []graph.Edge{edge, edge2}}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference)
	def := automation.Definition{ID: "daily", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Trigger: automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"JournalEntry"}}, Condition: automation.Condition{GQL: "MATCH (journal:Journal)-[r:HAS_ENTRY]->(changed:JournalEntry) RETURN changed, journal"}, Input: automation.Input{Target: "journal", Mode: automation.InputModeGQLTemplate, Template: "# {{journal.properties.date}}\n{{#each entries}}- {{entry.payload.text}}\n{{/each}}", Context: map[string]automation.ContextQuery{"entries": {GQL: "MATCH (journal)-[r:HAS_ENTRY]->(entry:JournalEntry) RETURN entry ORDER BY r.properties.position FETCH FIRST 20 ROWS ONLY", Limit: 20}}}, Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize journal", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "journal", Set: map[string]string{"payload.summary": "$result.text"}}}}}, Safety: automation.Safety{Idempotency: automation.Idempotency{Scope: "target", Target: "journal", SkipIfOutputUnchanged: true}}}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: entry.ID.String(), Node: &entry}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
	processed, err := mgr.ProcessPending(ctx, ids.domainID, 10)
	if err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d", processed)
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(invs) != 1 || invs[0].Status != "succeeded" {
		t.Fatalf("unexpected invocation state: %+v", invs)
	}
	updatedJournal := graphs.nodes[journal.ID.String()]
	if got := updatedJournal.Payload["summary"]; got != "result text" {
		t.Fatalf("journal summary payload = %#v", got)
	}
	if _, ok := graphs.nodes[entry.ID.String()].Payload["summary"]; ok {
		t.Fatal("entry should not have been updated")
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 1 {
		t.Fatalf("chat calls after first run = %d", chatCalls)
	}

	event.ID = uuid.New()
	event.Changes = []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: entry2.ID.String(), Node: &entry2}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange(second) error = %v", err)
	}
	processed, err = mgr.ProcessPending(ctx, ids.domainID, 10)
	if err != nil {
		t.Fatalf("ProcessPending(second) error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("second processed = %d", processed)
	}
	_, chatCalls = fake.Calls()
	if chatCalls != 1 {
		t.Fatalf("target-scoped idempotency should skip duplicate context, chat calls = %d", chatCalls)
	}
}

func TestGraphContextAutomationDebouncesByTarget(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	base := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	journalID := uuid.New()
	entryID1 := uuid.New()
	entryID2 := uuid.New()
	journal := graph.Node{ID: graph.NodeID(journalID), DomainID: ids.domainID, Labels: []string{"Journal"}, Payload: map[string]any{}}
	entry1 := graph.Node{ID: graph.NodeID(entryID1), DomainID: ids.domainID, Labels: []string{"JournalEntry"}, Payload: map[string]any{"text": "one"}}
	entry2 := graph.Node{ID: graph.NodeID(entryID2), DomainID: ids.domainID, Labels: []string{"JournalEntry"}, Payload: map[string]any{"text": "two"}}
	graphs := &automationE2EGraph{nodes: map[string]graph.Node{journal.ID.String(): journal, entry1.ID.String(): entry1, entry2.ID.String(): entry2}, edges: []graph.Edge{
		{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: journal.ID, ToID: entry1.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 1}},
		{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: journal.ID, ToID: entry2.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 2}},
	}}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference)
	mgr.now = func() time.Time { return base }
	def := automation.Definition{ID: "daily-debounce", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Trigger: automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"JournalEntry"}}, Condition: automation.Condition{GQL: "MATCH (journal:Journal)-[:HAS_ENTRY]->(changed:JournalEntry) RETURN changed, journal"}, Input: automation.Input{Target: "journal", Mode: automation.InputModeGQLTemplate, Template: "{{#each entries}}{{entry.payload.text}}\n{{/each}}", Context: map[string]automation.ContextQuery{"entries": {GQL: "MATCH (journal)-[r:HAS_ENTRY]->(entry:JournalEntry) RETURN entry ORDER BY r.properties.position FETCH FIRST 20 ROWS ONLY", Limit: 20}}}, Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize journal", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "journal", Set: map[string]string{"payload.summary": "$result.text"}}}}}, Safety: automation.Safety{Debounce: &automation.Debounce{Duration: "30s", CoalesceBy: "journal"}}}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	for _, node := range []graph.Node{entry1, entry2} {
		event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: node.ID.String(), Node: &node}}}
		if err := mgr.HandleGraphChange(ctx, event); err != nil {
			t.Fatalf("HandleGraphChange() error = %v", err)
		}
	}
	if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 0 {
		t.Fatalf("early ProcessPending() processed=%d err=%v", processed, err)
	}
	mgr.now = func() time.Time { return base.Add(31 * time.Second) }
	processed, err := mgr.ProcessPending(ctx, ids.domainID, 10)
	if err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if processed != 2 {
		t.Fatalf("processed = %d", processed)
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 1 {
		t.Fatalf("debounced context should call inference once, got %d", chatCalls)
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{AutomationID: def.ID})
	if err != nil {
		t.Fatal(err)
	}
	skipped := 0
	for _, inv := range invs {
		if inv.Status == "skipped" && inv.SkipReason == skipReasonCoalesced {
			skipped++
		}
	}
	if skipped != 1 {
		t.Fatalf("coalesced skipped invocations = %d, invs=%+v", skipped, invs)
	}
}

func TestGraphAutomationExecutesThroughInferenceEndToEnd(t *testing.T) {
	ctx := context.Background()
	inference, ids, _ := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	nodeID := uuid.New()
	node := graph.Node{ID: graph.NodeID(nodeID), DomainID: ids.domainID, Labels: []string{"Page"}, Properties: map[string]any{"title": "Hello"}, Payload: map[string]any{"text": "World"}}
	graphs := &automationE2EGraph{node: node}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference)
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Trigger: automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}}, Condition: automation.Condition{GQL: "MATCH (changed:Page) RETURN changed"}, Input: automation.Input{Target: "changed", Fields: []string{"properties.title", "payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
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
	runs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{Status: "succeeded"})
	if err != nil || len(runs) != 1 || runs[0].OnBehalfOfPrincipalID != "principal-a" {
		t.Fatalf("unexpected invocations: %+v err=%v", runs, err)
	}
	events, err := inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 || events[0].AutomationID != def.ID || events[0].UsageMode != domaininference.UsageModeAutomation || events[0].OnBehalfOfPrincipalID != "principal-a" {
		t.Fatalf("unexpected usage events: %#v err=%v", events, err)
	}
}

type automationInferenceIDs struct {
	spaceID      string
	domainID     graph.DomainID
	profileID    uuid.UUID
	endpointID   uuid.UUID
	modelID      uuid.UUID
	capabilityID uuid.UUID
	credentialID uuid.UUID
	secretID     uuid.UUID
	grantID      uuid.UUID
}

func newAutomationInferenceRuntime(t *testing.T, ctx context.Context, deny bool) (*inferenceservice.Module, automationInferenceIDs, *inferenceconnectors.FakeConnector) {
	t.Helper()
	module := inferenceservice.NewModule()
	if result := module.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	fake := &inferenceconnectors.FakeConnector{Text: "result text"}
	module.SetConnector(domaininference.ConnectorFake, fake)
	ids := automationInferenceIDs{spaceID: uuid.NewString(), domainID: graph.DomainID(uuid.New()), profileID: uuid.New(), endpointID: uuid.New(), modelID: uuid.New(), capabilityID: uuid.New(), credentialID: uuid.New(), secretID: uuid.New(), grantID: uuid.New()}
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, domaininference.Endpoint{ID: domaininference.EndpointID(ids.endpointID), Key: "fake", ConnectorType: domaininference.ConnectorFake, NetworkClass: domaininference.NetworkClassLocal, PrivacyClass: domaininference.PrivacyClassLocalOnly, Operations: []domaininference.Operation{domaininference.OperationChat}, Enabled: true}); err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	if _, err := module.GlobalManager().UpsertModel(ctx, domaininference.Model{ID: domaininference.ModelID(ids.modelID), Key: "fake-chat", Kind: domaininference.ModelKindGenerative, ProviderModelName: "fake-chat", Enabled: true}); err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := module.GlobalManager().UpsertCapability(ctx, domaininference.Capability{ID: domaininference.CapabilityID(ids.capabilityID), EndpointID: domaininference.EndpointID(ids.endpointID), ModelID: domaininference.ModelID(ids.modelID), Operation: domaininference.OperationChat, Enabled: true}); err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	if _, err := module.GlobalManager().UpsertSecret(ctx, domaininference.Secret{ID: domaininference.SecretID(ids.secretID), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", Kind: "none"}); err != nil {
		t.Fatalf("upsert secret: %v", err)
	}
	if _, err := module.GlobalManager().UpsertCredential(ctx, domaininference.Credential{ID: domaininference.CredentialID(ids.credentialID), Key: "cred", EndpointID: domaininference.EndpointID(ids.endpointID), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", AuthType: domaininference.CredentialAuthNone, SecretID: domaininference.SecretID(ids.secretID), Status: domaininference.CredentialStatusActive}); err != nil {
		t.Fatalf("upsert credential: %v", err)
	}
	spaceMgr, err := module.SpaceManager(ctx, ids.spaceID)
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	if _, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{ID: domaininference.ProfileID(ids.profileID), SpaceID: ids.spaceID, Key: "summarize", Operation: domaininference.OperationChat, DomainIDs: []string{ids.domainID.String()}, CapabilityRefs: []string{ids.capabilityID.String()}, Enabled: true}); err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	if _, err := spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(ids.grantID), SpaceID: ids.spaceID, CredentialID: domaininference.CredentialID(ids.credentialID), Scope: domaininference.Scope{SpaceID: ids.spaceID, DomainID: ids.domainID.String()}, Operations: []domaininference.Operation{domaininference.OperationChat}, ProfileRefs: []string{ids.profileID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeAutomation}, GranteePrincipals: []string{automationActor}, AllowOnBehalfOfPrincipals: []string{"principal-a"}, State: domaininference.GrantStateActive}); err != nil {
		t.Fatalf("upsert grant: %v", err)
	}
	action := domaininference.PolicyActionAllow
	if deny {
		action = domaininference.PolicyActionDeny
	}
	if _, err := spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: ids.spaceID, Scope: domaininference.Scope{SpaceID: ids.spaceID, DomainID: ids.domainID.String()}, Operations: []domaininference.Operation{domaininference.OperationChat}, ProfileRefs: []string{ids.profileID.String()}, Action: action, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert policy: %v", err)
	}
	return module, ids, fake
}

type automationE2ESessions struct {
	spaceID  string
	domainID string
}

func (s automationE2ESessions) OpenSession(context.Context, sessionservice.OpenSessionInput) (sessionservice.GraphSession, error) {
	return sessionservice.GraphSession{ID: "session", PrincipalID: automationActor, SpaceID: s.spaceID, DomainID: s.domainID, State: sessionservice.SessionStateActive}, nil
}
func (s automationE2ESessions) GetSession(context.Context, string, string) (sessionservice.GraphSession, error) {
	return sessionservice.GraphSession{}, nil
}
func (s automationE2ESessions) HeartbeatSession(context.Context, string, string, time.Duration) (sessionservice.GraphSession, error) {
	return sessionservice.GraphSession{}, nil
}
func (s automationE2ESessions) CloseSession(context.Context, string, string) (sessionservice.GraphSession, error) {
	return sessionservice.GraphSession{}, nil
}
func (s automationE2ESessions) BeginTransaction(context.Context, sessionservice.BeginTransactionInput) (sessionservice.GraphTransaction, error) {
	return sessionservice.GraphTransaction{ID: "tx", SessionID: "session", PrincipalID: automationActor, SpaceID: s.spaceID, DomainID: s.domainID, Mode: sessionservice.TransactionModeReadWrite, State: sessionservice.TransactionStateActive}, nil
}
func (s automationE2ESessions) GetTransaction(context.Context, string, string) (sessionservice.GraphTransaction, error) {
	return sessionservice.GraphTransaction{}, nil
}
func (s automationE2ESessions) CommitTransaction(context.Context, string, string, int32) (sessionservice.TransactionCommit, error) {
	return sessionservice.TransactionCommit{ID: "commit"}, nil
}
func (s automationE2ESessions) CommitTransactionAtRevision(context.Context, string, string, int32, int64) (sessionservice.TransactionCommit, error) {
	return sessionservice.TransactionCommit{ID: "commit"}, nil
}
func (s automationE2ESessions) RollbackTransaction(context.Context, string, string) (sessionservice.GraphTransaction, error) {
	return sessionservice.GraphTransaction{ID: "tx", State: sessionservice.TransactionStateRolledBack}, nil
}
func (s automationE2ESessions) CloseTransaction(context.Context, string, string) (sessionservice.GraphTransaction, error) {
	return sessionservice.GraphTransaction{}, nil
}

type automationE2EGraph struct {
	node  graph.Node
	nodes map[string]graph.Node
	edges []graph.Edge
}

func (g *automationE2EGraph) allNodes() []graph.Node {
	if len(g.nodes) == 0 {
		return []graph.Node{g.node}
	}
	out := make([]graph.Node, 0, len(g.nodes))
	for _, node := range g.nodes {
		out = append(out, node)
	}
	return out
}

func (g *automationE2EGraph) GetNode(_ context.Context, _ sessionservice.GraphTransaction, id string) (graph.Node, error) {
	if len(g.nodes) != 0 {
		if node, ok := g.nodes[id]; ok {
			return node, nil
		}
		return graph.Node{}, fmt.Errorf("node not found")
	}
	if g.node.ID.String() != id {
		return graph.Node{}, fmt.Errorf("node not found")
	}
	return g.node, nil
}
func (g *automationE2EGraph) ListNodes(_ context.Context, _ sessionservice.GraphTransaction, _ int, token string) ([]graph.Node, string, error) {
	if token != "" {
		return nil, "", nil
	}
	return g.allNodes(), "", nil
}
func (g *automationE2EGraph) CreateNode(context.Context, sessionservice.GraphTransaction, graphservice.NodeInput) (graph.Node, error) {
	return graph.Node{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) UpdateNode(_ context.Context, _ sessionservice.GraphTransaction, in graphservice.UpdateNodeInput) (graph.Node, error) {
	node, err := g.GetNode(context.Background(), sessionservice.GraphTransaction{}, in.NodeID)
	if err != nil {
		return graph.Node{}, err
	}
	node.Labels = append([]string(nil), in.Labels...)
	node.Properties = in.Properties
	node.Payload = in.Payload
	node.Meta = in.Meta
	if in.Content != nil {
		node.Content = *in.Content
	}
	node.Props = in.Props
	if len(g.nodes) != 0 {
		g.nodes[in.NodeID] = node
	} else {
		g.node = node
	}
	return node, nil
}
func (g *automationE2EGraph) UpsertNode(context.Context, sessionservice.GraphTransaction, graphservice.NodeInput) (graph.Node, error) {
	return graph.Node{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) DeleteNode(context.Context, sessionservice.GraphTransaction, string, bool) ([]string, []string, error) {
	return nil, nil, fmt.Errorf("unused")
}
func (g *automationE2EGraph) GetEdge(context.Context, sessionservice.GraphTransaction, string) (graph.Edge, error) {
	return graph.Edge{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) ListEdges(_ context.Context, _ sessionservice.GraphTransaction, _ int, token string) ([]graph.Edge, string, error) {
	if token != "" {
		return nil, "", nil
	}
	return g.edges, "", nil
}
func (g *automationE2EGraph) CreateEdge(context.Context, sessionservice.GraphTransaction, graphservice.EdgeInput) (graph.Edge, error) {
	return graph.Edge{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) UpdateEdge(context.Context, sessionservice.GraphTransaction, graphservice.UpdateEdgeInput) (graph.Edge, error) {
	return graph.Edge{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) DeleteEdge(context.Context, sessionservice.GraphTransaction, string) (string, error) {
	return "", fmt.Errorf("unused")
}
func (g *automationE2EGraph) ListChildren(context.Context, sessionservice.GraphTransaction, string) ([]graph.Edge, error) {
	return nil, fmt.Errorf("unused")
}
func (g *automationE2EGraph) GetParent(context.Context, sessionservice.GraphTransaction, string) (*graph.Edge, error) {
	return nil, fmt.Errorf("unused")
}
func (g *automationE2EGraph) MoveSubtree(context.Context, sessionservice.GraphTransaction, string, string, *int32) (graph.Edge, error) {
	return graph.Edge{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) ReorderChildren(context.Context, sessionservice.GraphTransaction, string, []string) ([]graph.Edge, error) {
	return nil, fmt.Errorf("unused")
}
func (g *automationE2EGraph) CurrentRevision(context.Context, string) (int64, error) { return 1, nil }
func (g *automationE2EGraph) CommitTransactionGraph(context.Context, sessionservice.GraphTransaction) (graphservice.CommitResult, error) {
	return graphservice.CommitResult{OperationCount: 1, CommittedRevision: 1}, nil
}
func (g *automationE2EGraph) DiscardTransactionGraph(context.Context, string) {}
func (g *automationE2EGraph) ConfigureIndexes(context.Context, sessionservice.GraphTransaction, string, []schemamodel.IndexDefinition) error {
	return nil
}
func (g *automationE2EGraph) ScanLabel(context.Context, sessionservice.GraphTransaction, graphservice.LabelScan) ([]graph.Node, string, graphservice.IndexedReadStats, error) {
	return nil, "", graphservice.IndexedReadStats{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) ScanTag(context.Context, sessionservice.GraphTransaction, graphservice.TagScan) ([]graph.Node, string, graphservice.IndexedReadStats, error) {
	return nil, "", graphservice.IndexedReadStats{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) ScanNodePropertyOrdered(context.Context, sessionservice.GraphTransaction, graphservice.OrderedNodePropertyScan) ([]graph.Node, string, graphservice.IndexedReadStats, error) {
	return nil, "", graphservice.IndexedReadStats{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) ScanAdjacency(_ context.Context, _ sessionservice.GraphTransaction, scan graphservice.AdjacencyScan) ([]graph.Edge, string, graphservice.IndexedReadStats, error) {
	if scan.Cursor != "" {
		return nil, "", graphservice.IndexedReadStats{}, nil
	}
	out := []graph.Edge{}
	for _, edge := range g.edges {
		if scan.Label != "" && !stringSliceContains(edge.Labels, scan.Label) {
			continue
		}
		switch scan.Direction {
		case graphservice.AdjacencyDirectionIn:
			if edge.ToID.String() == scan.NodeID {
				out = append(out, edge)
			}
		default:
			if edge.FromID.String() == scan.NodeID {
				out = append(out, edge)
			}
		}
	}
	return out, "", graphservice.IndexedReadStats{}, nil
}
func (g *automationE2EGraph) ScanSubtree(context.Context, sessionservice.GraphTransaction, graphservice.SubtreeScan) (graphservice.SubtreeResult, graphservice.IndexedReadStats, error) {
	return graphservice.SubtreeResult{}, graphservice.IndexedReadStats{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) BlobRefCount(context.Context, string, string) (int, error) {
	return 0, nil
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
