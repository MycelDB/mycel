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
	_, chatCalls := fake.Calls()
	if chatCalls != 0 {
		t.Fatalf("denied inference should not call connector, got %d", chatCalls)
	}
	if run.PolicyDecisionID == "" {
		t.Fatalf("denied run should retain policy decision: %+v", run)
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
	if _, err := module.GlobalManager().UpsertModel(ctx, domaininference.Model{ID: domaininference.ModelID(ids.modelID), Key: "fake-chat", Operation: domaininference.OperationChat, ProviderModelName: "fake-chat", Enabled: true}); err != nil {
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
	if _, err := spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(ids.grantID), SpaceID: ids.spaceID, CredentialID: domaininference.CredentialID(ids.credentialID), Scope: domaininference.Scope{SpaceID: ids.spaceID, DomainID: ids.domainID.String()}, Operations: []domaininference.Operation{domaininference.OperationChat}, ProfileRefs: []string{ids.profileID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeAutomation}, AllowOnBehalfOfPrincipals: []string{"principal-a"}, State: domaininference.GrantStateActive}); err != nil {
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

type automationE2EGraph struct{ node graph.Node }

func (g *automationE2EGraph) GetNode(_ context.Context, _ sessionservice.GraphTransaction, id string) (graph.Node, error) {
	if g.node.ID.String() != id {
		return graph.Node{}, fmt.Errorf("node not found")
	}
	return g.node, nil
}
func (g *automationE2EGraph) ListNodes(_ context.Context, _ sessionservice.GraphTransaction, _ int, token string) ([]graph.Node, string, error) {
	if token != "" {
		return nil, "", nil
	}
	return []graph.Node{g.node}, "", nil
}
func (g *automationE2EGraph) CreateNode(context.Context, sessionservice.GraphTransaction, graphservice.NodeInput) (graph.Node, error) {
	return graph.Node{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) UpdateNode(_ context.Context, _ sessionservice.GraphTransaction, in graphservice.UpdateNodeInput) (graph.Node, error) {
	if g.node.ID.String() != in.NodeID {
		return graph.Node{}, fmt.Errorf("node not found")
	}
	g.node.Labels = append([]string(nil), in.Labels...)
	g.node.Properties = in.Properties
	g.node.Payload = in.Payload
	g.node.Meta = in.Meta
	if in.Content != nil {
		g.node.Content = *in.Content
	}
	g.node.Props = in.Props
	return g.node, nil
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
func (g *automationE2EGraph) ListEdges(context.Context, sessionservice.GraphTransaction, int, string) ([]graph.Edge, string, error) {
	return nil, "", nil
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
func (g *automationE2EGraph) ScanAdjacency(context.Context, sessionservice.GraphTransaction, graphservice.AdjacencyScan) ([]graph.Edge, string, graphservice.IndexedReadStats, error) {
	return nil, "", graphservice.IndexedReadStats{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) ScanSubtree(context.Context, sessionservice.GraphTransaction, graphservice.SubtreeScan) (graphservice.SubtreeResult, graphservice.IndexedReadStats, error) {
	return graphservice.SubtreeResult{}, graphservice.IndexedReadStats{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) BlobRefCount(context.Context, string, string) (int, error) {
	return 0, nil
}
