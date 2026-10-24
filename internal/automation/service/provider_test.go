package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	inferenceconnectors "github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
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
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithInferenceInvoker(inference)
	run := automation.Run{ID: uuid.NewString(), BindingID: "summarize-binding", BindingVersion: 1, ProcedureID: "summarize", ProcedureVersion: 1, AttemptNumber: 1}
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, OwnerPrincipalID: "owner-a", Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: ids.spaceID, DomainID: ids.domainID, BindingID: run.BindingID, BindingVersion: run.BindingVersion, ProcedureID: run.ProcedureID, ProcedureVersion: run.ProcedureVersion, ChangedElementID: uuid.NewString(), ActorPrincipalID: "automation", OnBehalfOfPrincipalID: "principal-a", AutomationOwnerPrincipalID: "owner-a"}
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
	ledger, err := mgr.store.GetProcedureInferenceLedger(ctx, inv.SpaceID)
	if err != nil || ledger.HighWaterSequence != 5 || len(ledger.Attempts) != 1 || ledger.Attempts[0].State != automation.InferenceAttemptStateSucceeded {
		t.Fatalf("unexpected procedure inference ledger: %+v err=%v", ledger, err)
	}
	encoded, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	for _, prohibited := range []string{"input words", "result text", "\"provider_request_id\"", "\"credential_id\"", "\"error\""} {
		if strings.Contains(string(encoded), prohibited) {
			t.Fatalf("procedure inference ledger contains prohibited value %q: %s", prohibited, encoded)
		}
	}
}

func TestGenerateTextRecordsDenialWithoutConnectorCall(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, true)
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithInferenceInvoker(inference)
	run := automation.Run{ID: uuid.NewString(), BindingID: "summarize-binding", BindingVersion: 1, ProcedureID: "summarize", ProcedureVersion: 1, AttemptNumber: 1}
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: ids.spaceID, DomainID: ids.domainID, BindingID: run.BindingID, BindingVersion: run.BindingVersion, ProcedureID: run.ProcedureID, ProcedureVersion: run.ProcedureVersion, ChangedElementID: uuid.NewString(), ActorPrincipalID: "automation"}
	_, err := mgr.generateWithInference(ctx, def, inv, "input words", &run)
	if !errors.Is(err, inferenceruntime.ErrDenied) {
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
	ledger, err := mgr.store.GetProcedureInferenceLedger(ctx, inv.SpaceID)
	if err != nil || ledger.HighWaterSequence != 2 || len(ledger.Attempts) != 1 {
		t.Fatalf("unexpected denied attempt ledger: %+v err=%v", ledger, err)
	}
	attempt := ledger.Attempts[0]
	if attempt.State != automation.InferenceAttemptStateDenied || !attempt.StartedAt.IsZero() || attempt.PolicyDecisionID != run.PolicyDecisionID {
		t.Fatalf("denied attempt incorrectly crossed provider boundary: %+v", attempt)
	}
}

func TestGenerateTextPreservesPreProviderFailureAsRetryableEvidence(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store).WithInferenceInvoker(preProviderFailureInference{})
	domainID := graph.DomainID(uuid.New())
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: uuid.NewString(), DomainID: domainID, BindingID: "binding", BindingVersion: 1, ProcedureID: "proc", ProcedureVersion: 1, ChangedElementID: uuid.NewString()}
	run := automation.Run{ID: uuid.NewString(), BindingID: inv.BindingID, BindingVersion: 1, ProcedureID: inv.ProcedureID, ProcedureVersion: 1, AttemptNumber: 1}
	def := automation.Definition{ID: "proc", Version: 1, DomainID: domainID, Inference: automation.InferenceRef{Operation: "chat", Profile: "profile"}}
	_, err := mgr.generateWithInference(ctx, def, inv, "input", &run)
	if err == nil || errors.Is(err, ErrInferenceAttemptAlreadyStarted) || !strings.Contains(err.Error(), "routing unavailable") {
		t.Fatalf("generateWithInference() error=%v", err)
	}
	if disposition := inferenceFailurePublicationDisposition(err); disposition != "not_published_pre_provider_failure" {
		t.Fatalf("pre-provider disposition=%q", disposition)
	}
	if err := mgr.recordRunPublicationDispositionWithDiagnostics(ctx, run, inferenceFailurePublicationDisposition(err)); err != nil {
		t.Fatalf("record pre-provider disposition: %v", err)
	}
	ledger, ledgerErr := store.GetProcedureInferenceLedger(ctx, inv.SpaceID)
	if ledgerErr != nil || len(ledger.Attempts) != 1 || ledger.Attempts[0].State != automation.InferenceAttemptStateFailed || !ledger.Attempts[0].StartedAt.IsZero() || ledger.Attempts[0].PublicationDisposition != "not_published_pre_provider_failure" {
		t.Fatalf("unexpected pre-provider evidence: %+v err=%v", ledger, ledgerErr)
	}
}

type preProviderFailureInference struct{}

func (preProviderFailureInference) Invoke(context.Context, inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error) {
	return inferenceruntime.InvokeResponse{Status: domaininference.UsageStatusFailed}, errors.New("routing unavailable")
}

func TestTerminalAttemptPersistenceFailureIsAmbiguousAndDoesNotReplayProvider(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	baseStore := storage.NewFileStore(t.TempDir())
	store := &terminalAttemptFailureStore{Store: baseStore, remainingFailures: 1}
	var logs bytes.Buffer
	mgr := NewManager(store).WithInferenceInvoker(inference).WithLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	run := automation.Run{ID: uuid.NewString(), BindingID: "summarize-binding", BindingVersion: 1, ProcedureID: "summarize", ProcedureVersion: 1, AttemptNumber: 1}
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: ids.spaceID, DomainID: ids.domainID, BindingID: run.BindingID, BindingVersion: run.BindingVersion, ProcedureID: run.ProcedureID, ProcedureVersion: run.ProcedureVersion, ChangedElementID: uuid.NewString(), ActorPrincipalID: automationActor}

	_, err := mgr.generateWithInference(ctx, def, inv, "input", &run)
	if err == nil || inferenceFailurePublicationDisposition(err) != "not_published_provider_ambiguous" {
		t.Fatalf("terminal persistence error=%v disposition=%q", err, inferenceFailurePublicationDisposition(err))
	}
	if err := mgr.recordRunPublicationDispositionWithDiagnostics(ctx, run, inferenceFailurePublicationDisposition(err)); err == nil {
		t.Fatal("started attempt unexpectedly accepted a terminal publication disposition")
	}
	ledger, ledgerErr := baseStore.GetProcedureInferenceLedger(ctx, inv.SpaceID)
	if ledgerErr != nil || len(ledger.Attempts) != 1 || ledger.Attempts[0].State != automation.InferenceAttemptStateStarted {
		t.Fatalf("durable attempt is not started after ambiguous persistence: %+v err=%v", ledger, ledgerErr)
	}
	if !strings.Contains(logs.String(), `"stage":"disposition_not_published_provider_ambiguous"`) || !strings.Contains(logs.String(), `"result":"failed"`) {
		t.Fatalf("ambiguous persistence diagnostic missing: %s", logs.String())
	}

	_, secondErr := mgr.generateWithInference(ctx, def, inv, "input", &run)
	if secondErr == nil {
		t.Fatal("reprocessing a durable started attempt unexpectedly succeeded")
	}
	if _, calls := fake.Calls(); calls != 1 {
		t.Fatalf("provider replayed after terminal persistence ambiguity: calls=%d", calls)
	}
}

type terminalAttemptFailureStore struct {
	storage.Store
	remainingFailures int
}

func (s *terminalAttemptFailureStore) PutProcedureInferenceLedger(ctx context.Context, ledger automation.ProcedureInferenceLedger) error {
	if s.remainingFailures > 0 {
		for _, attempt := range ledger.Attempts {
			if automation.IsTerminalProcedureInferenceAttemptState(attempt.State) {
				s.remainingFailures--
				return errors.New("terminal attempt persistence unavailable")
			}
		}
	}
	return s.Store.PutProcedureInferenceLedger(ctx, ledger)
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
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithInferenceInvoker(inference).WithRunCeilings(1, 1)
	run := automation.Run{ID: uuid.NewString(), BindingID: "summarize-binding", BindingVersion: 1, ProcedureID: "summarize", ProcedureVersion: 1, AttemptNumber: 1}
	def := automation.Definition{ID: "summarize", Version: 1, DomainID: ids.domainID, Prompt: "prompt", Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: ids.spaceID, DomainID: ids.domainID, BindingID: run.BindingID, BindingVersion: run.BindingVersion, ProcedureID: run.ProcedureID, ProcedureVersion: run.ProcedureVersion}
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

func TestAutomationSuppressesPublicationWhenProviderInputChanges(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "before"}, Properties: map[string]any{}}
	graphs := &automationE2EGraph{node: node}
	sessions := automationE2ESessions{spaceID: spaceID, domainID: domainID.String()}
	inference := &conflictInjectingInference{text: "stale summary"}
	inference.onFirstInvoke = func(context.Context) error {
		graphs.node.Payload["text"] = "after"
		return nil
	}
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference)
	def := automation.Definition{ID: "summary", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: automation.InferenceRef{Operation: "chat", Profile: "summary"}, Prompt: "Summarize", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, AutomationID: def.ID, AutomationVersion: 1, BindingID: def.ID, BindingVersion: 1, ProcedureID: def.ID, ProcedureVersion: 1, ChangedElementID: node.ID.String(), ChangedElementKind: "node", EventType: automation.EventNodeUpdated, ActorPrincipalID: automationActor}
	if _, err := mgr.executeInvocation(ctx, def, inv); err == nil || !strings.Contains(err.Error(), "provider input changed") {
		t.Fatalf("executeInvocation() error=%v, want stale provider input fence", err)
	}
	if _, ok := graphs.node.Payload["summary"]; ok {
		t.Fatalf("stale provider output was published: %+v", graphs.node)
	}
	inference.mu.Lock()
	calls := inference.invocations
	inference.mu.Unlock()
	if calls != 1 {
		t.Fatalf("provider calls=%d, want 1", calls)
	}
}

func TestAutomationSuppressesPublicationWhenImageContentChanges(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	spaceID := uuid.NewString()
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Image"}, Payload: map[string]any{"blob_id": "blob-a", "mime_type": "image/png"}, Properties: map[string]any{}}
	graphs := &automationE2EGraph{node: node}
	sessions := automationE2ESessions{spaceID: spaceID, domainID: domainID.String()}
	blobs := fakeAutomationBlobReader{blobs: map[string]fakeAutomationBlob{"blob-a": {data: []byte("before")}}}
	inference := &conflictInjectingInference{text: "stale image description"}
	inference.onFirstInvoke = func(context.Context) error {
		blobs.blobs["blob-a"] = fakeAutomationBlob{data: []byte("after")}
		return nil
	}
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference).WithBlobReader(blobs)
	def := automation.Definition{ID: "describe", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.blob_id"}}, Inference: automation.InferenceRef{Operation: string(domaininference.OperationImageAnalysis), Profile: "image"}, Prompt: "Describe", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.description": "$result.text"}}}}}}
	inv := automation.Invocation{ID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, AutomationID: def.ID, AutomationVersion: 1, BindingID: def.ID, BindingVersion: 1, ProcedureID: def.ID, ProcedureVersion: 1, ChangedElementID: node.ID.String(), ChangedElementKind: "node", EventType: automation.EventNodeUpdated, ActorPrincipalID: automationActor}
	if _, err := mgr.executeInvocation(ctx, def, inv); err == nil || !strings.Contains(err.Error(), "provider input changed") {
		t.Fatalf("executeInvocation() error=%v, want stale image input fence", err)
	}
	if _, ok := graphs.node.Payload["description"]; ok {
		t.Fatalf("stale image output was published: %+v", graphs.node)
	}
}

func TestAutomationFinalizesAfterGraphCommitCancelsRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inference, ids, _ := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: ids.domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "World"}}
	acknowledgements := 0
	graphs := &automationE2EGraph{node: node, cancelOnCommit: cancel, acknowledgements: &acknowledgements}
	commitCalls := 0
	var commitContextError error
	sessions := automationE2ESessions{
		spaceID: ids.spaceID, domainID: ids.domainID.String(),
		commitCalls: &commitCalls, commitContextError: &commitContextError,
	}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference)
	def := automation.Definition{
		ID: "cancel-after-graph-commit", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled,
		Trigger:   automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}},
		Input:     automation.Input{Target: "changed", Fields: []string{"payload.text"}},
		Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize",
		Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}},
	}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	event := graphchange.CommittedEvent{
		ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID,
		Origin:  graphchange.OriginMetadata{PrincipalID: "principal-a"},
		Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: node.ID.String(), Node: &node}},
	}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
	_, err := mgr.ProcessPending(ctx, ids.domainID, 10)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("request context error = %v, want canceled", ctx.Err())
	}
	if commitCalls != 1 {
		t.Fatalf("session commit calls = %d, want 1", commitCalls)
	}
	if commitContextError != nil {
		t.Fatalf("session commit context error = %v, want nil", commitContextError)
	}
	if acknowledgements != 2 {
		t.Fatalf("graph finalization acknowledgements = %d, want input rollback and publication commit", acknowledgements)
	}
}

func TestAutomationPublicationTimeoutReconcilesWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: ids.domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "World"}}
	graphs := &automationE2EGraph{node: node, waitForCommitContext: true}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	var publicationLogs bytes.Buffer
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference).WithLogger(slog.New(slog.NewJSONHandler(&publicationLogs, nil)))
	mgr.publicationTimeout = 500 * time.Millisecond
	def := automation.Definition{
		ID: "bounded-publication", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled,
		Trigger:   automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}},
		Input:     automation.Input{Target: "changed", Fields: []string{"payload.text"}},
		Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize",
		Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}},
		Safety: automation.Safety{MaxAttempts: 1},
	}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: node.ID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	processed, err := mgr.ProcessPending(ctx, ids.domainID, 10)
	if err != nil || processed != 1 {
		t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("publication timeout was not bounded: %s", elapsed)
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 1 || invs[0].Status != "retryable" {
		t.Fatalf("invocations=%+v err=%v", invs, err)
	}
	if _, chatCalls := fake.Calls(); chatCalls != 1 {
		t.Fatalf("provider calls=%d", chatCalls)
	}
	logged := publicationLogs.String()
	for _, required := range []string{`"stage":"pending_persistence"`, `"stage":"graph_commit"`, `"result":"failed"`, `"category":"deadline_exceeded"`} {
		if !strings.Contains(logged, required) {
			t.Fatalf("publication diagnostics missing %s: %s", required, logged)
		}
	}
	inv := invs[0]
	mgr.publicationTimeout = 2 * time.Second
	reconciled, err := mgr.reconcileProviderStartedPublication(ctx, &inv)
	if err != nil || !reconciled {
		t.Fatalf("reconcileProviderStartedPublication() reconciled=%v err=%v", reconciled, err)
	}
	if _, chatCalls := fake.Calls(); chatCalls != 1 {
		t.Fatalf("provider replayed during publication recovery: calls=%d", chatCalls)
	}
	ledger, err := store.GetProcedureInferenceLedger(ctx, ids.spaceID)
	if err != nil || len(ledger.Attempts) != 1 || ledger.Attempts[0].PublicationDisposition != "published" {
		t.Fatalf("ledger=%+v err=%v", ledger, err)
	}
}

func TestAutomationPublicationReopenTimeoutIsBoundedWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	store := storage.NewFileStore(t.TempDir())
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: ids.domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "World"}}
	beginCalls := 0
	graphs := &automationE2EGraph{node: node}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String(), beginCalls: &beginCalls, waitForBeginCall: 2}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference)
	mgr.publicationTimeout = 100 * time.Millisecond
	def := automation.Definition{
		ID: "bounded-publication-reopen", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled,
		Trigger:   automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}},
		Input:     automation.Input{Target: "changed", Fields: []string{"payload.text"}},
		Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize",
		Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}},
		Safety: automation.Safety{MaxAttempts: 1},
	}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: node.ID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 1 {
		t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("publication reopen timeout was not bounded: %s", elapsed)
	}
	if _, calls := fake.Calls(); calls != 1 {
		t.Fatalf("provider calls=%d, want 1", calls)
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 1 || invs[0].Status != "retryable" {
		t.Fatalf("invocations=%+v err=%v", invs, err)
	}
	runs, err := store.ListRuns(ctx, ids.domainID)
	if err != nil || len(runs) != 1 || runs[0].Status != "running" || !runs[0].CompletedAt.IsZero() {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestAutomationPublicationLedgerLookupTimeoutIsBoundedWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	baseStore := storage.NewFileStore(t.TempDir())
	store := &blockingProcedureInferenceLedgerListStore{Store: baseStore}
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: ids.domainID, Labels: []string{"Page"}, Payload: map[string]any{"text": "World"}}
	mgr := NewManager(store).WithGraphRuntime(automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}, &automationE2EGraph{node: node}).WithInferenceInvoker(inference)
	mgr.publicationTimeout = 100 * time.Millisecond
	def := automation.Definition{
		ID: "bounded-publication-ledger", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled,
		Trigger:   automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"Page"}},
		Input:     automation.Input{Target: "changed", Fields: []string{"payload.text"}},
		Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize",
		Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}}}},
		Safety: automation.Safety{MaxAttempts: 1},
	}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: node.ID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 1 {
		t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("publication ledger lookup timeout was not bounded: %s", elapsed)
	}
	if _, calls := fake.Calls(); calls != 1 {
		t.Fatalf("provider calls=%d, want 1", calls)
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{})
	if err != nil || len(invs) != 1 || invs[0].Status != "retryable" {
		t.Fatalf("invocations=%+v err=%v", invs, err)
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
	domainRevisionCalls := [][2]string{}
	graphs := &automationE2EGraph{
		nodes:               map[string]graph.Node{journal.ID.String(): journal, entry.ID.String(): entry, entry2.ID.String(): entry2},
		edges:               []graph.Edge{edge, edge2},
		spaceRevision:       99,
		domainRevision:      7,
		domainRevisionCalls: &domainRevisionCalls,
	}
	beginInputs := []sessionmodel.BeginTransactionInput{}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String(), beginInputs: &beginInputs}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference)
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
	if len(domainRevisionCalls) != 2 {
		t.Fatalf("domain revision calls = %#v, want input and publication revisions", domainRevisionCalls)
	}
	for _, call := range domainRevisionCalls {
		if call != [2]string{ids.spaceID, ids.domainID.String()} {
			t.Fatalf("domain revision calls = %#v, want target space/domain", domainRevisionCalls)
		}
	}
	if len(beginInputs) != 2 {
		t.Fatalf("begin transaction inputs = %#v, want separate input and publication transactions", beginInputs)
	}
	for _, input := range beginInputs {
		if input.BaseRevision == nil || *input.BaseRevision != 7 {
			t.Fatalf("begin transaction inputs = %#v, want target domain revision 7 instead of space revision 99", beginInputs)
		}
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
	entryID3 := uuid.New()
	journal := graph.Node{ID: graph.NodeID(journalID), DomainID: ids.domainID, Labels: []string{"Journal"}, Payload: map[string]any{}}
	entry1 := graph.Node{ID: graph.NodeID(entryID1), DomainID: ids.domainID, Labels: []string{"JournalEntry"}, Payload: map[string]any{"text": "one"}}
	entry2 := graph.Node{ID: graph.NodeID(entryID2), DomainID: ids.domainID, Labels: []string{"JournalEntry"}, Payload: map[string]any{"text": "two"}}
	entry3 := graph.Node{ID: graph.NodeID(entryID3), DomainID: ids.domainID, Labels: []string{"JournalEntry"}, Payload: map[string]any{"text": "three"}}
	graphs := &automationE2EGraph{nodes: map[string]graph.Node{journal.ID.String(): journal, entry1.ID.String(): entry1, entry2.ID.String(): entry2, entry3.ID.String(): entry3}, edges: []graph.Edge{
		{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: journal.ID, ToID: entry1.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 1}},
		{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: journal.ID, ToID: entry2.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 2}},
		{ID: graph.EdgeID(uuid.New()), DomainID: ids.domainID, FromID: journal.ID, ToID: entry3.ID, Labels: []string{"HAS_ENTRY"}, Properties: map[string]any{"position": 3}},
	}}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference)
	mgr.now = func() time.Time { return base }
	def := automation.Definition{ID: "daily-debounce", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Trigger: automation.Trigger{Events: []string{automation.EventNodeUpdated}, Labels: []string{"JournalEntry"}}, Condition: automation.Condition{GQL: "MATCH (journal:Journal)-[:HAS_ENTRY]->(changed:JournalEntry) RETURN changed, journal"}, Input: automation.Input{Target: "journal", Mode: automation.InputModeGQLTemplate, Template: "{{#each entries}}{{entry.payload.text}}\n{{/each}}", Context: map[string]automation.ContextQuery{"entries": {GQL: "MATCH (journal)-[r:HAS_ENTRY]->(entry:JournalEntry) RETURN entry ORDER BY r.properties.position FETCH FIRST 20 ROWS ONLY", Limit: 20}}}, Inference: automation.InferenceRef{Operation: "chat", ProfileID: ids.profileID.String()}, Prompt: "Summarize journal", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "journal", Set: map[string]string{"payload.summary": "$result.text"}}}}}, Safety: automation.Safety{Debounce: &automation.Debounce{Duration: "30s", CoalesceBy: "journal"}}}
	if err := store.PutDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	for _, node := range []graph.Node{entry1, entry2, entry3} {
		event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(ids.spaceID)), DomainID: ids.domainID, Origin: graphchange.OriginMetadata{PrincipalID: "principal-a"}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeUpdated, NodeID: node.ID.String(), Node: &node}}}
		if err := mgr.HandleGraphChange(ctx, event); err != nil {
			t.Fatalf("HandleGraphChange() error = %v", err)
		}
	}
	invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{AutomationID: def.ID})
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(invs, func(a, b automation.Invocation) int { return strings.Compare(a.ID, b.ID) })
	recovered := invs[0]
	recovered.Status = "running"
	recovered.ClaimVersion = 1
	recovered.ClaimToken = "interrupted"
	recovered.ClaimExpiresAt = base.Add(-time.Second)
	if err := store.PutInvocation(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	recoveredRun := automation.Run{ID: automationRunID(recovered, 1), DomainID: ids.domainID, InvocationID: recovered.ID, BindingID: recovered.BindingID, BindingVersion: recovered.BindingVersion, ProcedureID: recovered.ProcedureID, ProcedureVersion: recovered.ProcedureVersion, AttemptNumber: 1, Status: "succeeded", CompletedAt: base}
	if err := store.PutRun(ctx, recoveredRun); err != nil {
		t.Fatal(err)
	}
	if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 1 {
		t.Fatalf("recovery ProcessPending() processed=%d err=%v", processed, err)
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
	invs, err = store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{AutomationID: def.ID})
	if err != nil {
		t.Fatal(err)
	}
	skipped := 0
	recoveredSucceeded := false
	for _, inv := range invs {
		if inv.ID == recovered.ID && inv.Status == "succeeded" {
			recoveredSucceeded = true
		}
	}
	if !recoveredSucceeded {
		t.Fatalf("terminal run was not reconciled before debounce: invs=%+v", invs)
	}
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
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceInvoker(inference)
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
	ledger, err := store.GetProcedureInferenceLedger(ctx, ids.spaceID)
	if err != nil || len(ledger.Attempts) != 1 || ledger.Attempts[0].PublicationDisposition != "published" {
		t.Fatalf("publication disposition ledger=%+v err=%v", ledger, err)
	}
}

func TestPublicationPendingDispositionCanBeReconciledByRunID(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFileStore(t.TempDir())
	mgr := NewManager(store)
	domainID := graph.DomainID(uuid.New())
	invocation := automation.Invocation{ID: "inv", SpaceID: uuid.NewString(), DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1}
	run := automation.Run{ID: "run", DomainID: domainID, InvocationID: invocation.ID, ProcedureID: invocation.ProcedureID, ProcedureVersion: 1, BindingID: invocation.BindingID, BindingVersion: 1, AttemptNumber: 1}
	attempt := newRequestedProcedureInferenceAttempt(invocation, run, "chat", "profile", mgr.now())
	run.Status = "running"
	run.InferenceAttemptIDs = []string{attempt.ID}
	if err := mgr.putRunRuntime(ctx, invocation.SpaceID, run); err != nil {
		t.Fatal(err)
	}
	if err := mgr.putProcedureInferenceAttemptRuntime(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	var err error
	attempt, err = mgr.authorizeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.ResolveResult{Allowed: true, Decision: domaininference.PolicyDecision{AuthorizationRevision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = mgr.startProcedureInferenceAttempt(ctx, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.completeProcedureInferenceAttempt(ctx, attempt, inferenceruntime.InvokeResponse{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := mgr.recordRunPublicationDisposition(ctx, run, "publication_pending"); err != nil {
		t.Fatal(err)
	}
	run.InferenceAttemptIDs = nil // recovery may only retain the stable run ID.
	if err := mgr.recordRunPublicationDisposition(ctx, run, "published"); err != nil {
		t.Fatal(err)
	}
	recoveredRun := run
	recoveredRun.Status = "skipped"
	if err := mgr.putRunRuntime(ctx, invocation.SpaceID, recoveredRun); err != nil {
		t.Fatal(err)
	}
	persistedRun, err := store.GetRun(ctx, domainID, run.ID)
	if err != nil || len(persistedRun.InferenceAttemptIDs) != 1 || persistedRun.InferenceAttemptIDs[0] != attempt.ID {
		t.Fatalf("reconciled run lost attempt reference: %+v err=%v", persistedRun, err)
	}
	ledger, err := store.GetProcedureInferenceLedger(ctx, invocation.SpaceID)
	if err != nil || len(ledger.Attempts) != 1 || ledger.Attempts[0].PublicationDisposition != "published" {
		t.Fatalf("reconciled ledger=%+v err=%v", ledger, err)
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

type blockingProcedureInferenceLedgerListStore struct{ storage.Store }

func (s *blockingProcedureInferenceLedgerListStore) ListProcedureInferenceLedgers(ctx context.Context) ([]automation.ProcedureInferenceLedger, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type automationE2ESessions struct {
	spaceID            string
	domainID           string
	commitCalls        *int
	commitContextError *error
	beginInputs        *[]sessionmodel.BeginTransactionInput
	beginCalls         *int
	waitForBeginCall   int
}

func (s automationE2ESessions) OpenSession(context.Context, sessionmodel.OpenSessionInput) (sessionmodel.GraphSession, error) {
	return sessionmodel.GraphSession{ID: "session", PrincipalID: automationActor, SpaceID: s.spaceID, DomainID: s.domainID, State: sessionmodel.SessionStateActive}, nil
}
func (s automationE2ESessions) BeginTransaction(ctx context.Context, input sessionmodel.BeginTransactionInput) (sessionmodel.GraphTransaction, error) {
	if s.beginInputs != nil {
		*s.beginInputs = append(*s.beginInputs, input)
	}
	if s.beginCalls != nil {
		*s.beginCalls++
		if s.waitForBeginCall > 0 && *s.beginCalls == s.waitForBeginCall {
			<-ctx.Done()
			return sessionmodel.GraphTransaction{}, ctx.Err()
		}
	}
	baseRevision := int64(0)
	if input.BaseRevision != nil {
		baseRevision = *input.BaseRevision
	}
	return sessionmodel.GraphTransaction{ID: "tx", SessionID: "session", PrincipalID: automationActor, SpaceID: s.spaceID, DomainID: s.domainID, Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive, BaseRevision: baseRevision}, nil
}
func (s automationE2ESessions) CommitTransactionAtRevision(ctx context.Context, _ string, _ string, _ int32, _ int64) (sessionmodel.TransactionCommit, error) {
	if s.commitCalls != nil {
		(*s.commitCalls)++
	}
	if s.commitContextError != nil {
		*s.commitContextError = ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return sessionmodel.TransactionCommit{}, err
	}
	return sessionmodel.TransactionCommit{ID: "commit"}, nil
}
func (s automationE2ESessions) RollbackTransaction(context.Context, string, string) (sessionmodel.GraphTransaction, error) {
	return sessionmodel.GraphTransaction{ID: "tx", State: sessionmodel.TransactionStateRolledBack}, nil
}

type automationE2EGraph struct {
	node                        graph.Node
	nodes                       map[string]graph.Node
	edges                       []graph.Edge
	cancelOnCommit              context.CancelFunc
	waitForCommitContext        bool
	acknowledgements            *int
	spaceRevision               int64
	domainRevision              int64
	domainRevisionCalls         *[][2]string
	idempotentCommits           map[string]graphchange.CommitResult
	idempotencyAcknowledgements *int
	idempotencyRetentions       *int
	acknowledgeIdempotency      func(context.Context, string, string, string, string, string) error
	retainIdempotency           func(context.Context, string, string, string, string, string) error
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

func (g *automationE2EGraph) GetNode(_ context.Context, _ sessionmodel.GraphTransaction, id string) (graph.Node, error) {
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
func (g *automationE2EGraph) ListNodes(_ context.Context, _ sessionmodel.GraphTransaction, _ int, token string) ([]graph.Node, string, error) {
	if token != "" {
		return nil, "", nil
	}
	return g.allNodes(), "", nil
}
func (g *automationE2EGraph) CreateNode(context.Context, sessionmodel.GraphTransaction, graph.NodeInput) (graph.Node, error) {
	return graph.Node{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) UpdateNode(_ context.Context, _ sessionmodel.GraphTransaction, in graph.UpdateNodeInput) (graph.Node, error) {
	node, err := g.GetNode(context.Background(), sessionmodel.GraphTransaction{}, in.NodeID)
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
func (g *automationE2EGraph) ListEdges(_ context.Context, _ sessionmodel.GraphTransaction, _ int, token string) ([]graph.Edge, string, error) {
	if token != "" {
		return nil, "", nil
	}
	return g.edges, "", nil
}
func (g *automationE2EGraph) CreateEdge(context.Context, sessionmodel.GraphTransaction, graph.EdgeInput) (graph.Edge, error) {
	return graph.Edge{}, fmt.Errorf("unused")
}
func (g *automationE2EGraph) CurrentRevision(context.Context, string) (int64, error) {
	if g.spaceRevision != 0 {
		return g.spaceRevision, nil
	}
	return 1, nil
}
func (g *automationE2EGraph) CurrentDomainRevision(_ context.Context, spaceID string, domainID string) (int64, error) {
	if g.domainRevisionCalls != nil {
		*g.domainRevisionCalls = append(*g.domainRevisionCalls, [2]string{spaceID, domainID})
	}
	if g.domainRevision != 0 {
		return g.domainRevision, nil
	}
	return 1, nil
}
func (g *automationE2EGraph) CommitTransactionGraph(ctx context.Context, tx sessionmodel.GraphTransaction) (graphchange.CommitResult, error) {
	if g.cancelOnCommit != nil {
		g.cancelOnCommit()
	}
	result := graphchange.CommitResult{CommitID: "commit-" + tx.IdempotencyKey, OperationCount: 1, CommittedRevision: 1, IdempotencyKey: tx.IdempotencyKey}
	if tx.IdempotencyKey != "" {
		if g.idempotentCommits == nil {
			g.idempotentCommits = map[string]graphchange.CommitResult{}
		}
		g.idempotentCommits[tx.PrincipalID+"\x00"+tx.IdempotencyKey] = result
	}
	if g.waitForCommitContext {
		<-ctx.Done()
		return graphchange.CommitResult{}, ctx.Err()
	}
	return result, nil
}
func (g *automationE2EGraph) LookupIdempotentCommit(_ context.Context, principalID, _, _, key string) (graphchange.CommitResult, bool, error) {
	result, ok := g.idempotentCommits[principalID+"\x00"+key]
	if ok && result.CommitID == "" {
		result.CommitID = "commit-" + key
	}
	return result, ok, nil
}
func (g *automationE2EGraph) RetainIdempotentCommitUntilAcknowledged(ctx context.Context, principalID, spaceID, domainID, key, commitID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.retainIdempotency != nil {
		if err := g.retainIdempotency(ctx, principalID, spaceID, domainID, key, commitID); err != nil {
			return err
		}
	}
	if g.idempotencyRetentions != nil {
		(*g.idempotencyRetentions)++
	}
	return nil
}
func (g *automationE2EGraph) AcknowledgeIdempotentCommit(ctx context.Context, principalID, spaceID, domainID, key, commitID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.acknowledgeIdempotency != nil {
		if err := g.acknowledgeIdempotency(ctx, principalID, spaceID, domainID, key, commitID); err != nil {
			return err
		}
	}
	if g.idempotencyAcknowledgements != nil {
		(*g.idempotencyAcknowledgements)++
	}
	return nil
}
func (g *automationE2EGraph) DiscardTransactionGraph(context.Context, string) error { return nil }
func (g *automationE2EGraph) AcknowledgeTransactionFinalization(ctx context.Context, _ sessionmodel.GraphTransaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.acknowledgements != nil {
		(*g.acknowledgements)++
	}
	return nil
}
func (g *automationE2EGraph) ScanAdjacency(_ context.Context, _ sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error) {
	if scan.Cursor != "" {
		return nil, "", graphquery.IndexedReadStats{}, nil
	}
	out := []graph.Edge{}
	for _, edge := range g.edges {
		if scan.Label != "" && !stringSliceContains(edge.Labels, scan.Label) {
			continue
		}
		switch scan.Direction {
		case graphquery.AdjacencyDirectionIn:
			if edge.ToID.String() == scan.NodeID {
				out = append(out, edge)
			}
		default:
			if edge.FromID.String() == scan.NodeID {
				out = append(out, edge)
			}
		}
	}
	return out, "", graphquery.IndexedReadStats{}, nil
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
