package connectors

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	identity "github.com/myceldb/mycel/internal/identity/model"
	inferenceconnectors "github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestInferenceAdapterEmbedsThroughStandaloneInference(t *testing.T) {
	ctx := context.Background()
	module := inferenceservice.NewModule()
	if result := module.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	fake := &inferenceconnectors.FakeConnector{Vector: []float64{0.4, 0.6}}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	ids := semanticInferenceAdapterIDs{spaceID: domainspace.SpaceID(uuid.New()), domainID: graph.DomainID(uuid.New()), indexID: domainsemantic.SemanticIndexID(uuid.New()), nodeID: graph.NodeID(uuid.New()), endpointID: domainsemantic.ModelEndpointID(uuid.New()), modelID: domainsemantic.InferenceModelID(uuid.New()), capID: domainsemantic.ModelEndpointCapabilityID(uuid.New()), credentialID: domainsemantic.InferenceCredentialID(uuid.New()), secretID: domainsemantic.SecretID(uuid.New()), grantID: domainsemantic.CredentialGrantID(uuid.New()), profileID: uuid.New()}
	seedStandaloneInferenceForSemanticAdapter(t, ctx, module, ids)

	resp, err := (InferenceAdapter{Manager: module}).Embed(ctx, EmbedInput{ModelEndpointID: ids.endpointID, ModelID: ids.modelID, ModelEndpointCapabilityID: ids.capID, CredentialID: ids.credentialID, CredentialGrantID: ids.grantID, SpaceID: ids.spaceID, DomainID: ids.domainID, SemanticIndexID: ids.indexID, TargetNodeID: ids.nodeID, ActorPrincipalID: identity.PrincipalID("actor-a"), InferenceProfileID: ids.profileID, Input: "semantic text", Reason: "semantic_backfill"})
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	if len(resp.Vector) != 2 || resp.PolicyDecisionID == uuid.Nil || resp.ProviderRequestID != "fake" || resp.CredentialGrantID != ids.grantID {
		t.Fatalf("unexpected response: %#v", resp)
	}
	embedCalls, _ := fake.Calls()
	if embedCalls != 1 {
		t.Fatalf("expected standalone fake connector to be called once, got %d", embedCalls)
	}
	events, err := module.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusSucceeded || events[0].SemanticIndexID != ids.indexID.String() {
		t.Fatalf("unexpected usage events: %#v err=%v", events, err)
	}
}

func TestInferenceAdapterRequiresProfile(t *testing.T) {
	ctx := context.Background()
	_, err := (InferenceAdapter{Manager: inferenceservice.NewModule()}).Embed(ctx, EmbedInput{Input: "semantic text"})
	if err == nil || !strings.Contains(err.Error(), "does not declare an inference profile") {
		t.Fatalf("expected missing profile error, got %v", err)
	}
}

type semanticInferenceAdapterIDs struct {
	spaceID      domainspace.SpaceID
	domainID     graph.DomainID
	indexID      domainsemantic.SemanticIndexID
	nodeID       graph.NodeID
	endpointID   domainsemantic.ModelEndpointID
	modelID      domainsemantic.InferenceModelID
	capID        domainsemantic.ModelEndpointCapabilityID
	credentialID domainsemantic.InferenceCredentialID
	secretID     domainsemantic.SecretID
	grantID      domainsemantic.CredentialGrantID
	profileID    uuid.UUID
}

func seedStandaloneInferenceForSemanticAdapter(t *testing.T, ctx context.Context, module *inferenceservice.Module, ids semanticInferenceAdapterIDs) {
	t.Helper()
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, domaininference.Endpoint{ID: domaininference.EndpointID(ids.endpointID), Key: "endpoint", ConnectorType: domaininference.ConnectorOpenAICompatible, NetworkClass: domaininference.NetworkClassPublicInternet, PrivacyClass: domaininference.PrivacyClassThirdParty, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, Enabled: true}); err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	if _, err := module.GlobalManager().UpsertModel(ctx, domaininference.Model{ID: domaininference.ModelID(ids.modelID), Key: "model", Operation: domaininference.OperationEmbeddings, ProviderModelName: "embed", Enabled: true}); err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := module.GlobalManager().UpsertCapability(ctx, domaininference.Capability{ID: domaininference.CapabilityID(ids.capID), EndpointID: domaininference.EndpointID(ids.endpointID), ModelID: domaininference.ModelID(ids.modelID), Operation: domaininference.OperationEmbeddings, Enabled: true}); err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	if _, err := module.GlobalManager().UpsertSecret(ctx, domaininference.Secret{ID: domaininference.SecretID(ids.secretID), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", Kind: "none"}); err != nil {
		t.Fatalf("upsert secret: %v", err)
	}
	if _, err := module.GlobalManager().UpsertCredential(ctx, domaininference.Credential{ID: domaininference.CredentialID(ids.credentialID), Key: "credential", EndpointID: domaininference.EndpointID(ids.endpointID), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", AuthType: domaininference.CredentialAuthNone, SecretID: domaininference.SecretID(ids.secretID), Status: domaininference.CredentialStatusActive}); err != nil {
		t.Fatalf("upsert credential: %v", err)
	}
	spaceMgr, err := module.SpaceManager(ctx, ids.spaceID.String())
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	if _, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{ID: domaininference.ProfileID(ids.profileID), SpaceID: ids.spaceID.String(), Key: "semantic", Operation: domaininference.OperationEmbeddings, DomainIDs: []string{ids.domainID.String()}, EndpointRefs: []string{ids.endpointID.String()}, ModelRefs: []string{ids.modelID.String()}, CapabilityRefs: []string{ids.capID.String()}, Enabled: true}); err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	if _, err := spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(ids.grantID), SpaceID: ids.spaceID.String(), CredentialID: domaininference.CredentialID(ids.credentialID), Scope: domaininference.Scope{SpaceID: ids.spaceID.String(), DomainID: ids.domainID.String(), SemanticIndexID: ids.indexID.String()}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{ids.profileID.String()}, EndpointRefs: []string{ids.endpointID.String()}, ModelRefs: []string{ids.modelID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeSemantic}, State: domaininference.GrantStateActive}); err != nil {
		t.Fatalf("upsert grant: %v", err)
	}
	if _, err := spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: ids.spaceID.String(), Scope: domaininference.Scope{SpaceID: ids.spaceID.String(), DomainID: ids.domainID.String(), SemanticIndexID: ids.indexID.String()}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{ids.profileID.String()}, Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert policy: %v", err)
	}
}
