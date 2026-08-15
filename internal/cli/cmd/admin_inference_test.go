package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	cliapp "github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestAdminInferencePackageApplyUsesDaemonGRPC(t *testing.T) {
	dataDir, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "inference-user", "inference-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Inference Space", "--owner-username", "inference-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	pkgPath := filepath.Join(dataDir, "inference-package.yaml")
	pkg := `name: test-openai
version: "1"
model_endpoints:
  - key: test-openai
    name: Test OpenAI
    connector_type: openai-compatible
    endpoint_url: http://example.invalid/v1
    network_class: external_https
    privacy_class: third_party
    auth_modes: [api_key]
    operations: [embeddings]
    enabled: true
models:
  - key: test/text-embedding
    operation: embeddings
    model_name: text-embedding
    dimensions: 3
    vector_space_key: test/text-embedding
    connector_types: [openai-compatible]
model_endpoint_capabilities:
  - model_endpoint: test-openai
    model: test/text-embedding
    operation: embeddings
    enabled: true
`
	if err := os.WriteFile(pkgPath, []byte(pkg), 0o600); err != nil {
		t.Fatalf("write package: %v", err)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "package", "apply", pkgPath)
	if err != nil {
		t.Fatalf("inference package apply failed: %v\n%s", err, out)
	}
	var applied adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse
	if err := json.Unmarshal([]byte(out), &applied); err != nil {
		t.Fatalf("decode package apply: %v\n%s", err, out)
	}
	if applied.GetPackage().GetName() != "test-openai" || len(applied.GetModelEndpoints()) != 1 || len(applied.GetModels()) != 1 || len(applied.GetModelEndpointCapabilities()) != 1 {
		t.Fatalf("unexpected applied package: %#v", &applied)
	}
	inferenceGlobal := inferencestorage.NewGlobalManager()
	if err := inferenceGlobal.Init(context.Background(), filepath.Join(dataDir, "meta")); err != nil {
		t.Fatalf("init standalone inference global manager: %v", err)
	}
	inferenceEndpoints, err := inferenceGlobal.ListEndpoints(context.Background())
	if err != nil || len(inferenceEndpoints) != 1 || inferenceEndpoints[0].Key != "test-openai" {
		t.Fatalf("standalone inference endpoint sync failed: %#v err=%v", inferenceEndpoints, err)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "endpoint", "list")
	if err != nil {
		t.Fatalf("model endpoint list failed: %v\n%s", err, out)
	}
	var endpoints adminv1.AdminInferenceCatalogServiceListModelEndpointsResponse
	if err := json.Unmarshal([]byte(out), &endpoints); err != nil || len(endpoints.GetModelEndpoints()) != 1 || endpoints.GetModelEndpoints()[0].GetKey() != "test-openai" {
		t.Fatalf("unexpected endpoints: %#v err=%v out=%s", &endpoints, err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "admin", "domain", "get", "default", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("admin domain get failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "index", "add", "pkg-notes", "--space-id", spaceID, "--domain", "default", "--source", "self", "--model-endpoint", "test-openai", "--model", "test/text-embedding", "--vector-store", "mycel-file")
	if err != nil {
		t.Fatalf("semantic index add with inference keys failed: %v\n%s", err, out)
	}
	var index clientv1.SemanticIndex
	if err := json.Unmarshal([]byte(out), &index); err != nil {
		t.Fatalf("decode semantic index: %v\n%s", err, out)
	}
	if index.GetKey() != "pkg-notes" {
		t.Fatalf("unexpected semantic index: %#v", &index)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "create", "bad-secret-ref", "--model-endpoint", "test-openai", "--owner-type", "system", "--owner-id", "daemon-test", "--external-ref", "vault://OPENAI_API_KEY")
	if err == nil || !strings.Contains(err.Error()+out, "env://NAME") {
		t.Fatalf("expected unsupported external ref failure, err=%v out=%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "create", "test-openai-key", "--model-endpoint", "test-openai", "--owner-type", "system", "--owner-id", "daemon-test", "--external-ref", "env://OPENAI_API_KEY")
	if err != nil {
		t.Fatalf("credential add failed: %v\n%s", err, out)
	}
	var createdCredential adminv1.AdminInferenceCredentialServiceCreateCredentialResponse
	if err := json.Unmarshal([]byte(out), &createdCredential); err != nil {
		t.Fatalf("decode credential add: %v\n%s", err, out)
	}
	if createdCredential.GetCredential().GetKey() != "test-openai-key" || createdCredential.GetSecret().GetKind() != "external_ref" {
		t.Fatalf("unexpected credential: %#v", &createdCredential)
	}
	inferenceGlobalAfterCredential := inferencestorage.NewGlobalManager()
	if err := inferenceGlobalAfterCredential.Init(context.Background(), filepath.Join(dataDir, "meta")); err != nil {
		t.Fatalf("reload standalone inference global manager: %v", err)
	}
	standaloneCredentials, err := inferenceGlobalAfterCredential.ListCredentials(context.Background())
	if err != nil || len(standaloneCredentials) != 1 || standaloneCredentials[0].Key != "test-openai-key" {
		t.Fatalf("standalone inference credential sync failed: %#v err=%v", standaloneCredentials, err)
	}
	conn, authCtx, _, err := loginDaemonPrincipal(context.Background(), &cliapp.App{DaemonAddr: addr, UserRef: "admin", Password: adminPassword})
	if err != nil {
		t.Fatalf("admin login for inference profile client failed: %v", err)
	}
	defer conn.Close()
	profileClient := adminv1.NewAdminInferenceProfileServiceClient(conn)
	createdProfile, err := profileClient.CreateInferenceProfile(authCtx, &adminv1.AdminInferenceProfileServiceCreateInferenceProfileRequest{SpaceId: spaceID, Key: "summarize-page", DisplayName: "Summarize page", Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT, Purpose: "automation", DomainIds: []string{"default"}, Enabled: true})
	if err != nil {
		t.Fatalf("create inference profile failed: %v", err)
	}
	if createdProfile.GetInferenceProfile().GetKey() != "summarize-page" || !createdProfile.GetInferenceProfile().GetEnabled() {
		t.Fatalf("unexpected profile: %#v", createdProfile.GetInferenceProfile())
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "profile", "get", "summarize-page", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("profile get failed: %v\n%s", err, out)
	}
	var fetchedProfile adminv1.AdminInferenceProfileServiceGetInferenceProfileResponse
	if err := json.Unmarshal([]byte(out), &fetchedProfile); err != nil || fetchedProfile.GetInferenceProfile().GetKey() != "summarize-page" {
		t.Fatalf("unexpected fetched profile: %#v err=%v out=%s", &fetchedProfile, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "profile", "disable", "summarize-page", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("profile disable failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "profile", "enable", "summarize-page", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("profile enable failed: %v\n%s", err, out)
	}
	listedProfiles, err := profileClient.ListInferenceProfiles(authCtx, &adminv1.AdminInferenceProfileServiceListInferenceProfilesRequest{SpaceId: spaceID})
	if err != nil || len(listedProfiles.GetInferenceProfiles()) != 1 {
		t.Fatalf("list inference profiles failed: profiles=%#v err=%v", listedProfiles.GetInferenceProfiles(), err)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "rotate", "test-openai-key", "--external-ref", "env://OPENAI_API_KEY_V2")
	if err != nil {
		t.Fatalf("rotate credential failed: %v\n%s", err, out)
	}
	var rotated adminv1.AdminInferenceCredentialServiceRotateCredentialResponse
	if err := json.Unmarshal([]byte(out), &rotated); err != nil {
		t.Fatalf("decode rotated credential: %v\n%s", err, out)
	}
	if rotated.GetCredential().GetCredentialId() != createdCredential.GetCredential().GetCredentialId() || rotated.GetSecret().GetExternalRef() != "env://OPENAI_API_KEY_V2" {
		t.Fatalf("unexpected rotated credential: %#v", &rotated)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "list", "--owner-type", "system", "--owner-id", "daemon-test")
	if err != nil {
		t.Fatalf("credential list failed: %v\n%s", err, out)
	}
	var credentials adminv1.AdminInferenceCredentialServiceListCredentialsResponse
	if err := json.Unmarshal([]byte(out), &credentials); err != nil || len(credentials.GetCredentials()) != 1 || credentials.GetCredentials()[0].GetKey() != "test-openai-key" {
		t.Fatalf("unexpected credentials: %#v err=%v out=%s", &credentials, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "grant", "test-openai-key", "--space-id", spaceID, "--domain", "default", "--semantic-index", index.GetSemanticIndexId(), "--model-endpoint", "test-openai", "--model", "test/text-embedding", "--allow-background-use", "--grantee-principal-id", "automation", "--allow-on-behalf-of-principal-id", "principal-a")
	if err != nil {
		t.Fatalf("credential grant failed: %v\n%s", err, out)
	}
	var createdGrant adminv1.AdminInferenceGrantServiceCreateCredentialGrantResponse
	if err := json.Unmarshal([]byte(out), &createdGrant); err != nil {
		t.Fatalf("decode credential grant: %v\n%s", err, out)
	}
	if createdGrant.GetCredentialGrant().GetCredentialId() != createdCredential.GetCredential().GetCredentialId() || !createdGrant.GetCredentialGrant().GetAllowBackgroundUse() || len(createdGrant.GetCredentialGrant().GetAllowOnBehalfOfPrincipalIds()) != 1 {
		t.Fatalf("unexpected grant: %#v", &createdGrant)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "policy", "allow", "--space-id", spaceID, "--domain", "default", "--reason", "daemon test")
	if err != nil {
		t.Fatalf("policy allow failed: %v\n%s", err, out)
	}
	var createdPolicy adminv1.AdminInferencePolicyServiceCreateInferencePolicyResponse
	if err := json.Unmarshal([]byte(out), &createdPolicy); err != nil {
		t.Fatalf("decode policy: %v\n%s", err, out)
	}
	if createdPolicy.GetInferencePolicy().GetEffect() != "allow" {
		t.Fatalf("unexpected policy: %#v", &createdPolicy)
	}
	inferenceSpace := inferencestorage.NewSpaceManager()
	if err := inferenceSpace.Init(context.Background(), filepath.Join(dataDir, "graphs", spaceID, "inference"), spaceID); err != nil {
		t.Fatalf("init standalone inference space manager: %v", err)
	}
	standaloneGrants, err := inferenceSpace.ListCredentialGrants(context.Background())
	if err != nil || len(standaloneGrants) != 1 || standaloneGrants[0].ID.String() != createdGrant.GetCredentialGrant().GetCredentialGrantId() {
		t.Fatalf("standalone inference grant sync failed: %#v err=%v", standaloneGrants, err)
	}
	standalonePolicies, err := inferenceSpace.ListPolicies(context.Background())
	if err != nil || len(standalonePolicies) != 1 || standalonePolicies[0].ID.String() != createdPolicy.GetInferencePolicy().GetInferencePolicyId() {
		t.Fatalf("standalone inference policy sync failed: %#v err=%v", standalonePolicies, err)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "policy", "list", "--space-id", spaceID, "--effect", "allow")
	if err != nil {
		t.Fatalf("policy list failed: %v\n%s", err, out)
	}
	var policies adminv1.AdminInferencePolicyServiceListInferencePoliciesResponse
	if err := json.Unmarshal([]byte(out), &policies); err != nil || len(policies.GetInferencePolicies()) != 1 || policies.GetInferencePolicies()[0].GetReason() != "daemon test" {
		t.Fatalf("unexpected policies: %#v err=%v out=%s", &policies, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "endpoint", "disable", "test-openai")
	if err != nil {
		t.Fatalf("model endpoint disable failed: %v\n%s", err, out)
	}
	var disabledEndpoint adminv1.AdminInferenceCatalogServiceSetModelEndpointEnabledResponse
	if err := json.Unmarshal([]byte(out), &disabledEndpoint); err != nil || disabledEndpoint.GetModelEndpoint().GetEnabled() {
		t.Fatalf("unexpected disabled endpoint: %#v err=%v out=%s", &disabledEndpoint, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "revoke", "test-openai-key")
	if err != nil {
		t.Fatalf("credential revoke failed: %v\n%s", err, out)
	}
	var revoked adminv1.AdminInferenceCredentialServiceSetCredentialStatusResponse
	if err := json.Unmarshal([]byte(out), &revoked); err != nil || revoked.GetCredential().GetStatus() != "revoked" {
		t.Fatalf("unexpected revoked credential: %#v err=%v out=%s", &revoked, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "delete", "test-openai-key")
	if err == nil || !strings.Contains(err.Error()+out, "referenced") {
		t.Fatalf("expected referenced credential delete failure, err=%v out=%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "index", "delete", index.GetSemanticIndexId(), "--space-id", spaceID)
	if err == nil || !strings.Contains(err.Error()+out, "purge_references") {
		t.Fatalf("expected referenced semantic index delete failure, err=%v out=%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "grant", "expire", createdGrant.GetCredentialGrant().GetCredentialGrantId(), "--space-id", spaceID)
	if err != nil {
		t.Fatalf("credential grant expire failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "policy", "expire", createdPolicy.GetInferencePolicy().GetInferencePolicyId(), "--space-id", spaceID)
	if err != nil {
		t.Fatalf("policy expire failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "index", "delete", index.GetSemanticIndexId(), "--space-id", spaceID, "--purge-references", "--purge-vectors")
	if err != nil {
		t.Fatalf("semantic index delete failed: %v\n%s", err, out)
	}
	var deletedIndex adminv1.DeleteSemanticIndexResponse
	if err := json.Unmarshal([]byte(out), &deletedIndex); err != nil || deletedIndex.GetCredentialGrantsDeleted() != 1 || !deletedIndex.GetVectorsPurged() {
		t.Fatalf("unexpected semantic index delete response: %#v err=%v out=%s", &deletedIndex, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "delete", "test-openai-key", "--delete-grants", "--delete-secret")
	if err != nil {
		t.Fatalf("credential delete failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "capability", "delete", applied.GetModelEndpointCapabilities()[0].GetModelEndpointCapabilityId())
	if err != nil {
		t.Fatalf("capability delete failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "endpoint", "delete", "test-openai")
	if err != nil {
		t.Fatalf("model endpoint delete failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "model", "delete", "test/text-embedding")
	if err != nil {
		t.Fatalf("model delete failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "vector-store", "delete", "mycel-file")
	if err != nil {
		t.Fatalf("vector store delete failed: %v\n%s", err, out)
	}
}

func TestAdminSemanticMigrationUsesDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "migration-user", "migration-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Migration Space", "--owner-username", "migration-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "migrate", "legacy-embeddings", "--space-id", createdSpace.GetSpace().GetSpaceId(), "--domain", "default", "--dry-run")
	if err == nil || !strings.Contains(err.Error()+out, "legacy embedding migration window is closed") {
		t.Fatalf("expected closed legacy migration error, err=%v out=%s", err, out)
	}
}

func TestAdminSemanticMaintenanceUsesDaemonGRPC(t *testing.T) {
	dataDir, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "maint-user", "maint-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Maintenance Space", "--owner-username", "maint-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "maintenance", "status", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("semantic maintenance status failed: %v\n%s", err, out)
	}
	var status adminv1.GetSemanticMaintenanceStatusResponse
	if err := json.Unmarshal([]byte(out), &status); err != nil || status.GetThrottleState() != "ok" {
		t.Fatalf("unexpected status: %#v err=%v out=%s", &status, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "maintenance", "analyze", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("semantic maintenance analyze failed: %v\n%s", err, out)
	}
	var analyzed adminv1.AnalyzeSemanticDirtyWorkResponse
	if err := json.Unmarshal([]byte(out), &analyzed); err != nil || analyzed.GetProcessedEvents() != 0 || analyzed.GetEnqueuedItems() != 0 {
		t.Fatalf("unexpected analyze result: %#v err=%v out=%s", &analyzed, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "maintenance", "process", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("semantic maintenance process failed: %v\n%s", err, out)
	}
	var processed adminv1.ProcessSemanticDirtyWorkResponse
	if err := json.Unmarshal([]byte(out), &processed); err != nil || processed.GetProcessedItems() != 0 || processed.GetCompletedItems() != 0 || processed.GetFailedItems() != 0 {
		t.Fatalf("unexpected process result: %#v err=%v out=%s", &processed, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Maintenance Work Space", "--owner-username", "maint-user")
	if err != nil {
		t.Fatalf("work space add failed: %v\n%s", err, out)
	}
	var createdWorkSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdWorkSpace); err != nil {
		t.Fatalf("decode work space add: %v\n%s", err, out)
	}
	workSpaceID := createdWorkSpace.GetSpace().GetSpaceId()
	seededWorkID := seedSemanticMaintenanceWork(t, dataDir, workSpaceID)
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "maintenance", "list", "--space-id", workSpaceID)
	if err != nil {
		t.Fatalf("semantic maintenance list failed: %v\n%s", err, out)
	}
	var listed adminv1.ListSemanticMaintenanceWorkResponse
	if err := json.Unmarshal([]byte(out), &listed); err != nil || len(listed.GetItems()) != 1 || listed.GetItems()[0].GetWorkItemId() != seededWorkID.String() {
		t.Fatalf("unexpected work list: %#v err=%v out=%s", &listed, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "maintenance", "cancel", "--space-id", workSpaceID, seededWorkID.String())
	if err != nil {
		t.Fatalf("semantic maintenance cancel failed: %v\n%s", err, out)
	}
	var cancelled adminv1.CancelSemanticMaintenanceWorkResponse
	if err := json.Unmarshal([]byte(out), &cancelled); err != nil || cancelled.GetItem().GetStatus() != string(domainsemantic.SemanticDirtyWorkStatusCancelled) {
		t.Fatalf("unexpected cancel response: %#v err=%v out=%s", &cancelled, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "maintenance", "retry", "--space-id", workSpaceID, seededWorkID.String())
	if err != nil {
		t.Fatalf("semantic maintenance retry failed: %v\n%s", err, out)
	}
	var retried adminv1.RetrySemanticMaintenanceWorkResponse
	if err := json.Unmarshal([]byte(out), &retried); err != nil || retried.GetItem().GetStatus() != string(domainsemantic.SemanticDirtyWorkStatusPending) || retried.GetItem().GetLastErrorCategory() != "" {
		t.Fatalf("unexpected retry response: %#v err=%v out=%s", &retried, err, out)
	}
}

func seedSemanticMaintenanceWork(t *testing.T, dataDir string, spaceIDText string) uuid.UUID {
	t.Helper()
	spaceID := domainspace.SpaceID(uuid.MustParse(spaceIDText))
	mgr := storesemantic.NewMaintenanceManager()
	if err := mgr.Init(context.Background(), filepath.Join(dataDir, "graphs", spaceIDText, "semantic", "maintenance"), spaceID); err != nil {
		t.Fatalf("init maintenance manager: %v", err)
	}
	item, err := mgr.UpsertDirtyWorkItem(context.Background(), domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), SemanticIndexID: domainsemantic.SemanticIndexID(uuid.New()), TargetNodeID: graph.NodeID(uuid.New()), Action: domainsemantic.SemanticDirtyWorkActionRefresh, Status: domainsemantic.SemanticDirtyWorkStatusPending, LastError: "secret provider payload should not leak", LastErrorCategory: "rate_limited"})
	if err != nil {
		t.Fatalf("seed work item: %v", err)
	}
	return item.ID
}
