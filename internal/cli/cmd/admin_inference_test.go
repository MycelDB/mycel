package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/identity"
	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/gen/go/mycel/client/v1"
	storeembedding "github.com/myceldb/mycel/store/embedding"
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
	var applied adminv1.AdminInferenceServiceApplyInferencePackageResponse
	if err := json.Unmarshal([]byte(out), &applied); err != nil {
		t.Fatalf("decode package apply: %v\n%s", err, out)
	}
	if applied.GetPackage().GetName() != "test-openai" || len(applied.GetModelEndpoints()) != 1 || len(applied.GetModels()) != 1 || len(applied.GetModelEndpointCapabilities()) != 1 {
		t.Fatalf("unexpected applied package: %#v", &applied)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "model-endpoint", "list")
	if err != nil {
		t.Fatalf("model endpoint list failed: %v\n%s", err, out)
	}
	var endpoints adminv1.AdminInferenceServiceListModelEndpointsResponse
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
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "add", "test-openai-key", "--model-endpoint", "test-openai", "--owner-type", "system", "--owner-id", "daemon-test", "--external-ref", "test-secret-ref")
	if err != nil {
		t.Fatalf("credential add failed: %v\n%s", err, out)
	}
	var createdCredential adminv1.AdminInferenceServiceCreateCredentialResponse
	if err := json.Unmarshal([]byte(out), &createdCredential); err != nil {
		t.Fatalf("decode credential add: %v\n%s", err, out)
	}
	if createdCredential.GetCredential().GetKey() != "test-openai-key" || createdCredential.GetSecret().GetKind() != "external_ref" {
		t.Fatalf("unexpected credential: %#v", &createdCredential)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "list", "--owner-type", "system", "--owner-id", "daemon-test")
	if err != nil {
		t.Fatalf("credential list failed: %v\n%s", err, out)
	}
	var credentials adminv1.AdminInferenceServiceListCredentialsResponse
	if err := json.Unmarshal([]byte(out), &credentials); err != nil || len(credentials.GetCredentials()) != 1 || credentials.GetCredentials()[0].GetKey() != "test-openai-key" {
		t.Fatalf("unexpected credentials: %#v err=%v out=%s", &credentials, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "grant", "test-openai-key", "--space-id", spaceID, "--domain", "default", "--semantic-index", index.GetSemanticIndexId(), "--model-endpoint", "test-openai", "--model", "test/text-embedding", "--allow-background-use")
	if err != nil {
		t.Fatalf("credential grant failed: %v\n%s", err, out)
	}
	var createdGrant adminv1.AdminInferenceServiceCreateCredentialGrantResponse
	if err := json.Unmarshal([]byte(out), &createdGrant); err != nil {
		t.Fatalf("decode credential grant: %v\n%s", err, out)
	}
	if createdGrant.GetCredentialGrant().GetCredentialId() != createdCredential.GetCredential().GetCredentialId() || !createdGrant.GetCredentialGrant().GetAllowBackgroundUse() {
		t.Fatalf("unexpected grant: %#v", &createdGrant)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "policy", "allow", "--space-id", spaceID, "--domain", "default", "--reason", "daemon test")
	if err != nil {
		t.Fatalf("policy allow failed: %v\n%s", err, out)
	}
	var createdPolicy adminv1.AdminInferenceServiceCreateInferencePolicyResponse
	if err := json.Unmarshal([]byte(out), &createdPolicy); err != nil {
		t.Fatalf("decode policy: %v\n%s", err, out)
	}
	if createdPolicy.GetInferencePolicy().GetEffect() != "allow" {
		t.Fatalf("unexpected policy: %#v", &createdPolicy)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "policy", "list", "--space-id", spaceID, "--effect", "allow")
	if err != nil {
		t.Fatalf("policy list failed: %v\n%s", err, out)
	}
	var policies adminv1.AdminInferenceServiceListInferencePoliciesResponse
	if err := json.Unmarshal([]byte(out), &policies); err != nil || len(policies.GetInferencePolicies()) != 1 || policies.GetInferencePolicies()[0].GetReason() != "daemon test" {
		t.Fatalf("unexpected policies: %#v err=%v out=%s", &policies, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "model-endpoint", "disable", "test-openai")
	if err != nil {
		t.Fatalf("model endpoint disable failed: %v\n%s", err, out)
	}
	var disabledEndpoint adminv1.AdminInferenceServiceSetModelEndpointEnabledResponse
	if err := json.Unmarshal([]byte(out), &disabledEndpoint); err != nil || disabledEndpoint.GetModelEndpoint().GetEnabled() {
		t.Fatalf("unexpected disabled endpoint: %#v err=%v out=%s", &disabledEndpoint, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "revoked", "test-openai-key")
	if err != nil {
		t.Fatalf("credential revoke failed: %v\n%s", err, out)
	}
	var revoked adminv1.AdminInferenceServiceSetCredentialStatusResponse
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
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "grant", "expire", createdGrant.GetCredentialGrant().GetCredentialGrantId(), "--space-id", spaceID)
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
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "credential", "delete", "test-openai-key", "--delete-secret")
	if err != nil {
		t.Fatalf("credential delete failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "capability", "delete", applied.GetModelEndpointCapabilities()[0].GetModelEndpointCapabilityId())
	if err != nil {
		t.Fatalf("capability delete failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "model-endpoint", "delete", "test-openai")
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
	dataDir, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
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
	ownerID := identity.UserID(uuid.MustParse(createdSpace.GetSpace().GetOwner().GetId()))
	embeddings := storeembedding.NewManager()
	if err := embeddings.Init(context.Background(), filepath.Join(dataDir, "meta", "embedding"), ""); err != nil {
		t.Fatalf("init legacy embedding store: %v", err)
	}
	if _, err := embeddings.AddKey(context.Background(), storeembedding.AddKeyInput{OwnerID: ownerID, ProviderID: "openai", Name: "OpenAI Legacy", APIKey: "sk-test", IsDefault: true}); err != nil {
		t.Fatalf("add legacy key: %v", err)
	}
	if _, err := embeddings.AddProfile(context.Background(), storeembedding.AddProfileInput{OwnerID: ownerID, Name: "Legacy Notes", ProviderID: "openai", ModelID: "openai/text-embedding-3-small", SourceMode: domainembedding.SourceModeSelf, MinimumTextLength: 1}); err != nil {
		t.Fatalf("add legacy profile: %v", err)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "migrate", "legacy-embeddings", "--space-id", createdSpace.GetSpace().GetSpaceId(), "--domain", "default", "--dry-run")
	if err != nil {
		t.Fatalf("semantic migration dry-run failed: %v\n%s", err, out)
	}
	var migrated adminv1.MigrateLegacyEmbeddingsResponse
	if err := json.Unmarshal([]byte(out), &migrated); err != nil {
		t.Fatalf("decode migration response: %v\n%s", err, out)
	}
	if !migrated.GetDryRun() || migrated.GetProfilesSeen() != 1 || migrated.GetProfilesMigrated() != 1 || migrated.GetProfilesSkipped() != 0 {
		t.Fatalf("unexpected migration response: %#v", &migrated)
	}
}

func TestAdminSemanticMaintenanceUsesDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
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
}
