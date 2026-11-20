package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestValidateProductionBackupSetAcceptsCompleteFixture(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {})
	_, marker, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true, ValidateLocalArtifactBytes: true, ExpectedNodeCount: 1, AvailableTargetBytes: 1 << 20})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	if marker == nil {
		t.Fatalf("marker=nil, want COMPLETE marker")
	}
	if !report.Valid() {
		t.Fatalf("report invalid: %#v", report.Errors)
	}
	if report.Capacity.RequiredBytes == 0 || report.Capacity.AvailableBytes == 0 {
		t.Fatalf("capacity estimate was not populated: %#v", report.Capacity)
	}
}

func TestValidateProductionBackupSetRejectsMissingComplete(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {})
	if err := os.Remove(filepath.Join(dir, "COMPLETE")); err != nil {
		t.Fatal(err)
	}
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "complete_marker_missing")
}

func TestValidateProductionBackupSetRejectsManifestChecksumMismatch(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {})
	path := filepath.Join(dir, BackupSetManifestName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, ' ', '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "complete_marker_manifest_checksum_mismatch")
}

func TestValidateProductionBackupSetRejectsCorruptRequiredArtifact(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {})
	if err := os.WriteFile(filepath.Join(dir, "node-0.tar"), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true, ValidateLocalArtifactBytes: true})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "artifact_checksum_mismatch")
}

func TestValidateProductionBackupSetRejectsUnsupportedRequiredFeature(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {
		m.Compatibility.RequiredFeatures = []string{"future.logical.import"}
	})
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "unsupported_required_feature")
}

func TestValidateProductionBackupSetRejectsTopologyMismatch(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {})
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true, ExpectedNodeCount: 3})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "target_topology_mismatch")
}

func TestValidateProductionBackupSetRejectsRawSecretMaterial(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {
		m.Repository.CredentialRef = "aws_secret_access_key=very-secret"
	})
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "raw_secret_material")
}

func TestValidateProductionBackupSetRejectsRawSecretArtifactEvidence(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {})
	if err := os.WriteFile(filepath.Join(dir, "node-0.manifest.json"), []byte("authorization: bearer abcdefghijklmnopqrstuvwxyz"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true, ValidateLocalArtifactBytes: true})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "raw_secret_material")
}

func TestValidateProductionBackupSetRejectsInsufficientCapacity(t *testing.T) {
	dir := writeProductionBackupSetFixture(t, func(m *BackupSetManifestV1) {})
	_, _, report, err := ValidateBackupSetPath(context.Background(), dir, BackupSetValidationOptions{RequireCompleteMarker: true, AvailableTargetBytes: 1})
	if err != nil {
		t.Fatalf("ValidateBackupSetPath() error = %v", err)
	}
	assertBackupSetIssue(t, report, "insufficient_target_capacity")
}

func TestComputeDeterministicTreeChecksumIsStableAndOrderIndependent(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	for _, dir := range []string{dirA, dirB} {
		if err := os.Mkdir(filepath.Join(dir, "nested"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dirA, "b.txt"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirA, "nested", "a.txt"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirB, "nested", "a.txt"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirB, "b.txt"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := ComputeDeterministicTreeChecksum(context.Background(), dirA)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ComputeDeterministicTreeChecksum(context.Background(), dirB)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("tree checksum mismatch: %#v != %#v", first, second)
	}
}

func assertBackupSetIssue(t *testing.T, report BackupSetValidationReport, reason string) {
	t.Helper()
	for _, issue := range report.Errors {
		if issue.Reason == reason {
			return
		}
	}
	t.Fatalf("report errors did not include %q: %#v", reason, report.Errors)
}

func writeProductionBackupSetFixture(t *testing.T, mutate func(*BackupSetManifestV1)) string {
	t.Helper()
	dir := t.TempDir()
	archive := []byte("node archive bytes")
	inventory := []byte(`{"paths":[]}`)
	nodeManifest := []byte(`{"schema":"node-manifest"}`)
	archiveChecksum := writeFixtureFile(t, filepath.Join(dir, "node-0.tar"), archive)
	inventoryChecksum := writeFixtureFile(t, filepath.Join(dir, "node-0.inventory.json"), inventory)
	writeFixtureFile(t, filepath.Join(dir, "node-0.manifest.json"), nodeManifest)
	now := time.Date(2026, 11, 20, 1, 2, 3, 0, time.UTC)
	policyHash := strings.Repeat("c", 64)
	manifest := BackupSetManifestV1{
		Schema:          ProductionBackupSetSchema,
		ManifestVersion: ProductionManifestVersion,
		BackupSetID:     "backup-set-20261120T010203Z-cluster-a",
		State:           "SUCCEEDED",
		Complete:        true,
		CreatedAt:       now,
		CompletedAt:     now.Add(time.Minute),
		Operation: BackupSetOperation{
			BackupOperationID: "backup-op-1",
			Trigger:           "TEST",
			StartedAt:         now,
			CompletedAt:       now.Add(time.Minute),
			PolicyID:          "policy-default",
			PolicyRevision:    "1",
			PolicyHashSHA256:  policyHash,
		},
		Source: BackupSetSource{
			ClusterID:             "cluster-a",
			ClusterName:           "cluster-a",
			Namespace:             "commonfolio-staging2",
			StatefulSet:           "myceld",
			DataDir:               "/data/mycel",
			MycelVersion:          "v0.24.0-dev",
			MycelAPIVersion:       "v0.24.0-dev",
			Image:                 "myceldb/mycel:v0.24.0-dev",
			ImageDigest:           "sha256:" + strings.Repeat("d", 64),
			StorageFormatVersions: map[string]string{"system_raft": "1", "graph": "1", "semantic_records": "1"},
			Topology:              BackupSetSourceTopology{NodeCount: 1, PartitionCount: 1, ReplicaFactor: 1, Nodes: []BackupSetSourceNode{{NodeID: "node-1", NodeName: "node-1", PodName: "myceld-0", Ordinal: 0, RaftNodeID: "1", PVCRef: "pvc/myceld-data-myceld-0"}}},
			MembershipHashSHA256:  strings.Repeat("a", 64),
			PlacementHashSHA256:   strings.Repeat("b", 64),
		},
		Repository:           BackupSetRepository{RepositoryID: "repo-local", Type: "S3_COMPATIBLE", URI: "file://" + dir, Bucket: "mycel-backups", Prefix: "clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a", EndpointFingerprint: "local:test", CredentialRef: "k8s-secret:backup/repo#credentials", Encryption: BackupSetEncryptionPolicy{Mode: "SSE_S3"}, LayoutVersion: 1},
		Policy:               BackupSetPolicy{PolicyID: "policy-default", PolicyRevision: "1", PolicyHashSHA256: policyHash, RetentionClass: "DEFAULT", Consistency: json.RawMessage(`{"quiesce_required":true}`), Capture: json.RawMessage(`{"provider":"FILESYSTEM_IMMUTABLE_COPY"}`), Upload: json.RawMessage(`{"parts":"single"}`), Validation: json.RawMessage(`{"checksum_algorithm":"SHA256"}`), DerivedSubsystems: []BackupSetDerivedPolicy{{Subsystem: "LEXICAL_SEARCH", Mode: "INCLUDE_OPTIONAL"}}},
		Compatibility:        BackupSetCompatibility{RequiredFeatures: []string{}, StorageFormatVersions: map[string]string{"system_raft": "1", "graph": "1", "semantic_records": "1"}, MinRestoreMycelVersion: "v0.24.0-dev", SupportedRestoreTargetModes: []string{"NEW_CLUSTER_BOOTSTRAP", "EXISTING_CLUSTER_REPLACE"}, SupportedIdentityModes: []string{"PRESERVE_BACKUP_ID"}, RequiresSameTopology: true, RequiresSourceFencingForPreservedIdentity: true},
		Consistency:          BackupSetConsistency{Quiescence: json.RawMessage(`{"mode":"BACKUP"}`), BarrierSet: CheckpointBarrierSetV1{SchemaVersion: 1, BarrierSetID: "barrier-1", BackupOperationID: "backup-op-1", BackupSetID: "backup-set-20261120T010203Z-cluster-a", ClusterID: "cluster-a", QuiesceLeaseID: "lease-1", EstablishedAt: now, CoordinatorNodeID: "node-1", CoordinatorRaftNodeID: "1", ReadAllowlistProfile: "CONTROL_ONLY", WriteGateClosedAt: now, Participants: []json.RawMessage{json.RawMessage(`{"name":"graph","state":"quiesced"}`)}, RaftGroups: []CheckpointRaftGroup{{GroupID: "system", GroupKind: "SYSTEM", BarrierIndex: "10", CommitIndex: "10", Term: "1", LeaderRaftNodeID: "1", Members: []string{"1"}, PerNode: []CheckpointRaftNodeState{{NodeID: "node-1", PodName: "myceld-0", RaftNodeID: "1", AppliedIndex: "10", LastIndex: "10", SnapshotIndex: "0"}}}}, SystemMetadata: CheckpointSystemMetadata{ClusterID: "cluster-a", ClusterName: "cluster-a", BootstrapEpoch: "1", NodeCount: 1, PartitionCount: 1, ReplicaFactor: 1, SystemRaftGroupID: "system", SystemRaftBarrierIndex: "10", MembershipHashSHA256: strings.Repeat("a", 64), PlacementHashSHA256: strings.Repeat("b", 64), ExpectedNodes: []BackupSetSourceNode{{NodeID: "node-1", NodeName: "node-1", PodName: "myceld-0", Ordinal: 0, RaftNodeID: "1"}}}, Identity: CheckpointIdentity{IdentityRaftGroupID: "system", IdentityRaftBarrierIndex: "10", AuthorizationRevision: "10", PrincipalStoreChecksumSHA256: strings.Repeat("1", 64), RoleBindingChecksumSHA256: strings.Repeat("2", 64), CapabilityGrantChecksumSHA256: strings.Repeat("3", 64), RefreshSessionStoreChecksumSHA256: strings.Repeat("4", 64), AuthSessionPolicy: "PRESERVE_RESTORED_SESSIONS"}}},
		Capture:              BackupSetCapture{Provider: "FILESYSTEM_IMMUTABLE_COPY", ProviderVersion: "test", DataMover: "local-test", ArchiveFormat: "TAR", TemporaryProviderRefsRedacted: true, ProviderEvidence: json.RawMessage(`{"scratch":"redacted"}`)},
		Artifacts:            BackupSetArtifacts{ArchiveFormat: "TAR", ChecksumAlgorithm: ProductionChecksumAlgorithmSHA256, Nodes: []BackupSetNodeArtifact{{ArtifactID: "node:0", PodName: "myceld-0", NodeID: "node-1", NodeName: "node-1", Ordinal: 0, RaftNodeID: "1", SourcePVCRef: "pvc/myceld-data-myceld-0", SourceDataDir: "/data/mycel", ArchiveName: "node-0.tar", ArchiveURI: "node-0.tar", ManifestName: "node-0.manifest.json", ManifestURI: "node-0.manifest.json", InventoryURI: "node-0.inventory.json", InventoryChecksumSHA256: inventoryChecksum, SizeBytes: decimalString(len(archive)), ChecksumSHA256: archiveChecksum, CaptureSource: json.RawMessage(`{"provider":"local"}`), AppliedIndexes: map[string]string{"system": "10"}, RaftFreeze: BackupSetNodeRaftFreeze{LeaseID: "lease-1", AcquiredAt: now, ReleasedAt: now.Add(30 * time.Second), ExpiresAt: now.Add(time.Minute), Groups: []BackupSetNodeRaftFreezeGroup{{GroupID: "system", BarrierIndex: "10", AppliedIndex: "10", CommitIndex: "10", Term: "1", LastIndex: "10", SnapshotIndex: "0", Leader: "1"}}}}}, AdditionalManifests: []BackupSetArtifactRef{}},
		ExternalObjectStores: []BackupSetExternalObjectStore{},
		DerivedState:         []BackupSetDerivedState{{Subsystem: "LEXICAL_SEARCH", ArtifactID: "lexical:rebuild", PolicyMode: "INCLUDE_OPTIONAL", Included: false, RestorePolicy: "REBUILD_REQUIRED", SourceFrontierKind: "GRAPH_REVISION", SourceFrontierValue: "10", ValidationState: "REBUILD_REQUIRED"}},
		Validation:           BackupSetValidation{Result: ProductionValidationStatePassed, ValidatedAt: now.Add(time.Minute), ValidatorVersion: "test", ExpectedNodeCount: 1, NodeArtifactCount: 1, RequiredArtifactCount: 1, ValidatedArtifactCount: 1, TotalArtifactBytes: decimalString(len(archive)), ChecksumAlgorithm: ProductionChecksumAlgorithmSHA256, Checks: []BackupSetCheckResult{{Name: "fixture", Result: ProductionValidationStatePassed, CheckedAt: now}}, Warnings: []string{}, Errors: []string{}},
		Restore:              BackupSetRestore{SupportedRestoreTargetModes: []string{"NEW_CLUSTER_BOOTSTRAP", "EXISTING_CLUSTER_REPLACE"}, SupportedIdentityModes: []string{"PRESERVE_BACKUP_ID"}, DefaultIdentityMode: "PRESERVE_BACKUP_ID", SupportedAuthSessionPolicies: []string{"PRESERVE_RESTORED_SESSIONS", "REVOKE_ALL_SESSIONS"}, RequiresSameTopology: true, RequiredSecretRefs: []string{"k8s-secret:mycel/user-store-key"}, RequiredExternalDependencies: []string{}},
		Audit:                BackupSetAudit{Labels: map[string]string{}, Annotations: map[string]string{}, Redactions: []string{"credential_ref"}},
	}
	mutate(&manifest)
	manifestRaw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	manifestRaw = append(manifestRaw, '\n')
	manifestPath := filepath.Join(dir, BackupSetManifestName)
	if err := os.WriteFile(manifestPath, manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestChecksum := sha256.Sum256(manifestRaw)
	complete := CompleteMarkerV1{Schema: ProductionBackupCompleteSchema, MarkerVersion: ProductionCompleteMarkerVersion, BackupSetID: manifest.BackupSetID, State: manifest.State, RepositoryID: manifest.Repository.RepositoryID, ManifestURI: "file://" + manifestPath, ManifestSizeBytes: decimalString(len(manifestRaw)), ManifestChecksumSHA256: hex.EncodeToString(manifestChecksum[:]), CompletedAt: manifest.CompletedAt, ArtifactCount: 1, TotalArtifactBytes: decimalString(len(archive))}
	completeRaw, err := json.MarshalIndent(complete, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "COMPLETE"), append(completeRaw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func writeFixtureFile(t *testing.T, path string, data []byte) string {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func decimalString(n int) string { return strconv.Itoa(n) }
