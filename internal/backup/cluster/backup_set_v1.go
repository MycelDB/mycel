package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ProductionBackupSetSchema         = "mycel.backup_set"
	ProductionBackupCompleteSchema    = "mycel.backup_complete"
	ProductionManifestVersion         = 1
	ProductionCompleteMarkerVersion   = 1
	ProductionValidationStatePassed   = "PASSED"
	ProductionValidationStateFailed   = "FAILED"
	ProductionChecksumAlgorithmSHA256 = "SHA256"
)

type BackupSetDocument struct {
	Manifest BackupSetManifestV1
	Raw      []byte
	Present  map[string]bool
}

type BackupSetManifestV1 struct {
	Schema               string                         `json:"schema"`
	ManifestVersion      int                            `json:"manifest_version"`
	BackupSetID          string                         `json:"backup_set_id"`
	State                string                         `json:"state"`
	Complete             bool                           `json:"complete"`
	CreatedAt            time.Time                      `json:"created_at"`
	CompletedAt          time.Time                      `json:"completed_at"`
	Operation            BackupSetOperation             `json:"operation"`
	Source               BackupSetSource                `json:"source"`
	Repository           BackupSetRepository            `json:"repository"`
	Policy               BackupSetPolicy                `json:"policy"`
	Compatibility        BackupSetCompatibility         `json:"compatibility"`
	Consistency          BackupSetConsistency           `json:"consistency"`
	Capture              BackupSetCapture               `json:"capture"`
	Artifacts            BackupSetArtifacts             `json:"artifacts"`
	ExternalObjectStores []BackupSetExternalObjectStore `json:"external_object_stores"`
	DerivedState         []BackupSetDerivedState        `json:"derived_state"`
	Validation           BackupSetValidation            `json:"validation"`
	Restore              BackupSetRestore               `json:"restore"`
	Audit                BackupSetAudit                 `json:"audit"`
	Extensions           map[string]json.RawMessage     `json:"extensions,omitempty"`
}

type BackupSetOperation struct {
	BackupOperationID  string    `json:"backup_operation_id"`
	Trigger            string    `json:"trigger"`
	Reason             string    `json:"reason,omitempty"`
	RequestedByRef     string    `json:"requested_by_ref,omitempty"`
	IdempotencyKeyHash string    `json:"idempotency_key_hash,omitempty"`
	StartedAt          time.Time `json:"started_at"`
	CompletedAt        time.Time `json:"completed_at"`
	ScheduleID         string    `json:"schedule_id,omitempty"`
	PolicyID           string    `json:"policy_id"`
	PolicyRevision     string    `json:"policy_revision"`
	PolicyHashSHA256   string    `json:"policy_hash_sha256"`
}

type BackupSetSource struct {
	ClusterID             string                  `json:"cluster_id"`
	ClusterName           string                  `json:"cluster_name"`
	Namespace             string                  `json:"namespace,omitempty"`
	StatefulSet           string                  `json:"statefulset,omitempty"`
	DataDir               string                  `json:"data_dir"`
	MycelVersion          string                  `json:"mycel_version"`
	MycelAPIVersion       string                  `json:"mycel_api_version"`
	Image                 string                  `json:"image,omitempty"`
	ImageDigest           string                  `json:"image_digest,omitempty"`
	StorageFormatVersions map[string]string       `json:"storage_format_versions"`
	Topology              BackupSetSourceTopology `json:"topology"`
	MembershipHashSHA256  string                  `json:"membership_hash_sha256"`
	PlacementHashSHA256   string                  `json:"placement_hash_sha256"`
}

type BackupSetSourceTopology struct {
	NodeCount      int                   `json:"node_count"`
	PartitionCount int                   `json:"partition_count"`
	ReplicaFactor  int                   `json:"replica_factor"`
	Nodes          []BackupSetSourceNode `json:"nodes"`
}

type BackupSetSourceNode struct {
	NodeID     string `json:"node_id"`
	NodeName   string `json:"node_name"`
	PodName    string `json:"pod_name"`
	Ordinal    int    `json:"ordinal"`
	RaftNodeID string `json:"raft_node_id"`
	PVCRef     string `json:"pvc_ref,omitempty"`
}

type BackupSetRepository struct {
	RepositoryID        string                    `json:"repository_id"`
	Type                string                    `json:"type"`
	URI                 string                    `json:"uri"`
	Bucket              string                    `json:"bucket,omitempty"`
	Prefix              string                    `json:"prefix,omitempty"`
	EndpointFingerprint string                    `json:"endpoint_fingerprint"`
	CredentialRef       string                    `json:"credential_ref"`
	Encryption          BackupSetEncryptionPolicy `json:"encryption"`
	LayoutVersion       int                       `json:"layout_version"`
}

type BackupSetEncryptionPolicy struct {
	Mode           string `json:"mode"`
	KMSKeyRef      string `json:"kms_key_ref,omitempty"`
	KeyFingerprint string `json:"key_fingerprint,omitempty"`
}

type BackupSetPolicy struct {
	PolicyID          string                   `json:"policy_id"`
	PolicyRevision    string                   `json:"policy_revision"`
	PolicyHashSHA256  string                   `json:"policy_hash_sha256"`
	RetentionClass    string                   `json:"retention_class"`
	Consistency       json.RawMessage          `json:"consistency"`
	Capture           json.RawMessage          `json:"capture"`
	Upload            json.RawMessage          `json:"upload"`
	Validation        json.RawMessage          `json:"validation"`
	DerivedSubsystems []BackupSetDerivedPolicy `json:"derived_subsystems"`
}

type BackupSetDerivedPolicy struct {
	Subsystem                string `json:"subsystem"`
	Mode                     string `json:"mode"`
	DegradedRestoreAllowance string `json:"degraded_restore_allowance,omitempty"`
}

type BackupSetCompatibility struct {
	RequiredFeatures                          []string          `json:"required_features"`
	StorageFormatVersions                     map[string]string `json:"storage_format_versions"`
	MinRestoreMycelVersion                    string            `json:"min_restore_mycel_version"`
	MaxRestoreMycelVersion                    string            `json:"max_restore_mycel_version,omitempty"`
	SupportedRestoreTargetModes               []string          `json:"supported_restore_target_modes"`
	SupportedIdentityModes                    []string          `json:"supported_identity_modes"`
	RequiresSameTopology                      bool              `json:"requires_same_topology"`
	RequiresSourceFencingForPreservedIdentity bool              `json:"requires_source_fencing_for_preserved_identity"`
}

type BackupSetConsistency struct {
	Quiescence json.RawMessage        `json:"quiescence"`
	BarrierSet CheckpointBarrierSetV1 `json:"barrier_set"`
}

type CheckpointBarrierSetV1 struct {
	SchemaVersion         int                          `json:"schema_version"`
	BarrierSetID          string                       `json:"barrier_set_id"`
	BackupOperationID     string                       `json:"backup_operation_id"`
	BackupSetID           string                       `json:"backup_set_id"`
	ClusterID             string                       `json:"cluster_id"`
	QuiesceLeaseID        string                       `json:"quiesce_lease_id"`
	EstablishedAt         time.Time                    `json:"established_at"`
	CoordinatorNodeID     string                       `json:"coordinator_node_id"`
	CoordinatorRaftNodeID string                       `json:"coordinator_raft_node_id"`
	ReadAllowlistProfile  string                       `json:"read_allowlist_profile"`
	WriteGateClosedAt     time.Time                    `json:"write_gate_closed_at"`
	Participants          []json.RawMessage            `json:"participants"`
	RaftGroups            []CheckpointRaftGroup        `json:"raft_groups"`
	SystemMetadata        CheckpointSystemMetadata     `json:"system_metadata"`
	GraphDomains          []CheckpointGraphDomain      `json:"graph_domains"`
	Identity              CheckpointIdentity           `json:"identity"`
	UsageAccounting       []CheckpointSubsystemBarrier `json:"usage_accounting"`
	ObjectReferences      []CheckpointObjectReference  `json:"object_references"`
	GraphChangeFeeds      []CheckpointSubsystemBarrier `json:"graph_change_feeds"`
	Automation            []CheckpointSubsystemBarrier `json:"automation"`
	Semantic              []CheckpointSubsystemBarrier `json:"semantic"`
	Lexical               []CheckpointSubsystemBarrier `json:"lexical"`
	Projections           []CheckpointSubsystemBarrier `json:"projections"`
	DerivedArtifacts      []CheckpointDerivedArtifact  `json:"derived_artifacts"`
}

type CheckpointRaftGroup struct {
	GroupID          string                    `json:"group_id"`
	GroupKind        string                    `json:"group_kind"`
	PartitionID      string                    `json:"partition_id,omitempty"`
	BarrierIndex     string                    `json:"barrier_index"`
	CommitIndex      string                    `json:"commit_index"`
	Term             string                    `json:"term"`
	LeaderRaftNodeID string                    `json:"leader_raft_node_id"`
	Members          []string                  `json:"members"`
	PerNode          []CheckpointRaftNodeState `json:"per_node"`
}

type CheckpointRaftNodeState struct {
	NodeID        string `json:"node_id"`
	PodName       string `json:"pod_name"`
	RaftNodeID    string `json:"raft_node_id"`
	AppliedIndex  string `json:"applied_index"`
	LastIndex     string `json:"last_index"`
	SnapshotIndex string `json:"snapshot_index"`
}

type CheckpointSystemMetadata struct {
	ClusterID              string                `json:"cluster_id"`
	ClusterName            string                `json:"cluster_name"`
	BootstrapEpoch         string                `json:"bootstrap_epoch"`
	NodeCount              int                   `json:"node_count"`
	PartitionCount         int                   `json:"partition_count"`
	ReplicaFactor          int                   `json:"replica_factor"`
	SystemRaftGroupID      string                `json:"system_raft_group_id"`
	SystemRaftBarrierIndex string                `json:"system_raft_barrier_index"`
	MembershipHashSHA256   string                `json:"membership_hash_sha256"`
	PlacementHashSHA256    string                `json:"placement_hash_sha256"`
	ExpectedNodes          []BackupSetSourceNode `json:"expected_nodes"`
}

type CheckpointGraphDomain struct {
	SpaceID                      string `json:"space_id"`
	DomainID                     string `json:"domain_id"`
	PartitionGroupID             string `json:"partition_group_id"`
	RaftBarrierIndex             string `json:"raft_barrier_index"`
	GraphRevision                string `json:"graph_revision"`
	DomainManifestChecksumSHA256 string `json:"domain_manifest_checksum_sha256"`
	SegmentSetChecksumSHA256     string `json:"segment_set_checksum_sha256"`
	LatestStateChecksumSHA256    string `json:"latest_state_checksum_sha256,omitempty"`
	GraphChangeJournalRevision   string `json:"graph_change_journal_revision,omitempty"`
	NotificationCurrentRevision  string `json:"notification_current_revision,omitempty"`
}

type CheckpointIdentity struct {
	IdentityRaftGroupID               string `json:"identity_raft_group_id"`
	IdentityRaftBarrierIndex          string `json:"identity_raft_barrier_index"`
	AuthorizationRevision             string `json:"authorization_revision"`
	PrincipalStoreChecksumSHA256      string `json:"principal_store_checksum_sha256"`
	RoleBindingChecksumSHA256         string `json:"role_binding_checksum_sha256"`
	CapabilityGrantChecksumSHA256     string `json:"capability_grant_checksum_sha256"`
	RefreshSessionStoreChecksumSHA256 string `json:"refresh_session_store_checksum_sha256"`
	AuthSessionPolicy                 string `json:"auth_session_policy"`
}

type CheckpointSubsystemBarrier struct {
	Subsystem             string `json:"subsystem"`
	Scope                 string `json:"scope"`
	SpaceID               string `json:"space_id,omitempty"`
	DomainID              string `json:"domain_id,omitempty"`
	OwnerRaftGroupID      string `json:"owner_raft_group_id"`
	OwnerRaftBarrierIndex string `json:"owner_raft_barrier_index"`
	SourceFrontierKind    string `json:"source_frontier_kind"`
	SourceFrontierValue   string `json:"source_frontier_value"`
	ChecksumSHA256        string `json:"checksum_sha256,omitempty"`
	ValidationState       string `json:"validation_state"`
	ValidationError       string `json:"validation_error,omitempty"`
}

type CheckpointObjectReference struct {
	Usage                                 string    `json:"usage"`
	Provider                              string    `json:"provider"`
	PayloadBackend                        string    `json:"payload_backend"`
	MetadataRaftGroupID                   string    `json:"metadata_raft_group_id"`
	MetadataRaftBarrierIndex              string    `json:"metadata_raft_barrier_index"`
	ObjectReferenceManifestURI            string    `json:"object_reference_manifest_uri"`
	ObjectReferenceManifestChecksumSHA256 string    `json:"object_reference_manifest_checksum_sha256"`
	ObjectCount                           string    `json:"object_count"`
	LogicalBytes                          string    `json:"logical_bytes"`
	ExternalStoreRef                      string    `json:"external_store_ref"`
	ExternalRecoveryPointRef              string    `json:"external_recovery_point_ref"`
	ValidatedAt                           time.Time `json:"validated_at"`
}

type CheckpointDerivedArtifact struct {
	Subsystem           string `json:"subsystem"`
	ArtifactID          string `json:"artifact_id"`
	PolicyMode          string `json:"policy_mode"`
	Included            bool   `json:"included"`
	RestorePolicy       string `json:"restore_policy"`
	SourceFrontierKind  string `json:"source_frontier_kind"`
	SourceFrontierValue string `json:"source_frontier_value"`
	ArtifactURI         string `json:"artifact_uri,omitempty"`
	ChecksumSHA256      string `json:"checksum_sha256,omitempty"`
	FormatVersion       string `json:"format_version,omitempty"`
	ValidationState     string `json:"validation_state"`
}

type BackupSetCapture struct {
	Provider                      string          `json:"provider"`
	ProviderVersion               string          `json:"provider_version"`
	FallbackUsed                  bool            `json:"fallback_used"`
	SnapshotClassName             string          `json:"snapshot_class_name,omitempty"`
	DataMover                     string          `json:"data_mover"`
	ArchiveFormat                 string          `json:"archive_format"`
	CompressionLevel              int             `json:"compression_level,omitempty"`
	TemporaryProviderRefsRedacted bool            `json:"temporary_provider_refs_redacted"`
	ProviderEvidence              json.RawMessage `json:"provider_evidence"`
}

type BackupSetArtifacts struct {
	ArchiveFormat       string                  `json:"archive_format"`
	ChecksumAlgorithm   string                  `json:"checksum_algorithm"`
	Nodes               []BackupSetNodeArtifact `json:"nodes"`
	AdditionalManifests []BackupSetArtifactRef  `json:"additional_manifests"`
}

type BackupSetArtifactRef struct {
	Purpose        string `json:"purpose"`
	URI            string `json:"uri"`
	ChecksumSHA256 string `json:"checksum_sha256"`
	SizeBytes      string `json:"size_bytes"`
}

type BackupSetNodeArtifact struct {
	ArtifactID              string                  `json:"artifact_id"`
	PodName                 string                  `json:"pod_name"`
	NodeID                  string                  `json:"node_id"`
	NodeName                string                  `json:"node_name"`
	Ordinal                 int                     `json:"ordinal"`
	RaftNodeID              string                  `json:"raft_node_id"`
	SourcePVCRef            string                  `json:"source_pvc_ref"`
	SourceDataDir           string                  `json:"source_data_dir"`
	ArchiveName             string                  `json:"archive_name"`
	ArchiveURI              string                  `json:"archive_uri"`
	ManifestName            string                  `json:"manifest_name"`
	ManifestURI             string                  `json:"manifest_uri"`
	InventoryURI            string                  `json:"inventory_uri"`
	InventoryChecksumSHA256 string                  `json:"inventory_checksum_sha256"`
	SizeBytes               string                  `json:"size_bytes"`
	ChecksumSHA256          string                  `json:"checksum_sha256"`
	CaptureSource           json.RawMessage         `json:"capture_source"`
	AppliedIndexes          map[string]string       `json:"applied_indexes"`
	RaftFreeze              BackupSetNodeRaftFreeze `json:"raft_freeze"`
}

type BackupSetNodeRaftFreeze struct {
	LeaseID    string                         `json:"lease_id"`
	AcquiredAt time.Time                      `json:"acquired_at"`
	ReleasedAt time.Time                      `json:"released_at"`
	ExpiresAt  time.Time                      `json:"expires_at"`
	Groups     []BackupSetNodeRaftFreezeGroup `json:"groups"`
}

type BackupSetNodeRaftFreezeGroup struct {
	GroupID       string `json:"group_id"`
	BarrierIndex  string `json:"barrier_index"`
	AppliedIndex  string `json:"applied_index"`
	CommitIndex   string `json:"commit_index,omitempty"`
	Term          string `json:"term,omitempty"`
	LastIndex     string `json:"last_index,omitempty"`
	SnapshotIndex string `json:"snapshot_index,omitempty"`
	Leader        string `json:"leader,omitempty"`
}

type BackupSetExternalObjectStore struct {
	Usage                                 string `json:"usage"`
	Provider                              string `json:"provider"`
	IncludePayloadBytes                   bool   `json:"include_payload_bytes"`
	Bucket                                string `json:"bucket"`
	Prefix                                string `json:"prefix"`
	EndpointFingerprint                   string `json:"endpoint_fingerprint"`
	CredentialRef                         string `json:"credential_ref"`
	ExternalStoreRef                      string `json:"external_store_ref,omitempty"`
	BackupPolicyRef                       string `json:"backup_policy_ref,omitempty"`
	RecoveryPointRef                      string `json:"recovery_point_ref,omitempty"`
	ObjectReferenceManifestURI            string `json:"object_reference_manifest_uri"`
	ObjectReferenceManifestChecksumSHA256 string `json:"object_reference_manifest_checksum_sha256"`
	ObjectCount                           string `json:"object_count"`
	LogicalBytes                          string `json:"logical_bytes"`
	ValidationState                       string `json:"validation_state"`
}

type BackupSetDerivedState struct {
	Subsystem           string          `json:"subsystem"`
	ArtifactID          string          `json:"artifact_id"`
	PolicyMode          string          `json:"policy_mode"`
	Included            bool            `json:"included"`
	RestorePolicy       string          `json:"restore_policy"`
	SourceFrontierKind  string          `json:"source_frontier_kind,omitempty"`
	SourceFrontierValue string          `json:"source_frontier_value,omitempty"`
	ArtifactURI         string          `json:"artifact_uri,omitempty"`
	SizeBytes           string          `json:"size_bytes,omitempty"`
	ChecksumSHA256      string          `json:"checksum_sha256,omitempty"`
	FormatVersion       string          `json:"format_version,omitempty"`
	ValidationState     string          `json:"validation_state"`
	Metadata            json.RawMessage `json:"metadata,omitempty"`
}

type BackupSetValidation struct {
	Result                 string                 `json:"result"`
	ValidatedAt            time.Time              `json:"validated_at"`
	ValidatorVersion       string                 `json:"validator_version"`
	ExpectedNodeCount      int                    `json:"expected_node_count"`
	NodeArtifactCount      int                    `json:"node_artifact_count"`
	RequiredArtifactCount  int                    `json:"required_artifact_count"`
	ValidatedArtifactCount int                    `json:"validated_artifact_count"`
	TotalArtifactBytes     string                 `json:"total_artifact_bytes"`
	ChecksumAlgorithm      string                 `json:"checksum_algorithm"`
	Checks                 []BackupSetCheckResult `json:"checks"`
	Warnings               []string               `json:"warnings"`
	Errors                 []string               `json:"errors"`
}

type BackupSetCheckResult struct {
	Name      string    `json:"name"`
	Result    string    `json:"result"`
	CheckedAt time.Time `json:"checked_at"`
	Detail    string    `json:"detail,omitempty"`
}

type BackupSetRestore struct {
	SupportedRestoreTargetModes  []string `json:"supported_restore_target_modes"`
	SupportedIdentityModes       []string `json:"supported_identity_modes"`
	DefaultIdentityMode          string   `json:"default_identity_mode"`
	SupportedAuthSessionPolicies []string `json:"supported_auth_session_policies"`
	RequiresSameTopology         bool     `json:"requires_same_topology"`
	RequiredSecretRefs           []string `json:"required_secret_refs"`
	RequiredExternalDependencies []string `json:"required_external_dependencies"`
	OperatorNotes                string   `json:"operator_notes,omitempty"`
}

type BackupSetAudit struct {
	Labels      map[string]string          `json:"labels"`
	Annotations map[string]string          `json:"annotations"`
	Redactions  []string                   `json:"redactions"`
	Lineage     map[string]json.RawMessage `json:"lineage,omitempty"`
}

type CompleteMarkerV1 struct {
	Schema                 string    `json:"schema"`
	MarkerVersion          int       `json:"marker_version"`
	BackupSetID            string    `json:"backup_set_id"`
	State                  string    `json:"state"`
	RepositoryID           string    `json:"repository_id"`
	ManifestURI            string    `json:"manifest_uri"`
	ManifestSizeBytes      string    `json:"manifest_size_bytes"`
	ManifestChecksumSHA256 string    `json:"manifest_checksum_sha256"`
	CompletedAt            time.Time `json:"completed_at"`
	ArtifactCount          int       `json:"artifact_count"`
	TotalArtifactBytes     string    `json:"total_artifact_bytes"`
}

type BackupSetValidationOptions struct {
	RequireCompleteMarker      bool
	SupportedRequiredFeatures  map[string]bool
	ExpectedNodeCount          int
	AvailableTargetBytes       int64
	ValidateLocalArtifactBytes bool
	AllowLocalArtifactURIs     bool
	AllowS3ArtifactURIs        bool
}

type BackupSetValidationReport struct {
	State                 string
	CheckedAt             time.Time
	Errors                []BackupSetValidationIssue
	Warnings              []BackupSetValidationIssue
	Capacity              BackupSetCapacityEstimate
	TotalArtifactBytes    int64
	RequiredFeatures      []string
	StorageFormatVersions map[string]string
}

type BackupSetValidationIssue struct {
	Reason    string
	Detail    string
	Retryable bool
	FieldPath string
}

type BackupSetCapacityEstimate struct {
	RequiredBytes  int64
	AvailableBytes int64
}

type DeterministicTreeChecksum struct {
	Algorithm      string
	ChecksumSHA256 string
	FileCount      int
	TotalBytes     uint64
}

func ComputeDeterministicTreeChecksum(ctx context.Context, root string) (DeterministicTreeChecksum, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return DeterministicTreeChecksum{}, fmt.Errorf("root path is required")
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return DeterministicTreeChecksum{}, err
	}
	if !rootInfo.IsDir() {
		return DeterministicTreeChecksum{}, fmt.Errorf("root path must be a directory")
	}
	h := sha256.New()
	var fileCount int
	var totalBytes uint64
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == root {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			_, _ = fmt.Fprintf(h, "dir\t%s\t%o\n", rel, info.Mode().Perm())
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported non-regular file in checksum tree: %s", rel)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		fileHash := sha256.New()
		size, err := copyWithContext(ctx, fileHash, file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		_, _ = fmt.Fprintf(h, "file\t%s\t%d\t%o\t%s\n", rel, size, info.Mode().Perm(), hex.EncodeToString(fileHash.Sum(nil)))
		fileCount++
		totalBytes += uint64(size)
		return nil
	}); err != nil {
		return DeterministicTreeChecksum{}, err
	}
	return DeterministicTreeChecksum{Algorithm: ProductionChecksumAlgorithmSHA256, ChecksumSHA256: hex.EncodeToString(h.Sum(nil)), FileCount: fileCount, TotalBytes: totalBytes}, nil
}

func ParseBackupSetDocument(raw []byte) (BackupSetDocument, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return BackupSetDocument{}, err
	}
	present := make(map[string]bool, len(fields))
	for key := range fields {
		present[key] = true
	}
	var manifest BackupSetManifestV1
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return BackupSetDocument{}, err
	}
	return BackupSetDocument{Manifest: manifest, Raw: append([]byte(nil), raw...), Present: present}, nil
}

func ParseCompleteMarker(raw []byte) (CompleteMarkerV1, error) {
	var marker CompleteMarkerV1
	if err := json.Unmarshal(raw, &marker); err != nil {
		return CompleteMarkerV1{}, err
	}
	return marker, nil
}

func ValidateBackupSetPath(ctx context.Context, path string, options BackupSetValidationOptions) (BackupSetDocument, *CompleteMarkerV1, BackupSetValidationReport, error) {
	if err := ctx.Err(); err != nil {
		return BackupSetDocument{}, nil, BackupSetValidationReport{}, err
	}
	manifestPath, baseDir, err := resolveBackupSetManifestPath(path)
	if err != nil {
		return BackupSetDocument{}, nil, BackupSetValidationReport{}, err
	}
	manifestRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		return BackupSetDocument{}, nil, BackupSetValidationReport{}, err
	}
	doc, err := ParseBackupSetDocument(manifestRaw)
	if err != nil {
		return BackupSetDocument{}, nil, BackupSetValidationReport{}, err
	}
	markerPath := filepath.Join(baseDir, "COMPLETE")
	var marker *CompleteMarkerV1
	if raw, err := os.ReadFile(markerPath); err == nil {
		parsed, err := ParseCompleteMarker(raw)
		if err != nil {
			report := newBackupSetValidationReport(doc.Manifest)
			report.addError("complete_marker_parse_failed", err.Error(), false, "COMPLETE")
			report.finish()
			return doc, nil, report, nil
		}
		marker = &parsed
		report := ValidateBackupSetDocument(ctx, doc, marker, options.withDefaults())
		report.validateMarkerBytes(markerRawEvidence{Raw: raw, ManifestRaw: manifestRaw})
		report.validateLocalArtifacts(ctx, baseDir, doc.Manifest, options.withDefaults())
		report.finish()
		return doc, marker, report, nil
	} else if !os.IsNotExist(err) {
		return BackupSetDocument{}, nil, BackupSetValidationReport{}, err
	}
	report := ValidateBackupSetDocument(ctx, doc, nil, options.withDefaults())
	report.validateLocalArtifacts(ctx, baseDir, doc.Manifest, options.withDefaults())
	report.finish()
	return doc, nil, report, nil
}

func ValidateBackupSetDocument(ctx context.Context, doc BackupSetDocument, marker *CompleteMarkerV1, options BackupSetValidationOptions) BackupSetValidationReport {
	_ = ctx
	options = options.withDefaults()
	report := newBackupSetValidationReport(doc.Manifest)
	m := doc.Manifest
	for _, key := range []string{"schema", "manifest_version", "backup_set_id", "state", "complete", "created_at", "completed_at", "operation", "source", "repository", "policy", "compatibility", "consistency", "capture", "artifacts", "external_object_stores", "derived_state", "validation", "restore", "audit"} {
		if !doc.Present[key] {
			report.addError("missing_required_section", "backup-set manifest is missing "+key, false, key)
		}
	}
	if m.Schema != ProductionBackupSetSchema {
		report.addError("unsupported_manifest_schema", fmt.Sprintf("schema=%q want %q", m.Schema, ProductionBackupSetSchema), false, "schema")
	}
	if m.ManifestVersion != ProductionManifestVersion {
		report.addError("unsupported_manifest_version", fmt.Sprintf("manifest_version=%d want %d", m.ManifestVersion, ProductionManifestVersion), false, "manifest_version")
	}
	if strings.TrimSpace(m.BackupSetID) == "" {
		report.addError("backup_set_id_required", "backup_set_id is required", false, "backup_set_id")
	}
	if m.State != "SUCCEEDED" {
		report.addError("backup_set_not_succeeded", "state must be SUCCEEDED", false, "state")
	}
	if !m.Complete {
		report.addError("backup_set_not_complete", "complete must be true", false, "complete")
	}
	report.checkTime("created_at", m.CreatedAt)
	report.checkTime("completed_at", m.CompletedAt)
	if !m.CompletedAt.IsZero() && !m.CreatedAt.IsZero() && m.CompletedAt.Before(m.CreatedAt) {
		report.addError("invalid_time_order", "completed_at must be after created_at", false, "completed_at")
	}
	if options.RequireCompleteMarker && marker == nil {
		report.addError("complete_marker_missing", "COMPLETE marker is required for restore discovery", true, "COMPLETE")
	}
	if marker != nil {
		report.validateMarkerIdentity(m, *marker)
	}
	report.validateNoSecretMaterial("backup-set.json", doc.Raw)
	report.validateOperation(m)
	report.validateSource(m, options)
	report.validateRepository(m, options)
	report.validatePolicy(m)
	report.validateCompatibility(m, options)
	report.validateBarrierSet(m)
	report.validateCapture(m)
	report.validateArtifacts(m, options)
	report.validateExternalObjectStores(m)
	report.validateDerivedState(m)
	report.validatePublishedValidation(m)
	report.validateRestore(m)
	report.finish()
	return report
}

func (o BackupSetValidationOptions) withDefaults() BackupSetValidationOptions {
	if !o.AllowLocalArtifactURIs && !o.AllowS3ArtifactURIs {
		o.AllowLocalArtifactURIs = true
		o.AllowS3ArtifactURIs = true
	}
	return o
}

func newBackupSetValidationReport(m BackupSetManifestV1) BackupSetValidationReport {
	versions := map[string]string{}
	for k, v := range m.Compatibility.StorageFormatVersions {
		versions[k] = v
	}
	return BackupSetValidationReport{State: ProductionValidationStatePassed, CheckedAt: time.Now().UTC(), RequiredFeatures: append([]string(nil), m.Compatibility.RequiredFeatures...), StorageFormatVersions: versions}
}

func (r *BackupSetValidationReport) addError(reason, detail string, retryable bool, fieldPath string) {
	r.Errors = append(r.Errors, BackupSetValidationIssue{Reason: reason, Detail: detail, Retryable: retryable, FieldPath: fieldPath})
}

func (r *BackupSetValidationReport) addWarning(reason, detail string, fieldPath string) {
	r.Warnings = append(r.Warnings, BackupSetValidationIssue{Reason: reason, Detail: detail, FieldPath: fieldPath})
}

func (r *BackupSetValidationReport) finish() {
	if len(r.Errors) > 0 {
		r.State = ProductionValidationStateFailed
	} else {
		r.State = ProductionValidationStatePassed
	}
}

func (r BackupSetValidationReport) Valid() bool { return len(r.Errors) == 0 }

func (r BackupSetValidationReport) Error() error {
	if r.Valid() {
		return nil
	}
	parts := make([]string, 0, len(r.Errors))
	for _, issue := range r.Errors {
		if issue.FieldPath != "" {
			parts = append(parts, issue.FieldPath+": "+issue.Detail)
		} else {
			parts = append(parts, issue.Detail)
		}
	}
	return fmt.Errorf("invalid backup-set manifest: %s", strings.Join(parts, "; "))
}

func (r *BackupSetValidationReport) checkTime(field string, value time.Time) {
	if value.IsZero() {
		r.addError("timestamp_required", field+" is required", false, field)
	}
}

func (r *BackupSetValidationReport) validateMarkerIdentity(m BackupSetManifestV1, marker CompleteMarkerV1) {
	if marker.Schema != ProductionBackupCompleteSchema {
		r.addError("complete_marker_schema_invalid", fmt.Sprintf("schema=%q want %q", marker.Schema, ProductionBackupCompleteSchema), false, "COMPLETE.schema")
	}
	if marker.MarkerVersion != ProductionCompleteMarkerVersion {
		r.addError("complete_marker_version_invalid", fmt.Sprintf("marker_version=%d want %d", marker.MarkerVersion, ProductionCompleteMarkerVersion), false, "COMPLETE.marker_version")
	}
	if marker.BackupSetID != m.BackupSetID {
		r.addError("complete_marker_backup_set_mismatch", "COMPLETE backup_set_id does not match manifest", false, "COMPLETE.backup_set_id")
	}
	if marker.State != m.State {
		r.addError("complete_marker_state_mismatch", "COMPLETE state does not match manifest", false, "COMPLETE.state")
	}
	if marker.RepositoryID != m.Repository.RepositoryID {
		r.addError("complete_marker_repository_mismatch", "COMPLETE repository_id does not match manifest", false, "COMPLETE.repository_id")
	}
	if strings.TrimSpace(marker.ManifestURI) == "" {
		r.addError("complete_marker_manifest_uri_required", "COMPLETE manifest_uri is required", false, "COMPLETE.manifest_uri")
	}
	r.checkDecimalString("COMPLETE.manifest_size_bytes", marker.ManifestSizeBytes, true)
	if !validSHA256(marker.ManifestChecksumSHA256) {
		r.addError("complete_marker_checksum_invalid", "COMPLETE manifest_checksum_sha256 must be lowercase hex SHA-256", false, "COMPLETE.manifest_checksum_sha256")
	}
	r.checkTime("COMPLETE.completed_at", marker.CompletedAt)
	r.checkDecimalString("COMPLETE.total_artifact_bytes", marker.TotalArtifactBytes, true)
	if marker.ArtifactCount < 0 {
		r.addError("complete_marker_artifact_count_invalid", "COMPLETE artifact_count must not be negative", false, "COMPLETE.artifact_count")
	}
}

type markerRawEvidence struct {
	Raw         []byte
	ManifestRaw []byte
}

func (r *BackupSetValidationReport) validateMarkerBytes(e markerRawEvidence) {
	r.validateNoSecretMaterial("COMPLETE", e.Raw)
	var marker CompleteMarkerV1
	if err := json.Unmarshal(e.Raw, &marker); err != nil {
		return
	}
	size := int64(len(e.ManifestRaw))
	if got, ok := parseDecimalUint64(marker.ManifestSizeBytes); ok && got != uint64(size) {
		r.addError("complete_marker_manifest_size_mismatch", fmt.Sprintf("COMPLETE manifest_size_bytes=%d want %d", got, size), false, "COMPLETE.manifest_size_bytes")
	}
	digest := sha256.Sum256(e.ManifestRaw)
	checksum := hex.EncodeToString(digest[:])
	if marker.ManifestChecksumSHA256 != checksum {
		r.addError("complete_marker_manifest_checksum_mismatch", "COMPLETE manifest checksum does not match backup-set.json", false, "COMPLETE.manifest_checksum_sha256")
	}
}

func (r *BackupSetValidationReport) validateOperation(m BackupSetManifestV1) {
	if strings.TrimSpace(m.Operation.BackupOperationID) == "" {
		r.addError("operation_id_required", "operation.backup_operation_id is required", false, "operation.backup_operation_id")
	}
	if strings.TrimSpace(m.Operation.Trigger) == "" {
		r.addError("operation_trigger_required", "operation.trigger is required", false, "operation.trigger")
	}
	if strings.TrimSpace(m.Operation.PolicyID) == "" || strings.TrimSpace(m.Operation.PolicyRevision) == "" {
		r.addError("policy_identity_required", "operation policy_id and policy_revision are required", false, "operation.policy_id")
	}
	if !validSHA256(m.Operation.PolicyHashSHA256) {
		r.addError("policy_hash_invalid", "operation.policy_hash_sha256 must be lowercase hex SHA-256", false, "operation.policy_hash_sha256")
	}
	if m.Policy.PolicyHashSHA256 != "" && m.Operation.PolicyHashSHA256 != m.Policy.PolicyHashSHA256 {
		r.addError("policy_hash_mismatch", "operation.policy_hash_sha256 must match policy.policy_hash_sha256", false, "operation.policy_hash_sha256")
	}
	r.checkTime("operation.started_at", m.Operation.StartedAt)
	r.checkTime("operation.completed_at", m.Operation.CompletedAt)
}

func (r *BackupSetValidationReport) validateSource(m BackupSetManifestV1, options BackupSetValidationOptions) {
	if strings.TrimSpace(m.Source.ClusterID) == "" {
		r.addError("cluster_id_required", "source.cluster_id is required", false, "source.cluster_id")
	}
	if strings.TrimSpace(m.Source.ClusterName) == "" {
		r.addError("cluster_name_required", "source.cluster_name is required", false, "source.cluster_name")
	}
	if strings.TrimSpace(m.Source.DataDir) == "" {
		r.addError("source_data_dir_required", "source.data_dir is required", false, "source.data_dir")
	}
	if strings.TrimSpace(m.Source.MycelVersion) == "" || strings.TrimSpace(m.Source.MycelAPIVersion) == "" {
		r.addError("source_version_required", "source mycel_version and mycel_api_version are required", false, "source.mycel_version")
	}
	if len(m.Source.StorageFormatVersions) == 0 {
		r.addError("storage_format_versions_required", "source.storage_format_versions is required", false, "source.storage_format_versions")
	}
	if m.Source.Topology.NodeCount <= 0 {
		r.addError("source_node_count_invalid", "source.topology.node_count must be positive", false, "source.topology.node_count")
	}
	if len(m.Source.Topology.Nodes) != m.Source.Topology.NodeCount {
		r.addError("source_topology_node_count_mismatch", fmt.Sprintf("source topology has %d nodes, node_count=%d", len(m.Source.Topology.Nodes), m.Source.Topology.NodeCount), false, "source.topology.nodes")
	}
	if options.ExpectedNodeCount > 0 && options.ExpectedNodeCount != m.Source.Topology.NodeCount {
		r.addError("target_topology_mismatch", fmt.Sprintf("target node count %d does not match backup node count %d", options.ExpectedNodeCount, m.Source.Topology.NodeCount), false, "source.topology.node_count")
	}
	if !validSHA256(m.Source.MembershipHashSHA256) {
		r.addError("membership_hash_invalid", "source.membership_hash_sha256 must be lowercase hex SHA-256", false, "source.membership_hash_sha256")
	}
	if !validSHA256(m.Source.PlacementHashSHA256) {
		r.addError("placement_hash_invalid", "source.placement_hash_sha256 must be lowercase hex SHA-256", false, "source.placement_hash_sha256")
	}
	validateSortedSourceNodes(r, "source.topology.nodes", m.Source.Topology.Nodes)
}

func (r *BackupSetValidationReport) validateRepository(m BackupSetManifestV1, options BackupSetValidationOptions) {
	if strings.TrimSpace(m.Repository.RepositoryID) == "" {
		r.addError("repository_id_required", "repository.repository_id is required", false, "repository.repository_id")
	}
	if strings.TrimSpace(m.Repository.Type) == "" {
		r.addError("repository_type_required", "repository.type is required", false, "repository.type")
	}
	if !r.checkURI("repository.uri", m.Repository.URI, options) {
		return
	}
	if m.Repository.Type == "S3_COMPATIBLE" && (strings.TrimSpace(m.Repository.Bucket) == "" || strings.TrimSpace(m.Repository.Prefix) == "") {
		r.addError("repository_s3_location_required", "S3-compatible repository requires bucket and prefix", false, "repository.bucket")
	}
	if strings.TrimSpace(m.Repository.CredentialRef) == "" {
		r.addError("repository_credential_ref_required", "repository.credential_ref is required", false, "repository.credential_ref")
	}
	if secretReferenceLooksRaw(m.Repository.CredentialRef) {
		r.addError("raw_secret_material", "repository.credential_ref looks like raw credential material", false, "repository.credential_ref")
	}
	if m.Repository.LayoutVersion != 1 {
		r.addError("repository_layout_version_invalid", "repository.layout_version must be 1", false, "repository.layout_version")
	}
	if strings.TrimSpace(m.Repository.Encryption.Mode) == "" {
		r.addError("repository_encryption_required", "repository.encryption.mode is required", false, "repository.encryption.mode")
	}
}

func (r *BackupSetValidationReport) validatePolicy(m BackupSetManifestV1) {
	if strings.TrimSpace(m.Policy.PolicyID) == "" || strings.TrimSpace(m.Policy.PolicyRevision) == "" {
		r.addError("policy_identity_required", "policy_id and policy_revision are required", false, "policy.policy_id")
	}
	if !validSHA256(m.Policy.PolicyHashSHA256) {
		r.addError("policy_hash_invalid", "policy.policy_hash_sha256 must be lowercase hex SHA-256", false, "policy.policy_hash_sha256")
	}
	if strings.TrimSpace(m.Policy.RetentionClass) == "" {
		r.addError("retention_class_required", "policy.retention_class is required", false, "policy.retention_class")
	}
	if len(m.Policy.Consistency) == 0 || bytes.Equal(m.Policy.Consistency, []byte("null")) {
		r.addError("policy_consistency_required", "policy.consistency is required", false, "policy.consistency")
	}
	if len(m.Policy.Capture) == 0 || len(m.Policy.Upload) == 0 || len(m.Policy.Validation) == 0 {
		r.addError("policy_sections_required", "policy capture, upload, and validation sections are required", false, "policy")
	}
	if len(m.Policy.DerivedSubsystems) == 0 {
		r.addError("derived_subsystem_policy_required", "policy.derived_subsystems must not be empty", false, "policy.derived_subsystems")
	}
}

func (r *BackupSetValidationReport) validateCompatibility(m BackupSetManifestV1, options BackupSetValidationOptions) {
	if len(m.Compatibility.StorageFormatVersions) == 0 {
		r.addError("compatibility_storage_versions_required", "compatibility.storage_format_versions is required", false, "compatibility.storage_format_versions")
	}
	if len(m.Compatibility.RequiredFeatures) > 0 {
		for _, feature := range m.Compatibility.RequiredFeatures {
			feature = strings.TrimSpace(feature)
			if feature == "" {
				r.addError("required_feature_empty", "compatibility.required_features contains an empty value", false, "compatibility.required_features")
				continue
			}
			if !options.SupportedRequiredFeatures[feature] {
				r.addError("unsupported_required_feature", "required feature is not supported by this validator: "+feature, false, "compatibility.required_features")
			}
		}
	}
	if strings.TrimSpace(m.Compatibility.MinRestoreMycelVersion) == "" {
		r.addError("min_restore_version_required", "compatibility.min_restore_mycel_version is required", false, "compatibility.min_restore_mycel_version")
	}
	if len(m.Compatibility.SupportedRestoreTargetModes) == 0 || len(m.Compatibility.SupportedIdentityModes) == 0 {
		r.addError("restore_modes_required", "compatibility supported restore target and identity modes are required", false, "compatibility")
	}
}

func (r *BackupSetValidationReport) validateBarrierSet(m BackupSetManifestV1) {
	b := m.Consistency.BarrierSet
	if b.SchemaVersion != 1 {
		r.addError("barrier_set_version_invalid", "consistency.barrier_set.schema_version must be 1", false, "consistency.barrier_set.schema_version")
	}
	if strings.TrimSpace(b.BarrierSetID) == "" || strings.TrimSpace(b.BackupOperationID) == "" || strings.TrimSpace(b.BackupSetID) == "" || strings.TrimSpace(b.ClusterID) == "" {
		r.addError("barrier_set_identity_required", "barrier_set identity fields are required", false, "consistency.barrier_set")
	}
	if b.BackupSetID != "" && b.BackupSetID != m.BackupSetID {
		r.addError("barrier_set_backup_set_mismatch", "barrier_set.backup_set_id must match manifest backup_set_id", false, "consistency.barrier_set.backup_set_id")
	}
	if b.ClusterID != "" && b.ClusterID != m.Source.ClusterID {
		r.addError("barrier_set_cluster_mismatch", "barrier_set.cluster_id must match source.cluster_id", false, "consistency.barrier_set.cluster_id")
	}
	if strings.TrimSpace(b.QuiesceLeaseID) == "" {
		r.addError("barrier_set_quiesce_lease_required", "barrier_set.quiesce_lease_id is required", false, "consistency.barrier_set.quiesce_lease_id")
	}
	r.checkTime("consistency.barrier_set.established_at", b.EstablishedAt)
	r.checkTime("consistency.barrier_set.write_gate_closed_at", b.WriteGateClosedAt)
	if len(b.RaftGroups) == 0 {
		if !backupSetManifestIsStandalone(m) {
			r.addError("raft_groups_required", "barrier_set.raft_groups must not be empty for clustered backup sets", false, "consistency.barrier_set.raft_groups")
		}
	}
	for i, group := range b.RaftGroups {
		path := fmt.Sprintf("consistency.barrier_set.raft_groups[%d]", i)
		barrier, ok := r.checkDecimalString(path+".barrier_index", group.BarrierIndex, true)
		commit, commitOK := r.checkDecimalString(path+".commit_index", group.CommitIndex, true)
		r.checkDecimalString(path+".term", group.Term, true)
		if strings.TrimSpace(group.GroupID) == "" {
			r.addError("raft_group_id_required", "raft group_id is required", false, path+".group_id")
		}
		if ok && commitOK && commit < barrier {
			r.addError("raft_commit_before_barrier", "raft group commit_index is below barrier_index", false, path+".commit_index")
		}
		if len(group.PerNode) == 0 {
			r.addError("raft_group_per_node_required", "raft group per_node evidence is required", false, path+".per_node")
		}
		for j, node := range group.PerNode {
			nodePath := fmt.Sprintf("%s.per_node[%d]", path, j)
			applied, appliedOK := r.checkDecimalString(nodePath+".applied_index", node.AppliedIndex, true)
			r.checkDecimalString(nodePath+".last_index", node.LastIndex, true)
			r.checkDecimalString(nodePath+".snapshot_index", node.SnapshotIndex, true)
			if strings.TrimSpace(node.NodeID) == "" || strings.TrimSpace(node.PodName) == "" || strings.TrimSpace(node.RaftNodeID) == "" {
				r.addError("raft_group_per_node_identity_required", "per_node node_id, pod_name, and raft_node_id are required", false, nodePath)
			}
			if ok && appliedOK && applied < barrier {
				r.addError("node_applied_index_before_barrier", "node applied_index is below raft barrier_index", false, nodePath+".applied_index")
			}
		}
	}
	if b.SystemMetadata.ClusterID != "" && b.SystemMetadata.ClusterID != m.Source.ClusterID {
		r.addError("system_metadata_cluster_mismatch", "system_metadata.cluster_id must match source.cluster_id", false, "consistency.barrier_set.system_metadata.cluster_id")
	}
	if b.SystemMetadata.NodeCount != 0 && b.SystemMetadata.NodeCount != m.Source.Topology.NodeCount {
		r.addError("system_metadata_node_count_mismatch", "system_metadata.node_count must match source topology", false, "consistency.barrier_set.system_metadata.node_count")
	}
}

func (r *BackupSetValidationReport) validateCapture(m BackupSetManifestV1) {
	provider := strings.TrimSpace(m.Capture.Provider)
	if provider == "" || strings.TrimSpace(m.Capture.ProviderVersion) == "" || strings.TrimSpace(m.Capture.DataMover) == "" {
		r.addError("capture_fields_required", "capture provider, provider_version, and data_mover are required", false, "capture")
	}
	if m.Capture.ArchiveFormat == "" || m.Artifacts.ArchiveFormat != "" && m.Capture.ArchiveFormat != m.Artifacts.ArchiveFormat {
		r.addError("archive_format_mismatch", "capture.archive_format must be set and match artifacts.archive_format", false, "capture.archive_format")
	}
	if !m.Capture.TemporaryProviderRefsRedacted {
		r.addError("provider_refs_not_redacted", "capture.temporary_provider_refs_redacted must be true", false, "capture.temporary_provider_refs_redacted")
	}
	if provider == "CSI_VOLUME_SNAPSHOT" || provider == "AWS_EBS_SNAPSHOT" {
		if len(bytes.TrimSpace(m.Capture.ProviderEvidence)) == 0 || bytes.Equal(bytes.TrimSpace(m.Capture.ProviderEvidence), []byte("null")) {
			r.addError("snapshot_provider_evidence_required", "snapshot capture providers require capture.provider_evidence", false, "capture.provider_evidence")
		}
		for i, node := range m.Artifacts.Nodes {
			if len(bytes.TrimSpace(node.CaptureSource)) == 0 || bytes.Equal(bytes.TrimSpace(node.CaptureSource), []byte("null")) {
				r.addError("snapshot_node_capture_source_required", "snapshot capture providers require per-node capture_source evidence", false, fmt.Sprintf("artifacts.nodes[%d].capture_source", i))
			}
		}
	}
}

func (r *BackupSetValidationReport) validateArtifacts(m BackupSetManifestV1, options BackupSetValidationOptions) {
	if m.Artifacts.ChecksumAlgorithm != ProductionChecksumAlgorithmSHA256 {
		r.addError("checksum_algorithm_invalid", "artifacts.checksum_algorithm must be SHA256", false, "artifacts.checksum_algorithm")
	}
	if len(m.Artifacts.Nodes) != m.Source.Topology.NodeCount {
		r.addError("node_artifact_count_mismatch", fmt.Sprintf("artifacts has %d nodes, source topology node_count=%d", len(m.Artifacts.Nodes), m.Source.Topology.NodeCount), false, "artifacts.nodes")
	}
	var total uint64
	seenOrdinals := map[int]bool{}
	seenArtifacts := map[string]bool{}
	var previousOrdinal = -1
	for i, node := range m.Artifacts.Nodes {
		path := fmt.Sprintf("artifacts.nodes[%d]", i)
		if node.Ordinal < 0 {
			r.addError("node_ordinal_invalid", "node ordinal must be non-negative", false, path+".ordinal")
		}
		if i > 0 && node.Ordinal <= previousOrdinal {
			r.addError("node_artifacts_not_sorted", "node artifacts must be sorted by ordinal", false, "artifacts.nodes")
		}
		previousOrdinal = node.Ordinal
		if seenOrdinals[node.Ordinal] {
			r.addError("duplicate_node_ordinal", fmt.Sprintf("duplicate node ordinal %d", node.Ordinal), false, path+".ordinal")
		}
		seenOrdinals[node.Ordinal] = true
		if strings.TrimSpace(node.ArtifactID) == "" {
			r.addError("artifact_id_required", "node artifact_id is required", false, path+".artifact_id")
		} else if seenArtifacts[node.ArtifactID] {
			r.addError("duplicate_artifact_id", "duplicate node artifact_id "+node.ArtifactID, false, path+".artifact_id")
		}
		seenArtifacts[node.ArtifactID] = true
		for field, value := range map[string]string{"pod_name": node.PodName, "node_id": node.NodeID, "node_name": node.NodeName, "raft_node_id": node.RaftNodeID, "source_data_dir": node.SourceDataDir, "archive_name": node.ArchiveName, "archive_uri": node.ArchiveURI, "manifest_name": node.ManifestName, "manifest_uri": node.ManifestURI, "inventory_uri": node.InventoryURI} {
			if strings.TrimSpace(value) == "" {
				r.addError("node_artifact_field_required", field+" is required", false, path+"."+field)
			}
		}
		if !validBaseName(node.ArchiveName) {
			r.addError("archive_name_invalid", "archive_name must be a base name", false, path+".archive_name")
		}
		if !validBaseName(node.ManifestName) {
			r.addError("manifest_name_invalid", "manifest_name must be a base name", false, path+".manifest_name")
		}
		if !r.checkURI(path+".archive_uri", node.ArchiveURI, options) || !r.checkURI(path+".manifest_uri", node.ManifestURI, options) || !r.checkURI(path+".inventory_uri", node.InventoryURI, options) {
			continue
		}
		size, ok := r.checkDecimalString(path+".size_bytes", node.SizeBytes, true)
		if ok {
			total += size
		}
		if !validSHA256(node.ChecksumSHA256) {
			r.addError("artifact_checksum_invalid", "checksum_sha256 must be lowercase hex SHA-256", false, path+".checksum_sha256")
		}
		if !validSHA256(node.InventoryChecksumSHA256) {
			r.addError("inventory_checksum_invalid", "inventory_checksum_sha256 must be lowercase hex SHA-256", false, path+".inventory_checksum_sha256")
		}
		if len(node.AppliedIndexes) == 0 {
			if !backupSetManifestIsStandalone(m) {
				r.addError("applied_indexes_required", "node applied_indexes are required for clustered backup sets", false, path+".applied_indexes")
			}
		}
		for groupID, value := range node.AppliedIndexes {
			applied, appliedOK := r.checkDecimalString(path+".applied_indexes."+groupID, value, true)
			if barrier, ok := raftBarrier(m.Consistency.BarrierSet, groupID); ok && appliedOK && applied < barrier {
				r.addError("artifact_applied_index_before_barrier", "node applied index is below barrier for "+groupID, false, path+".applied_indexes."+groupID)
			}
		}
		r.validateNodeRaftFreeze(m, node, path)
	}
	if total > uint64(1<<63-1) {
		r.TotalArtifactBytes = int64(1<<63 - 1)
	} else {
		r.TotalArtifactBytes = int64(total)
	}
	r.Capacity.RequiredBytes = capacityRequiredBytes(total)
	if options.AvailableTargetBytes > 0 {
		r.Capacity.AvailableBytes = options.AvailableTargetBytes
		if uint64(options.AvailableTargetBytes) < uint64(r.Capacity.RequiredBytes) {
			r.addError("insufficient_target_capacity", fmt.Sprintf("available target bytes %d below required estimate %d", options.AvailableTargetBytes, r.Capacity.RequiredBytes), false, "capacity.available_bytes")
		}
	}
	if manifestTotal, ok := parseDecimalUint64(m.Validation.TotalArtifactBytes); ok && manifestTotal != total {
		r.addError("total_artifact_bytes_mismatch", fmt.Sprintf("validation.total_artifact_bytes=%d want %d", manifestTotal, total), false, "validation.total_artifact_bytes")
	}
}

func (r *BackupSetValidationReport) validateNodeRaftFreeze(m BackupSetManifestV1, node BackupSetNodeArtifact, path string) {
	freeze := node.RaftFreeze
	if backupSetManifestIsStandalone(m) && strings.TrimSpace(freeze.LeaseID) == "" && len(freeze.Groups) == 0 {
		return
	}
	if strings.TrimSpace(freeze.LeaseID) == "" {
		r.addError("raft_freeze_lease_required", "node raft_freeze.lease_id is required", false, path+".raft_freeze.lease_id")
	}
	r.checkTime(path+".raft_freeze.acquired_at", freeze.AcquiredAt)
	r.checkTime(path+".raft_freeze.released_at", freeze.ReleasedAt)
	r.checkTime(path+".raft_freeze.expires_at", freeze.ExpiresAt)
	if len(freeze.Groups) == 0 {
		r.addError("raft_freeze_groups_required", "node raft_freeze.groups is required", false, path+".raft_freeze.groups")
	}
	seenGroups := map[string]bool{}
	for i, group := range freeze.Groups {
		gpath := fmt.Sprintf("%s.raft_freeze.groups[%d]", path, i)
		if strings.TrimSpace(group.GroupID) == "" {
			r.addError("raft_freeze_group_id_required", "raft freeze group_id is required", false, gpath+".group_id")
			continue
		}
		if seenGroups[group.GroupID] {
			r.addError("duplicate_raft_freeze_group", "duplicate raft freeze group "+group.GroupID, false, gpath+".group_id")
		}
		seenGroups[group.GroupID] = true
		barrier, barrierOK := r.checkDecimalString(gpath+".barrier_index", group.BarrierIndex, true)
		applied, appliedOK := r.checkDecimalString(gpath+".applied_index", group.AppliedIndex, true)
		if expected, ok := raftBarrier(m.Consistency.BarrierSet, group.GroupID); ok && barrierOK && barrier != expected {
			r.addError("raft_freeze_barrier_mismatch", "raft freeze barrier does not match barrier set", false, gpath+".barrier_index")
		}
		if barrierOK && appliedOK && applied < barrier {
			r.addError("raft_freeze_applied_before_barrier", "raft freeze applied_index is below barrier_index", false, gpath+".applied_index")
		}
	}
}

func (r *BackupSetValidationReport) validateExternalObjectStores(m BackupSetManifestV1) {
	for i, store := range m.ExternalObjectStores {
		path := fmt.Sprintf("external_object_stores[%d]", i)
		if strings.TrimSpace(store.Usage) == "" || strings.TrimSpace(store.Provider) == "" || strings.TrimSpace(store.CredentialRef) == "" {
			r.addError("external_object_store_fields_required", "usage, provider, and credential_ref are required", false, path)
		}
		if strings.TrimSpace(store.ExternalStoreRef) == "" {
			r.addError("external_store_ref_required", "external_store_ref is required", false, path+".external_store_ref")
		}
		if secretReferenceLooksRaw(store.CredentialRef) {
			r.addError("raw_secret_material", "external object-store credential_ref looks like raw credential material", false, path+".credential_ref")
		}
		if devOnlyCredentialReferenceRequired(store.CredentialRef) && !credentialReferenceMarkedDevOnly(store.CredentialRef) {
			r.addError("dev_only_credential_ref_required", "external object-store env/file credential refs must be marked dev_only=true", false, path+".credential_ref")
		}
		if store.IncludePayloadBytes {
			r.addWarning("external_payload_bytes_included", "external object-store payload bytes are marked included; ensure policy explicitly allows this", path+".include_payload_bytes")
		} else if strings.TrimSpace(store.RecoveryPointRef) == "" {
			r.addError("external_payload_recovery_point_required", "external object-store payload bytes are not included; recovery_point_ref is required for restore dependency validation", false, path+".recovery_point_ref")
		}
		if strings.TrimSpace(store.ObjectReferenceManifestURI) == "" || !validSHA256(store.ObjectReferenceManifestChecksumSHA256) {
			r.addError("object_reference_manifest_required", "object reference manifest URI and checksum are required", false, path+".object_reference_manifest_uri")
		}
		r.checkDecimalString(path+".object_count", store.ObjectCount, true)
		r.checkDecimalString(path+".logical_bytes", store.LogicalBytes, true)
		if strings.TrimSpace(store.ValidationState) == "" || strings.Contains(strings.ToUpper(store.ValidationState), "FAILED") {
			r.addError("external_dependency_not_verified", "external object-store dependency must be verified for published backup sets", false, path+".validation_state")
		}
	}
}

func (r *BackupSetValidationReport) validateDerivedState(m BackupSetManifestV1) {
	previousKey := ""
	seen := map[string]bool{}
	for i, row := range m.DerivedState {
		path := fmt.Sprintf("derived_state[%d]", i)
		key := row.Subsystem + "\x00" + row.ArtifactID
		if i > 0 && key <= previousKey {
			r.addError("derived_state_not_sorted", "derived_state entries must be sorted by subsystem then artifact_id", false, "derived_state")
		}
		previousKey = key
		if strings.TrimSpace(row.Subsystem) == "" || strings.TrimSpace(row.ArtifactID) == "" || strings.TrimSpace(row.PolicyMode) == "" || strings.TrimSpace(row.RestorePolicy) == "" || strings.TrimSpace(row.ValidationState) == "" {
			r.addError("derived_state_fields_required", "derived_state subsystem, artifact_id, policy_mode, restore_policy, and validation_state are required", false, path)
		}
		if seen[key] {
			r.addError("duplicate_derived_state", "duplicate derived_state entry", false, path)
		}
		seen[key] = true
		mode := strings.ToUpper(strings.TrimSpace(row.PolicyMode))
		restorePolicy := strings.ToUpper(strings.TrimSpace(row.RestorePolicy))
		validationState := strings.ToUpper(strings.TrimSpace(row.ValidationState))
		hasArtifactEvidence := strings.TrimSpace(row.ArtifactURI) != "" || strings.TrimSpace(row.SizeBytes) != "" || strings.TrimSpace(row.ChecksumSHA256) != "" || strings.TrimSpace(row.FormatVersion) != ""
		if row.Included {
			if strings.TrimSpace(row.ArtifactURI) == "" || strings.TrimSpace(row.SizeBytes) == "" || !validSHA256(row.ChecksumSHA256) || strings.TrimSpace(row.FormatVersion) == "" {
				r.addError("included_derived_artifact_evidence_required", "included derived artifact requires URI, size, checksum, and format_version", false, path)
			}
			r.checkDecimalString(path+".size_bytes", row.SizeBytes, true)
		} else if hasArtifactEvidence {
			r.addError("omitted_derived_artifact_evidence_present", "omitted derived artifacts must not publish partial artifact URI, size, checksum, or format evidence", false, path)
		}
		if mode == "INCLUDE_REQUIRED" && (!row.Included || validationState != "INCLUDED_AND_VALIDATED" || restorePolicy != "PRESERVED") {
			r.addError("required_derived_artifact_not_validated", "required derived artifact must be included, preserved, and validated", false, path)
		}
		if (mode == "INCLUDE_OPTIONAL" || mode == "REBUILD_ALLOWED") && row.Included && validationState != "INCLUDED_AND_VALIDATED" {
			r.addError("optional_derived_artifact_invalidly_included", "optional derived artifact may be included only when valid", false, path)
		}
		if (mode == "INCLUDE_OPTIONAL" || mode == "REBUILD_ALLOWED") && !row.Included && (restorePolicy != "REBUILD_REQUIRED" || validationState != "REBUILD_REQUIRED") {
			r.addError("optional_derived_artifact_omission_not_rebuild_required", "omitted optional derived artifacts must be marked rebuild-required", false, path)
		}
		if mode == "DISCARD" && (row.Included || hasArtifactEvidence || restorePolicy != "DISCARD" || validationState != "DISCARDED") {
			r.addError("discarded_derived_artifact_policy_invalid", "discarded derived artifacts must not include artifact evidence and must be marked DISCARDED", false, path)
		}
		if mode != "INCLUDE_REQUIRED" && mode != "INCLUDE_OPTIONAL" && mode != "REBUILD_ALLOWED" && mode != "DISCARD" && strings.TrimSpace(row.PolicyMode) != "" {
			r.addError("derived_state_policy_mode_invalid", "derived_state policy_mode is not recognized", false, path+".policy_mode")
		}
	}
}

func (r *BackupSetValidationReport) validatePublishedValidation(m BackupSetManifestV1) {
	if m.Validation.Result != ProductionValidationStatePassed {
		r.addError("published_validation_not_passed", "validation.result must be PASSED", false, "validation.result")
	}
	r.checkTime("validation.validated_at", m.Validation.ValidatedAt)
	if strings.TrimSpace(m.Validation.ValidatorVersion) == "" {
		r.addError("validator_version_required", "validation.validator_version is required", false, "validation.validator_version")
	}
	if m.Validation.ExpectedNodeCount != m.Source.Topology.NodeCount || m.Validation.NodeArtifactCount != len(m.Artifacts.Nodes) {
		r.addError("validation_node_counts_mismatch", "validation node counts must match source topology and artifact inventory", false, "validation")
	}
	if m.Validation.RequiredArtifactCount < 0 || m.Validation.ValidatedArtifactCount < m.Validation.RequiredArtifactCount {
		r.addError("validation_artifact_counts_invalid", "validated_artifact_count must cover required_artifact_count", false, "validation.validated_artifact_count")
	}
	if m.Validation.ChecksumAlgorithm != ProductionChecksumAlgorithmSHA256 {
		r.addError("validation_checksum_algorithm_invalid", "validation.checksum_algorithm must be SHA256", false, "validation.checksum_algorithm")
	}
	if len(m.Validation.Errors) > 0 {
		r.addError("published_validation_errors_present", "published backup-set validation.errors must be empty", false, "validation.errors")
	}
}

func (r *BackupSetValidationReport) validateRestore(m BackupSetManifestV1) {
	if len(m.Restore.SupportedRestoreTargetModes) == 0 || len(m.Restore.SupportedIdentityModes) == 0 || strings.TrimSpace(m.Restore.DefaultIdentityMode) == "" {
		r.addError("restore_modes_required", "restore supported modes and default identity mode are required", false, "restore")
	}
	if !stringSliceContains(m.Restore.SupportedIdentityModes, m.Restore.DefaultIdentityMode) {
		r.addError("default_identity_mode_unsupported", "restore.default_identity_mode must be listed in supported_identity_modes", false, "restore.default_identity_mode")
	}
	for i, ref := range m.Restore.RequiredSecretRefs {
		if secretReferenceLooksRaw(ref) {
			r.addError("raw_secret_material", "restore.required_secret_refs contains raw-looking credential material", false, fmt.Sprintf("restore.required_secret_refs[%d]", i))
		}
	}
}

func (r *BackupSetValidationReport) validateLocalArtifacts(ctx context.Context, baseDir string, m BackupSetManifestV1, options BackupSetValidationOptions) {
	if !options.ValidateLocalArtifactBytes {
		return
	}
	for i, node := range m.Artifacts.Nodes {
		pathPrefix := fmt.Sprintf("artifacts.nodes[%d]", i)
		r.validateLocalArtifact(ctx, baseDir, pathPrefix+".archive_uri", node.ArchiveURI, node.SizeBytes, node.ChecksumSHA256, false)
		r.validateLocalArtifact(ctx, baseDir, pathPrefix+".manifest_uri", node.ManifestURI, "", "", true)
		r.validateLocalArtifact(ctx, baseDir, pathPrefix+".inventory_uri", node.InventoryURI, "", node.InventoryChecksumSHA256, true)
	}
	for i, ref := range m.Artifacts.AdditionalManifests {
		r.validateLocalArtifact(ctx, baseDir, fmt.Sprintf("artifacts.additional_manifests[%d].uri", i), ref.URI, ref.SizeBytes, ref.ChecksumSHA256, true)
	}
}

func (r *BackupSetValidationReport) validateLocalArtifact(ctx context.Context, baseDir, fieldPath, rawURI, expectedSize, expectedChecksum string, scanSecrets bool) {
	path, ok := localArtifactPath(rawURI)
	if !ok || path == "" {
		return
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, path)
	}
	file, err := os.Open(path)
	if err != nil {
		r.addError("artifact_missing", err.Error(), true, fieldPath)
		return
	}
	defer file.Close()
	h := sha256.New()
	size, err := copyWithContext(ctx, h, file)
	if err != nil {
		r.addError("artifact_read_failed", err.Error(), true, fieldPath)
		return
	}
	checksum := hex.EncodeToString(h.Sum(nil))
	if scanSecrets {
		if _, err := file.Seek(0, io.SeekStart); err == nil {
			raw, readErr := io.ReadAll(io.LimitReader(file, 10<<20))
			if readErr != nil {
				r.addError("artifact_read_failed", readErr.Error(), true, fieldPath)
				return
			}
			if secretMaterialPattern.Match(raw) {
				r.addError("raw_secret_material", fieldPath+" contains raw-looking secret material", false, fieldPath)
			}
		}
	}
	if expectedChecksum != "" && checksum != expectedChecksum {
		r.addError("artifact_checksum_mismatch", fmt.Sprintf("checksum mismatch got %s want %s", checksum, expectedChecksum), true, fieldPath)
	}
	if expectedSize != "" {
		if want, ok := parseDecimalUint64(expectedSize); ok && uint64(size) != want {
			r.addError("artifact_size_mismatch", fmt.Sprintf("size mismatch got %d want %d", size, want), true, fieldPath)
		}
	}
}

func (r *BackupSetValidationReport) validateNoSecretMaterial(name string, raw []byte) {
	if secretMaterialPattern.Match(raw) {
		r.addError("raw_secret_material", name+" contains raw-looking secret material", false, name)
	}
}

func (r *BackupSetValidationReport) checkDecimalString(field, value string, required bool) (uint64, bool) {
	if strings.TrimSpace(value) == "" {
		if required {
			r.addError("decimal_string_required", field+" is required", false, field)
		}
		return 0, false
	}
	parsed, ok := parseDecimalUint64(value)
	if !ok {
		r.addError("decimal_string_invalid", field+" must be a non-negative decimal string", false, field)
		return 0, false
	}
	return parsed, true
}

func (r *BackupSetValidationReport) checkURI(field, value string, options BackupSetValidationOptions) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		r.addError("uri_required", field+" is required", false, field)
		return false
	}
	if strings.Contains(value, "\x00") {
		r.addError("uri_invalid", field+" contains NUL", false, field)
		return false
	}
	if strings.HasPrefix(value, "s3://") {
		if !options.AllowS3ArtifactURIs {
			r.addError("s3_uri_not_allowed", field+" must not use s3:// in this validation context", false, field)
			return false
		}
		u, err := url.Parse(value)
		if err != nil || u.Host == "" || strings.Trim(u.Path, "/") == "" {
			r.addError("uri_invalid", field+" must be a valid s3:// URI", false, field)
			return false
		}
		return true
	}
	if strings.HasPrefix(value, "file://") || filepath.IsAbs(value) || strings.HasPrefix(value, ".") || !strings.Contains(value, "://") {
		if !options.AllowLocalArtifactURIs {
			r.addError("local_uri_not_allowed", field+" must use repository storage, not local filesystem", false, field)
			return false
		}
		if path, ok := localArtifactPath(value); !ok || unsafeRelativePath(path) {
			r.addError("uri_invalid", field+" must be a safe local path or file:// URI", false, field)
			return false
		}
		return true
	}
	r.addError("uri_invalid", field+" must be s3://, file://, absolute, or relative local path", false, field)
	return false
}

func resolveBackupSetManifestPath(path string) (manifestPath string, baseDir string, err error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", "", fmt.Errorf("backup set path is required")
	}
	if strings.HasPrefix(path, "file://") {
		p, ok := localArtifactPath(path)
		if !ok {
			return "", "", fmt.Errorf("invalid file backup-set URI %q", path)
		}
		path = p
	} else if strings.Contains(path, "://") {
		return "", "", fmt.Errorf("backup-set must be a local path or file:// URI")
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", "", err
	}
	if info.IsDir() {
		return filepath.Join(path, BackupSetManifestName), path, nil
	}
	return path, filepath.Dir(path), nil
}

func validateSortedSourceNodes(r *BackupSetValidationReport, field string, nodes []BackupSetSourceNode) {
	seenOrdinals := map[int]bool{}
	previous := -1
	for i, node := range nodes {
		path := fmt.Sprintf("%s[%d]", field, i)
		if node.Ordinal < 0 {
			r.addError("node_ordinal_invalid", "source node ordinal must be non-negative", false, path+".ordinal")
		}
		if i > 0 && node.Ordinal <= previous {
			r.addError("source_nodes_not_sorted", "source nodes must be sorted by ordinal", false, field)
		}
		previous = node.Ordinal
		if seenOrdinals[node.Ordinal] {
			r.addError("duplicate_source_node_ordinal", fmt.Sprintf("duplicate source node ordinal %d", node.Ordinal), false, path+".ordinal")
		}
		seenOrdinals[node.Ordinal] = true
		if strings.TrimSpace(node.NodeID) == "" || strings.TrimSpace(node.PodName) == "" || strings.TrimSpace(node.RaftNodeID) == "" {
			r.addError("source_node_identity_required", "source node_id, pod_name, and raft_node_id are required", false, path)
		}
	}
}

func backupSetManifestIsStandalone(m BackupSetManifestV1) bool {
	return m.Source.Topology.NodeCount == 1 && len(m.Source.Topology.Nodes) == 1 && len(m.Consistency.BarrierSet.RaftGroups) == 0
}

func raftBarrier(set CheckpointBarrierSetV1, groupID string) (uint64, bool) {
	for _, group := range set.RaftGroups {
		if group.GroupID == groupID {
			return parseDecimalUint64(group.BarrierIndex)
		}
	}
	return 0, false
}

func parseDecimalUint64(value string) (uint64, bool) {
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "+") || strings.HasPrefix(value, "-") {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil
}

func capacityRequiredBytes(totalArtifactBytes uint64) int64 {
	const maxInt64AsUint64 = uint64(1<<63 - 1)
	overhead := totalArtifactBytes / 5
	if totalArtifactBytes > maxInt64AsUint64-overhead {
		return int64(maxInt64AsUint64)
	}
	return int64(totalArtifactBytes + overhead)
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 1024*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			written, err := dst.Write(buf[:n])
			if err != nil {
				return total, err
			}
			total += int64(written)
		}
		if readErr == io.EOF {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

func validBaseName(name string) bool {
	name = strings.TrimSpace(name)
	return name != "" && filepath.Base(name) == name && !strings.Contains(name, "\x00")
}

func unsafeRelativePath(path string) bool {
	if path == "" || filepath.IsAbs(path) {
		return false
	}
	clean := filepath.Clean(path)
	return clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

var secretMaterialPattern = regexp.MustCompile(`(?i)(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_secret_access_key\s*[=:]\s*[^\s",}]+|secret_access_key\s*[=:]\s*[^\s",}]+|session_token\s*[=:]\s*[^\s",}]+|x-amz-security-token=|authorization:\s*bearer\s+[A-Za-z0-9._\-]+|bearer\s+[A-Za-z0-9._\-]{20,})`)

func secretReferenceLooksRaw(value string) bool {
	return secretMaterialPattern.MatchString(strings.TrimSpace(value))
}

func devOnlyCredentialReferenceRequired(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.HasPrefix(value, "env:") || strings.HasPrefix(value, "file-ref:")
}

func credentialReferenceMarkedDevOnly(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.Contains(value, "dev_only=true") || strings.Contains(value, "dev-only=true")
}

func SortBackupSetDerivedState(rows []BackupSetDerivedState) []BackupSetDerivedState {
	out := append([]BackupSetDerivedState(nil), rows...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Subsystem != out[j].Subsystem {
			return out[i].Subsystem < out[j].Subsystem
		}
		return out[i].ArtifactID < out[j].ArtifactID
	})
	return out
}
