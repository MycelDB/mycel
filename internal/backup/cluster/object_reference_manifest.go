package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	ObjectReferenceManifestSchema  = "mycel.object_reference_manifest"
	ObjectReferenceManifestVersion = 1
)

// ExternalObjectDependencyRequest describes the backup cut a dependency provider
// should bind to when building non-secret object-store dependency manifests.
type ExternalObjectDependencyRequest struct {
	BackupSetID        string
	ClusterID          string
	GeneratedAt        time.Time
	RaftBarrierIndexes map[string]string
}

// ExternalObjectStoreDependency is a provider-produced dependency manifest plus
// the backup-set/checkpoint rows that should reference it after upload.
type ExternalObjectStoreDependency struct {
	Store      BackupSetExternalObjectStore
	Checkpoint CheckpointObjectReference
	Manifest   ObjectReferenceManifestV1
}

// ExternalObjectStoreDependencyValidationInput is supplied to restore-time
// dependency validators after the object reference manifest has been fetched
// and structurally verified from repository truth.
type ExternalObjectStoreDependencyValidationInput struct {
	BackupSetID string
	Store       BackupSetExternalObjectStore
	Manifest    ObjectReferenceManifestV1
}

// ExternalObjectStoreDependencyValidationResult contains non-secret dependency
// check evidence. Errors become restore-plan blockers.
type ExternalObjectStoreDependencyValidationResult struct {
	CheckedAt time.Time
	Errors    []BackupSetValidationIssue
	Warnings  []BackupSetValidationIssue
	Evidence  map[string]string
}

// ObjectReferenceManifestV1 inventories external object-store payloads required
// by a backup set. It intentionally carries bucket/key/checksum evidence and
// credential references only; raw access keys, secret values, and endpoint URLs
// must never be embedded here.
type ObjectReferenceManifestV1 struct {
	Schema                   string                 `json:"schema"`
	ManifestVersion          int                    `json:"manifest_version"`
	BackupSetID              string                 `json:"backup_set_id"`
	Usage                    string                 `json:"usage"`
	Provider                 string                 `json:"provider"`
	PayloadBackend           string                 `json:"payload_backend"`
	ExternalStoreRef         string                 `json:"external_store_ref"`
	Bucket                   string                 `json:"bucket"`
	Prefix                   string                 `json:"prefix,omitempty"`
	EndpointFingerprint      string                 `json:"endpoint_fingerprint"`
	RecoveryPointRef         string                 `json:"recovery_point_ref"`
	MetadataRaftGroupID      string                 `json:"metadata_raft_group_id"`
	MetadataRaftBarrierIndex string                 `json:"metadata_raft_barrier_index"`
	ObjectCount              string                 `json:"object_count"`
	LogicalBytes             string                 `json:"logical_bytes"`
	GeneratedAt              time.Time              `json:"generated_at"`
	Objects                  []ObjectReferenceEntry `json:"objects"`
}

type ObjectReferenceEntry struct {
	SpaceID                  string `json:"space_id"`
	DomainID                 string `json:"domain_id,omitempty"`
	BlobID                   string `json:"blob_id"`
	PayloadBackend           string `json:"payload_backend"`
	Bucket                   string `json:"bucket"`
	Key                      string `json:"key"`
	Region                   string `json:"region,omitempty"`
	ETag                     string `json:"etag,omitempty"`
	SizeBytes                string `json:"size_bytes"`
	ChecksumAlgorithm        string `json:"checksum_algorithm"`
	ChecksumHex              string `json:"checksum_hex"`
	PayloadState             string `json:"payload_state,omitempty"`
	MetadataRaftGroupID      string `json:"metadata_raft_group_id"`
	MetadataRaftBarrierIndex string `json:"metadata_raft_barrier_index"`
}

func MarshalObjectReferenceManifest(manifest ObjectReferenceManifestV1) ([]byte, error) {
	manifest = NormalizeObjectReferenceManifest(manifest)
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func ParseObjectReferenceManifest(raw []byte) (ObjectReferenceManifestV1, error) {
	var manifest ObjectReferenceManifestV1
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return ObjectReferenceManifestV1{}, err
	}
	return NormalizeObjectReferenceManifest(manifest), nil
}

func NormalizeObjectReferenceManifest(manifest ObjectReferenceManifestV1) ObjectReferenceManifestV1 {
	manifest.Objects = SortObjectReferenceEntries(manifest.Objects)
	return manifest
}

func SortObjectReferenceEntries(entries []ObjectReferenceEntry) []ObjectReferenceEntry {
	out := append([]ObjectReferenceEntry(nil), entries...)
	sort.SliceStable(out, func(i, j int) bool {
		left := objectReferenceEntrySortKey(out[i])
		right := objectReferenceEntrySortKey(out[j])
		return left < right
	})
	return out
}

func ValidateObjectReferenceManifest(ctx context.Context, raw []byte, manifest ObjectReferenceManifestV1, store BackupSetExternalObjectStore) []BackupSetValidationIssue {
	_ = ctx
	issues := []BackupSetValidationIssue{}
	add := func(reason, detail string, retryable bool, fieldPath string) {
		issues = append(issues, BackupSetValidationIssue{Reason: reason, Detail: detail, Retryable: retryable, FieldPath: fieldPath})
	}
	if len(raw) > 0 && secretMaterialPattern.Match(raw) {
		add("raw_secret_material", "object reference manifest contains raw-looking secret material", false, "object_reference_manifest")
	}
	if manifest.Schema != ObjectReferenceManifestSchema {
		add("object_reference_manifest_schema_invalid", fmt.Sprintf("schema=%q want %q", manifest.Schema, ObjectReferenceManifestSchema), false, "object_reference_manifest.schema")
	}
	if manifest.ManifestVersion != ObjectReferenceManifestVersion {
		add("object_reference_manifest_version_invalid", fmt.Sprintf("manifest_version=%d want %d", manifest.ManifestVersion, ObjectReferenceManifestVersion), false, "object_reference_manifest.manifest_version")
	}
	if strings.TrimSpace(manifest.BackupSetID) == "" {
		add("object_reference_manifest_backup_set_required", "backup_set_id is required", false, "object_reference_manifest.backup_set_id")
	}
	if strings.TrimSpace(store.Usage) != "" && strings.TrimSpace(manifest.Usage) != strings.TrimSpace(store.Usage) {
		add("object_reference_manifest_usage_mismatch", "object reference manifest usage does not match backup-set external object-store row", false, "object_reference_manifest.usage")
	}
	if strings.TrimSpace(store.Provider) != "" && strings.TrimSpace(manifest.Provider) != strings.TrimSpace(store.Provider) {
		add("object_reference_manifest_provider_mismatch", "object reference manifest provider does not match backup-set external object-store row", false, "object_reference_manifest.provider")
	}
	if strings.TrimSpace(store.Bucket) != "" && strings.TrimSpace(manifest.Bucket) != strings.TrimSpace(store.Bucket) {
		add("object_reference_manifest_bucket_mismatch", "object reference manifest bucket does not match backup-set external object-store row", false, "object_reference_manifest.bucket")
	}
	if cleanManifestPrefix(store.Prefix) != "" && cleanManifestPrefix(manifest.Prefix) != cleanManifestPrefix(store.Prefix) {
		add("object_reference_manifest_prefix_mismatch", "object reference manifest prefix does not match backup-set external object-store row", false, "object_reference_manifest.prefix")
	}
	if strings.TrimSpace(store.EndpointFingerprint) != "" && strings.TrimSpace(manifest.EndpointFingerprint) != strings.TrimSpace(store.EndpointFingerprint) {
		add("object_reference_manifest_endpoint_mismatch", "object reference manifest endpoint fingerprint does not match backup-set external object-store row", false, "object_reference_manifest.endpoint_fingerprint")
	}
	if strings.TrimSpace(store.RecoveryPointRef) != "" && strings.TrimSpace(manifest.RecoveryPointRef) != strings.TrimSpace(store.RecoveryPointRef) {
		add("object_reference_manifest_recovery_point_mismatch", "object reference manifest recovery point does not match backup-set external object-store row", false, "object_reference_manifest.recovery_point_ref")
	}
	if strings.TrimSpace(manifest.ExternalStoreRef) == "" {
		add("object_reference_manifest_store_ref_required", "external_store_ref is required", false, "object_reference_manifest.external_store_ref")
	} else if strings.TrimSpace(store.ExternalStoreRef) != "" && strings.TrimSpace(manifest.ExternalStoreRef) != strings.TrimSpace(store.ExternalStoreRef) {
		add("object_reference_manifest_store_ref_mismatch", "object reference manifest external_store_ref does not match backup-set external object-store row", false, "object_reference_manifest.external_store_ref")
	}
	if strings.TrimSpace(manifest.RecoveryPointRef) == "" {
		add("object_reference_manifest_recovery_point_required", "recovery_point_ref is required", false, "object_reference_manifest.recovery_point_ref")
	}
	if manifest.GeneratedAt.IsZero() {
		add("object_reference_manifest_generated_at_required", "generated_at is required", false, "object_reference_manifest.generated_at")
	}
	objectCount, objectCountOK := parseDecimalUint64(manifest.ObjectCount)
	if !objectCountOK {
		add("object_reference_manifest_object_count_invalid", "object_count must be a non-negative decimal string", false, "object_reference_manifest.object_count")
	}
	logicalBytes, logicalBytesOK := parseDecimalUint64(manifest.LogicalBytes)
	if !logicalBytesOK {
		add("object_reference_manifest_logical_bytes_invalid", "logical_bytes must be a non-negative decimal string", false, "object_reference_manifest.logical_bytes")
	}
	if objectCountOK && objectCount != uint64(len(manifest.Objects)) {
		add("object_reference_manifest_object_count_mismatch", fmt.Sprintf("object_count=%d want %d", objectCount, len(manifest.Objects)), false, "object_reference_manifest.object_count")
	}
	var total uint64
	previous := ""
	seen := map[string]bool{}
	for i, object := range manifest.Objects {
		path := fmt.Sprintf("object_reference_manifest.objects[%d]", i)
		key := objectReferenceEntrySortKey(object)
		if i > 0 && key < previous {
			add("object_reference_manifest_not_sorted", "object reference entries must be sorted by bucket, key, space, domain, and blob", false, "object_reference_manifest.objects")
		}
		previous = key
		if seen[key] {
			add("duplicate_object_reference", "duplicate object reference entry", false, path)
		}
		seen[key] = true
		if strings.TrimSpace(object.SpaceID) == "" || strings.TrimSpace(object.BlobID) == "" || strings.TrimSpace(object.Bucket) == "" || strings.TrimSpace(object.Key) == "" {
			add("object_reference_identity_required", "space_id, blob_id, bucket, and key are required", false, path)
		}
		if strings.TrimSpace(object.PayloadBackend) == "" {
			add("object_reference_backend_required", "payload_backend is required", false, path+".payload_backend")
		}
		if strings.TrimSpace(object.MetadataRaftGroupID) == "" || strings.TrimSpace(object.MetadataRaftBarrierIndex) == "" {
			add("object_reference_metadata_barrier_required", "metadata raft group and barrier index are required", false, path)
		}
		if _, ok := parseDecimalUint64(object.MetadataRaftBarrierIndex); !ok {
			add("object_reference_metadata_barrier_invalid", "metadata_raft_barrier_index must be a non-negative decimal string", false, path+".metadata_raft_barrier_index")
		}
		size, sizeOK := parseDecimalUint64(object.SizeBytes)
		if !sizeOK {
			add("object_reference_size_invalid", "size_bytes must be a non-negative decimal string", false, path+".size_bytes")
		} else {
			total += size
		}
		if strings.EqualFold(strings.TrimSpace(object.ChecksumAlgorithm), "sha256") || strings.EqualFold(strings.TrimSpace(object.ChecksumAlgorithm), "SHA256") {
			checksum := strings.TrimSpace(object.ChecksumHex)
			if checksum != strings.ToLower(checksum) || !validSHA256(checksum) {
				add("object_reference_checksum_invalid", "sha256 checksum_hex must be lowercase hex SHA-256", false, path+".checksum_hex")
			}
		} else if strings.TrimSpace(object.ChecksumAlgorithm) == "" || strings.TrimSpace(object.ChecksumHex) == "" {
			add("object_reference_checksum_required", "checksum_algorithm and checksum_hex are required", false, path)
		}
		if strings.EqualFold(strings.TrimSpace(object.PayloadState), "invalid") {
			add("object_reference_payload_invalid", "invalid payload state cannot be published as a verified external dependency", false, path+".payload_state")
		}
	}
	if logicalBytesOK && logicalBytes != total {
		add("object_reference_manifest_logical_bytes_mismatch", fmt.Sprintf("logical_bytes=%d want %d", logicalBytes, total), false, "object_reference_manifest.logical_bytes")
	}
	return issues
}

func ObjectReferenceManifestChecksum(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func EqualObjectReferenceManifestBytes(left, right []byte) bool {
	return bytes.Equal(bytes.TrimSpace(left), bytes.TrimSpace(right))
}

func objectReferenceEntrySortKey(entry ObjectReferenceEntry) string {
	return strings.TrimSpace(entry.Bucket) + "\x00" + strings.TrimSpace(entry.Key) + "\x00" + strings.TrimSpace(entry.SpaceID) + "\x00" + strings.TrimSpace(entry.DomainID) + "\x00" + strings.TrimSpace(entry.BlobID)
}

func cleanManifestPrefix(prefix string) string {
	return strings.Trim(strings.TrimSpace(prefix), "/")
}
