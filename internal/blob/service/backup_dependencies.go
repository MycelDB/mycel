package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	blobmodel "github.com/myceldb/mycel/internal/blob/model"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const (
	objectReferenceUsageBlobPayloads = "BLOB_PAYLOADS"
	objectReferenceProviderS3        = "S3_COMPATIBLE"
)

func (m *Module) BackupExternalObjectStoreDependencies(ctx context.Context, request clusterbackup.ExternalObjectDependencyRequest) ([]clusterbackup.ExternalObjectStoreDependency, error) {
	if m == nil {
		return nil, nil
	}
	cfg := effectiveBlobConfig(m.config)
	if !isObjectStoreBackend(cfg.Backend) {
		return nil, nil
	}
	metas, err := m.objectStoreBlobMetas()
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(cfg.ObjectStoreRecoveryPointRef) == "" {
		return nil, fmt.Errorf("MYCELD_BLOB_OBJECT_STORE_RECOVERY_POINT_REF is required when external blob payloads are omitted from a cluster backup")
	}
	if m.s3Store == nil {
		return nil, fmt.Errorf("object store blob backend is not initialized")
	}
	generatedAt := request.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}
	objects := make([]clusterbackup.ObjectReferenceEntry, 0, len(metas))
	var logicalBytes int64
	for _, meta := range metas {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		desc := descriptorFromMeta(meta)
		if !isObjectStoreBackend(payloadBackend(desc)) {
			continue
		}
		if desc.State == blobmodel.PayloadStateInvalid {
			return nil, fmt.Errorf("blob payload %s is marked invalid and cannot be published as a verified external dependency", desc.BlobID)
		}
		bucket, key := m.s3Store.bucketKey(desc)
		ok, err := m.s3Store.Exists(ctx, desc)
		if err != nil {
			return nil, fmt.Errorf("verify external blob payload %s: %w", desc.BlobID, err)
		}
		if !ok {
			return nil, fmt.Errorf("external blob payload %s is missing from object store", desc.BlobID)
		}
		logicalBytes += desc.SizeBytes
		groupID := m.metadataRaftGroupID(desc.SpaceID)
		barrier := request.RaftBarrierIndexes[groupID]
		if strings.TrimSpace(barrier) == "" {
			barrier = "0"
		}
		objects = append(objects, clusterbackup.ObjectReferenceEntry{
			SpaceID:                  desc.SpaceID,
			DomainID:                 desc.DomainID,
			BlobID:                   desc.BlobID,
			PayloadBackend:           payloadBackend(desc),
			Bucket:                   bucket,
			Key:                      key,
			Region:                   firstNonEmpty(desc.S3Region, cfg.S3Region),
			ETag:                     strings.TrimSpace(desc.S3ETag),
			SizeBytes:                decimalInt64Blob(desc.SizeBytes),
			ChecksumAlgorithm:        firstNonEmpty(desc.ChecksumAlgorithm, "sha256"),
			ChecksumHex:              strings.ToLower(strings.TrimSpace(firstNonEmpty(desc.ChecksumHex, strings.TrimPrefix(meta.Digest, "sha256:")))),
			PayloadState:             firstNonEmpty(string(desc.State), string(blobmodel.PayloadStateAvailable)),
			MetadataRaftGroupID:      groupID,
			MetadataRaftBarrierIndex: barrier,
		})
	}
	if len(objects) == 0 {
		return nil, nil
	}
	objects = clusterbackup.SortObjectReferenceEntries(objects)
	aggregateGroup, aggregateBarrier := aggregateObjectReferenceBarrier(objects)
	externalStoreRef := blobExternalStoreRef(cfg)
	manifest := clusterbackup.ObjectReferenceManifestV1{
		Schema:                   clusterbackup.ObjectReferenceManifestSchema,
		ManifestVersion:          clusterbackup.ObjectReferenceManifestVersion,
		BackupSetID:              strings.TrimSpace(request.BackupSetID),
		Usage:                    objectReferenceUsageBlobPayloads,
		Provider:                 objectReferenceProviderS3,
		PayloadBackend:           blobBackendObjectStore,
		ExternalStoreRef:         externalStoreRef,
		Bucket:                   cfg.S3Bucket,
		Prefix:                   cfg.S3Prefix,
		EndpointFingerprint:      blobEndpointFingerprint(cfg),
		RecoveryPointRef:         cfg.ObjectStoreRecoveryPointRef,
		MetadataRaftGroupID:      aggregateGroup,
		MetadataRaftBarrierIndex: aggregateBarrier,
		ObjectCount:              strconv.Itoa(len(objects)),
		LogicalBytes:             decimalInt64Blob(logicalBytes),
		GeneratedAt:              generatedAt,
		Objects:                  objects,
	}
	store := clusterbackup.BackupSetExternalObjectStore{
		Usage:               objectReferenceUsageBlobPayloads,
		Provider:            objectReferenceProviderS3,
		IncludePayloadBytes: false,
		Bucket:              cfg.S3Bucket,
		Prefix:              cfg.S3Prefix,
		EndpointFingerprint: manifest.EndpointFingerprint,
		CredentialRef:       blobObjectStoreCredentialRef(cfg),
		BackupPolicyRef:     cfg.ObjectStoreBackupPolicyRef,
		RecoveryPointRef:    cfg.ObjectStoreRecoveryPointRef,
		ObjectCount:         manifest.ObjectCount,
		LogicalBytes:        manifest.LogicalBytes,
		ValidationState:     "VERIFIED",
	}
	store.ExternalStoreRef = externalStoreRef
	checkpoint := clusterbackup.CheckpointObjectReference{
		Usage:                    store.Usage,
		Provider:                 store.Provider,
		PayloadBackend:           manifest.PayloadBackend,
		MetadataRaftGroupID:      aggregateGroup,
		MetadataRaftBarrierIndex: aggregateBarrier,
		ObjectCount:              manifest.ObjectCount,
		LogicalBytes:             manifest.LogicalBytes,
		ExternalStoreRef:         externalStoreRef,
		ExternalRecoveryPointRef: store.RecoveryPointRef,
		ValidatedAt:              generatedAt,
	}
	return []clusterbackup.ExternalObjectStoreDependency{{Store: store, Checkpoint: checkpoint, Manifest: manifest}}, nil
}

func (m *Module) ValidateExternalObjectStoreDependency(ctx context.Context, input clusterbackup.ExternalObjectStoreDependencyValidationInput) clusterbackup.ExternalObjectStoreDependencyValidationResult {
	result := clusterbackup.ExternalObjectStoreDependencyValidationResult{CheckedAt: time.Now().UTC(), Evidence: map[string]string{"validator": "blob-object-store"}}
	add := func(reason, detail string, retryable bool, fieldPath string) {
		result.Errors = append(result.Errors, clusterbackup.BackupSetValidationIssue{Reason: reason, Detail: detail, Retryable: retryable, FieldPath: fieldPath})
	}
	if m == nil {
		add("external_object_dependency_validator_unavailable", "blob module is not configured", true, "external_object_stores")
		return result
	}
	cfg := effectiveBlobConfig(m.config)
	if !isObjectStoreBackend(cfg.Backend) {
		add("external_object_store_not_configured", "restore requires MYCELD_BLOB_BACKEND=object_store to validate omitted blob payload bytes", true, "external_object_stores")
		return result
	}
	if m.s3Store == nil {
		add("external_object_store_not_initialized", "object store blob backend is not initialized", true, "external_object_stores")
		return result
	}
	if strings.TrimSpace(input.Store.RecoveryPointRef) == "" {
		add("external_recovery_point_required", "external object-store dependency is missing recovery_point_ref", false, "external_object_stores.recovery_point_ref")
	} else if strings.TrimSpace(cfg.ObjectStoreRecoveryPointRef) == "" {
		add("external_recovery_point_not_configured", "restore daemon must be configured with MYCELD_BLOB_OBJECT_STORE_RECOVERY_POINT_REF matching the backup-set recovery point", true, "external_object_stores.recovery_point_ref")
	} else if strings.TrimSpace(cfg.ObjectStoreRecoveryPointRef) != strings.TrimSpace(input.Store.RecoveryPointRef) {
		add("external_recovery_point_mismatch", "configured object-store recovery point does not match backup-set recovery_point_ref", false, "external_object_stores.recovery_point_ref")
	}
	if strings.TrimSpace(input.Store.Bucket) != "" && strings.TrimSpace(cfg.S3Bucket) != strings.TrimSpace(input.Store.Bucket) {
		add("external_object_store_bucket_mismatch", "configured object-store bucket does not match backup-set dependency", false, "external_object_stores.bucket")
	}
	if strings.Trim(strings.TrimSpace(input.Store.Prefix), "/") != strings.Trim(strings.TrimSpace(cfg.S3Prefix), "/") {
		add("external_object_store_prefix_mismatch", "configured object-store prefix does not match backup-set dependency", false, "external_object_stores.prefix")
	}
	if strings.TrimSpace(input.Store.EndpointFingerprint) != "" && strings.TrimSpace(input.Store.EndpointFingerprint) != blobEndpointFingerprint(cfg) {
		add("external_object_store_endpoint_mismatch", "configured object-store endpoint fingerprint does not match backup-set dependency", false, "external_object_stores.endpoint_fingerprint")
	}
	if len(result.Errors) > 0 {
		return result
	}
	var checked int
	for i, object := range input.Manifest.Objects {
		if err := ctx.Err(); err != nil {
			add("external_object_dependency_check_canceled", err.Error(), true, fmt.Sprintf("object_reference_manifest.objects[%d]", i))
			return result
		}
		desc := objectReferenceEntryDescriptor(object)
		ok, err := m.s3Store.Exists(ctx, desc)
		if err != nil {
			add("external_object_dependency_check_failed", fmt.Sprintf("head external object %s: %v", object.BlobID, err), true, fmt.Sprintf("object_reference_manifest.objects[%d]", i))
			continue
		}
		if !ok {
			add("external_object_missing", fmt.Sprintf("external object for blob %s is missing or has incompatible size", object.BlobID), true, fmt.Sprintf("object_reference_manifest.objects[%d]", i))
			continue
		}
		checked++
	}
	result.Evidence["object_count"] = strconv.Itoa(len(input.Manifest.Objects))
	result.Evidence["objects_checked"] = strconv.Itoa(checked)
	result.Evidence["external_store_ref"] = input.Store.ExternalStoreRef
	result.Evidence["recovery_point_ref"] = input.Store.RecoveryPointRef
	return result
}

func (m *Module) objectStoreBlobMetas() ([]blobmodel.BlobMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if strings.TrimSpace(m.metaDir) == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(m.metaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	metas := []blobmodel.BlobMeta{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		spaceID := strings.TrimSuffix(entry.Name(), ".json")
		spaceMetas, err := m.loadSpaceMetaLocked(spaceID)
		if err != nil {
			return nil, err
		}
		for _, meta := range spaceMetas {
			desc := descriptorFromMeta(meta)
			if isObjectStoreBackend(payloadBackend(desc)) {
				metas = append(metas, meta)
			}
		}
	}
	sort.SliceStable(metas, func(i, j int) bool {
		return blobMetaSortKey(metas[i]) < blobMetaSortKey(metas[j])
	})
	return metas, nil
}

func (m *Module) metadataRaftGroupID(spaceID string) string {
	if m == nil || m.raftPartitionCount == 0 {
		return "local"
	}
	parsed, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil || parsed == uuid.Nil {
		return "partition:unknown"
	}
	partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsed), m.raftPartitionCount)
	if err != nil {
		return "partition:unknown"
	}
	return string(consensus.PartitionGroupID(partitionID.Uint32()))
}

func aggregateObjectReferenceBarrier(objects []clusterbackup.ObjectReferenceEntry) (string, string) {
	if len(objects) == 0 {
		return "", "0"
	}
	groupID := objects[0].MetadataRaftGroupID
	allSame := true
	maxBarrier := uint64(0)
	for _, object := range objects {
		if object.MetadataRaftGroupID != groupID {
			allSame = false
		}
		if value, err := strconv.ParseUint(strings.TrimSpace(object.MetadataRaftBarrierIndex), 10, 64); err == nil && value > maxBarrier {
			maxBarrier = value
		}
	}
	if !allSame {
		groupID = "multiple"
	}
	return groupID, strconv.FormatUint(maxBarrier, 10)
}

func objectReferenceEntryDescriptor(entry clusterbackup.ObjectReferenceEntry) blobmodel.PayloadDescriptor {
	size, _ := strconv.ParseInt(strings.TrimSpace(entry.SizeBytes), 10, 64)
	state := blobmodel.PayloadState(strings.TrimSpace(entry.PayloadState))
	if state == "" {
		state = blobmodel.PayloadStateAvailable
	}
	return blobmodel.PayloadDescriptor{Backend: firstNonEmpty(entry.PayloadBackend, blobBackendObjectStore), SpaceID: strings.TrimSpace(entry.SpaceID), DomainID: strings.TrimSpace(entry.DomainID), BlobID: strings.TrimSpace(entry.BlobID), SizeBytes: size, ChecksumAlgorithm: strings.TrimSpace(entry.ChecksumAlgorithm), ChecksumHex: strings.TrimSpace(entry.ChecksumHex), S3Bucket: strings.TrimSpace(entry.Bucket), S3Key: strings.TrimSpace(entry.Key), S3Region: strings.TrimSpace(entry.Region), S3ETag: strings.TrimSpace(entry.ETag), State: state}
}

func blobMetaSortKey(meta blobmodel.BlobMeta) string {
	desc := descriptorFromMeta(meta)
	return strings.TrimSpace(desc.S3Bucket) + "\x00" + strings.TrimSpace(desc.S3Key) + "\x00" + strings.TrimSpace(meta.SpaceID) + "\x00" + strings.TrimSpace(meta.DomainID) + "\x00" + strings.TrimSpace(meta.BlobID)
}

func blobExternalStoreRef(cfg Config) string {
	fingerprint := blobEndpointFingerprint(cfg)
	if strings.HasPrefix(fingerprint, "sha256:") && len(fingerprint) > len("sha256:")+12 {
		fingerprint = fingerprint[:len("sha256:")+12]
	}
	prefix := strings.Trim(strings.TrimSpace(cfg.S3Prefix), "/")
	if prefix == "" {
		return fmt.Sprintf("s3-compatible:%s@%s", strings.TrimSpace(cfg.S3Bucket), fingerprint)
	}
	return fmt.Sprintf("s3-compatible:%s/%s@%s", strings.TrimSpace(cfg.S3Bucket), prefix, fingerprint)
}

func blobEndpointFingerprint(cfg Config) string {
	endpoint := strings.TrimSpace(cfg.S3EndpointURL)
	if endpoint == "" {
		endpoint = "aws-s3:" + strings.TrimSpace(cfg.S3Region)
	}
	digest := sha256.Sum256([]byte(endpoint))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func blobObjectStoreCredentialRef(cfg Config) string {
	if strings.TrimSpace(cfg.ObjectStoreCredentialRef) != "" {
		return strings.TrimSpace(cfg.ObjectStoreCredentialRef)
	}
	return "aws-irsa"
}

func decimalInt64Blob(value int64) string {
	if value < 0 {
		value = 0
	}
	return strconv.FormatInt(value, 10)
}
