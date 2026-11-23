package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
)

// RepositoryClusterBackupSetManager exposes repository-truth backup-set discovery.
type RepositoryClusterBackupSetManager interface {
	ListRepositoryClusterBackupSets(context.Context, string) ([]ClusterBackupSetValidationResult, error)
	GetRepositoryClusterBackupSet(context.Context, string, string) (ClusterBackupSetValidationResult, error)
	ValidateRepositoryClusterBackupSet(context.Context, string, string, clusterbackup.BackupSetValidationOptions) (ClusterBackupSetValidationResult, error)
	DeleteRepositoryClusterBackupSet(context.Context, string, string) (int, error)
}

func (m *Module) ListRepositoryClusterBackupSets(ctx context.Context, repositoryID string) ([]ClusterBackupSetValidationResult, error) {
	repo, store, err := m.repositoryCatalogStore(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	prefix := joinRepositoryKey(repo.S3.Prefix, "backup-sets")
	keys, err := store.ListKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := []ClusterBackupSetValidationResult{}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if filepath.Base(key) != "COMPLETE" {
			continue
		}
		backupSetID := filepath.Base(filepath.Dir(key))
		if backupSetID == "." || backupSetID == "/" || seen[backupSetID] {
			continue
		}
		seen[backupSetID] = true
		result, err := m.readRepositoryClusterBackupSet(ctx, repo, store, backupSetID, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true})
		if err != nil {
			if m.logger != nil {
				m.logger.Warn("skipping invalid repository backup set", "repository_id", repositoryID, "backup_set_id", backupSetID, "error", err)
			}
			continue
		}
		if result.ProductionReport != nil && !result.ProductionReport.Valid() {
			continue
		}
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Manifest.CreatedAt.After(out[j].Manifest.CreatedAt) })
	return out, nil
}

func (m *Module) GetRepositoryClusterBackupSet(ctx context.Context, repositoryID, backupSetID string) (ClusterBackupSetValidationResult, error) {
	return m.ValidateRepositoryClusterBackupSet(ctx, repositoryID, backupSetID, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true})
}

func (m *Module) ValidateRepositoryClusterBackupSet(ctx context.Context, repositoryID, backupSetID string, options clusterbackup.BackupSetValidationOptions) (ClusterBackupSetValidationResult, error) {
	repo, store, err := m.repositoryCatalogStore(ctx, repositoryID)
	if err != nil {
		return ClusterBackupSetValidationResult{}, err
	}
	if strings.TrimSpace(backupSetID) == "" {
		return ClusterBackupSetValidationResult{}, fmt.Errorf("backup_set_id is required")
	}
	options.RequireCompleteMarker = true
	options.AllowS3ArtifactURIs = true
	return m.readRepositoryClusterBackupSet(ctx, repo, store, backupSetID, options)
}

func (m *Module) DeleteRepositoryClusterBackupSet(ctx context.Context, repositoryID, backupSetID string) (int, error) {
	repo, store, err := m.repositoryCatalogStore(ctx, repositoryID)
	if err != nil {
		return 0, err
	}
	backupSetID = strings.TrimSpace(backupSetID)
	if backupSetID == "" {
		return 0, fmt.Errorf("backup_set_id is required")
	}
	if active, _ := m.clusterBackupSnapshot(); active == backupSetID {
		return 0, fmt.Errorf("backup set %s has an active backup operation", backupSetID)
	}
	for _, restore := range m.ListClusterRestores() {
		if restore.BackupSetID == backupSetID && !clusterRestoreTerminalState(restore.State) {
			return 0, fmt.Errorf("backup set %s has an active restore operation", backupSetID)
		}
	}
	if _, err := m.readRepositoryClusterBackupSet(ctx, repo, store, backupSetID, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true}); err != nil {
		return 0, err
	}
	prefix := repositoryBackupSetPrefix(repo, backupSetID)
	deleted, err := store.DeletePrefix(ctx, prefix)
	if err != nil {
		return deleted, err
	}
	if deleted == 0 {
		return 0, fmt.Errorf("backup set %s not found", backupSetID)
	}
	return deleted, nil
}

func (m *Module) repositoryCatalogStore(ctx context.Context, repositoryID string) (BackupRepository, backupRepositoryObjectStore, error) {
	repo, err := m.GetBackupRepository(repositoryID)
	if err != nil {
		return BackupRepository{}, nil, err
	}
	store, err := m.repositoryObjectStore(ctx, repo)
	if err != nil {
		return BackupRepository{}, nil, err
	}
	return repo, store, nil
}

func (m *Module) readRepositoryClusterBackupSet(ctx context.Context, repo BackupRepository, store backupRepositoryObjectStore, backupSetID string, options clusterbackup.BackupSetValidationOptions) (ClusterBackupSetValidationResult, error) {
	backupSetID = strings.TrimSpace(backupSetID)
	if backupSetID == "" {
		return ClusterBackupSetValidationResult{}, fmt.Errorf("backup_set_id is required")
	}
	prefix := repositoryBackupSetPrefix(repo, backupSetID)
	manifestKey := joinRepositoryKey(prefix, clusterbackup.BackupSetManifestName)
	markerKey := joinRepositoryKey(prefix, "COMPLETE")
	manifestRaw, _, err := store.GetBytes(ctx, manifestKey, 128<<20)
	if err != nil {
		return ClusterBackupSetValidationResult{}, fmt.Errorf("read backup-set manifest: %w", err)
	}
	markerRaw, _, err := store.GetBytes(ctx, markerKey, 4<<20)
	if err != nil {
		return ClusterBackupSetValidationResult{}, fmt.Errorf("read COMPLETE marker: %w", err)
	}
	doc, err := clusterbackup.ParseBackupSetDocument(manifestRaw)
	if err != nil {
		return ClusterBackupSetValidationResult{}, err
	}
	marker, err := clusterbackup.ParseCompleteMarker(markerRaw)
	if err != nil {
		report := clusterbackup.ValidateBackupSetDocument(ctx, doc, nil, options)
		return ClusterBackupSetValidationResult{Manifest: productionBackupSetManifestSummary(doc.Manifest), ProductionManifest: &doc.Manifest, ProductionReport: &report}, fmt.Errorf("parse COMPLETE marker: %w", err)
	}
	report := clusterbackup.ValidateBackupSetDocument(ctx, doc, &marker, options)
	if err := validateRepositoryCompleteMarkerBytes(marker, markerRaw, manifestRaw); err != nil {
		return ClusterBackupSetValidationResult{Manifest: productionBackupSetManifestSummary(doc.Manifest), ProductionManifest: &doc.Manifest, ProductionMarker: &marker, ProductionReport: &report}, err
	}
	m.validateRepositoryExternalObjectDependencies(ctx, repo, store, doc.Manifest, &report)
	summary := productionBackupSetManifestSummary(doc.Manifest)
	if strings.TrimSpace(summary.ManifestURI) == "" {
		summary.ManifestURI = repositoryObjectURI(repo, manifestKey)
	}
	result := ClusterBackupSetValidationResult{Manifest: summary, ProductionManifest: &doc.Manifest, ProductionMarker: &marker, ProductionReport: &report}
	if err := report.Error(); err != nil {
		return result, err
	}
	return result, nil
}

func (m *Module) validateRepositoryExternalObjectDependencies(ctx context.Context, repo BackupRepository, store backupRepositoryObjectStore, manifest clusterbackup.BackupSetManifestV1, report *clusterbackup.BackupSetValidationReport) {
	if report == nil || len(manifest.ExternalObjectStores) == 0 {
		return
	}
	for i, dependency := range manifest.ExternalObjectStores {
		fieldPath := fmt.Sprintf("external_object_stores[%d].object_reference_manifest_uri", i)
		key, err := repositoryKeyFromS3URI(repo, dependency.ObjectReferenceManifestURI)
		if err != nil {
			appendBackupSetReportError(report, "object_reference_manifest_uri_invalid", err.Error(), false, fieldPath)
			continue
		}
		raw, _, err := store.GetBytes(ctx, key, 64<<20)
		if err != nil {
			appendBackupSetReportError(report, "object_reference_manifest_missing", fmt.Sprintf("read object reference manifest: %v", err), true, fieldPath)
			continue
		}
		checksum := hashBytes(raw)
		if strings.TrimSpace(dependency.ObjectReferenceManifestChecksumSHA256) != checksum {
			appendBackupSetReportError(report, "object_reference_manifest_checksum_mismatch", fmt.Sprintf("checksum mismatch got %s want %s", checksum, dependency.ObjectReferenceManifestChecksumSHA256), true, fieldPath)
			continue
		}
		objectManifest, err := clusterbackup.ParseObjectReferenceManifest(raw)
		if err != nil {
			appendBackupSetReportError(report, "object_reference_manifest_parse_failed", err.Error(), false, fieldPath)
			continue
		}
		for _, issue := range clusterbackup.ValidateObjectReferenceManifest(ctx, raw, objectManifest, dependency) {
			appendBackupSetReportIssue(report, issue)
		}
		if m == nil || m.externalObjectDependencyValidator == nil {
			if !dependency.IncludePayloadBytes {
				appendBackupSetReportError(report, "external_object_dependency_validator_unavailable", "restore validation requires an external object-store dependency validator for payload bytes omitted from the backup set", true, fmt.Sprintf("external_object_stores[%d]", i))
			}
			continue
		}
		result := m.externalObjectDependencyValidator.ValidateExternalObjectStoreDependency(ctx, clusterbackup.ExternalObjectStoreDependencyValidationInput{BackupSetID: manifest.BackupSetID, Store: dependency, Manifest: objectManifest})
		for _, warning := range result.Warnings {
			appendBackupSetReportWarning(report, warning)
		}
		for _, issue := range result.Errors {
			appendBackupSetReportIssue(report, issue)
		}
	}
	finishBackupSetReport(report)
}

func appendBackupSetReportError(report *clusterbackup.BackupSetValidationReport, reason, detail string, retryable bool, fieldPath string) {
	appendBackupSetReportIssue(report, clusterbackup.BackupSetValidationIssue{Reason: reason, Detail: detail, Retryable: retryable, FieldPath: fieldPath})
}

func appendBackupSetReportWarning(report *clusterbackup.BackupSetValidationReport, issue clusterbackup.BackupSetValidationIssue) {
	if issue.Reason == "" {
		issue.Reason = "external_object_dependency_warning"
	}
	report.Warnings = append(report.Warnings, issue)
}

func appendBackupSetReportIssue(report *clusterbackup.BackupSetValidationReport, issue clusterbackup.BackupSetValidationIssue) {
	if issue.Reason == "" {
		issue.Reason = "external_object_dependency_invalid"
	}
	report.Errors = append(report.Errors, issue)
}

func finishBackupSetReport(report *clusterbackup.BackupSetValidationReport) {
	if report == nil {
		return
	}
	if len(report.Errors) > 0 {
		report.State = clusterbackup.ProductionValidationStateFailed
	} else {
		report.State = clusterbackup.ProductionValidationStatePassed
	}
}

func clusterRestoreTerminalState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case clusterRestoreStateSucceeded, clusterRestoreStateFailedRequiresOp, clusterRestoreStateCanceled:
		return true
	default:
		return false
	}
}

func validateRepositoryCompleteMarkerBytes(marker clusterbackup.CompleteMarkerV1, markerRaw, manifestRaw []byte) error {
	_ = markerRaw
	if strings.TrimSpace(marker.ManifestSizeBytes) != decimalInt64(int64(len(manifestRaw))) {
		return fmt.Errorf("COMPLETE manifest_size_bytes does not match backup-set.json")
	}
	if marker.ManifestChecksumSHA256 != hashBytes(manifestRaw) {
		return fmt.Errorf("COMPLETE manifest_checksum_sha256 does not match backup-set.json")
	}
	if marker.CompletedAt.IsZero() || marker.CompletedAt.After(time.Now().UTC().Add(24*time.Hour)) {
		return fmt.Errorf("COMPLETE completed_at is invalid")
	}
	return nil
}
