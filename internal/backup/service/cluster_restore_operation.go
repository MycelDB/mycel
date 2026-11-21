package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	clusterRestoreStatePending             = "PENDING"
	clusterRestoreStateValidatingTarget    = "VALIDATING_TARGET"
	clusterRestoreStateStagingArtifacts    = "STAGING_ARTIFACTS"
	clusterRestoreStateApplyingArtifacts   = "APPLYING_ARTIFACTS"
	clusterRestoreStatePostRestoreValidate = "POST_RESTORE_VALIDATING"
	clusterRestoreStateSucceeded           = "SUCCEEDED"
	clusterRestoreStateCanceling           = "CANCELING"
	clusterRestoreStateCanceled            = "CANCELED"
	clusterRestoreStateFailed              = "FAILED"
	clusterRestoreStateFailedRequiresOp    = "FAILED_REQUIRES_OPERATOR"
)

type clusterRestoreRequestRecord struct {
	Operation ClusterRestoreOperation `json:"operation"`
}

type clusterRestoreUpdateRecord struct {
	Operation ClusterRestoreOperation `json:"operation"`
}

func (m *Module) StartClusterRestore(ctx context.Context, input StartClusterRestoreInput) (ClusterRestoreOperation, error) {
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.IdempotencyKey != "" {
		if existing, ok := m.findClusterRestoreByIdempotencyKey(input.IdempotencyKey); ok {
			return existing, nil
		}
	}
	createdAt := time.Now().UTC()
	plan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: input.RepositoryID, BackupSetID: input.BackupSetID, TargetMode: input.TargetMode, IdentityMode: input.IdentityMode, AuthSessionPolicy: input.AuthSessionPolicy, Target: input.Target, Staging: input.Staging})
	if err != nil {
		return ClusterRestoreOperation{}, err
	}
	op := ClusterRestoreOperation{RestoreOperationID: restoreOperationID(plan, input.IdempotencyKey, createdAt), RestorePlanID: plan.RestorePlanID, RepositoryID: plan.RepositoryID, BackupSetID: plan.BackupSetID, State: clusterRestoreStatePending, TargetMode: plan.TargetMode, IdentityMode: plan.IdentityMode, AuthSessionPolicy: plan.AuthSessionPolicy, CreatedAt: createdAt, UpdatedAt: createdAt, Target: plan.Target, Staging: plan.Staging, Preconditions: input.Preconditions, IdempotencyKey: input.IdempotencyKey}
	blockers := m.restoreExecutionBlockers(plan, input)
	if len(blockers) > 0 {
		op.State = clusterRestoreStateFailedRequiresOp
		op.Blockers = blockers
		op.Error = "cluster restore execution preconditions are not satisfied"
		op.CompletedAt = time.Now().UTC()
		op.UpdatedAt = op.CompletedAt
		if err := m.commitClusterRestore(ctx, recordTypeClusterRestoreRequest, clusterRestoreRequestRecord{Operation: op}); err != nil {
			return ClusterRestoreOperation{}, err
		}
		return m.ClusterRestoreStatus(op.RestoreOperationID)
	}
	if err := m.commitClusterRestore(ctx, recordTypeClusterRestoreRequest, clusterRestoreRequestRecord{Operation: op}); err != nil {
		return ClusterRestoreOperation{}, err
	}
	if stored, err := m.ClusterRestoreStatus(op.RestoreOperationID); err == nil && !stored.CreatedAt.Equal(op.CreatedAt) {
		return stored, nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	if m.clusterRestoreCancels == nil {
		m.clusterRestoreCancels = map[string]context.CancelFunc{}
	}
	m.clusterRestoreCancels[op.RestoreOperationID] = cancel
	m.mu.Unlock()
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.clusterRestoreCancels, op.RestoreOperationID)
			m.mu.Unlock()
		}()
		m.runClusterRestoreOperation(runCtx, input, plan, op)
	}()
	return m.ClusterRestoreStatus(op.RestoreOperationID)
}

func (m *Module) runClusterRestoreOperation(ctx context.Context, input StartClusterRestoreInput, plan ClusterRestorePlan, op ClusterRestoreOperation) {
	update := func(next ClusterRestoreOperation) {
		_ = m.persistClusterRestoreOperation(context.Background(), next)
	}
	final, err := m.executeClusterRestoreOperation(ctx, input, plan, op, update)
	if err != nil {
		if ctx.Err() != nil {
			if current, statusErr := m.ClusterRestoreStatus(op.RestoreOperationID); statusErr == nil && current.Error != "" {
				final.Error = current.Error
			}
			final.State = clusterRestoreStateCanceled
			final.Error = firstNonEmptyString(final.Error, ctx.Err().Error())
		} else if final.State == "" || !clusterRestoreStateTerminal(final.State) {
			final.State = clusterRestoreStateFailed
			final.Error = err.Error()
		}
		final.CompletedAt = time.Now().UTC()
		final.UpdatedAt = final.CompletedAt
		final = m.cleanupClusterRestoreOperation(context.Background(), final)
		_ = m.persistClusterRestoreOperation(context.Background(), final)
		return
	}
	final = m.cleanupClusterRestoreOperation(context.Background(), final)
	_ = m.persistClusterRestoreOperation(context.Background(), final)
}

func (m *Module) executeClusterRestoreOperation(ctx context.Context, input StartClusterRestoreInput, plan ClusterRestorePlan, op ClusterRestoreOperation, update func(ClusterRestoreOperation)) (ClusterRestoreOperation, error) {
	_ = input
	if err := ctx.Err(); err != nil {
		return op, err
	}
	op.State = clusterRestoreStateValidatingTarget
	op.Progress.CurrentPhase = "validating_target"
	op.UpdatedAt = time.Now().UTC()
	update(op)
	repo, store, err := m.repositoryCatalogStore(ctx, plan.RepositoryID)
	if err != nil {
		return op, err
	}
	localStagingRequired := restorePlanRequiresLocalStaging(plan.Target.Nodes)
	stagingPath := ""
	if localStagingRequired {
		var stagingErr error
		stagingPath, stagingErr = localRestoreStagingPath(plan.Staging.StagingURI)
		if stagingErr != nil {
			op.State = clusterRestoreStateFailedRequiresOp
			op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_staging_not_local", Detail: stagingErr.Error(), FieldPath: "staging.staging_uri"})
			op.Error = stagingErr.Error()
			op.CompletedAt = time.Now().UTC()
			op.UpdatedAt = op.CompletedAt
			return op, nil
		}
		stagingPath = restoreOperationStagingPath(stagingPath, op.RestoreOperationID)
		if plan.Staging.CleanBeforeUse {
			if err := os.RemoveAll(stagingPath); err != nil {
				return op, fmt.Errorf("clean restore staging path: %w", err)
			}
		}
		if err := os.MkdirAll(stagingPath, 0o700); err != nil {
			return op, fmt.Errorf("prepare restore staging path: %w", err)
		}
		op.CleanupTasks = append(op.CleanupTasks, CleanupTask{TaskID: "staging-directory", ResourceKind: "local_directory", ResourceURI: stagingPath, State: "created"})
	}
	manifest := plan.ProductionManifest
	if manifest == nil {
		return op, fmt.Errorf("restore plan is missing production manifest")
	}
	format := archiveFormatFromProductionManifest(manifest.Artifacts.ArchiveFormat)
	if !backupcore.IsSupportedArchiveFormat(format) {
		return op, fmt.Errorf("unsupported restore archive format %q", manifest.Artifacts.ArchiveFormat)
	}
	artifacts := map[int]clusterbackup.BackupSetNodeArtifact{}
	for _, artifact := range manifest.Artifacts.Nodes {
		artifacts[artifact.Ordinal] = artifact
		op.Progress.BytesTotal += parseSignedDecimalStringLocal(artifact.SizeBytes)
	}
	op.State = clusterRestoreStateStagingArtifacts
	op.Progress.TotalNodes = len(plan.Target.Nodes)
	op.Progress.CurrentPhase = "staging_artifacts"
	op.UpdatedAt = time.Now().UTC()
	update(op)
	sortedMappings := append([]ClusterRestoreNodeMapping(nil), plan.Target.Nodes...)
	sort.Slice(sortedMappings, func(i, j int) bool { return sortedMappings[i].Ordinal < sortedMappings[j].Ordinal })
	for _, mapping := range sortedMappings {
		if err := ctx.Err(); err != nil {
			return op, err
		}
		artifact, ok := artifacts[mapping.Ordinal]
		if !ok {
			return op, fmt.Errorf("backup set %s has no artifact for ordinal %d", plan.BackupSetID, mapping.Ordinal)
		}
		progress := ClusterRestoreNodeProgress{Ordinal: mapping.Ordinal, State: "staging", SourceArchiveURI: artifact.ArchiveURI, ChecksumSHA256: artifact.ChecksumSHA256, BytesTotal: parseSignedDecimalStringLocal(artifact.SizeBytes)}
		op.Progress.CurrentNode = fmt.Sprintf("ordinal-%d", mapping.Ordinal)
		op.Progress.Nodes = append(op.Progress.Nodes, progress)
		op.UpdatedAt = time.Now().UTC()
		update(op)
		if restoreNodeUsesPVCHelper(mapping) {
			op.Progress.Nodes[len(op.Progress.Nodes)-1].State = "ready_for_pvc_helper"
			op.Progress.StagedNodes++
			op.UpdatedAt = time.Now().UTC()
			update(op)
			continue
		}
		archiveKey, err := repositoryKeyFromS3URI(repo, artifact.ArchiveURI)
		if err != nil {
			op.Progress.Nodes[len(op.Progress.Nodes)-1].State = "failed"
			op.Progress.Nodes[len(op.Progress.Nodes)-1].LastError = err.Error()
			return op, err
		}
		archivePath := filepath.Join(stagingPath, fmt.Sprintf("ordinal-%d", mapping.Ordinal), artifact.ArchiveName)
		info, err := store.GetFile(ctx, archiveKey, archivePath)
		if err != nil {
			op.Progress.Nodes[len(op.Progress.Nodes)-1].State = "failed"
			op.Progress.Nodes[len(op.Progress.Nodes)-1].LastError = err.Error()
			return op, fmt.Errorf("stage ordinal %d archive: %w", mapping.Ordinal, err)
		}
		checksum, size, err := fileSHA256Local(ctx, archivePath)
		if err != nil {
			return op, fmt.Errorf("checksum staged ordinal %d archive: %w", mapping.Ordinal, err)
		}
		if artifact.ChecksumSHA256 != "" && checksum != artifact.ChecksumSHA256 {
			return op, fmt.Errorf("checksum staged ordinal %d archive: got %s want %s", mapping.Ordinal, checksum, artifact.ChecksumSHA256)
		}
		if info.SizeBytes > 0 && info.SizeBytes != size {
			return op, fmt.Errorf("size staged ordinal %d archive: got %d want repository %d", mapping.Ordinal, size, info.SizeBytes)
		}
		op.Progress.Nodes[len(op.Progress.Nodes)-1].State = "staged"
		op.Progress.Nodes[len(op.Progress.Nodes)-1].StagingURI = archivePath
		op.Progress.Nodes[len(op.Progress.Nodes)-1].BytesCompleted = size
		op.Progress.BytesCompleted += size
		op.Progress.StagedNodes++
		op.UpdatedAt = time.Now().UTC()
		update(op)
	}
	op.State = clusterRestoreStateApplyingArtifacts
	op.Progress.CurrentPhase = "applying_artifacts"
	op.Progress.CurrentNode = ""
	op.UpdatedAt = time.Now().UTC()
	update(op)
	for i, mapping := range sortedMappings {
		if err := ctx.Err(); err != nil {
			return op, err
		}
		artifact := artifacts[mapping.Ordinal]
		dataDir := strings.TrimSpace(mapping.TargetDataDir)
		op.Progress.CurrentNode = fmt.Sprintf("ordinal-%d", mapping.Ordinal)
		op.Progress.Nodes[i].State = "applying"
		op.UpdatedAt = time.Now().UTC()
		update(op)
		if restoreNodeUsesPVCHelper(mapping) {
			if m.clusterRestorePVCHelper == nil {
				applyErr := fmt.Errorf("daemon PVC restore helper is not configured")
				op.Progress.Nodes[i].State = "failed"
				op.Progress.Nodes[i].LastError = applyErr.Error()
				return op, fmt.Errorf("apply ordinal %d archive: %w", mapping.Ordinal, applyErr)
			}
			marker := clusterbackup.CompleteMarkerV1{}
			if plan.ProductionMarker != nil {
				marker = *plan.ProductionMarker
			}
			backupSetPrefix := repositoryBackupSetPrefix(repo, plan.BackupSetID)
			if strings.TrimSpace(marker.ManifestURI) != "" {
				if key, keyErr := repositoryKeyFromS3URI(repo, marker.ManifestURI); keyErr == nil {
					backupSetPrefix = strings.TrimSuffix(key, "/"+clusterbackup.BackupSetManifestName)
				}
			}
			result, helperErr := m.clusterRestorePVCHelper.RestoreRepositoryBackupNodeToPVC(ctx, ClusterRestorePVCHelperRequest{RestoreOperationID: op.RestoreOperationID, Repository: repo, BackupSetID: plan.BackupSetID, BackupSetPrefix: backupSetPrefix, Manifest: *manifest, CompleteMarker: marker, Artifact: artifact, Target: plan.Target, Node: mapping, ArchiveFormat: format, store: store})
			op.CleanupTasks = append(op.CleanupTasks, result.CleanupTasks...)
			if helperErr != nil {
				op.Progress.Nodes[i].State = "failed"
				op.Progress.Nodes[i].LastError = helperErr.Error()
				return op, fmt.Errorf("apply ordinal %d archive with PVC helper: %w", mapping.Ordinal, helperErr)
			}
			if !result.Verified {
				applyErr := fmt.Errorf("PVC restore helper did not return verified=true")
				op.Progress.Nodes[i].State = "failed"
				op.Progress.Nodes[i].LastError = applyErr.Error()
				return op, fmt.Errorf("apply ordinal %d archive with PVC helper: %w", mapping.Ordinal, applyErr)
			}
			completed := result.BytesCompleted
			if completed <= 0 {
				completed = parseSignedDecimalStringLocal(artifact.SizeBytes)
			}
			op.Progress.Nodes[i].BytesCompleted = completed
			op.Progress.BytesCompleted += completed
			op.Progress.Nodes[i].StagingURI = "pvc://" + strings.TrimSpace(mapping.TargetPVCName)
			op.Progress.AppliedNodes++
			op.State = clusterRestoreStatePostRestoreValidate
			op.Progress.CurrentPhase = "post_restore_validating"
			op.Progress.Nodes[i].State = "verifying"
			op.UpdatedAt = time.Now().UTC()
			update(op)
			op.Progress.VerifiedNodes++
			op.Progress.Nodes[i].State = "verified"
			op.UpdatedAt = time.Now().UTC()
			update(op)
			continue
		}
		archivePath := filepath.Join(stagingPath, fmt.Sprintf("ordinal-%d", mapping.Ordinal), artifact.ArchiveName)
		var applyErr error
		if strings.TrimSpace(mapping.TargetPVCName) != "" {
			applyErr = applyRestoreArchiveToMountedDataDir(ctx, format, archivePath, dataDir)
		} else {
			applyErr = applyRestoreArchiveToDataDir(ctx, format, archivePath, dataDir, op.RestoreOperationID, mapping.Ordinal)
		}
		if applyErr != nil {
			op.Progress.Nodes[i].State = "failed"
			op.Progress.Nodes[i].LastError = applyErr.Error()
			return op, fmt.Errorf("apply ordinal %d archive: %w", mapping.Ordinal, applyErr)
		}
		op.Progress.AppliedNodes++
		op.State = clusterRestoreStatePostRestoreValidate
		op.Progress.CurrentPhase = "post_restore_validating"
		op.Progress.Nodes[i].State = "verifying"
		op.UpdatedAt = time.Now().UTC()
		update(op)
		if err := clusterbackup.VerifyRestoredDataDir(dataDir); err != nil {
			op.Progress.Nodes[i].State = "failed"
			op.Progress.Nodes[i].LastError = err.Error()
			return op, fmt.Errorf("verify restored ordinal %d: %w", mapping.Ordinal, err)
		}
		op.Progress.VerifiedNodes++
		op.Progress.Nodes[i].State = "verified"
		op.UpdatedAt = time.Now().UTC()
		update(op)
	}
	op.State = clusterRestoreStateSucceeded
	op.Progress.CurrentPhase = "succeeded"
	op.Progress.CurrentNode = ""
	op.CompletedAt = time.Now().UTC()
	op.UpdatedAt = op.CompletedAt
	return op, nil
}

func restoreOperationStagingPath(root, operationID string) string {
	root = filepath.Clean(root)
	component := repositorySafePathComponent(operationID)
	if component == "" {
		component = "restore-operation"
	}
	if filepath.Base(root) == component {
		return root
	}
	return filepath.Join(root, component)
}

func applyRestoreArchiveToDataDir(ctx context.Context, format backupcore.ArchiveFormat, archivePath, dataDir, operationID string, ordinal int) error {
	dataDir = strings.TrimSpace(dataDir)
	if err := ensureRestoreDataDirEmpty(dataDir); err != nil {
		return fmt.Errorf("target data dir is not clean: %w", err)
	}
	tmp := restoreTempDataDirPath(dataDir, operationID, ordinal)
	if tmp == "" {
		return fmt.Errorf("target data dir %s cannot be restored atomically", dataDir)
	}
	if err := os.RemoveAll(tmp); err != nil {
		return fmt.Errorf("clean temporary restore dir: %w", err)
	}
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return fmt.Errorf("create temporary restore dir: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := clusterbackup.ExtractArchive(ctx, format, archivePath, tmp); err != nil {
		return fmt.Errorf("extract archive to temporary restore dir: %w", err)
	}
	if err := clusterbackup.VerifyRestoredDataDir(tmp); err != nil {
		return fmt.Errorf("verify temporary restore dir: %w", err)
	}
	if err := ensureRestoreDataDirEmpty(dataDir); err != nil {
		return fmt.Errorf("target data dir changed during restore: %w", err)
	}
	if err := removeExistingEmptyRestoreDataDir(dataDir); err != nil {
		return fmt.Errorf("prepare target data dir for atomic restore: %w", err)
	}
	if err := os.Rename(tmp, dataDir); err != nil {
		return fmt.Errorf("commit restored data dir atomically: %w", err)
	}
	committed = true
	return nil
}

func applyRestoreArchiveToMountedDataDir(ctx context.Context, format backupcore.ArchiveFormat, archivePath, dataDir string) error {
	dataDir = strings.TrimSpace(dataDir)
	if err := ensureRestoreDataDirEmpty(dataDir); err != nil {
		return fmt.Errorf("mounted target data dir is not clean: %w", err)
	}
	if err := clusterbackup.ExtractArchive(ctx, format, archivePath, dataDir); err != nil {
		return fmt.Errorf("extract archive to mounted target data dir: %w", err)
	}
	if err := clusterbackup.VerifyRestoredDataDir(dataDir); err != nil {
		return fmt.Errorf("verify mounted target data dir: %w", err)
	}
	return nil
}

func restoreTempDataDirPath(dataDir, operationID string, ordinal int) string {
	dataDir = filepath.Clean(strings.TrimSpace(dataDir))
	if dataDir == "." || dataDir == string(filepath.Separator) {
		return ""
	}
	parent := filepath.Dir(dataDir)
	base := filepath.Base(dataDir)
	return filepath.Join(parent, "."+base+".restore-"+repositorySafePathComponent(operationID)+fmt.Sprintf("-%d", ordinal))
}

func (m *Module) cleanupClusterRestoreOperation(ctx context.Context, op ClusterRestoreOperation) ClusterRestoreOperation {
	for i := range op.CleanupTasks {
		task := &op.CleanupTasks[i]
		if task.ResourceKind != "local_directory" || strings.TrimSpace(task.ResourceURI) == "" {
			continue
		}
		if task.State == "deleted" || task.State == "skipped" {
			continue
		}
		path := filepath.Clean(task.ResourceURI)
		if !restoreCleanupDirectoryAllowed(path, op.RestoreOperationID) {
			task.State = "skipped"
			task.LastError = "cleanup path is not operation-scoped"
			continue
		}
		if err := ctx.Err(); err != nil {
			task.State = "failed"
			task.LastError = err.Error()
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			task.State = "failed"
			task.LastError = err.Error()
			continue
		}
		task.State = "deleted"
		task.LastError = ""
	}
	return op
}

func restoreCleanupDirectoryAllowed(path, operationID string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	component := repositorySafePathComponent(operationID)
	if component == "" {
		return false
	}
	return filepath.Base(path) == component
}

func (m *Module) reconcileInterruptedClusterRestores(ctx context.Context) error {
	m.mu.Lock()
	m.ensureClusterRestoreStateLocked()
	var interrupted []ClusterRestoreOperation
	for _, op := range m.clusterRestores {
		if !clusterRestoreStateTerminal(op.State) {
			interrupted = append(interrupted, cloneClusterRestoreOperation(op))
		}
	}
	m.mu.Unlock()
	for _, op := range interrupted {
		op = recoverInterruptedRestoreApply(op)
		now := time.Now().UTC()
		op.State = clusterRestoreStateFailedRequiresOp
		op.Error = "cluster restore operation was interrupted by daemon restart"
		op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_operation_interrupted", Detail: "daemon restarted before restore operation reached a terminal state; inspect target data dirs/PVCs and restart from a fresh restore plan", FieldPath: "restore_operation_id"})
		op.CompletedAt = now
		op.UpdatedAt = now
		op = m.cleanupClusterRestoreOperation(ctx, op)
		if err := m.persistClusterRestoreOperation(ctx, op); err != nil {
			return fmt.Errorf("mark interrupted cluster restore %s failed: %w", op.RestoreOperationID, err)
		}
	}
	return nil
}

func recoverInterruptedRestoreApply(op ClusterRestoreOperation) ClusterRestoreOperation {
	for i, mapping := range op.Target.Nodes {
		dataDir := strings.TrimSpace(mapping.TargetDataDir)
		if dataDir == "" {
			continue
		}
		tmp := restoreTempDataDirPath(dataDir, op.RestoreOperationID, mapping.Ordinal)
		if tmp == "" {
			continue
		}
		if _, err := os.Stat(tmp); err != nil {
			continue
		}
		fieldPath := fmt.Sprintf("target.nodes[%d].target_data_dir", i)
		if err := clusterbackup.VerifyRestoredDataDir(tmp); err != nil {
			op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_temp_dir_invalid", Detail: err.Error(), FieldPath: fieldPath})
			continue
		}
		if err := ensureRestoreDataDirEmpty(dataDir); err != nil {
			op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_target_not_clean_after_restart", Detail: err.Error(), FieldPath: fieldPath})
			continue
		}
		if err := removeExistingEmptyRestoreDataDir(dataDir); err != nil {
			op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_target_prepare_failed_after_restart", Detail: err.Error(), FieldPath: fieldPath})
			continue
		}
		if err := os.Rename(tmp, dataDir); err != nil {
			op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_temp_commit_failed_after_restart", Detail: err.Error(), FieldPath: fieldPath})
			continue
		}
		op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_temp_committed_after_restart", Detail: "completed temporary restore directory was committed after daemon restart; operator validation is still required", FieldPath: fieldPath})
	}
	return op
}

func (m *Module) ClusterRestoreStatus(restoreOperationID string) (ClusterRestoreOperation, error) {
	restoreOperationID = strings.TrimSpace(restoreOperationID)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureClusterRestoreStateLocked()
	if restoreOperationID == "" {
		restoreOperationID = m.activeClusterRestoreID
	}
	if restoreOperationID == "" {
		return ClusterRestoreOperation{}, fmt.Errorf("restore_operation_id is required")
	}
	op, ok := m.clusterRestores[restoreOperationID]
	if !ok {
		return ClusterRestoreOperation{}, fmt.Errorf("cluster restore operation %s is not known", restoreOperationID)
	}
	return cloneClusterRestoreOperation(op), nil
}

func (m *Module) ListClusterRestores() []ClusterRestoreOperation {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureClusterRestoreStateLocked()
	items := make([]ClusterRestoreOperation, 0, len(m.clusterRestores))
	for _, op := range m.clusterRestores {
		items = append(items, cloneClusterRestoreOperation(op))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].RestoreOperationID < items[j].RestoreOperationID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	return items
}

func (m *Module) CancelClusterRestore(ctx context.Context, restoreOperationID, reason string) (ClusterRestoreOperation, error) {
	restoreOperationID = strings.TrimSpace(restoreOperationID)
	if restoreOperationID == "" {
		m.mu.Lock()
		restoreOperationID = m.activeClusterRestoreID
		m.mu.Unlock()
	}
	if restoreOperationID == "" {
		return ClusterRestoreOperation{}, fmt.Errorf("restore_operation_id is required")
	}
	m.mu.Lock()
	cancel := m.clusterRestoreCancels[restoreOperationID]
	op, ok := m.clusterRestores[restoreOperationID]
	m.mu.Unlock()
	if !ok {
		return ClusterRestoreOperation{}, fmt.Errorf("cluster restore operation %s is not known", restoreOperationID)
	}
	if clusterRestoreStateTerminal(op.State) {
		return cloneClusterRestoreOperation(op), nil
	}
	if cancel != nil {
		cancel()
		op.State = clusterRestoreStateCanceling
		op.Error = firstNonEmptyString(reason, "cluster restore cancellation requested")
		op.UpdatedAt = time.Now().UTC()
		if err := m.persistClusterRestoreOperation(ctx, op); err != nil {
			return ClusterRestoreOperation{}, err
		}
		return m.ClusterRestoreStatus(restoreOperationID)
	}
	op.State = clusterRestoreStateCanceled
	op.Error = firstNonEmptyString(reason, "cluster restore canceled before execution could resume")
	op.CompletedAt = time.Now().UTC()
	op.UpdatedAt = op.CompletedAt
	if err := m.persistClusterRestoreOperation(ctx, op); err != nil {
		return ClusterRestoreOperation{}, err
	}
	return m.ClusterRestoreStatus(restoreOperationID)
}

func (m *Module) applyClusterRestoreRequest(ctx context.Context, rec wal.Record) error {
	var payload clusterRestoreRequestRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	op := normalizeClusterRestoreOperation(payload.Operation)
	if op.RestoreOperationID == "" {
		return fmt.Errorf("restore_operation_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureClusterRestoreStateLocked()
	if existing, ok := m.clusterRestores[op.RestoreOperationID]; ok {
		if existing.IdempotencyKey != "" && existing.IdempotencyKey == op.IdempotencyKey {
			return nil
		}
		return fmt.Errorf("cluster restore operation %s already exists", op.RestoreOperationID)
	}
	if m.activeClusterBackupID != "" && !clusterRestoreStateTerminal(op.State) {
		return fmt.Errorf("cluster backup %s is already active", m.activeClusterBackupID)
	}
	if m.activeClusterRestoreID != "" && m.activeClusterRestoreID != op.RestoreOperationID && !clusterRestoreStateTerminal(op.State) {
		return fmt.Errorf("cluster restore operation %s is already active", m.activeClusterRestoreID)
	}
	m.clusterRestores[op.RestoreOperationID] = op
	if !clusterRestoreStateTerminal(op.State) {
		m.activeClusterRestoreID = op.RestoreOperationID
	}
	return nil
}

func (m *Module) applyClusterRestoreUpdate(ctx context.Context, rec wal.Record) error {
	var payload clusterRestoreUpdateRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	op := normalizeClusterRestoreOperation(payload.Operation)
	if op.RestoreOperationID == "" {
		return fmt.Errorf("restore_operation_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureClusterRestoreStateLocked()
	if _, ok := m.clusterRestores[op.RestoreOperationID]; !ok {
		return fmt.Errorf("cluster restore operation %s is not known", op.RestoreOperationID)
	}
	m.clusterRestores[op.RestoreOperationID] = op
	if clusterRestoreStateTerminal(op.State) {
		if m.activeClusterRestoreID == op.RestoreOperationID {
			m.activeClusterRestoreID = ""
		}
		return nil
	}
	m.activeClusterRestoreID = op.RestoreOperationID
	return nil
}

func (m *Module) clusterRestoreWALAppliers() map[wal.RecordType]wal.Applier {
	return map[wal.RecordType]wal.Applier{
		recordTypeClusterRestoreRequest: wal.ApplierFunc(m.applyClusterRestoreRequest),
		recordTypeClusterRestoreUpdate:  wal.ApplierFunc(m.applyClusterRestoreUpdate),
	}
}

func (m *Module) persistClusterRestoreOperation(ctx context.Context, op ClusterRestoreOperation) error {
	return m.commitClusterRestore(ctx, recordTypeClusterRestoreUpdate, clusterRestoreUpdateRecord{Operation: op})
}

func (m *Module) commitClusterRestore(ctx context.Context, typ wal.RecordType, payload any) error {
	if m.raftGroups != nil {
		return m.commitRaft(ctx, typ, payload)
	}
	if m.wal != nil {
		return m.commitWAL(ctx, typ, payload)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	applier, ok := m.clusterRestoreWALAppliers()[typ]
	if !ok {
		return fmt.Errorf("unsupported cluster restore record type %s", typ)
	}
	return applier.ApplyWAL(ctx, wal.Record{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: raw})
}

func (m *Module) findClusterRestoreByIdempotencyKey(key string) (ClusterRestoreOperation, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return ClusterRestoreOperation{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureClusterRestoreStateLocked()
	for _, op := range m.clusterRestores {
		if op.IdempotencyKey == key {
			return cloneClusterRestoreOperation(op), true
		}
	}
	return ClusterRestoreOperation{}, false
}

func (m *Module) ensureClusterRestoreStateLocked() {
	if m.clusterRestores == nil {
		m.clusterRestores = map[string]ClusterRestoreOperation{}
	}
}

func (m *Module) clusterRestoreSnapshot() (string, map[string]ClusterRestoreOperation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeClusterRestoreID, cloneClusterRestoreOperations(m.clusterRestores)
}

func (m *Module) restoreClusterRestoreState(activeID string, operations map[string]ClusterRestoreOperation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clusterRestores = cloneClusterRestoreOperations(operations)
	m.activeClusterRestoreID = ""
	if activeID != "" {
		if op, ok := m.clusterRestores[activeID]; ok && !clusterRestoreStateTerminal(op.State) {
			m.activeClusterRestoreID = activeID
			return
		}
	}
	for id, op := range m.clusterRestores {
		if !clusterRestoreStateTerminal(op.State) {
			m.activeClusterRestoreID = id
			return
		}
	}
}

func normalizeClusterRestoreOperation(op ClusterRestoreOperation) ClusterRestoreOperation {
	op.RestoreOperationID = strings.TrimSpace(op.RestoreOperationID)
	op.RestorePlanID = strings.TrimSpace(op.RestorePlanID)
	op.RepositoryID = strings.TrimSpace(op.RepositoryID)
	op.BackupSetID = strings.TrimSpace(op.BackupSetID)
	op.State = strings.ToUpper(strings.TrimSpace(op.State))
	if op.State == "" {
		op.State = clusterRestoreStatePending
	}
	op.TargetMode = normalizeRestoreEnum(op.TargetMode)
	op.IdentityMode = normalizeRestoreEnum(op.IdentityMode)
	op.AuthSessionPolicy = normalizeRestoreEnum(op.AuthSessionPolicy)
	op.IdempotencyKey = strings.TrimSpace(op.IdempotencyKey)
	if op.CreatedAt.IsZero() {
		op.CreatedAt = time.Now().UTC()
	}
	if op.UpdatedAt.IsZero() {
		op.UpdatedAt = op.CreatedAt
	}
	op.Target = normalizeRestoreTarget(op.Target)
	op.Staging = normalizeRestoreStaging(op.Staging)
	return cloneClusterRestoreOperation(op)
}

func cloneClusterRestoreOperations(in map[string]ClusterRestoreOperation) map[string]ClusterRestoreOperation {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]ClusterRestoreOperation, len(in))
	for key, op := range in {
		out[key] = cloneClusterRestoreOperation(op)
	}
	return out
}

func cloneClusterRestoreOperation(op ClusterRestoreOperation) ClusterRestoreOperation {
	op.Blockers = append([]ValidationBlocker(nil), op.Blockers...)
	op.CleanupTasks = append([]CleanupTask(nil), op.CleanupTasks...)
	op.Target = cloneClusterRestoreTarget(op.Target)
	op.Staging.Blockers = append([]ValidationBlocker(nil), op.Staging.Blockers...)
	op.Preconditions = cloneClusterRestorePreconditions(op.Preconditions)
	op.Progress.Nodes = append([]ClusterRestoreNodeProgress(nil), op.Progress.Nodes...)
	return op
}

func cloneClusterRestoreTarget(in ClusterRestoreTarget) ClusterRestoreTarget {
	in.Nodes = append([]ClusterRestoreNodeMapping(nil), in.Nodes...)
	in.Labels = copyStringMapLocal(in.Labels)
	in.Annotations = copyStringMapLocal(in.Annotations)
	return in
}

func cloneClusterRestorePreconditions(in ClusterRestorePreconditionEvidence) ClusterRestorePreconditionEvidence {
	in.Blockers = append([]ValidationBlocker(nil), in.Blockers...)
	in.CleanTarget.CheckedNodes = append([]ClusterRestoreNodeMapping(nil), in.CleanTarget.CheckedNodes...)
	in.CleanTarget.Blockers = append([]ValidationBlocker(nil), in.CleanTarget.Blockers...)
	in.SourceFencing.FencedEndpoints = append([]string(nil), in.SourceFencing.FencedEndpoints...)
	in.SourceFencing.Blockers = append([]ValidationBlocker(nil), in.SourceFencing.Blockers...)
	return in
}

func copyStringMapLocal(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func clusterRestoreStateTerminal(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case clusterRestoreStateSucceeded, clusterRestoreStateCanceled, clusterRestoreStateFailed, clusterRestoreStateFailedRequiresOp:
		return true
	default:
		return false
	}
}
