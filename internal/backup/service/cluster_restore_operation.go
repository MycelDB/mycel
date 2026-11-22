package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/encryption"
	"github.com/myceldb/mycel/internal/wal"
	"golang.org/x/sys/unix"
)

const (
	clusterRestoreStatePending             = "PENDING"
	clusterRestoreStateValidatingTarget    = "VALIDATING_TARGET"
	clusterRestoreStatePreparingTarget     = "PREPARING_TARGET"
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

type clusterRestorePruneRecord struct {
	RestoreOperationIDs []string  `json:"restore_operation_ids"`
	PrunedAt            time.Time `json:"pruned_at"`
	Reason              string    `json:"reason"`
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
	executorID, executorNodeID, executorNodeName, executorRaftNodeID := m.clusterRestoreExecutorIdentity()
	op := ClusterRestoreOperation{RestoreOperationID: restoreOperationID(plan, input.IdempotencyKey, createdAt), RestorePlanID: plan.RestorePlanID, RepositoryID: plan.RepositoryID, BackupSetID: plan.BackupSetID, State: clusterRestoreStatePending, TargetMode: plan.TargetMode, IdentityMode: plan.IdentityMode, AuthSessionPolicy: plan.AuthSessionPolicy, IdempotencyKey: input.IdempotencyKey, ExecutorID: executorID, ExecutorNodeID: executorNodeID, ExecutorNodeName: executorNodeName, ExecutorRaftNodeID: executorRaftNodeID, CreatedAt: createdAt, UpdatedAt: createdAt, Target: plan.Target, Staging: plan.Staging, Preconditions: input.Preconditions}
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
		_ = m.pruneClusterRestoreHistory(ctx)
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
			if current, statusErr := m.ClusterRestoreStatus(op.RestoreOperationID); statusErr == nil {
				final.CancelRequestedAt = current.CancelRequestedAt
				final.CancelRequestedState = current.CancelRequestedState
				final.CancelReason = current.CancelReason
				if current.Error != "" {
					final.Error = current.Error
				}
			}
			final = finalizeClusterRestoreCancellation(final, ctx.Err())
		} else if final.State == "" || !clusterRestoreStateTerminal(final.State) {
			final.State = clusterRestoreStateFailed
			final.Error = err.Error()
		}
		final.CompletedAt = time.Now().UTC()
		final.UpdatedAt = final.CompletedAt
		final = m.cleanupClusterRestoreOperation(context.Background(), final)
		_ = m.persistClusterRestoreOperation(context.Background(), final)
		_ = m.pruneClusterRestoreHistory(context.Background())
		return
	}
	final = m.cleanupClusterRestoreOperation(context.Background(), final)
	_ = m.persistClusterRestoreOperation(context.Background(), final)
	_ = m.pruneClusterRestoreHistory(context.Background())
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
	sortedMappings := append([]ClusterRestoreNodeMapping(nil), plan.Target.Nodes...)
	sort.Slice(sortedMappings, func(i, j int) bool { return sortedMappings[i].Ordinal < sortedMappings[j].Ordinal })
	artifacts := map[int]clusterbackup.BackupSetNodeArtifact{}
	for _, artifact := range manifest.Artifacts.Nodes {
		artifacts[artifact.Ordinal] = artifact
		op.Progress.BytesTotal += parseSignedDecimalStringLocal(artifact.SizeBytes)
	}
	op.Progress.TotalNodes = len(sortedMappings)
	if blockers := restoreExecutionCapacityBlockers(plan, sortedMappings, artifacts, stagingPath); len(blockers) > 0 {
		op.State = clusterRestoreStateFailedRequiresOp
		op.Error = "cluster restore execution capacity checks failed"
		op.Blockers = append(op.Blockers, blockers...)
		op.CompletedAt = time.Now().UTC()
		op.UpdatedAt = op.CompletedAt
		update(op)
		return op, nil
	}
	pvcControllerEvidence := map[int]map[string]string(nil)
	if controller := m.clusterRestorePVCTargetController(); controller != nil && restoreMappingsUsePVCHelper(sortedMappings) {
		op.State = clusterRestoreStatePreparingTarget
		op.Progress.CurrentPhase = "preparing_kubernetes_target"
		op.Progress.CurrentNode = ""
		op.UpdatedAt = time.Now().UTC()
		update(op)
		prepared, prepareErr := controller.PrepareRepositoryRestoreTarget(ctx, ClusterRestorePVCTargetControllerRequest{RestoreOperationID: op.RestoreOperationID, Target: plan.Target, Nodes: sortedMappings})
		op.CleanupTasks = append(op.CleanupTasks, prepared.CleanupTasks...)
		if prepared.CleanTarget.TargetEmpty || len(prepared.CleanTarget.CheckedNodes) > 0 || len(prepared.CleanTarget.Blockers) > 0 || prepared.CleanTarget.OperatorAttestation != "" {
			op.Preconditions.CleanTarget = prepared.CleanTarget
		}
		if prepared.SourceFencing.SourceClusterFenced || !prepared.SourceFencing.FencedAt.IsZero() || prepared.SourceFencing.FenceTokenRef != "" || len(prepared.SourceFencing.FencedEndpoints) > 0 || len(prepared.SourceFencing.Blockers) > 0 {
			op.Preconditions.SourceFencing = prepared.SourceFencing
		}
		pvcControllerEvidence = prepared.NodeEvidence
		op.UpdatedAt = time.Now().UTC()
		update(op)
		if prepareErr != nil {
			op.State = clusterRestoreStateFailedRequiresOp
			op.Error = fmt.Errorf("prepare Kubernetes restore target: %w", prepareErr).Error()
			op.Blockers = append(op.Blockers, prepared.CleanTarget.Blockers...)
			op.Blockers = append(op.Blockers, prepared.SourceFencing.Blockers...)
			op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "kubernetes_target_prepare_failed", Detail: prepareErr.Error(), FieldPath: "target", Retryable: true})
			op.UpdatedAt = time.Now().UTC()
			update(op)
			return op, fmt.Errorf("prepare Kubernetes restore target: %w", prepareErr)
		}
	}
	op.State = clusterRestoreStateStagingArtifacts
	op.Progress.CurrentPhase = "staging_artifacts"
	op.UpdatedAt = time.Now().UTC()
	update(op)
	for _, mapping := range sortedMappings {
		if err := ctx.Err(); err != nil {
			return op, err
		}
		artifact, ok := artifacts[mapping.Ordinal]
		if !ok {
			return op, fmt.Errorf("backup set %s has no artifact for ordinal %d", plan.BackupSetID, mapping.Ordinal)
		}
		progress := ClusterRestoreNodeProgress{Ordinal: mapping.Ordinal, State: "staging", SourceArchiveURI: artifact.ArchiveURI, ChecksumSHA256: artifact.ChecksumSHA256, BytesTotal: parseSignedDecimalStringLocal(artifact.SizeBytes), Evidence: copyStringMapLocal(pvcControllerEvidence[mapping.Ordinal])}
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
			op.Progress.Nodes[i].Evidence = mergeRestoreNodeEvidence(op.Progress.Nodes[i].Evidence, result.Evidence)
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
			validationEvidence, validationErr := restorePostValidationEvidence(plan, mapping, artifact, "helper_verified", "")
			if validationErr != nil {
				op.Progress.Nodes[i].State = "failed"
				op.Progress.Nodes[i].LastError = validationErr.Error()
				return op, fmt.Errorf("collect post-restore validation evidence for ordinal %d: %w", mapping.Ordinal, validationErr)
			}
			op.Progress.Nodes[i].Evidence = mergeRestoreNodeEvidence(op.Progress.Nodes[i].Evidence, validationEvidence)
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
		extractPath, cleanupExtractPath, prepareErr := m.restoreArchiveExtractionPath(ctx, format, artifact, archivePath)
		if cleanupExtractPath != nil {
			defer cleanupExtractPath()
		}
		if prepareErr != nil {
			op.Progress.Nodes[i].State = "failed"
			op.Progress.Nodes[i].LastError = prepareErr.Error()
			return op, fmt.Errorf("prepare ordinal %d archive for extraction: %w", mapping.Ordinal, prepareErr)
		}
		var applyErr error
		if strings.TrimSpace(mapping.TargetPVCName) != "" {
			applyErr = applyRestoreArchiveToMountedDataDir(ctx, format, extractPath, dataDir)
		} else {
			applyErr = applyRestoreArchiveToDataDir(ctx, format, extractPath, dataDir, op.RestoreOperationID, mapping.Ordinal)
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
		validationEvidence, err := validateRestoredDataDirForOperation(dataDir, plan, mapping, artifact, op.RestoreOperationID)
		if err != nil {
			op.Progress.Nodes[i].State = "failed"
			op.Progress.Nodes[i].LastError = err.Error()
			return op, fmt.Errorf("verify restored ordinal %d: %w", mapping.Ordinal, err)
		}
		op.Progress.Nodes[i].Evidence = mergeRestoreNodeEvidence(op.Progress.Nodes[i].Evidence, validationEvidence)
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

func (m *Module) restoreArchiveExtractionPath(ctx context.Context, format backupcore.ArchiveFormat, artifact clusterbackup.BackupSetNodeArtifact, archivePath string) (string, func(), error) {
	isEnvelope, err := restoreArchiveLooksEncrypted(archivePath)
	if err != nil {
		return "", nil, err
	}
	if !isEnvelope {
		return archivePath, nil, nil
	}
	if m == nil || m.encryption == nil || !m.encryption.Enabled() {
		return "", nil, fmt.Errorf("archive %s is encrypted but no restore encryption service is configured", artifact.ArchiveName)
	}
	raw, err := os.ReadFile(archivePath)
	if err != nil {
		return "", nil, err
	}
	aad, err := encryptedBackupArchiveAAD(format, artifact.ArchiveName)
	if err != nil {
		return "", nil, err
	}
	plain, err := m.encryption.DecryptRecord(ctx, raw, aad)
	if err != nil {
		return "", nil, fmt.Errorf("decrypt encrypted backup archive: %w", err)
	}
	plainPath := archivePath + ".decrypted"
	if err := os.WriteFile(plainPath, plain, 0o600); err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.Remove(plainPath) }
	return plainPath, cleanup, nil
}

func restoreArchiveLooksEncrypted(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	buf := make([]byte, len("MYCELENC"))
	n, err := io.ReadFull(file, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return encryption.IsEnvelope(buf[:n]), nil
}

func encryptedBackupArchiveAAD(format backupcore.ArchiveFormat, archiveName string) ([]byte, error) {
	ext, err := backupcore.ArchiveExtension(format)
	if err != nil {
		return nil, err
	}
	archiveName = strings.TrimSpace(archiveName)
	if archiveName == "" {
		return nil, fmt.Errorf("archive name is required")
	}
	if !strings.HasSuffix(archiveName, ext) {
		return nil, fmt.Errorf("archive name %q does not match archive format %q", archiveName, format)
	}
	backupID := strings.TrimSuffix(archiveName, ext)
	return []byte("backup-archive:v1:id=" + backupID + ":name=" + archiveName), nil
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
		if m.clusterRestoreCanResumeAfterRestart(op) {
			if err := m.resumeClusterRestoreOperation(ctx, op); err != nil {
				op = recoverInterruptedRestoreApply(op)
				now := time.Now().UTC()
				op.State = clusterRestoreStateFailedRequiresOp
				op.Error = "cluster restore operation could not resume after daemon restart"
				op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_resume_failed", Detail: err.Error(), FieldPath: "restore_operation_id", Retryable: true})
				op.CompletedAt = now
				op.UpdatedAt = now
				op = m.cleanupClusterRestoreOperation(ctx, op)
				if persistErr := m.persistClusterRestoreOperation(ctx, op); persistErr != nil {
					return fmt.Errorf("mark interrupted cluster restore %s failed after resume failure: %w", op.RestoreOperationID, persistErr)
				}
			}
			continue
		}
		op = recoverInterruptedRestoreApply(op)
		now := time.Now().UTC()
		op.State = clusterRestoreStateFailedRequiresOp
		op.Error = "cluster restore operation was interrupted by daemon restart"
		op.Blockers = append(op.Blockers, clusterRestoreInterruptedBlocker(op))
		op.CompletedAt = now
		op.UpdatedAt = now
		op = m.cleanupClusterRestoreOperation(ctx, op)
		if err := m.persistClusterRestoreOperation(ctx, op); err != nil {
			return fmt.Errorf("mark interrupted cluster restore %s failed: %w", op.RestoreOperationID, err)
		}
	}
	return m.pruneClusterRestoreHistory(ctx)
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

func (m *Module) clusterRestoreExecutorIdentity() (executorID, nodeID, nodeName string, raftNodeID uint64) {
	if m == nil {
		return "", "", "", 0
	}
	identity := m.localIdentity
	nodeID = strings.TrimSpace(identity.NodeID)
	nodeName = strings.TrimSpace(identity.NodeName)
	raftNodeID = identity.RaftNodeID
	switch {
	case identity.RaftMode && raftNodeID != 0:
		executorID = fmt.Sprintf("raft-node:%d", raftNodeID)
	case nodeID != "":
		executorID = "node-id:" + nodeID
	case nodeName != "":
		executorID = "node-name:" + nodeName
	case strings.TrimSpace(identity.BackendAdvertiseAddr) != "":
		executorID = "backend:" + strings.TrimSpace(identity.BackendAdvertiseAddr)
	case strings.TrimSpace(m.dataDir) != "":
		executorID = "data-dir:" + filepath.Clean(m.dataDir)
	}
	return executorID, nodeID, nodeName, raftNodeID
}

func (m *Module) clusterRestoreCanResumeAfterRestart(op ClusterRestoreOperation) bool {
	if m == nil || strings.TrimSpace(op.ExecutorID) == "" || op.Progress.AppliedNodes != 0 || op.Progress.VerifiedNodes != 0 {
		return false
	}
	currentExecutor, _, _, _ := m.clusterRestoreExecutorIdentity()
	if currentExecutor == "" || currentExecutor != strings.TrimSpace(op.ExecutorID) {
		return false
	}
	if len(op.Blockers) != 0 || op.TargetMode != ClusterRestoreTargetModeNewClusterBootstrap || op.IdentityMode != ClusterRestoreIdentityModePreserveBackupID {
		return false
	}
	for _, node := range op.Progress.Nodes {
		switch strings.ToLower(strings.TrimSpace(node.State)) {
		case "", "staging", "staged":
		default:
			return false
		}
	}
	switch strings.ToUpper(strings.TrimSpace(op.State)) {
	case clusterRestoreStatePending, clusterRestoreStateValidatingTarget, clusterRestoreStateStagingArtifacts:
		return true
	default:
		return false
	}
}

func (m *Module) resumeClusterRestoreOperation(ctx context.Context, op ClusterRestoreOperation) error {
	plan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: op.RepositoryID, BackupSetID: op.BackupSetID, TargetMode: op.TargetMode, IdentityMode: op.IdentityMode, AuthSessionPolicy: op.AuthSessionPolicy, Target: op.Target, Staging: op.Staging})
	if err != nil {
		return fmt.Errorf("plan restore for restart resume: %w", err)
	}
	if !plan.Restorable {
		return fmt.Errorf("restore plan %s is no longer restorable: %v", plan.RestorePlanID, plan.Blockers)
	}
	now := time.Now().UTC()
	op = m.cleanupClusterRestoreOperation(ctx, op)
	op.RestorePlanID = plan.RestorePlanID
	op.Target = plan.Target
	op.Staging = plan.Staging
	op.State = clusterRestoreStatePending
	op.Error = ""
	op.Blockers = nil
	op.Progress = ClusterRestoreProgress{CurrentPhase: "resuming_after_restart"}
	op.ResumeCount++
	op.LastResumeAt = now
	op.UpdatedAt = now
	if err := m.persistClusterRestoreOperation(ctx, op); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	if m.clusterRestoreCancels == nil {
		m.clusterRestoreCancels = map[string]context.CancelFunc{}
	}
	m.clusterRestoreCancels[op.RestoreOperationID] = cancel
	m.mu.Unlock()
	input := StartClusterRestoreInput{RestorePlanID: plan.RestorePlanID, PlanFreshnessToken: plan.RestorePlanID, RepositoryID: op.RepositoryID, BackupSetID: op.BackupSetID, TargetMode: op.TargetMode, IdentityMode: op.IdentityMode, AuthSessionPolicy: op.AuthSessionPolicy, Target: plan.Target, Staging: plan.Staging, Preconditions: op.Preconditions, IdempotencyKey: op.IdempotencyKey}
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.clusterRestoreCancels, op.RestoreOperationID)
			m.mu.Unlock()
		}()
		m.runClusterRestoreOperation(runCtx, input, plan, op)
	}()
	return nil
}

func clusterRestoreInterruptedBlocker(op ClusterRestoreOperation) ValidationBlocker {
	state := strings.ToUpper(strings.TrimSpace(op.State))
	detail := "daemon restarted before restore operation reached a terminal state; inspect target data dirs/PVCs and restart from a fresh restore plan"
	if !clusterRestoreStateRestartSafe(state) {
		detail = fmt.Sprintf("daemon restarted during %s, a phase that may have modified target PVCs or data directories; inspect target state, cleanup or recreate targets, then restart from a fresh restore plan", state)
	} else if strings.TrimSpace(op.ExecutorID) == "" {
		detail = "daemon restarted before restore completed, and the original executor identity was not recorded; inspect target state and restart from a fresh restore plan"
	}
	return ValidationBlocker{Reason: "restore_operation_interrupted", Detail: detail, FieldPath: "restore_operation_id"}
}

func clusterRestoreStateRestartSafe(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case clusterRestoreStatePending, clusterRestoreStateValidatingTarget, clusterRestoreStateStagingArtifacts:
		return true
	default:
		return false
	}
}

func finalizeClusterRestoreCancellation(op ClusterRestoreOperation, cause error) ClusterRestoreOperation {
	phase := firstNonEmptyString(op.CancelRequestedState, op.State)
	reason := firstNonEmptyString(op.CancelReason, op.Error, "cluster restore cancellation requested")
	if clusterRestoreCancelRequiresOperator(phase, op) {
		op.State = clusterRestoreStateFailedRequiresOp
		op.Error = firstNonEmptyString(reason, "cluster restore cancellation requires operator validation")
		op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_cancel_requires_operator_validation", Detail: fmt.Sprintf("restore cancellation was requested during %s after target preparation/apply may have modified target PVCs or data directories; inspect and cleanup target state before retrying", strings.ToUpper(strings.TrimSpace(phase))), FieldPath: "restore_operation_id"})
		return op
	}
	op.State = clusterRestoreStateCanceled
	op.Error = firstNonEmptyString(reason, fmt.Sprint(cause))
	return op
}

func clusterRestoreCancelRequiresOperator(phase string, op ClusterRestoreOperation) bool {
	if op.Progress.AppliedNodes > 0 || op.Progress.VerifiedNodes > 0 {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(phase)) {
	case clusterRestoreStatePreparingTarget, clusterRestoreStateApplyingArtifacts, clusterRestoreStatePostRestoreValidate:
		return true
	default:
		return false
	}
}

func restoreExecutionCapacityBlockers(plan ClusterRestorePlan, mappings []ClusterRestoreNodeMapping, artifacts map[int]clusterbackup.BackupSetNodeArtifact, stagingPath string) []ValidationBlocker {
	var blockers []ValidationBlocker
	required := plan.Capacity.RequiredBytes
	if required <= 0 {
		for _, artifact := range artifacts {
			required += parseSignedDecimalStringLocal(artifact.SizeBytes)
		}
	}
	if plan.Staging.AvailableBytes > 0 && required > 0 && plan.Staging.AvailableBytes < required {
		blockers = append(blockers, ValidationBlocker{Reason: "restore_staging_capacity_exhausted", Detail: fmt.Sprintf("restore plan requires at least %d bytes but staging reports %d bytes available", required, plan.Staging.AvailableBytes), FieldPath: "staging.available_bytes", Retryable: true})
	}
	if restorePlanRequiresLocalStaging(mappings) && strings.TrimSpace(stagingPath) != "" && required > 0 {
		if available, err := restoreFilesystemAvailableBytes(stagingPath); err != nil {
			blockers = append(blockers, ValidationBlocker{Reason: "restore_staging_capacity_check_failed", Detail: err.Error(), FieldPath: "staging.staging_uri", Retryable: true})
		} else if available < required {
			blockers = append(blockers, ValidationBlocker{Reason: "restore_staging_capacity_exhausted", Detail: fmt.Sprintf("local staging path %s has %d bytes available, restore requires at least %d bytes", stagingPath, available, required), FieldPath: "staging.staging_uri", Retryable: true})
		}
	}
	for i, mapping := range mappings {
		if restoreNodeUsesPVCHelper(mapping) || strings.TrimSpace(mapping.TargetDataDir) == "" {
			continue
		}
		artifact := artifacts[mapping.Ordinal]
		nodeRequired := parseSignedDecimalStringLocal(artifact.SizeBytes)
		if nodeRequired <= 0 {
			continue
		}
		parent := filepath.Dir(filepath.Clean(strings.TrimSpace(mapping.TargetDataDir)))
		capacityPath, err := restoreExistingCapacityPath(parent)
		if err != nil {
			blockers = append(blockers, ValidationBlocker{Reason: "restore_target_capacity_check_failed", Detail: err.Error(), FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i), Retryable: true})
			continue
		}
		if available, err := restoreFilesystemAvailableBytes(capacityPath); err != nil {
			blockers = append(blockers, ValidationBlocker{Reason: "restore_target_capacity_check_failed", Detail: err.Error(), FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i), Retryable: true})
		} else if available < nodeRequired {
			blockers = append(blockers, ValidationBlocker{Reason: "restore_target_capacity_exhausted", Detail: fmt.Sprintf("target filesystem for %s has %d bytes available, ordinal %d archive requires at least %d bytes", mapping.TargetDataDir, available, mapping.Ordinal, nodeRequired), FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i), Retryable: true})
		}
	}
	return blockers
}

func restoreFilesystemAvailableBytes(path string) (int64, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return 0, fmt.Errorf("capacity path is required")
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

func restoreExistingCapacityPath(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return "", fmt.Errorf("capacity path is required")
	}
	for {
		if info, err := os.Stat(path); err == nil {
			if !info.IsDir() {
				return "", fmt.Errorf("capacity path %s is not a directory", path)
			}
			return path, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "", fmt.Errorf("no existing filesystem path found for capacity check")
		}
		path = parent
	}
}

func validateRestoredDataDirForOperation(dataDir string, plan ClusterRestorePlan, mapping ClusterRestoreNodeMapping, artifact clusterbackup.BackupSetNodeArtifact, restoreOperationID string) (map[string]string, error) {
	if err := clusterbackup.VerifyRestoredDataDir(dataDir); err != nil {
		return nil, err
	}
	restoredNodeID, err := restoredDataDirNodeID(dataDir)
	if err != nil {
		return nil, err
	}
	expectedNodeID := firstNonEmptyString(mapping.SourceNodeID, artifact.NodeID)
	if expectedNodeID != "" && restoredNodeID != expectedNodeID {
		return nil, fmt.Errorf("restored node_id=%s does not match expected source node_id=%s for ordinal %d", restoredNodeID, expectedNodeID, mapping.Ordinal)
	}
	evidence, err := restorePostValidationEvidence(plan, mapping, artifact, "passed", restoredNodeID)
	if err != nil {
		return nil, err
	}
	if plan.ProductionManifest != nil {
		doc := clusterbackup.BuildRestoredDerivedReadiness(*plan.ProductionManifest, clusterbackup.RestoredDerivedReadinessOptions{RestoreOperationID: restoreOperationID, RestorePlanID: plan.RestorePlanID, Ordinal: mapping.Ordinal, NodeID: restoredNodeID, PodName: artifact.PodName, RestoredAt: time.Now().UTC(), Evidence: map[string]string{"post_restore_validation": "passed"}})
		if err := clusterbackup.WriteRestoredDerivedReadiness(dataDir, doc); err != nil {
			return nil, fmt.Errorf("write restored derived readiness marker: %w", err)
		}
		evidence["restored_derived_readiness_marker"] = clusterbackup.RestoredDerivedReadinessRelativePath
		evidence["restored_derived_ready"] = fmt.Sprintf("%t", doc.DerivedReady)
	}
	return evidence, nil
}

func restoredDataDirNodeID(dataDir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dataDir, "meta", "clustering", "node.json"))
	if err != nil {
		return "", fmt.Errorf("read restored clustering metadata: %w", err)
	}
	var identity struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(raw, &identity); err != nil {
		return "", fmt.Errorf("parse restored clustering metadata: %w", err)
	}
	if strings.TrimSpace(identity.NodeID) == "" {
		return "", fmt.Errorf("restored clustering metadata has empty node_id")
	}
	return strings.TrimSpace(identity.NodeID), nil
}

func restorePostValidationEvidence(plan ClusterRestorePlan, mapping ClusterRestoreNodeMapping, artifact clusterbackup.BackupSetNodeArtifact, state, restoredNodeID string) (map[string]string, error) {
	manifest := plan.ProductionManifest
	if manifest == nil {
		return nil, fmt.Errorf("restore plan is missing production manifest")
	}
	identity := manifest.Consistency.BarrierSet.Identity
	system := manifest.Consistency.BarrierSet.SystemMetadata
	evidence := map[string]string{
		"post_restore_validation":              state,
		"post_restore_validated_at":            time.Now().UTC().Format(time.RFC3339Nano),
		"post_restore_validator":               "cluster_restore_operation/v1",
		"restore_identity_mode":                plan.IdentityMode,
		"restore_auth_session_policy":          plan.AuthSessionPolicy,
		"manifest_cluster_id":                  system.ClusterID,
		"manifest_bootstrap_epoch":             system.BootstrapEpoch,
		"manifest_system_raft_group_id":        system.SystemRaftGroupID,
		"manifest_system_raft_barrier_index":   system.SystemRaftBarrierIndex,
		"manifest_identity_raft_group_id":      identity.IdentityRaftGroupID,
		"manifest_identity_raft_barrier_index": identity.IdentityRaftBarrierIndex,
		"authorization_revision":               identity.AuthorizationRevision,
		"auth_session_policy":                  identity.AuthSessionPolicy,
		"expected_node_id":                     firstNonEmptyString(mapping.SourceNodeID, artifact.NodeID),
		"expected_pod_name":                    firstNonEmptyString(mapping.SourcePodName, artifact.PodName),
		"expected_raft_node_id":                artifact.RaftNodeID,
		"raft_identity_lineage":                plan.IdentityMode,
		"semantic_barrier_count":               fmt.Sprint(len(manifest.Consistency.BarrierSet.Semantic)),
		"lexical_barrier_count":                fmt.Sprint(len(manifest.Consistency.BarrierSet.Lexical)),
		"projection_barrier_count":             fmt.Sprint(len(manifest.Consistency.BarrierSet.Projections)),
		"derived_rebuild_required_subsystems":  strings.Join(restoreDerivedSubsystems(manifest.DerivedState, "REBUILD_REQUIRED"), ","),
		"derived_included_subsystems":          strings.Join(restoreDerivedIncludedSubsystems(manifest.DerivedState), ","),
	}
	if restoredNodeID != "" {
		evidence["restored_node_id"] = restoredNodeID
		evidence["vector_store_catalog"] = "valid"
	}
	if identity.PrincipalStoreChecksumSHA256 != "" {
		evidence["principal_store_checksum_present"] = "true"
	}
	if identity.RoleBindingChecksumSHA256 != "" {
		evidence["role_binding_checksum_present"] = "true"
	}
	if identity.CapabilityGrantChecksumSHA256 != "" {
		evidence["capability_grant_checksum_present"] = "true"
	}
	if identity.RefreshSessionStoreChecksumSHA256 != "" {
		evidence["refresh_session_checksum_present"] = "true"
	}
	return evidence, nil
}

func restoreDerivedSubsystems(rows []clusterbackup.BackupSetDerivedState, want string) []string {
	want = normalizeProductionEnum(want)
	seen := map[string]bool{}
	var out []string
	for _, row := range rows {
		subsystem := normalizeProductionEnum(row.Subsystem)
		if subsystem == "" || seen[subsystem] {
			continue
		}
		if normalizeProductionEnum(row.RestorePolicy) == want || normalizeProductionEnum(row.ValidationState) == want {
			seen[subsystem] = true
			out = append(out, subsystem)
		}
	}
	sort.Strings(out)
	return out
}

func restoreDerivedIncludedSubsystems(rows []clusterbackup.BackupSetDerivedState) []string {
	seen := map[string]bool{}
	var out []string
	for _, row := range rows {
		subsystem := normalizeProductionEnum(row.Subsystem)
		if subsystem == "" || seen[subsystem] || !row.Included {
			continue
		}
		seen[subsystem] = true
		out = append(out, subsystem)
	}
	sort.Strings(out)
	return out
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
	now := time.Now().UTC()
	if cancel != nil {
		cancel()
		op.CancelRequestedAt = now
		op.CancelRequestedState = op.State
		op.CancelReason = firstNonEmptyString(reason, "cluster restore cancellation requested")
		op.State = clusterRestoreStateCanceling
		op.Error = op.CancelReason
		op.UpdatedAt = now
		if err := m.persistClusterRestoreOperation(ctx, op); err != nil {
			return ClusterRestoreOperation{}, err
		}
		return m.ClusterRestoreStatus(restoreOperationID)
	}
	op.CancelRequestedAt = now
	op.CancelRequestedState = op.State
	op.CancelReason = firstNonEmptyString(reason, "cluster restore canceled before execution could resume")
	op.Error = op.CancelReason
	op = finalizeClusterRestoreCancellation(op, nil)
	op.CompletedAt = now
	op.UpdatedAt = now
	op = m.cleanupClusterRestoreOperation(ctx, op)
	if err := m.persistClusterRestoreOperation(ctx, op); err != nil {
		return ClusterRestoreOperation{}, err
	}
	_ = m.pruneClusterRestoreHistory(ctx)
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

func (m *Module) applyClusterRestorePrune(ctx context.Context, rec wal.Record) error {
	var payload clusterRestorePruneRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureClusterRestoreStateLocked()
	for _, id := range payload.RestoreOperationIDs {
		id = strings.TrimSpace(id)
		if id == "" || id == m.activeClusterRestoreID {
			continue
		}
		op, ok := m.clusterRestores[id]
		if !ok || !clusterRestoreStateTerminal(op.State) {
			continue
		}
		delete(m.clusterRestores, id)
	}
	return nil
}

func (m *Module) clusterRestoreWALAppliers() map[wal.RecordType]wal.Applier {
	return map[wal.RecordType]wal.Applier{
		recordTypeClusterRestoreRequest: wal.ApplierFunc(m.applyClusterRestoreRequest),
		recordTypeClusterRestoreUpdate:  wal.ApplierFunc(m.applyClusterRestoreUpdate),
		recordTypeClusterRestorePrune:   wal.ApplierFunc(m.applyClusterRestorePrune),
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

func (m *Module) pruneClusterRestoreHistory(ctx context.Context) error {
	limit := m.clusterRestoreHistoryLimit()
	m.mu.Lock()
	activeID := m.activeClusterRestoreID
	operations := cloneClusterRestoreOperations(m.clusterRestores)
	m.mu.Unlock()
	ids := clusterRestorePruneIDs(operations, activeID, limit)
	if len(ids) == 0 {
		return nil
	}
	return m.commitClusterRestore(ctx, recordTypeClusterRestorePrune, clusterRestorePruneRecord{RestoreOperationIDs: ids, PrunedAt: time.Now().UTC(), Reason: "status_history_limit"})
}

func (m *Module) clusterRestoreHistoryLimit() int {
	if m != nil && m.policy.StatusHistoryLimit > 0 {
		return m.policy.StatusHistoryLimit
	}
	if m != nil && m.config.StatusHistoryLimit > 0 {
		return m.config.StatusHistoryLimit
	}
	return backupcore.DefaultStatusHistoryLimit
}

func clusterRestorePruneIDs(operations map[string]ClusterRestoreOperation, activeID string, limit int) []string {
	if limit <= 0 {
		limit = backupcore.DefaultStatusHistoryLimit
	}
	terminal := make([]ClusterRestoreOperation, 0, len(operations))
	for _, op := range operations {
		if op.RestoreOperationID == activeID || !clusterRestoreStateTerminal(op.State) {
			continue
		}
		terminal = append(terminal, cloneClusterRestoreOperation(op))
	}
	if len(terminal) <= limit {
		return nil
	}
	sort.Slice(terminal, func(i, j int) bool {
		left := clusterRestoreHistorySortTime(terminal[i])
		right := clusterRestoreHistorySortTime(terminal[j])
		if left.Equal(right) {
			return terminal[i].RestoreOperationID > terminal[j].RestoreOperationID
		}
		return left.After(right)
	})
	ids := make([]string, 0, len(terminal)-limit)
	for _, op := range terminal[limit:] {
		ids = append(ids, op.RestoreOperationID)
	}
	sort.Strings(ids)
	return ids
}

func clusterRestoreHistorySortTime(op ClusterRestoreOperation) time.Time {
	if !op.CompletedAt.IsZero() {
		return op.CompletedAt
	}
	if !op.UpdatedAt.IsZero() {
		return op.UpdatedAt
	}
	return op.CreatedAt
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
	op.ExecutorID = strings.TrimSpace(op.ExecutorID)
	op.ExecutorNodeID = strings.TrimSpace(op.ExecutorNodeID)
	op.ExecutorNodeName = strings.TrimSpace(op.ExecutorNodeName)
	op.CancelRequestedState = strings.ToUpper(strings.TrimSpace(op.CancelRequestedState))
	op.CancelReason = strings.TrimSpace(op.CancelReason)
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
	for i := range op.Progress.Nodes {
		op.Progress.Nodes[i].Evidence = copyStringMapLocal(op.Progress.Nodes[i].Evidence)
	}
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
