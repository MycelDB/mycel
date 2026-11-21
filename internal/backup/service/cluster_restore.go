package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
)

const (
	ClusterRestoreTargetModeNewClusterBootstrap    = "NEW_CLUSTER_BOOTSTRAP"
	ClusterRestoreTargetModeExistingClusterReplace = "EXISTING_CLUSTER_REPLACE"

	ClusterRestoreIdentityModePreserveBackupID = "PRESERVE_BACKUP_ID"
	ClusterRestoreIdentityModeAdoptTargetID    = "ADOPT_TARGET_ID"
	ClusterRestoreIdentityModeGenerateNewID    = "GENERATE_NEW_ID"

	ClusterRestoreAuthSessionPolicyPreserveRestoredSessions = "PRESERVE_RESTORED_SESSIONS"
	ClusterRestoreAuthSessionPolicyRevokeAllSessions        = "REVOKE_ALL_SESSIONS"
)

type PlanClusterRestoreInput struct {
	RepositoryID      string
	BackupSetID       string
	TargetMode        string
	IdentityMode      string
	AuthSessionPolicy string
	ValidationOptions clusterbackup.BackupSetValidationOptions
	Target            ClusterRestoreTarget
	Staging           ClusterRestoreStagingLocation
}

type ClusterRestoreTarget struct {
	TargetClusterID   string
	TargetClusterName string
	Namespace         string
	StatefulSet       string
	ExpectedNodeCount int
	Nodes             []ClusterRestoreNodeMapping
	Labels            map[string]string
	Annotations       map[string]string
}

type ClusterRestoreNodeMapping struct {
	Ordinal          int
	SourcePodName    string
	SourceNodeID     string
	SourceArchiveURI string
	TargetPodName    string
	TargetNodeName   string
	TargetPVCName    string
	TargetDataDir    string
	StagingPath      string
}

type ClusterRestoreStagingLocation struct {
	StagingURI     string
	StorageClass   string
	RequiredBytes  int64
	AvailableBytes int64
	CleanBeforeUse bool
	Blockers       []ValidationBlocker
}

type ClusterRestorePreconditionEvidence struct {
	CleanTarget   ClusterRestoreCleanTargetProof
	SourceFencing ClusterRestoreSourceFencingEvidence
	Blockers      []ValidationBlocker
}

type StartClusterRestoreInput struct {
	RestorePlanID           string
	RepositoryID            string
	BackupSetID             string
	TargetMode              string
	IdentityMode            string
	AuthSessionPolicy       string
	IdempotencyKey          string
	DestructiveConfirmation string
	Target                  ClusterRestoreTarget
	Staging                 ClusterRestoreStagingLocation
	Preconditions           ClusterRestorePreconditionEvidence
	PlanFreshnessToken      string
}

type ClusterRestoreOperation struct {
	RestoreOperationID string
	RestorePlanID      string
	RepositoryID       string
	BackupSetID        string
	State              string
	TargetMode         string
	IdentityMode       string
	AuthSessionPolicy  string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        time.Time
	Blockers           []ValidationBlocker
	CleanupTasks       []CleanupTask
	Error              string
	Target             ClusterRestoreTarget
	Staging            ClusterRestoreStagingLocation
	Preconditions      ClusterRestorePreconditionEvidence
	Progress           ClusterRestoreProgress
}

type CleanupTask struct {
	TaskID       string
	ResourceKind string
	ResourceURI  string
	State        string
	LastError    string
}

type ClusterRestoreProgress struct {
	TotalNodes     int
	StagedNodes    int
	AppliedNodes   int
	VerifiedNodes  int
	BytesTotal     int64
	BytesCompleted int64
	CurrentPhase   string
	CurrentNode    string
	Nodes          []ClusterRestoreNodeProgress
}

type ClusterRestoreNodeProgress struct {
	Ordinal          int
	State            string
	SourceArchiveURI string
	StagingURI       string
	BytesTotal       int64
	BytesCompleted   int64
	ChecksumSHA256   string
	LastError        string
}

type ClusterRestoreCleanTargetProof struct {
	ProofID             string
	CheckedAt           time.Time
	TargetEmpty         bool
	CheckedNodes        []ClusterRestoreNodeMapping
	Blockers            []ValidationBlocker
	OperatorAttestation string
}

type ClusterRestoreSourceFencingEvidence struct {
	SourceClusterFenced bool
	FencedAt            time.Time
	FenceTokenRef       string
	FencedEndpoints     []string
	Blockers            []ValidationBlocker
}

type ClusterRestorePlan struct {
	RestorePlanID      string
	RepositoryID       string
	BackupSetID        string
	TargetMode         string
	IdentityMode       string
	AuthSessionPolicy  string
	Restorable         bool
	Validation         *clusterbackup.BackupSetValidationReport
	Compatibility      clusterbackup.BackupSetCompatibility
	Capacity           clusterbackup.BackupSetCapacityEstimate
	DerivedActions     []backupcore.DerivedSubsystemPolicy
	Blockers           []ValidationBlocker
	PlannedAt          time.Time
	Target             ClusterRestoreTarget
	Staging            ClusterRestoreStagingLocation
	Preconditions      ClusterRestorePreconditionEvidence
	ProductionManifest *clusterbackup.BackupSetManifestV1
	ProductionMarker   *clusterbackup.CompleteMarkerV1
}

func (m *Module) PlanClusterRestore(ctx context.Context, input PlanClusterRestoreInput) (ClusterRestorePlan, error) {
	repositoryID := strings.TrimSpace(input.RepositoryID)
	backupSetID := strings.TrimSpace(input.BackupSetID)
	if repositoryID == "" {
		return ClusterRestorePlan{}, fmt.Errorf("repository_id is required")
	}
	if backupSetID == "" {
		return ClusterRestorePlan{}, fmt.Errorf("backup_set_id is required")
	}
	options := input.ValidationOptions
	options.RequireCompleteMarker = true
	options.AllowS3ArtifactURIs = true
	result, validateErr := m.ValidateRepositoryClusterBackupSet(ctx, repositoryID, backupSetID, options)
	if validateErr != nil && result.ProductionManifest == nil {
		return ClusterRestorePlan{}, validateErr
	}
	manifest := result.ProductionManifest
	if manifest == nil {
		return ClusterRestorePlan{}, fmt.Errorf("repository backup set %s has no production manifest", backupSetID)
	}
	plannedAt := time.Now().UTC()
	targetMode := normalizeRestoreEnum(input.TargetMode)
	if targetMode == "" {
		targetMode = ClusterRestoreTargetModeNewClusterBootstrap
	}
	identityMode := normalizeRestoreEnum(input.IdentityMode)
	if identityMode == "" {
		identityMode = normalizeRestoreEnum(manifest.Restore.DefaultIdentityMode)
	}
	if identityMode == "" {
		identityMode = ClusterRestoreIdentityModePreserveBackupID
	}
	authPolicy := normalizeRestoreEnum(input.AuthSessionPolicy)
	if authPolicy == "" {
		if identityMode == ClusterRestoreIdentityModePreserveBackupID {
			authPolicy = ClusterRestoreAuthSessionPolicyPreserveRestoredSessions
		} else {
			authPolicy = ClusterRestoreAuthSessionPolicyRevokeAllSessions
		}
	}
	target := normalizeRestoreTarget(input.Target)
	staging := normalizeRestoreStaging(input.Staging)
	plan := ClusterRestorePlan{RestorePlanID: restorePlanID(repositoryID, backupSetID, targetMode, identityMode, authPolicy, target, staging, result), RepositoryID: repositoryID, BackupSetID: backupSetID, TargetMode: targetMode, IdentityMode: identityMode, AuthSessionPolicy: authPolicy, Validation: result.ProductionReport, Compatibility: manifest.Compatibility, DerivedActions: restoreDerivedActions(*manifest), PlannedAt: plannedAt, Target: target, Staging: staging, Preconditions: restorePlanRequiredPreconditions(*manifest, targetMode, identityMode), ProductionManifest: manifest, ProductionMarker: result.ProductionMarker}
	if result.ProductionReport != nil {
		plan.Capacity = result.ProductionReport.Capacity
		for _, issue := range result.ProductionReport.Errors {
			plan.Blockers = append(plan.Blockers, ValidationBlocker{Reason: issue.Reason, Detail: issue.Detail, Retryable: issue.Retryable, FieldPath: issue.FieldPath})
		}
	}
	if plan.Capacity.RequiredBytes == 0 {
		plan.Capacity.RequiredBytes = parseSignedDecimalStringLocal(manifest.Validation.TotalArtifactBytes)
	}
	plan.Blockers = append(plan.Blockers, restorePlanPolicyBlockers(*manifest, targetMode, identityMode, authPolicy)...)
	plan.Blockers = append(plan.Blockers, restorePlanTargetBlockers(*manifest, target)...)
	plan.Blockers = append(plan.Blockers, staging.Blockers...)
	if validateErr != nil {
		plan.Blockers = append(plan.Blockers, ValidationBlocker{Reason: "backup_set_invalid", Detail: validateErr.Error(), FieldPath: "backup_set"})
	}
	plan.Restorable = len(plan.Blockers) == 0
	return plan, nil
}

func (m *Module) StartClusterRestore(ctx context.Context, input StartClusterRestoreInput) (ClusterRestoreOperation, error) {
	createdAt := time.Now().UTC()
	plan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: input.RepositoryID, BackupSetID: input.BackupSetID, TargetMode: input.TargetMode, IdentityMode: input.IdentityMode, AuthSessionPolicy: input.AuthSessionPolicy, Target: input.Target, Staging: input.Staging})
	if err != nil {
		return ClusterRestoreOperation{}, err
	}
	op := ClusterRestoreOperation{RestoreOperationID: restoreOperationID(plan, strings.TrimSpace(input.IdempotencyKey)), RestorePlanID: plan.RestorePlanID, RepositoryID: plan.RepositoryID, BackupSetID: plan.BackupSetID, State: "VALIDATING_TARGET", TargetMode: plan.TargetMode, IdentityMode: plan.IdentityMode, AuthSessionPolicy: plan.AuthSessionPolicy, CreatedAt: createdAt, UpdatedAt: createdAt, Target: plan.Target, Staging: plan.Staging, Preconditions: input.Preconditions}
	blockers := restoreExecutionBlockers(plan, input)
	if len(blockers) > 0 {
		op.State = "FAILED_REQUIRES_OPERATOR"
		op.Blockers = blockers
		op.Error = "cluster restore execution preconditions are not satisfied"
		op.CompletedAt = time.Now().UTC()
		op.UpdatedAt = op.CompletedAt
		return op, nil
	}
	repo, store, err := m.repositoryCatalogStore(ctx, plan.RepositoryID)
	if err != nil {
		return ClusterRestoreOperation{}, err
	}
	stagingPath, err := localRestoreStagingPath(plan.Staging.StagingURI)
	if err != nil {
		op.State = "FAILED_REQUIRES_OPERATOR"
		op.Blockers = append(op.Blockers, ValidationBlocker{Reason: "restore_staging_not_local", Detail: err.Error(), FieldPath: "staging.staging_uri"})
		op.Error = err.Error()
		op.CompletedAt = time.Now().UTC()
		op.UpdatedAt = op.CompletedAt
		return op, nil
	}
	if plan.Staging.CleanBeforeUse {
		if err := os.RemoveAll(stagingPath); err != nil {
			return ClusterRestoreOperation{}, fmt.Errorf("clean restore staging path: %w", err)
		}
	}
	if err := os.MkdirAll(stagingPath, 0o700); err != nil {
		return ClusterRestoreOperation{}, fmt.Errorf("prepare restore staging path: %w", err)
	}
	op.CleanupTasks = append(op.CleanupTasks, CleanupTask{TaskID: "staging-directory", ResourceKind: "local_directory", ResourceURI: stagingPath, State: "created"})
	manifest := plan.ProductionManifest
	if manifest == nil {
		return ClusterRestoreOperation{}, fmt.Errorf("restore plan is missing production manifest")
	}
	format := archiveFormatFromProductionManifest(manifest.Artifacts.ArchiveFormat)
	if !backupcore.IsSupportedArchiveFormat(format) {
		return ClusterRestoreOperation{}, fmt.Errorf("unsupported restore archive format %q", manifest.Artifacts.ArchiveFormat)
	}
	artifacts := map[int]clusterbackup.BackupSetNodeArtifact{}
	for _, artifact := range manifest.Artifacts.Nodes {
		artifacts[artifact.Ordinal] = artifact
		op.Progress.BytesTotal += parseSignedDecimalStringLocal(artifact.SizeBytes)
	}
	op.Progress.TotalNodes = len(plan.Target.Nodes)
	op.Progress.CurrentPhase = "staging_artifacts"
	sortedMappings := append([]ClusterRestoreNodeMapping(nil), plan.Target.Nodes...)
	sort.Slice(sortedMappings, func(i, j int) bool { return sortedMappings[i].Ordinal < sortedMappings[j].Ordinal })
	for _, mapping := range sortedMappings {
		artifact, ok := artifacts[mapping.Ordinal]
		if !ok {
			return ClusterRestoreOperation{}, fmt.Errorf("backup set %s has no artifact for ordinal %d", plan.BackupSetID, mapping.Ordinal)
		}
		progress := ClusterRestoreNodeProgress{Ordinal: mapping.Ordinal, State: "staging", SourceArchiveURI: artifact.ArchiveURI, ChecksumSHA256: artifact.ChecksumSHA256, BytesTotal: parseSignedDecimalStringLocal(artifact.SizeBytes)}
		archiveKey, err := repositoryKeyFromS3URI(repo, artifact.ArchiveURI)
		if err != nil {
			progress.State = "failed"
			progress.LastError = err.Error()
			op.Progress.Nodes = append(op.Progress.Nodes, progress)
			return ClusterRestoreOperation{}, err
		}
		archivePath := filepath.Join(stagingPath, fmt.Sprintf("ordinal-%d", mapping.Ordinal), artifact.ArchiveName)
		info, err := store.GetFile(ctx, archiveKey, archivePath)
		if err != nil {
			progress.State = "failed"
			progress.LastError = err.Error()
			op.Progress.Nodes = append(op.Progress.Nodes, progress)
			return ClusterRestoreOperation{}, fmt.Errorf("stage ordinal %d archive: %w", mapping.Ordinal, err)
		}
		checksum, size, err := fileSHA256Local(ctx, archivePath)
		if err != nil {
			return ClusterRestoreOperation{}, fmt.Errorf("checksum staged ordinal %d archive: %w", mapping.Ordinal, err)
		}
		if artifact.ChecksumSHA256 != "" && checksum != artifact.ChecksumSHA256 {
			return ClusterRestoreOperation{}, fmt.Errorf("checksum staged ordinal %d archive: got %s want %s", mapping.Ordinal, checksum, artifact.ChecksumSHA256)
		}
		if info.SizeBytes > 0 && info.SizeBytes != size {
			return ClusterRestoreOperation{}, fmt.Errorf("size staged ordinal %d archive: got %d want repository %d", mapping.Ordinal, size, info.SizeBytes)
		}
		progress.State = "staged"
		progress.StagingURI = archivePath
		progress.BytesCompleted = size
		op.Progress.BytesCompleted += size
		op.Progress.StagedNodes++
		op.Progress.Nodes = append(op.Progress.Nodes, progress)
	}
	op.State = "APPLYING_ARTIFACTS"
	op.UpdatedAt = time.Now().UTC()
	op.Progress.CurrentPhase = "applying_artifacts"
	for i, mapping := range sortedMappings {
		artifact := artifacts[mapping.Ordinal]
		archivePath := filepath.Join(stagingPath, fmt.Sprintf("ordinal-%d", mapping.Ordinal), artifact.ArchiveName)
		dataDir := strings.TrimSpace(mapping.TargetDataDir)
		if err := ensureRestoreDataDirEmpty(dataDir); err != nil {
			return ClusterRestoreOperation{}, fmt.Errorf("target data dir for ordinal %d is not clean: %w", mapping.Ordinal, err)
		}
		op.Progress.CurrentNode = fmt.Sprintf("ordinal-%d", mapping.Ordinal)
		op.Progress.Nodes[i].State = "applying"
		if err := clusterbackup.ExtractArchive(ctx, format, archivePath, dataDir); err != nil {
			op.Progress.Nodes[i].State = "failed"
			op.Progress.Nodes[i].LastError = err.Error()
			return ClusterRestoreOperation{}, fmt.Errorf("apply ordinal %d archive: %w", mapping.Ordinal, err)
		}
		op.Progress.AppliedNodes++
		op.Progress.Nodes[i].State = "verifying"
		if err := clusterbackup.VerifyRestoredDataDir(dataDir); err != nil {
			op.Progress.Nodes[i].State = "failed"
			op.Progress.Nodes[i].LastError = err.Error()
			return ClusterRestoreOperation{}, fmt.Errorf("verify restored ordinal %d: %w", mapping.Ordinal, err)
		}
		op.Progress.VerifiedNodes++
		op.Progress.Nodes[i].State = "verified"
	}
	op.State = "SUCCEEDED"
	op.Progress.CurrentPhase = "succeeded"
	op.Progress.CurrentNode = ""
	op.CompletedAt = time.Now().UTC()
	op.UpdatedAt = op.CompletedAt
	return op, nil
}

func restorePlanPolicyBlockers(manifest clusterbackup.BackupSetManifestV1, targetMode, identityMode, authPolicy string) []ValidationBlocker {
	var blockers []ValidationBlocker
	if !containsRestoreEnum(manifest.Restore.SupportedRestoreTargetModes, targetMode) && !containsRestoreEnum(manifest.Compatibility.SupportedRestoreTargetModes, targetMode) {
		blockers = append(blockers, ValidationBlocker{Reason: "unsupported_restore_target_mode", Detail: fmt.Sprintf("backup set does not support target mode %s", targetMode), FieldPath: "target_mode"})
	}
	if targetMode == ClusterRestoreTargetModeExistingClusterReplace {
		blockers = append(blockers, ValidationBlocker{Reason: "existing_cluster_replace_not_implemented", Detail: "EXISTING_CLUSTER_REPLACE remains blocked until destructive restore locks, source fencing, and safety backup gates are implemented", FieldPath: "target_mode"})
	} else if targetMode != ClusterRestoreTargetModeNewClusterBootstrap {
		blockers = append(blockers, ValidationBlocker{Reason: "unsupported_restore_target_mode", Detail: fmt.Sprintf("unsupported target mode %s", targetMode), FieldPath: "target_mode"})
	}
	if !containsRestoreEnum(manifest.Restore.SupportedIdentityModes, identityMode) && !containsRestoreEnum(manifest.Compatibility.SupportedIdentityModes, identityMode) {
		blockers = append(blockers, ValidationBlocker{Reason: "unsupported_restore_identity_mode", Detail: fmt.Sprintf("backup set does not support identity mode %s", identityMode), FieldPath: "identity_mode"})
	}
	if identityMode != ClusterRestoreIdentityModePreserveBackupID {
		blockers = append(blockers, ValidationBlocker{Reason: "identity_transform_not_implemented", Detail: "ADOPT_TARGET_ID and GENERATE_NEW_ID are blocked for raw physical backup sets until safe identity transform or logical import exists", FieldPath: "identity_mode"})
	}
	if !containsRestoreEnum(manifest.Restore.SupportedAuthSessionPolicies, authPolicy) {
		blockers = append(blockers, ValidationBlocker{Reason: "unsupported_auth_session_policy", Detail: fmt.Sprintf("backup set does not support auth-session policy %s", authPolicy), FieldPath: "auth_session_policy"})
	}
	if identityMode != ClusterRestoreIdentityModePreserveBackupID && authPolicy != ClusterRestoreAuthSessionPolicyRevokeAllSessions {
		blockers = append(blockers, ValidationBlocker{Reason: "unsafe_auth_session_policy", Detail: "identity-transform restore modes require REVOKE_ALL_SESSIONS", FieldPath: "auth_session_policy"})
	}
	return blockers
}

func restoreDerivedActions(manifest clusterbackup.BackupSetManifestV1) []backupcore.DerivedSubsystemPolicy {
	if len(manifest.DerivedState) > 0 {
		out := make([]backupcore.DerivedSubsystemPolicy, 0, len(manifest.DerivedState))
		seen := map[string]bool{}
		for _, row := range manifest.DerivedState {
			subsystem := normalizeProductionEnum(row.Subsystem)
			if subsystem == "" || seen[subsystem] {
				continue
			}
			seen[subsystem] = true
			mode := normalizeProductionEnum(row.PolicyMode)
			if mode == "" {
				mode = normalizeProductionEnum(row.RestorePolicy)
			}
			out = append(out, backupcore.DerivedSubsystemPolicy{Subsystem: subsystem, Mode: mode})
		}
		return out
	}
	out := make([]backupcore.DerivedSubsystemPolicy, 0, len(manifest.Policy.DerivedSubsystems))
	for _, row := range manifest.Policy.DerivedSubsystems {
		out = append(out, backupcore.DerivedSubsystemPolicy{Subsystem: normalizeProductionEnum(row.Subsystem), Mode: normalizeProductionEnum(row.Mode)})
	}
	return out
}

func restoreExecutionBlockers(plan ClusterRestorePlan, input StartClusterRestoreInput) []ValidationBlocker {
	blockers := append([]ValidationBlocker(nil), plan.Blockers...)
	providedPlanID := strings.TrimSpace(input.RestorePlanID)
	providedFreshnessToken := strings.TrimSpace(input.PlanFreshnessToken)
	if providedPlanID == "" && providedFreshnessToken == "" {
		blockers = append(blockers, ValidationBlocker{Reason: "restore_plan_freshness_required", Detail: "restore execution requires restore_plan_id or plan_freshness_token from a current plan", FieldPath: "restore_plan_id"})
	}
	if providedPlanID != "" && providedPlanID != plan.RestorePlanID {
		blockers = append(blockers, ValidationBlocker{Reason: "restore_plan_stale", Detail: fmt.Sprintf("restore_plan_id %s does not match current plan %s", input.RestorePlanID, plan.RestorePlanID), FieldPath: "restore_plan_id"})
	}
	if providedFreshnessToken != "" && providedFreshnessToken != plan.RestorePlanID {
		blockers = append(blockers, ValidationBlocker{Reason: "restore_plan_stale", Detail: "plan_freshness_token does not match the current restore plan", FieldPath: "plan_freshness_token"})
	}
	if plan.TargetMode != ClusterRestoreTargetModeNewClusterBootstrap {
		blockers = append(blockers, ValidationBlocker{Reason: "restore_target_mode_not_executable", Detail: "only NEW_CLUSTER_BOOTSTRAP restore execution is implemented", FieldPath: "target_mode"})
	}
	if plan.IdentityMode != ClusterRestoreIdentityModePreserveBackupID {
		blockers = append(blockers, ValidationBlocker{Reason: "restore_identity_mode_not_executable", Detail: "only PRESERVE_BACKUP_ID restore execution is implemented", FieldPath: "identity_mode"})
	}
	if len(plan.Target.Nodes) == 0 {
		blockers = append(blockers, ValidationBlocker{Reason: "target_node_mapping_required", Detail: "restore execution requires explicit target node mappings", FieldPath: "target.nodes"})
	}
	for i, node := range plan.Target.Nodes {
		if strings.TrimSpace(node.TargetPVCName) != "" {
			blockers = append(blockers, ValidationBlocker{Reason: "target_pvc_restore_not_implemented", Detail: "Phase 7 execution MVP supports target_data_dir mappings only; PVC helper restore remains a Lab/Kubernetes wrapper", FieldPath: fmt.Sprintf("target.nodes[%d].target_pvc_name", i)})
		}
		if strings.TrimSpace(node.TargetDataDir) == "" {
			blockers = append(blockers, ValidationBlocker{Reason: "target_data_dir_required", Detail: "restore execution requires target_data_dir for every node mapping", FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i)})
		}
	}
	if strings.TrimSpace(plan.Staging.StagingURI) == "" {
		blockers = append(blockers, ValidationBlocker{Reason: "staging_location_required", Detail: "restore execution requires a local staging URI", FieldPath: "staging.staging_uri"})
	} else if _, err := localRestoreStagingPath(plan.Staging.StagingURI); err != nil {
		blockers = append(blockers, ValidationBlocker{Reason: "staging_location_unsupported", Detail: err.Error(), FieldPath: "staging.staging_uri"})
	}
	if !input.Preconditions.CleanTarget.TargetEmpty {
		blockers = append(blockers, ValidationBlocker{Reason: "clean_target_proof_required", Detail: "restore execution requires clean_target.target_empty=true and rechecks target data dirs before apply", FieldPath: "preconditions.clean_target.target_empty"})
	}
	if plan.IdentityMode == ClusterRestoreIdentityModePreserveBackupID && !input.Preconditions.SourceFencing.SourceClusterFenced {
		blockers = append(blockers, ValidationBlocker{Reason: "source_fencing_required", Detail: "PRESERVE_BACKUP_ID restore execution requires source_fencing.source_cluster_fenced=true", FieldPath: "preconditions.source_fencing.source_cluster_fenced"})
	}
	blockers = append(blockers, input.Preconditions.Blockers...)
	blockers = append(blockers, input.Preconditions.CleanTarget.Blockers...)
	blockers = append(blockers, input.Preconditions.SourceFencing.Blockers...)
	return blockers
}

func localRestoreStagingPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("staging URI is required")
	}
	if strings.HasPrefix(raw, "file://") {
		u, err := url.Parse(raw)
		if err != nil || u.Path == "" || u.Host != "" {
			return "", fmt.Errorf("staging URI must be a local file:// path")
		}
		raw = u.Path
	} else if strings.Contains(raw, "://") {
		return "", fmt.Errorf("staging URI must be a local absolute path or file:// URI")
	}
	if !filepath.IsAbs(raw) {
		return "", fmt.Errorf("staging path must be absolute")
	}
	return filepath.Clean(raw), nil
}

func repositoryKeyFromS3URI(repo BackupRepository, raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "s3" || u.Host == "" || strings.Trim(u.Path, "/") == "" {
		return "", fmt.Errorf("artifact URI %q is not a valid s3:// URI", raw)
	}
	if repo.S3.Bucket != "" && u.Host != repo.S3.Bucket {
		return "", fmt.Errorf("artifact URI bucket %q does not match repository bucket %q", u.Host, repo.S3.Bucket)
	}
	return cleanRepositoryObjectKey(u.Path), nil
}

func ensureRestoreDataDirEmpty(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("target data directory is required")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("target data directory %s is not empty", path)
	}
	return nil
}

func fileSHA256Local(ctx context.Context, path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	h := sha256.New()
	size, err := copyRepositoryObjectWithContext(ctx, h, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

func restoreOperationID(plan ClusterRestorePlan, idempotencyKey string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{plan.RestorePlanID, idempotencyKey}, "\x00")))
	return "restore-op-" + hex.EncodeToString(sum[:8])
}

func restorePlanTargetBlockers(manifest clusterbackup.BackupSetManifestV1, target ClusterRestoreTarget) []ValidationBlocker {
	var blockers []ValidationBlocker
	if target.ExpectedNodeCount > 0 && target.ExpectedNodeCount != manifest.Source.Topology.NodeCount {
		blockers = append(blockers, ValidationBlocker{Reason: "target_topology_mismatch", Detail: fmt.Sprintf("target expected_node_count=%d does not match backup node_count=%d", target.ExpectedNodeCount, manifest.Source.Topology.NodeCount), FieldPath: "target.expected_node_count"})
	}
	if len(target.Nodes) > 0 && len(target.Nodes) != manifest.Source.Topology.NodeCount {
		blockers = append(blockers, ValidationBlocker{Reason: "target_node_mapping_incomplete", Detail: fmt.Sprintf("target provides %d node mappings for backup node_count=%d", len(target.Nodes), manifest.Source.Topology.NodeCount), FieldPath: "target.nodes"})
	}
	seen := map[int]bool{}
	for i, node := range target.Nodes {
		if node.Ordinal < 0 || node.Ordinal >= manifest.Source.Topology.NodeCount {
			blockers = append(blockers, ValidationBlocker{Reason: "target_node_mapping_invalid", Detail: fmt.Sprintf("target node mapping ordinal %d is outside backup node range", node.Ordinal), FieldPath: fmt.Sprintf("target.nodes[%d].ordinal", i)})
		}
		if seen[node.Ordinal] {
			blockers = append(blockers, ValidationBlocker{Reason: "target_node_mapping_duplicate", Detail: fmt.Sprintf("target node mapping repeats ordinal %d", node.Ordinal), FieldPath: fmt.Sprintf("target.nodes[%d].ordinal", i)})
		}
		seen[node.Ordinal] = true
		if strings.TrimSpace(node.TargetPVCName) == "" && strings.TrimSpace(node.TargetDataDir) == "" {
			blockers = append(blockers, ValidationBlocker{Reason: "target_node_mapping_destination_required", Detail: "target node mapping requires target_pvc_name or target_data_dir before restore execution", FieldPath: fmt.Sprintf("target.nodes[%d]", i)})
		}
	}
	return blockers
}

func restorePlanRequiredPreconditions(manifest clusterbackup.BackupSetManifestV1, targetMode, identityMode string) ClusterRestorePreconditionEvidence {
	out := ClusterRestorePreconditionEvidence{}
	if targetMode == ClusterRestoreTargetModeNewClusterBootstrap {
		out.CleanTarget.Blockers = append(out.CleanTarget.Blockers, ValidationBlocker{Reason: "clean_target_proof_required", Detail: "NEW_CLUSTER_BOOTSTRAP restore execution requires proof that target data dirs/PVCs are empty", FieldPath: "preconditions.clean_target"})
	}
	if identityMode == ClusterRestoreIdentityModePreserveBackupID || manifest.Compatibility.RequiresSourceFencingForPreservedIdentity {
		out.SourceFencing.Blockers = append(out.SourceFencing.Blockers, ValidationBlocker{Reason: "source_fencing_required", Detail: "preserved-identity restore execution requires source-fencing evidence", FieldPath: "preconditions.source_fencing"})
	}
	return out
}

func normalizeRestoreTarget(in ClusterRestoreTarget) ClusterRestoreTarget {
	in.TargetClusterID = strings.TrimSpace(in.TargetClusterID)
	in.TargetClusterName = strings.TrimSpace(in.TargetClusterName)
	in.Namespace = strings.TrimSpace(in.Namespace)
	in.StatefulSet = strings.TrimSpace(in.StatefulSet)
	for i := range in.Nodes {
		in.Nodes[i].SourcePodName = strings.TrimSpace(in.Nodes[i].SourcePodName)
		in.Nodes[i].SourceNodeID = strings.TrimSpace(in.Nodes[i].SourceNodeID)
		in.Nodes[i].SourceArchiveURI = strings.TrimSpace(in.Nodes[i].SourceArchiveURI)
		in.Nodes[i].TargetPodName = strings.TrimSpace(in.Nodes[i].TargetPodName)
		in.Nodes[i].TargetNodeName = strings.TrimSpace(in.Nodes[i].TargetNodeName)
		in.Nodes[i].TargetPVCName = strings.TrimSpace(in.Nodes[i].TargetPVCName)
		in.Nodes[i].TargetDataDir = strings.TrimSpace(in.Nodes[i].TargetDataDir)
		in.Nodes[i].StagingPath = strings.TrimSpace(in.Nodes[i].StagingPath)
	}
	return in
}

func normalizeRestoreStaging(in ClusterRestoreStagingLocation) ClusterRestoreStagingLocation {
	in.StagingURI = strings.TrimSpace(in.StagingURI)
	in.StorageClass = strings.TrimSpace(in.StorageClass)
	return in
}

func restorePlanID(repositoryID, backupSetID, targetMode, identityMode, authPolicy string, target ClusterRestoreTarget, staging ClusterRestoreStagingLocation, result ClusterBackupSetValidationResult) string {
	markerChecksum := ""
	if result.ProductionMarker != nil {
		markerChecksum = result.ProductionMarker.ManifestChecksumSHA256
	}
	targetHash := stableRestorePlanComponent(target)
	stagingHash := stableRestorePlanComponent(staging)
	sum := sha256.Sum256([]byte(strings.Join([]string{repositoryID, backupSetID, targetMode, identityMode, authPolicy, markerChecksum, targetHash, stagingHash}, "\x00")))
	return "restore-plan-" + hex.EncodeToString(sum[:8])
}

func stableRestorePlanComponent(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func normalizeRestoreEnum(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func normalizeProductionEnum(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func containsRestoreEnum(values []string, want string) bool {
	want = normalizeRestoreEnum(want)
	for _, value := range values {
		if normalizeRestoreEnum(value) == want {
			return true
		}
	}
	return false
}
