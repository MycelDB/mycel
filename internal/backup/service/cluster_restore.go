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

type ClusterRestorePVCHelper interface {
	RestoreRepositoryBackupNodeToPVC(context.Context, ClusterRestorePVCHelperRequest) (ClusterRestorePVCHelperResult, error)
}

type ClusterRestorePVCTargetController interface {
	PrepareRepositoryRestoreTarget(context.Context, ClusterRestorePVCTargetControllerRequest) (ClusterRestorePVCTargetControllerResult, error)
}

type ClusterRestorePVCTargetControllerRequest struct {
	RestoreOperationID string
	Target             ClusterRestoreTarget
	Nodes              []ClusterRestoreNodeMapping
}

type ClusterRestorePVCTargetControllerResult struct {
	CleanTarget   ClusterRestoreCleanTargetProof
	SourceFencing ClusterRestoreSourceFencingEvidence
	CleanupTasks  []CleanupTask
	NodeEvidence  map[int]map[string]string
}

type ClusterRestorePVCHelperRequest struct {
	RestoreOperationID string
	Repository         BackupRepository
	BackupSetID        string
	BackupSetPrefix    string
	Manifest           clusterbackup.BackupSetManifestV1
	CompleteMarker     clusterbackup.CompleteMarkerV1
	Artifact           clusterbackup.BackupSetNodeArtifact
	Target             ClusterRestoreTarget
	Node               ClusterRestoreNodeMapping
	ArchiveFormat      backupcore.ArchiveFormat
	store              backupRepositoryObjectStore
}

type ClusterRestorePVCHelperResult struct {
	TargetPVCName  string
	TargetDataDir  string
	Verified       bool
	BytesCompleted int64
	CleanupTasks   []CleanupTask
	Evidence       map[string]string
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
	IdempotencyKey     string
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
	Evidence         map[string]string
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

func (m *Module) restoreExecutionBlockers(plan ClusterRestorePlan, input StartClusterRestoreInput) []ValidationBlocker {
	return restoreExecutionBlockersWithPVCHelper(plan, input, m != nil && m.clusterRestorePVCHelper != nil, m != nil && m.clusterRestorePVCTargetController() != nil, m == nil || m.clusterRestorePVCTargetControllerCanFence())
}

func restoreExecutionBlockers(plan ClusterRestorePlan, input StartClusterRestoreInput) []ValidationBlocker {
	return restoreExecutionBlockersWithPVCHelper(plan, input, false, false, false)
}

func restoreExecutionBlockersWithPVCHelper(plan ClusterRestorePlan, input StartClusterRestoreInput, pvcHelperAvailable bool, pvcControllerAvailable bool, pvcControllerCanFence bool) []ValidationBlocker {
	blockers := append([]ValidationBlocker(nil), plan.Blockers...)
	controllerWillPrepare := pvcControllerAvailable && restoreMappingsUsePVCHelper(plan.Target.Nodes)
	controllerWillFence := controllerWillPrepare && pvcControllerCanFence && strings.TrimSpace(plan.Target.StatefulSet) != ""
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
		dataDir := strings.TrimSpace(node.TargetDataDir)
		pvcName := strings.TrimSpace(node.TargetPVCName)
		if dataDir == "" {
			if pvcName != "" {
				if !pvcHelperAvailable {
					blockers = append(blockers, ValidationBlocker{Reason: "target_pvc_restore_requires_mounted_data_dir", Detail: "PVC restore execution requires either target_data_dir for a mounted PVC or a configured daemon PVC restore helper/controller", FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i)})
				}
			} else {
				blockers = append(blockers, ValidationBlocker{Reason: "target_data_dir_required", Detail: "restore execution requires target_data_dir for every node mapping", FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i)})
			}
			continue
		}
		if !filepath.IsAbs(dataDir) {
			blockers = append(blockers, ValidationBlocker{Reason: "target_data_dir_not_absolute", Detail: "restore execution requires an absolute target_data_dir path", FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i)})
		} else if filepath.Clean(dataDir) == string(filepath.Separator) {
			blockers = append(blockers, ValidationBlocker{Reason: "target_data_dir_unsafe", Detail: "restore execution refuses to target the filesystem root", FieldPath: fmt.Sprintf("target.nodes[%d].target_data_dir", i)})
		}
	}
	if restorePlanRequiresLocalStaging(plan.Target.Nodes) {
		if strings.TrimSpace(plan.Staging.StagingURI) == "" {
			blockers = append(blockers, ValidationBlocker{Reason: "staging_location_required", Detail: "restore execution requires a local staging URI for mappings applied by the daemon", FieldPath: "staging.staging_uri"})
		} else if _, err := localRestoreStagingPath(plan.Staging.StagingURI); err != nil {
			blockers = append(blockers, ValidationBlocker{Reason: "staging_location_unsupported", Detail: err.Error(), FieldPath: "staging.staging_uri"})
		}
	}
	if !input.Preconditions.CleanTarget.TargetEmpty && !controllerWillPrepare {
		blockers = append(blockers, ValidationBlocker{Reason: "clean_target_proof_required", Detail: "restore execution requires clean_target.target_empty=true and rechecks target data dirs before apply", FieldPath: "preconditions.clean_target.target_empty"})
	}
	if !controllerWillPrepare {
		blockers = append(blockers, restoreCleanTargetProofMappingBlockers(plan.Target, input.Preconditions.CleanTarget)...)
	}
	if plan.IdentityMode == ClusterRestoreIdentityModePreserveBackupID && !input.Preconditions.SourceFencing.SourceClusterFenced {
		if controllerWillPrepare && strings.TrimSpace(plan.Target.StatefulSet) == "" {
			blockers = append(blockers, ValidationBlocker{Reason: "source_fencing_requires_statefulset", Detail: "controller-managed preserved-identity restore requires target.stateful_set so the daemon can scale the source/target StatefulSet to zero before PVC replacement", FieldPath: "target.stateful_set"})
		} else if !controllerWillFence {
			blockers = append(blockers, ValidationBlocker{Reason: "source_fencing_required", Detail: "PRESERVE_BACKUP_ID restore execution requires source_fencing.source_cluster_fenced=true or an enabled Kubernetes restore controller that can scale target.stateful_set to zero", FieldPath: "preconditions.source_fencing.source_cluster_fenced"})
		}
	}
	blockers = append(blockers, input.Preconditions.Blockers...)
	blockers = append(blockers, input.Preconditions.CleanTarget.Blockers...)
	blockers = append(blockers, input.Preconditions.SourceFencing.Blockers...)
	return blockers
}

func restoreCleanTargetProofMappingBlockers(target ClusterRestoreTarget, proof ClusterRestoreCleanTargetProof) []ValidationBlocker {
	if len(proof.CheckedNodes) == 0 {
		return nil
	}
	checkedByOrdinal := map[int]ClusterRestoreNodeMapping{}
	for _, checked := range proof.CheckedNodes {
		checkedByOrdinal[checked.Ordinal] = checked
	}
	var blockers []ValidationBlocker
	for i, node := range target.Nodes {
		checked, ok := checkedByOrdinal[node.Ordinal]
		if !ok {
			blockers = append(blockers, ValidationBlocker{Reason: "clean_target_proof_incomplete", Detail: fmt.Sprintf("clean target proof did not check target ordinal %d", node.Ordinal), FieldPath: fmt.Sprintf("preconditions.clean_target.checked_nodes[%d]", i)})
			continue
		}
		if strings.TrimSpace(node.TargetPVCName) != "" && strings.TrimSpace(checked.TargetPVCName) != "" && strings.TrimSpace(node.TargetPVCName) != strings.TrimSpace(checked.TargetPVCName) {
			blockers = append(blockers, ValidationBlocker{Reason: "clean_target_proof_mismatch", Detail: fmt.Sprintf("clean target proof checked PVC %q for ordinal %d but restore targets PVC %q", checked.TargetPVCName, node.Ordinal, node.TargetPVCName), FieldPath: fmt.Sprintf("preconditions.clean_target.checked_nodes[%d].target_pvc_name", i)})
		}
		if strings.TrimSpace(node.TargetDataDir) != "" && strings.TrimSpace(checked.TargetDataDir) != "" && filepath.Clean(strings.TrimSpace(node.TargetDataDir)) != filepath.Clean(strings.TrimSpace(checked.TargetDataDir)) {
			blockers = append(blockers, ValidationBlocker{Reason: "clean_target_proof_mismatch", Detail: fmt.Sprintf("clean target proof checked data dir %q for ordinal %d but restore targets %q", checked.TargetDataDir, node.Ordinal, node.TargetDataDir), FieldPath: fmt.Sprintf("preconditions.clean_target.checked_nodes[%d].target_data_dir", i)})
		}
	}
	return blockers
}

func (m *Module) clusterRestorePVCTargetController() ClusterRestorePVCTargetController {
	if m == nil || m.clusterRestorePVCHelper == nil {
		return nil
	}
	controller, ok := m.clusterRestorePVCHelper.(ClusterRestorePVCTargetController)
	if !ok {
		return nil
	}
	return controller
}

func (m *Module) clusterRestorePVCTargetControllerCanFence() bool {
	if m == nil || m.clusterRestorePVCTargetController() == nil {
		return false
	}
	if helper, ok := m.clusterRestorePVCHelper.(*KubernetesPVCRestoreHelper); ok {
		return helper.config.ScaleDownStatefulSet
	}
	return true
}

func restoreNodeUsesPVCHelper(node ClusterRestoreNodeMapping) bool {
	return strings.TrimSpace(node.TargetPVCName) != "" && strings.TrimSpace(node.TargetDataDir) == ""
}

func restoreMappingsUsePVCHelper(nodes []ClusterRestoreNodeMapping) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, node := range nodes {
		if !restoreNodeUsesPVCHelper(node) {
			return false
		}
	}
	return true
}

func mergeRestoreNodeEvidence(left, right map[string]string) map[string]string {
	if len(left) == 0 {
		return copyStringMapLocal(right)
	}
	out := copyStringMapLocal(left)
	for key, value := range right {
		out[key] = value
	}
	return out
}

func restorePlanRequiresLocalStaging(nodes []ClusterRestoreNodeMapping) bool {
	if len(nodes) == 0 {
		return true
	}
	for _, node := range nodes {
		if !restoreNodeUsesPVCHelper(node) {
			return true
		}
	}
	return false
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
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(filepath.Dir(path), 0o700)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("target data directory %s is not a directory", path)
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

func removeExistingEmptyRestoreDataDir(path string) error {
	path = strings.TrimSpace(path)
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("target data directory %s is not a directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("target data directory %s is not empty", path)
	}
	return os.Remove(path)
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

func restoreOperationID(plan ClusterRestorePlan, idempotencyKey string, createdAt time.Time) string {
	parts := []string{plan.RestorePlanID, strings.TrimSpace(idempotencyKey)}
	if strings.TrimSpace(idempotencyKey) == "" {
		parts = append(parts, createdAt.UTC().Format(time.RFC3339Nano))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
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
