package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	plan := ClusterRestorePlan{RestorePlanID: restorePlanID(repositoryID, backupSetID, targetMode, identityMode, authPolicy, result), RepositoryID: repositoryID, BackupSetID: backupSetID, TargetMode: targetMode, IdentityMode: identityMode, AuthSessionPolicy: authPolicy, Validation: result.ProductionReport, Compatibility: manifest.Compatibility, DerivedActions: restoreDerivedActions(*manifest), PlannedAt: plannedAt, ProductionManifest: manifest, ProductionMarker: result.ProductionMarker}
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

func restorePlanID(repositoryID, backupSetID, targetMode, identityMode, authPolicy string, result ClusterBackupSetValidationResult) string {
	markerChecksum := ""
	if result.ProductionMarker != nil {
		markerChecksum = result.ProductionMarker.ManifestChecksumSHA256
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{repositoryID, backupSetID, targetMode, identityMode, authPolicy, markerChecksum}, "\x00")))
	return "restore-plan-" + hex.EncodeToString(sum[:8])
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
