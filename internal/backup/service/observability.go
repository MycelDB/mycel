package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
)

const (
	BackupAlertSeverityInfo     = "info"
	BackupAlertSeverityWarning  = "warning"
	BackupAlertSeverityCritical = "critical"
)

type BackupObservabilityOptions struct {
	RepositoryID          string
	IncludeRetentionProbe bool
	StaleOperationAfter   time.Duration
	BackupFreshnessAfter  time.Duration
	IncompleteBackupAfter time.Duration
	Now                   time.Time
}

type BackupObservabilitySnapshot struct {
	ObservedAt   time.Time
	RepositoryID string
	Metrics      []BackupMetric
	Alerts       []BackupAlertCondition
	SummaryState string
}

type BackupMetric struct {
	Name        string
	Description string
	Unit        string
	Value       float64
	Labels      map[string]string
}

type BackupAlertCondition struct {
	Name       string
	Severity   string
	Firing     bool
	Reason     string
	Expression string
	Labels     map[string]string
	Evidence   []string
}

type BackupObservabilityManager interface {
	GetBackupObservability(context.Context, BackupObservabilityOptions) (BackupObservabilitySnapshot, error)
}

func (m *Module) GetBackupObservability(ctx context.Context, options BackupObservabilityOptions) (BackupObservabilitySnapshot, error) {
	now := options.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	policy := m.Policy()
	repositoryID := strings.TrimSpace(options.RepositoryID)
	if repositoryID == "" {
		repositoryID = strings.TrimSpace(policy.DefaultRepositoryID)
	}
	thresholds := backupObservabilityThresholds{
		staleOperationAfter:   options.StaleOperationAfter,
		backupFreshnessAfter:  options.BackupFreshnessAfter,
		incompleteBackupAfter: options.IncompleteBackupAfter,
	}
	if thresholds.staleOperationAfter <= 0 {
		thresholds.staleOperationAfter = policy.BackupTimeout
	}
	if thresholds.staleOperationAfter <= 0 {
		thresholds.staleOperationAfter = time.Hour
	}
	if thresholds.backupFreshnessAfter <= 0 && policy.Interval > 0 {
		thresholds.backupFreshnessAfter = 2 * policy.Interval
	}
	if thresholds.backupFreshnessAfter <= 0 {
		thresholds.backupFreshnessAfter = 24 * time.Hour
	}
	if thresholds.incompleteBackupAfter <= 0 {
		retention := backupcore.EffectivePolicy(m.dataDir, policy).Retention
		thresholds.incompleteBackupAfter = retention.DeleteIncompleteAfter
	}
	if thresholds.incompleteBackupAfter <= 0 {
		thresholds.incompleteBackupAfter = backupcore.DefaultDeleteIncompleteAfter
	}

	snapshot := BackupObservabilitySnapshot{ObservedAt: now, RepositoryID: repositoryID, SummaryState: "healthy"}
	backups := m.ListClusterBackups()
	restores := m.ListClusterRestores()
	observeClusterBackups(&snapshot, backups, now, thresholds)
	observeClusterRestores(&snapshot, restores, now, thresholds)
	if options.IncludeRetentionProbe {
		if repositoryID == "" {
			addBackupAlert(&snapshot, BackupAlertCondition{Name: "MycelBackupRetentionProbeUnavailable", Severity: BackupAlertSeverityWarning, Firing: true, Reason: "repository_id_required", Expression: "mycel_backup_repository_retention_probe_failed > 0", Evidence: []string{"no default repository is configured"}})
			addBackupMetric(&snapshot, "mycel_backup_repository_retention_probe_failed", "Repository retention probe failed", "count", 1, map[string]string{"reason": "repository_id_required"})
		} else {
			plan, err := m.PlanRepositoryRetention(ctx, RepositoryRetentionPlanOptions{RepositoryID: repositoryID, Now: now})
			if err != nil {
				addBackupAlert(&snapshot, BackupAlertCondition{Name: "MycelBackupRetentionProbeUnavailable", Severity: BackupAlertSeverityWarning, Firing: true, Reason: "retention_probe_failed", Expression: "mycel_backup_repository_retention_probe_failed > 0", Labels: map[string]string{"repository_id": repositoryID}, Evidence: []string{"retention probe failed; validate repository access and daemon diagnostics"}})
				addBackupMetric(&snapshot, "mycel_backup_repository_retention_probe_failed", "Repository retention probe failed", "count", 1, map[string]string{"repository_id": repositoryID, "reason": "probe_failed"})
			} else {
				observeRepositoryRetentionPlan(&snapshot, plan, now, thresholds)
			}
		}
	}
	if observation, ok := m.repositoryRetentionObservationSnapshot(repositoryID); ok {
		observeRepositoryRetentionObservation(&snapshot, observation)
	}
	finalizeBackupObservabilitySnapshot(&snapshot)
	return snapshot, nil
}

type backupObservabilityThresholds struct {
	staleOperationAfter   time.Duration
	backupFreshnessAfter  time.Duration
	incompleteBackupAfter time.Duration
}

func observeClusterBackups(snapshot *BackupObservabilitySnapshot, backups []ClusterBackupRunStatus, now time.Time, thresholds backupObservabilityThresholds) {
	counts := map[string]float64{}
	var latest *ClusterBackupRunStatus
	var latestSuccess *ClusterBackupRunStatus
	activeStale := []string{}
	blockerEvidence := []string{}
	for i := range backups {
		st := backups[i]
		state := normalizedBackupPhase(st.Phase)
		phase := strings.TrimSpace(st.Phase)
		if phase == "" {
			phase = "unknown"
		}
		countKey := state + "\x00" + phase + "\x00" + st.RepositoryID
		counts[countKey]++
		if latest == nil || st.CreatedAt.After(latest.CreatedAt) {
			latest = &backups[i]
		}
		if state == "succeeded" && !st.CompletedAt.IsZero() && (latestSuccess == nil || st.CompletedAt.After(latestSuccess.CompletedAt)) {
			latestSuccess = &backups[i]
		}
		labels := backupOperationLabels(st, state, phase)
		bytes := clusterBackupStatusBytes(st)
		addBackupMetric(snapshot, "mycel_backup_cluster_operation_bytes", "Bytes captured by a cluster backup operation", "bytes", float64(bytes), labels)
		addBackupMetric(snapshot, "mycel_backup_cluster_operation_nodes", "Expected or completed nodes in a cluster backup operation", "count", float64(len(st.Expected)), mergeStringLabels(labels, map[string]string{"kind": "expected"}))
		addBackupMetric(snapshot, "mycel_backup_cluster_operation_nodes", "Expected or completed nodes in a cluster backup operation", "count", float64(len(st.Nodes)), mergeStringLabels(labels, map[string]string{"kind": "completed"}))
		if backupStatusTerminal(state) {
			duration := operationDuration(st.CreatedAt, firstNonZeroTime(st.CompletedAt, st.UpdatedAt))
			addBackupMetric(snapshot, "mycel_backup_cluster_operation_duration_seconds", "Cluster backup operation duration", "seconds", duration.Seconds(), labels)
		} else {
			age := now.Sub(st.CreatedAt)
			if age < 0 {
				age = 0
			}
			addBackupMetric(snapshot, "mycel_backup_cluster_active_operation_age_seconds", "Age of an active cluster backup operation", "seconds", age.Seconds(), labels)
			if age > thresholds.staleOperationAfter {
				activeStale = append(activeStale, fmt.Sprintf("backup_set=%s phase=%s age_seconds=%.0f", st.BackupSetID, phase, age.Seconds()))
			}
		}
		if len(st.Blockers) > 0 {
			byReason := map[string]int{}
			for _, blocker := range st.Blockers {
				reason := strings.TrimSpace(blocker.Reason)
				if reason == "" {
					reason = "unspecified"
				}
				byReason[reason]++
			}
			for reason, count := range byReason {
				addBackupMetric(snapshot, "mycel_backup_cluster_blocker_count", "Cluster backup blockers by reason", "count", float64(count), mergeStringLabels(labels, map[string]string{"reason": reason}))
				blockerEvidence = append(blockerEvidence, fmt.Sprintf("backup_set=%s reason=%s count=%d", st.BackupSetID, reason, count))
			}
		}
	}
	for key, value := range counts {
		parts := strings.Split(key, "\x00")
		labels := map[string]string{"state": parts[0], "phase": parts[1]}
		if parts[2] != "" {
			labels["repository_id"] = parts[2]
		}
		addBackupMetric(snapshot, "mycel_backup_cluster_operation_count", "Cluster backup operations by state and phase", "count", value, labels)
	}
	if latest != nil {
		state := normalizedBackupPhase(latest.Phase)
		phase := firstNonEmptyObservability(strings.TrimSpace(latest.Phase), "unknown")
		addBackupMetric(snapshot, "mycel_backup_cluster_latest_operation_state", "Latest cluster backup operation state", "count", 1, backupOperationLabels(*latest, state, phase))
		failed := backupStatusFailed(state)
		addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterBackupFailed", Severity: BackupAlertSeverityCritical, Firing: failed, Reason: firingReason(failed, "latest_backup_failed"), Expression: `mycel_backup_cluster_latest_operation_state{state=~"failed|aborted"} > 0`, Labels: backupOperationLabels(*latest, state, phase), Evidence: []string{fmt.Sprintf("latest_backup_set=%s state=%s phase=%s", latest.BackupSetID, state, phase)}})
	}
	if latestSuccess != nil {
		age := now.Sub(latestSuccess.CompletedAt)
		if age < 0 {
			age = 0
		}
		addBackupMetric(snapshot, "mycel_backup_cluster_latest_success_age_seconds", "Age of the latest successful cluster backup", "seconds", age.Seconds(), map[string]string{"backup_set_id": latestSuccess.BackupSetID, "repository_id": latestSuccess.RepositoryID})
		addBackupMetric(snapshot, "mycel_backup_cluster_latest_success_timestamp_seconds", "Completion timestamp of the latest successful cluster backup", "seconds", float64(latestSuccess.CompletedAt.Unix()), map[string]string{"backup_set_id": latestSuccess.BackupSetID, "repository_id": latestSuccess.RepositoryID})
		stale := age > thresholds.backupFreshnessAfter
		addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelBackupFreshnessStale", Severity: BackupAlertSeverityWarning, Firing: stale, Reason: firingReason(stale, "latest_success_stale"), Expression: fmt.Sprintf("mycel_backup_cluster_latest_success_age_seconds > %.0f", thresholds.backupFreshnessAfter.Seconds()), Labels: map[string]string{"backup_set_id": latestSuccess.BackupSetID, "repository_id": latestSuccess.RepositoryID}, Evidence: []string{fmt.Sprintf("latest_success_age_seconds=%.0f threshold_seconds=%.0f", age.Seconds(), thresholds.backupFreshnessAfter.Seconds())}})
	} else {
		addBackupMetric(snapshot, "mycel_backup_cluster_latest_success_age_seconds", "Age of the latest successful cluster backup", "seconds", -1, map[string]string{"repository_id": snapshot.RepositoryID})
		addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelBackupFreshnessStale", Severity: BackupAlertSeverityWarning, Firing: true, Reason: "no_successful_cluster_backup", Expression: fmt.Sprintf("mycel_backup_cluster_latest_success_age_seconds < 0 or mycel_backup_cluster_latest_success_age_seconds > %.0f", thresholds.backupFreshnessAfter.Seconds()), Labels: map[string]string{"repository_id": snapshot.RepositoryID}, Evidence: []string{"no successful cluster backup operation is present in local history"}})
	}
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterBackupOperationStale", Severity: BackupAlertSeverityWarning, Firing: len(activeStale) > 0, Reason: firingReason(len(activeStale) > 0, "active_backup_operation_stale"), Expression: fmt.Sprintf("mycel_backup_cluster_active_operation_age_seconds > %.0f", thresholds.staleOperationAfter.Seconds()), Evidence: activeStale})
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterBackupBlockers", Severity: BackupAlertSeverityWarning, Firing: len(blockerEvidence) > 0, Reason: firingReason(len(blockerEvidence) > 0, "backup_blockers_present"), Expression: "mycel_backup_cluster_blocker_count > 0", Evidence: blockerEvidence})
}

func observeClusterRestores(snapshot *BackupObservabilitySnapshot, restores []ClusterRestoreOperation, now time.Time, thresholds backupObservabilityThresholds) {
	counts := map[string]float64{}
	var latest *ClusterRestoreOperation
	activeStale := []string{}
	blockerEvidence := []string{}
	cleanupEvidence := []string{}
	for i := range restores {
		op := restores[i]
		state := normalizedRestoreState(op.State)
		phase := firstNonEmptyObservability(strings.TrimSpace(op.Progress.CurrentPhase), state)
		countKey := state + "\x00" + phase + "\x00" + op.RepositoryID
		counts[countKey]++
		if latest == nil || op.CreatedAt.After(latest.CreatedAt) {
			latest = &restores[i]
		}
		labels := restoreOperationLabels(op, state, phase)
		addBackupMetric(snapshot, "mycel_restore_cluster_operation_bytes", "Bytes completed by a cluster restore operation", "bytes", float64(op.Progress.BytesCompleted), labels)
		addBackupMetric(snapshot, "mycel_restore_cluster_operation_nodes", "Expected, staged, applied, or verified nodes in a cluster restore operation", "count", float64(op.Progress.TotalNodes), mergeStringLabels(labels, map[string]string{"kind": "expected"}))
		addBackupMetric(snapshot, "mycel_restore_cluster_operation_nodes", "Expected, staged, applied, or verified nodes in a cluster restore operation", "count", float64(op.Progress.StagedNodes), mergeStringLabels(labels, map[string]string{"kind": "staged"}))
		addBackupMetric(snapshot, "mycel_restore_cluster_operation_nodes", "Expected, staged, applied, or verified nodes in a cluster restore operation", "count", float64(op.Progress.AppliedNodes), mergeStringLabels(labels, map[string]string{"kind": "applied"}))
		addBackupMetric(snapshot, "mycel_restore_cluster_operation_nodes", "Expected, staged, applied, or verified nodes in a cluster restore operation", "count", float64(op.Progress.VerifiedNodes), mergeStringLabels(labels, map[string]string{"kind": "verified"}))
		if clusterRestoreStateTerminal(op.State) {
			duration := operationDuration(op.CreatedAt, firstNonZeroTime(op.CompletedAt, op.UpdatedAt))
			addBackupMetric(snapshot, "mycel_restore_cluster_operation_duration_seconds", "Cluster restore operation duration", "seconds", duration.Seconds(), labels)
		} else {
			age := now.Sub(op.CreatedAt)
			if age < 0 {
				age = 0
			}
			addBackupMetric(snapshot, "mycel_restore_cluster_active_operation_age_seconds", "Age of an active cluster restore operation", "seconds", age.Seconds(), labels)
			if age > thresholds.staleOperationAfter {
				activeStale = append(activeStale, fmt.Sprintf("restore_operation=%s state=%s age_seconds=%.0f", op.RestoreOperationID, state, age.Seconds()))
			}
		}
		if len(op.Blockers) > 0 {
			byReason := map[string]int{}
			for _, blocker := range op.Blockers {
				reason := strings.TrimSpace(blocker.Reason)
				if reason == "" {
					reason = "unspecified"
				}
				byReason[reason]++
			}
			for reason, count := range byReason {
				addBackupMetric(snapshot, "mycel_restore_cluster_blocker_count", "Cluster restore blockers by reason", "count", float64(count), mergeStringLabels(labels, map[string]string{"reason": reason}))
				blockerEvidence = append(blockerEvidence, fmt.Sprintf("restore_operation=%s reason=%s count=%d", op.RestoreOperationID, reason, count))
			}
		}
		if len(op.CleanupTasks) > 0 {
			byStateKind := map[string]int{}
			for _, task := range op.CleanupTasks {
				stateKind := firstNonEmptyObservability(strings.TrimSpace(task.State), "unknown") + "\x00" + firstNonEmptyObservability(strings.TrimSpace(task.ResourceKind), "unknown")
				byStateKind[stateKind]++
			}
			for key, count := range byStateKind {
				parts := strings.Split(key, "\x00")
				addBackupMetric(snapshot, "mycel_restore_cluster_cleanup_task_count", "Cluster restore cleanup tasks by state and kind", "count", float64(count), mergeStringLabels(labels, map[string]string{"cleanup_state": parts[0], "resource_kind": parts[1]}))
				if parts[0] != "completed" && parts[0] != "succeeded" {
					cleanupEvidence = append(cleanupEvidence, fmt.Sprintf("restore_operation=%s cleanup_state=%s resource_kind=%s count=%d", op.RestoreOperationID, parts[0], parts[1], count))
				}
			}
		}
	}
	for key, value := range counts {
		parts := strings.Split(key, "\x00")
		labels := map[string]string{"state": parts[0], "phase": parts[1]}
		if parts[2] != "" {
			labels["repository_id"] = parts[2]
		}
		addBackupMetric(snapshot, "mycel_restore_cluster_operation_count", "Cluster restore operations by state and phase", "count", value, labels)
	}
	if latest != nil {
		state := normalizedRestoreState(latest.State)
		phase := firstNonEmptyObservability(strings.TrimSpace(latest.Progress.CurrentPhase), state)
		labels := restoreOperationLabels(*latest, state, phase)
		addBackupMetric(snapshot, "mycel_restore_cluster_latest_operation_state", "Latest cluster restore operation state", "count", 1, labels)
		failed := restoreStatusFailed(state)
		addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterRestoreFailed", Severity: BackupAlertSeverityCritical, Firing: failed, Reason: firingReason(failed, "latest_restore_failed"), Expression: `mycel_restore_cluster_latest_operation_state{state=~"failed|failed_requires_operator"} > 0`, Labels: labels, Evidence: []string{fmt.Sprintf("latest_restore_operation=%s state=%s", latest.RestoreOperationID, state)}})
	} else {
		addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterRestoreFailed", Severity: BackupAlertSeverityCritical, Firing: false, Reason: "ok", Expression: `mycel_restore_cluster_latest_operation_state{state=~"failed|failed_requires_operator"} > 0`})
	}
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterRestoreOperationStale", Severity: BackupAlertSeverityWarning, Firing: len(activeStale) > 0, Reason: firingReason(len(activeStale) > 0, "active_restore_operation_stale"), Expression: fmt.Sprintf("mycel_restore_cluster_active_operation_age_seconds > %.0f", thresholds.staleOperationAfter.Seconds()), Evidence: activeStale})
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterRestoreBlockers", Severity: BackupAlertSeverityCritical, Firing: len(blockerEvidence) > 0, Reason: firingReason(len(blockerEvidence) > 0, "restore_blockers_present"), Expression: "mycel_restore_cluster_blocker_count > 0", Evidence: blockerEvidence})
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelClusterRestoreCleanupIncomplete", Severity: BackupAlertSeverityWarning, Firing: len(cleanupEvidence) > 0, Reason: firingReason(len(cleanupEvidence) > 0, "restore_cleanup_incomplete"), Expression: `mycel_restore_cluster_cleanup_task_count{cleanup_state!~"completed|succeeded"} > 0`, Evidence: cleanupEvidence})
}

func observeRepositoryRetentionObservation(snapshot *BackupObservabilitySnapshot, observation repositoryRetentionObservation) {
	labels := map[string]string{"repository_id": observation.RepositoryID}
	failedValue := 0.0
	if observation.Failed {
		failedValue = 1
	}
	addBackupMetric(snapshot, "mycel_backup_repository_last_retention_apply_timestamp_seconds", "Timestamp of the latest repository retention apply attempt", "seconds", float64(observation.ObservedAt.Unix()), labels)
	addBackupMetric(snapshot, "mycel_backup_repository_last_retention_apply_failed", "Whether the latest repository retention apply attempt failed before producing an actionable plan", "count", failedValue, labels)
	addBackupMetric(snapshot, "mycel_backup_repository_last_retention_apply_warning_count", "Warning count from the latest repository retention apply attempt", "count", float64(observation.WarningCount), labels)
	addBackupMetric(snapshot, "mycel_backup_repository_last_retention_apply_error_count", "Action error count from the latest repository retention apply attempt", "count", float64(observation.ActionErrors), labels)
	addBackupMetric(snapshot, "mycel_backup_repository_last_retention_apply_blocked_count", "Blocked or warning action count from the latest repository retention apply attempt", "count", float64(observation.BlockedActions), labels)
	addBackupMetric(snapshot, "mycel_backup_repository_last_retention_apply_deleted_objects", "Objects deleted by the latest repository retention apply attempt", "count", float64(observation.DeletedObjects), labels)
	failed := observation.Failed || observation.ActionErrors > 0
	evidence := []string{}
	if failed {
		evidence = append(evidence, fmt.Sprintf("repository=%s action_errors=%d warnings=%d", observation.RepositoryID, observation.ActionErrors, observation.WarningCount))
	}
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelRepositoryRetentionApplyFailed", Severity: BackupAlertSeverityWarning, Firing: failed, Reason: firingReason(failed, "repository_retention_apply_failed"), Expression: "mycel_backup_repository_last_retention_apply_failed > 0 or mycel_backup_repository_last_retention_apply_error_count > 0", Labels: labels, Evidence: evidence})
}

func observeRepositoryRetentionPlan(snapshot *BackupObservabilitySnapshot, plan RepositoryRetentionPlan, now time.Time, thresholds backupObservabilityThresholds) {
	actionsByNameReason := map[string]int{}
	incompleteEvidence := []string{}
	blockedEvidence := []string{}
	errorEvidence := []string{}
	for _, action := range plan.Actions {
		name := firstNonEmptyObservability(strings.TrimSpace(action.Action), "unknown")
		reason := firstNonEmptyObservability(strings.TrimSpace(action.Reason), "unspecified")
		actionsByNameReason[name+"\x00"+reason]++
		if name == RetentionActionDeleteIncomplete || strings.HasPrefix(reason, "incomplete_") {
			age := time.Duration(0)
			if !action.CreatedAt.IsZero() {
				age = now.Sub(action.CreatedAt)
			}
			if action.CreatedAt.IsZero() || age >= thresholds.incompleteBackupAfter {
				incompleteEvidence = append(incompleteEvidence, fmt.Sprintf("backup_set=%s action=%s reason=%s", action.BackupSetID, name, reason))
			}
		}
		if action.Blocked || name == RetentionActionBlocked {
			blockedEvidence = append(blockedEvidence, fmt.Sprintf("backup_set=%s reason=%s", action.BackupSetID, reason))
		}
		if strings.TrimSpace(action.Error) != "" {
			errorEvidence = append(errorEvidence, fmt.Sprintf("backup_set=%s action=%s reason=%s", action.BackupSetID, name, reason))
		}
	}
	for key, count := range actionsByNameReason {
		parts := strings.Split(key, "\x00")
		addBackupMetric(snapshot, "mycel_backup_repository_retention_action_count", "Repository retention dry-run actions by action and reason", "count", float64(count), map[string]string{"repository_id": plan.RepositoryID, "action": parts[0], "reason": parts[1]})
	}
	addBackupMetric(snapshot, "mycel_backup_repository_retention_warning_count", "Repository retention dry-run warning count", "count", float64(len(plan.Warnings)), map[string]string{"repository_id": plan.RepositoryID})
	addBackupMetric(snapshot, "mycel_backup_repository_incomplete_backup_count", "Repository backup prefixes that retention identifies as incomplete and actionable", "count", float64(len(incompleteEvidence)), map[string]string{"repository_id": plan.RepositoryID})
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelRepositoryIncompleteBackups", Severity: BackupAlertSeverityWarning, Firing: len(incompleteEvidence) > 0, Reason: firingReason(len(incompleteEvidence) > 0, "incomplete_repository_backups"), Expression: "mycel_backup_repository_incomplete_backup_count > 0", Labels: map[string]string{"repository_id": plan.RepositoryID}, Evidence: incompleteEvidence})
	retentionWarningEvidence := append([]string{}, errorEvidence...)
	if len(plan.Warnings) > 0 {
		retentionWarningEvidence = append(retentionWarningEvidence, fmt.Sprintf("warning_count=%d", len(plan.Warnings)))
	}
	retentionWarningEvidence = append(retentionWarningEvidence, blockedEvidence...)
	addBackupAlert(snapshot, BackupAlertCondition{Name: "MycelRepositoryRetentionWarnings", Severity: BackupAlertSeverityWarning, Firing: len(retentionWarningEvidence) > 0, Reason: firingReason(len(retentionWarningEvidence) > 0, "repository_retention_warnings"), Expression: "mycel_backup_repository_retention_warning_count > 0 or mycel_backup_repository_retention_action_count{action=~\"blocked|warn\"} > 0", Labels: map[string]string{"repository_id": plan.RepositoryID}, Evidence: retentionWarningEvidence})
}

func addBackupMetric(snapshot *BackupObservabilitySnapshot, name, description, unit string, value float64, labels map[string]string) {
	if snapshot == nil {
		return
	}
	snapshot.Metrics = append(snapshot.Metrics, BackupMetric{Name: name, Description: description, Unit: unit, Value: value, Labels: cleanMetricLabels(labels)})
}

func addBackupAlert(snapshot *BackupObservabilitySnapshot, alert BackupAlertCondition) {
	if snapshot == nil {
		return
	}
	if strings.TrimSpace(alert.Reason) == "" {
		alert.Reason = firingReason(alert.Firing, strings.TrimSpace(alert.Name))
	}
	alert.Labels = cleanMetricLabels(alert.Labels)
	alert.Evidence = cleanEvidence(alert.Evidence)
	snapshot.Alerts = append(snapshot.Alerts, alert)
}

func finalizeBackupObservabilitySnapshot(snapshot *BackupObservabilitySnapshot) {
	if snapshot == nil {
		return
	}
	state := "healthy"
	for _, alert := range snapshot.Alerts {
		if !alert.Firing {
			continue
		}
		if alert.Severity == BackupAlertSeverityCritical {
			state = "critical"
			break
		}
		if state == "healthy" {
			state = "warning"
		}
	}
	snapshot.SummaryState = state
	sort.SliceStable(snapshot.Metrics, func(i, j int) bool {
		left := snapshot.Metrics[i]
		right := snapshot.Metrics[j]
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return labelSignature(left.Labels) < labelSignature(right.Labels)
	})
	sort.SliceStable(snapshot.Alerts, func(i, j int) bool {
		if snapshot.Alerts[i].Name != snapshot.Alerts[j].Name {
			return snapshot.Alerts[i].Name < snapshot.Alerts[j].Name
		}
		return labelSignature(snapshot.Alerts[i].Labels) < labelSignature(snapshot.Alerts[j].Labels)
	})
}

func backupOperationLabels(st ClusterBackupRunStatus, state, phase string) map[string]string {
	labels := map[string]string{"backup_set_id": st.BackupSetID, "state": state, "phase": phase}
	if strings.TrimSpace(st.RepositoryID) != "" {
		labels["repository_id"] = strings.TrimSpace(st.RepositoryID)
	}
	if strings.TrimSpace(st.ClusterID) != "" {
		labels["cluster_id"] = strings.TrimSpace(st.ClusterID)
	}
	return labels
}

func restoreOperationLabels(op ClusterRestoreOperation, state, phase string) map[string]string {
	labels := map[string]string{"restore_operation_id": op.RestoreOperationID, "backup_set_id": op.BackupSetID, "state": state, "phase": phase}
	if strings.TrimSpace(op.RepositoryID) != "" {
		labels["repository_id"] = strings.TrimSpace(op.RepositoryID)
	}
	if strings.TrimSpace(op.TargetMode) != "" {
		labels["target_mode"] = strings.TrimSpace(op.TargetMode)
	}
	if strings.TrimSpace(op.IdentityMode) != "" {
		labels["identity_mode"] = strings.TrimSpace(op.IdentityMode)
	}
	return labels
}

func normalizedBackupPhase(phase string) string {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "succeeded":
		return "succeeded"
	case "failed", "aborted":
		return "failed"
	case "canceled":
		return "canceled"
	case "canceling":
		return "canceling"
	case "":
		return "unknown"
	default:
		return "active"
	}
}

func normalizedRestoreState(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case clusterRestoreStateSucceeded:
		return "succeeded"
	case clusterRestoreStateCanceled:
		return "canceled"
	case clusterRestoreStateFailed:
		return "failed"
	case clusterRestoreStateFailedRequiresOp:
		return "failed_requires_operator"
	case "":
		return "unknown"
	default:
		return strings.ToLower(strings.TrimSpace(state))
	}
}

func backupStatusTerminal(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "succeeded", "failed", "canceled":
		return true
	default:
		return false
	}
}

func backupStatusFailed(state string) bool {
	return strings.ToLower(strings.TrimSpace(state)) == "failed"
}

func restoreStatusFailed(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "failed", "failed_requires_operator":
		return true
	default:
		return false
	}
}

func clusterBackupStatusBytes(st ClusterBackupRunStatus) int64 {
	var total int64
	for _, node := range st.Nodes {
		total += node.SizeBytes
	}
	return total
}

func operationDuration(start, end time.Time) time.Duration {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start)
}

func mergeStringLabels(base, extra map[string]string) map[string]string {
	out := cleanMetricLabels(base)
	if out == nil {
		out = map[string]string{}
	}
	for key, value := range extra {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			out[key] = value
		}
	}
	return out
}

func cleanMetricLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := map[string]string{}
	for key, value := range labels {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cleanEvidence(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func labelSignature(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, ",")
}

func firingReason(firing bool, reason string) string {
	if firing {
		return firstNonEmptyObservability(reason, "firing")
	}
	return "ok"
}

func firstNonEmptyObservability(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
