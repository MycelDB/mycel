package service

import (
	"context"
	"testing"
	"time"

	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
)

func TestBackupObservabilityReportsStaleBackupAndRestoreFailure(t *testing.T) {
	m := newInitializedBackupModule(t)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	backupSetID := "backup-set-20261122T100000Z-cluster-test"
	m.restoreClusterBackupState(backupSetID, map[string]clusterBackupRun{
		backupSetID: {
			BackupSetID:  backupSetID,
			ClusterID:    "cluster-test",
			RepositoryID: "repo-prod",
			Phase:        clusterBackupPhaseQuiescing,
			CreatedAt:    now.Add(-2 * time.Hour),
			UpdatedAt:    now.Add(-90 * time.Minute),
			Expected: []clusterBackupExpectedNode{
				{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1},
				{PodName: "myceld-1", NodeID: "node-2", Ordinal: 1, RaftNodeID: 2},
			},
			NodeResults: map[string]clusterbackup.NodeArtifact{"myceld-0": {PodName: "myceld-0", NodeID: "node-1", SizeBytes: 1024}},
			Blockers:    []ClusterBackupBlocker{{Reason: "raft_apply_lag", NodeName: "myceld-1"}},
		},
	})
	m.clusterRestores = map[string]ClusterRestoreOperation{
		"restore-1": {
			RestoreOperationID: "restore-1",
			RepositoryID:       "repo-prod",
			BackupSetID:        backupSetID,
			State:              clusterRestoreStateFailedRequiresOp,
			TargetMode:         "NEW_CLUSTER_BOOTSTRAP",
			IdentityMode:       "PRESERVE_BACKUP_ID",
			CreatedAt:          now.Add(-30 * time.Minute),
			UpdatedAt:          now.Add(-20 * time.Minute),
			CompletedAt:        now.Add(-20 * time.Minute),
			Blockers:           []ValidationBlocker{{Reason: "external_recovery_point_mismatch"}},
			CleanupTasks:       []CleanupTask{{TaskID: "cleanup-1", ResourceKind: "helper_pod", State: "pending"}},
			Progress:           ClusterRestoreProgress{TotalNodes: 2, StagedNodes: 1, CurrentPhase: "validating_external_dependencies", BytesCompleted: 512},
		},
	}

	snapshot, err := m.GetBackupObservability(context.Background(), BackupObservabilityOptions{RepositoryID: "repo-prod", Now: now, StaleOperationAfter: time.Hour, BackupFreshnessAfter: 24 * time.Hour})
	if err != nil {
		t.Fatalf("GetBackupObservability() error = %v", err)
	}
	if snapshot.SummaryState != "critical" {
		t.Fatalf("summary_state=%s, want critical", snapshot.SummaryState)
	}
	assertBackupAlertFiring(t, snapshot, "MycelClusterBackupOperationStale", true)
	assertBackupAlertFiring(t, snapshot, "MycelClusterBackupBlockers", true)
	assertBackupAlertFiring(t, snapshot, "MycelClusterRestoreFailed", true)
	assertBackupAlertFiring(t, snapshot, "MycelClusterRestoreBlockers", true)
	if metric := findBackupMetric(snapshot, "mycel_backup_cluster_active_operation_age_seconds", map[string]string{"backup_set_id": backupSetID}); metric == nil || metric.Value < 7200 {
		t.Fatalf("active backup age metric=%#v, want at least 7200 seconds", metric)
	}
	if metric := findBackupMetric(snapshot, "mycel_restore_cluster_cleanup_task_count", map[string]string{"restore_operation_id": "restore-1", "cleanup_state": "pending"}); metric == nil || metric.Value != 1 {
		t.Fatalf("restore cleanup metric=%#v, want pending count", metric)
	}
}

func TestBackupObservabilityRetentionProbeReportsIncompletePrefixes(t *testing.T) {
	ctx := context.Background()
	m, repo, store := newRetentionRepositoryFixture(t)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	incompleteID := "backup-set-20261120T120000Z-cluster-test"
	if _, err := store.PutBytes(ctx, joinRepositoryKey(repositoryBackupSetPrefix(repo, incompleteID), "nodes", "myceld-0", "partial.tar"), []byte("partial"), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	m.recordRepositoryRetentionObservation(RepositoryRetentionPlan{RepositoryID: repo.RepositoryID, PlannedAt: now.Add(-time.Minute), Warnings: []string{"partial delete"}, Actions: []RepositoryRetentionAction{{BackupSetID: incompleteID, Action: RetentionActionDeleteIncomplete, Error: "delete failed"}}}, repo.RepositoryID, nil)
	snapshot, err := m.GetBackupObservability(ctx, BackupObservabilityOptions{RepositoryID: repo.RepositoryID, IncludeRetentionProbe: true, Now: now, IncompleteBackupAfter: time.Hour})
	if err != nil {
		t.Fatalf("GetBackupObservability() error = %v", err)
	}
	assertBackupAlertFiring(t, snapshot, "MycelRepositoryIncompleteBackups", true)
	assertBackupAlertFiring(t, snapshot, "MycelRepositoryRetentionApplyFailed", true)
	metric := findBackupMetric(snapshot, "mycel_backup_repository_incomplete_backup_count", map[string]string{"repository_id": repo.RepositoryID})
	if metric == nil || metric.Value != 1 {
		t.Fatalf("incomplete metric=%#v, want count=1", metric)
	}
	metric = findBackupMetric(snapshot, "mycel_backup_repository_last_retention_apply_error_count", map[string]string{"repository_id": repo.RepositoryID})
	if metric == nil || metric.Value != 1 {
		t.Fatalf("retention apply error metric=%#v, want count=1", metric)
	}
}

func assertBackupAlertFiring(t *testing.T, snapshot BackupObservabilitySnapshot, name string, want bool) {
	t.Helper()
	for _, alert := range snapshot.Alerts {
		if alert.Name == name {
			if alert.Firing != want {
				t.Fatalf("alert %s firing=%t, want %t: %#v", name, alert.Firing, want, alert)
			}
			return
		}
	}
	t.Fatalf("alert %s not found in %#v", name, snapshot.Alerts)
}

func findBackupMetric(snapshot BackupObservabilitySnapshot, name string, labels map[string]string) *BackupMetric {
	for i := range snapshot.Metrics {
		metric := &snapshot.Metrics[i]
		if metric.Name != name {
			continue
		}
		matched := true
		for key, value := range labels {
			if metric.Labels[key] != value {
				matched = false
				break
			}
		}
		if matched {
			return metric
		}
	}
	return nil
}
