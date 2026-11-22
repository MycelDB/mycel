package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
)

const (
	RetentionActionKeep             = "keep"
	RetentionActionDelete           = "delete"
	RetentionActionBlocked          = "blocked"
	RetentionActionDeleteIncomplete = "delete_incomplete"
	RetentionActionWarn             = "warn"
)

type RepositoryRetentionPlanOptions struct {
	RepositoryID string
	Policy       backupcore.RetentionPolicy
	Now          time.Time
	Apply        bool
}

type RepositoryRetentionPlan struct {
	RepositoryID   string
	PlannedAt      time.Time
	Policy         backupcore.RetentionPolicy
	DryRun         bool
	Actions        []RepositoryRetentionAction
	Warnings       []string
	DeletedObjects int
}

type RepositoryRetentionAction struct {
	BackupSetID     string
	Prefix          string
	Action          string
	Reason          string
	CreatedAt       time.Time
	CompletedAt     time.Time
	ObjectCount     int
	DeletedObjects  int
	Retryable       bool
	Blocked         bool
	Error           string
	RetentionCutoff time.Time
}

type RepositoryRetentionManager interface {
	PlanRepositoryRetention(context.Context, RepositoryRetentionPlanOptions) (RepositoryRetentionPlan, error)
	ApplyRepositoryRetention(context.Context, RepositoryRetentionPlanOptions) (RepositoryRetentionPlan, error)
}

func (m *Module) PlanRepositoryRetention(ctx context.Context, options RepositoryRetentionPlanOptions) (RepositoryRetentionPlan, error) {
	options.Apply = false
	return m.planRepositoryRetention(ctx, options)
}

func (m *Module) ApplyRepositoryRetention(ctx context.Context, options RepositoryRetentionPlanOptions) (RepositoryRetentionPlan, error) {
	options.Apply = true
	plan, err := m.planRepositoryRetention(ctx, options)
	m.recordRepositoryRetentionObservation(plan, options.RepositoryID, err)
	return plan, err
}

func (m *Module) planRepositoryRetention(ctx context.Context, options RepositoryRetentionPlanOptions) (RepositoryRetentionPlan, error) {
	repositoryID := strings.TrimSpace(options.RepositoryID)
	if repositoryID == "" {
		repositoryID = strings.TrimSpace(m.policy.DefaultRepositoryID)
	}
	if repositoryID == "" {
		return RepositoryRetentionPlan{}, fmt.Errorf("repository_id is required")
	}
	repo, store, err := m.repositoryCatalogStore(ctx, repositoryID)
	if err != nil {
		return RepositoryRetentionPlan{}, err
	}
	policy := options.Policy
	if policy == (backupcore.RetentionPolicy{}) {
		policy = backupcore.EffectivePolicy(m.dataDir, m.Policy()).Retention
	} else {
		policy = normalizeRepositoryRetentionPolicy(policy)
	}
	now := options.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	plan := RepositoryRetentionPlan{RepositoryID: repositoryID, PlannedAt: now, Policy: policy, DryRun: !options.Apply}
	completed, incomplete, warnings, err := m.repositoryRetentionInventory(ctx, repo, store)
	plan.Warnings = append(plan.Warnings, warnings...)
	if err != nil {
		return plan, err
	}
	active := m.activeRepositoryBackupSetIDs()
	keepCutoff := time.Time{}
	if policy.KeepFor > 0 {
		keepCutoff = now.Add(-policy.KeepFor)
	}
	for index, item := range completed {
		action := RepositoryRetentionAction{BackupSetID: item.BackupSetID, Prefix: item.Prefix, CreatedAt: item.CreatedAt, CompletedAt: item.CompletedAt, Action: RetentionActionKeep, Reason: "within_retention", RetentionCutoff: keepCutoff}
		if active[item.BackupSetID] {
			action.Action = RetentionActionBlocked
			action.Reason = "active_operation_reference"
			action.Blocked = true
			plan.Actions = append(plan.Actions, action)
			continue
		}
		withinKeepLast := policy.KeepLastSuccessful > 0 && index < policy.KeepLastSuccessful
		withinKeepFor := policy.KeepFor > 0 && !item.CompletedAt.IsZero() && item.CompletedAt.After(keepCutoff)
		if withinKeepLast {
			action.Reason = "keep_last_successful"
		} else if withinKeepFor {
			action.Reason = "keep_for"
		} else {
			action.Action = RetentionActionDelete
			action.Reason = "retention_expired"
		}
		plan.Actions = append(plan.Actions, action)
	}
	if policy.DeleteIncompleteAfter > 0 {
		incompleteCutoff := now.Add(-policy.DeleteIncompleteAfter)
		for _, item := range incomplete {
			action := RepositoryRetentionAction{BackupSetID: item.BackupSetID, Prefix: item.Prefix, CreatedAt: item.CreatedAt, ObjectCount: item.ObjectCount, Action: RetentionActionKeep, Reason: "incomplete_not_old_enough", RetentionCutoff: incompleteCutoff}
			if active[item.BackupSetID] {
				action.Action = RetentionActionBlocked
				action.Reason = "active_operation_reference"
				action.Blocked = true
				plan.Actions = append(plan.Actions, action)
				continue
			}
			if item.CreatedAt.IsZero() {
				action.Action = RetentionActionWarn
				action.Reason = "incomplete_age_unknown"
				plan.Actions = append(plan.Actions, action)
				continue
			}
			if item.CreatedAt.Before(incompleteCutoff) || item.CreatedAt.Equal(incompleteCutoff) {
				action.Action = RetentionActionDeleteIncomplete
				action.Reason = "incomplete_retention_expired"
			}
			plan.Actions = append(plan.Actions, action)
		}
	}
	if options.Apply {
		m.applyRepositoryRetentionActions(ctx, store, &plan)
	}
	return plan, nil
}

type repositoryRetentionObservation struct {
	RepositoryID   string
	ObservedAt     time.Time
	Failed         bool
	WarningCount   int
	ActionErrors   int
	BlockedActions int
	DeletedObjects int
}

func (m *Module) recordRepositoryRetentionObservation(plan RepositoryRetentionPlan, requestedRepositoryID string, err error) {
	observedAt := plan.PlannedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	obs := repositoryRetentionObservation{RepositoryID: firstNonEmptyString(plan.RepositoryID, strings.TrimSpace(requestedRepositoryID)), ObservedAt: observedAt, Failed: err != nil, WarningCount: len(plan.Warnings), DeletedObjects: plan.DeletedObjects}
	for _, action := range plan.Actions {
		if strings.TrimSpace(action.Error) != "" {
			obs.ActionErrors++
		}
		if action.Blocked || action.Action == RetentionActionBlocked || action.Action == RetentionActionWarn {
			obs.BlockedActions++
		}
	}
	m.mu.Lock()
	m.lastRepositoryRetentionObservation = obs
	m.mu.Unlock()
}

func (m *Module) repositoryRetentionObservationSnapshot(repositoryID string) (repositoryRetentionObservation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obs := m.lastRepositoryRetentionObservation
	if obs.ObservedAt.IsZero() {
		return repositoryRetentionObservation{}, false
	}
	if strings.TrimSpace(repositoryID) != "" && strings.TrimSpace(obs.RepositoryID) != "" && strings.TrimSpace(repositoryID) != strings.TrimSpace(obs.RepositoryID) {
		return repositoryRetentionObservation{}, false
	}
	return obs, true
}

type repositoryRetentionCompletedItem struct {
	BackupSetID string
	Prefix      string
	CreatedAt   time.Time
	CompletedAt time.Time
}

type repositoryRetentionIncompleteItem struct {
	BackupSetID string
	Prefix      string
	CreatedAt   time.Time
	ObjectCount int
}

func normalizeRepositoryRetentionPolicy(policy backupcore.RetentionPolicy) backupcore.RetentionPolicy {
	if policy.KeepLastSuccessful <= 0 {
		policy.KeepLastSuccessful = backupcore.DefaultRetentionCount
	}
	if policy.KeepFor <= 0 {
		policy.KeepFor = backupcore.DefaultKeepFor
	}
	if policy.DeleteIncompleteAfter <= 0 {
		policy.DeleteIncompleteAfter = backupcore.DefaultDeleteIncompleteAfter
	}
	return policy
}

func (m *Module) repositoryRetentionInventory(ctx context.Context, repo BackupRepository, store backupRepositoryObjectStore) ([]repositoryRetentionCompletedItem, []repositoryRetentionIncompleteItem, []string, error) {
	prefix := joinRepositoryKey(repo.S3.Prefix, "backup-sets")
	keys, err := store.ListKeys(ctx, prefix)
	if err != nil {
		return nil, nil, nil, err
	}
	grouped := map[string][]string{}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		backupSetID, ok := repositoryBackupSetIDFromKey(prefix, key)
		if !ok {
			continue
		}
		grouped[backupSetID] = append(grouped[backupSetID], key)
	}
	completed := []repositoryRetentionCompletedItem{}
	incomplete := []repositoryRetentionIncompleteItem{}
	warnings := []string{}
	backupSetIDs := make([]string, 0, len(grouped))
	for backupSetID := range grouped {
		backupSetIDs = append(backupSetIDs, backupSetID)
	}
	sort.Strings(backupSetIDs)
	for _, backupSetID := range backupSetIDs {
		groupKeys := grouped[backupSetID]
		prefix := repositoryBackupSetPrefix(repo, backupSetID)
		hasComplete := false
		for _, key := range groupKeys {
			if filepath.Base(key) == "COMPLETE" {
				hasComplete = true
				break
			}
		}
		if hasComplete {
			result, err := m.readRepositoryClusterBackupSet(ctx, repo, store, backupSetID, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true})
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("skip invalid completed backup set %s: %v", backupSetID, err))
				continue
			}
			if result.ProductionReport != nil && !result.ProductionReport.Valid() {
				warnings = append(warnings, fmt.Sprintf("skip invalid completed backup set %s", backupSetID))
				continue
			}
			completed = append(completed, repositoryRetentionCompletedItem{BackupSetID: backupSetID, Prefix: prefix, CreatedAt: result.Manifest.CreatedAt, CompletedAt: firstNonZeroTime(result.Manifest.CompletedAt, result.Manifest.CreatedAt)})
			continue
		}
		createdAt, ok := parseClusterBackupSetIDTime(backupSetID)
		if !ok {
			warnings = append(warnings, fmt.Sprintf("incomplete backup set %s has no parseable timestamp", backupSetID))
		}
		incomplete = append(incomplete, repositoryRetentionIncompleteItem{BackupSetID: backupSetID, Prefix: prefix, CreatedAt: createdAt, ObjectCount: len(groupKeys)})
	}
	sort.Slice(completed, func(i, j int) bool {
		left := firstNonZeroTime(completed[i].CompletedAt, completed[i].CreatedAt)
		right := firstNonZeroTime(completed[j].CompletedAt, completed[j].CreatedAt)
		if left.Equal(right) {
			return completed[i].BackupSetID > completed[j].BackupSetID
		}
		return left.After(right)
	})
	sort.Slice(incomplete, func(i, j int) bool {
		if incomplete[i].CreatedAt.Equal(incomplete[j].CreatedAt) {
			return incomplete[i].BackupSetID < incomplete[j].BackupSetID
		}
		return incomplete[i].CreatedAt.Before(incomplete[j].CreatedAt)
	})
	return completed, incomplete, warnings, nil
}

func repositoryBackupSetIDFromKey(rootPrefix, key string) (string, bool) {
	rootPrefix = cleanRepositoryObjectKey(rootPrefix)
	key = cleanRepositoryObjectKey(key)
	if rootPrefix != "" {
		if key != rootPrefix && !strings.HasPrefix(key, rootPrefix+"/") {
			return "", false
		}
		key = strings.TrimPrefix(key, rootPrefix)
		key = strings.TrimPrefix(key, "/")
	}
	parts := strings.Split(key, "/")
	if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" {
		return "", false
	}
	return parts[0], true
}

func parseClusterBackupSetIDTime(backupSetID string) (time.Time, bool) {
	const prefix = "backup-set-"
	backupSetID = strings.TrimSpace(backupSetID)
	if !strings.HasPrefix(backupSetID, prefix) || len(backupSetID) < len(prefix)+len("20060102T150405Z") {
		return time.Time{}, false
	}
	ts := backupSetID[len(prefix) : len(prefix)+len("20060102T150405Z")]
	parsed, err := time.Parse("20060102T150405Z", ts)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func (m *Module) activeRepositoryBackupSetIDs() map[string]bool {
	active := map[string]bool{}
	if backupSetID, _ := m.clusterBackupSnapshot(); strings.TrimSpace(backupSetID) != "" {
		active[backupSetID] = true
	}
	for _, restore := range m.ListClusterRestores() {
		if strings.TrimSpace(restore.BackupSetID) != "" && !clusterRestoreTerminalState(restore.State) {
			active[restore.BackupSetID] = true
		}
	}
	return active
}

func (m *Module) applyRepositoryRetentionActions(ctx context.Context, store backupRepositoryObjectStore, plan *RepositoryRetentionPlan) {
	if plan == nil || store == nil {
		return
	}
	for i := range plan.Actions {
		action := &plan.Actions[i]
		if action.Action != RetentionActionDelete && action.Action != RetentionActionDeleteIncomplete {
			continue
		}
		deleted, err := store.DeletePrefix(ctx, action.Prefix)
		action.DeletedObjects = deleted
		plan.DeletedObjects += deleted
		if err != nil {
			action.Error = err.Error()
			action.Retryable = true
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("delete %s failed after %d objects: %v", action.BackupSetID, deleted, err))
			if m.logger != nil {
				m.logger.Warn("repository retention delete failed", "repository_id", plan.RepositoryID, "backup_set_id", action.BackupSetID, "prefix", action.Prefix, "deleted_objects", deleted, "error", err)
			}
			continue
		}
		if deleted == 0 {
			action.Error = "no objects deleted"
			action.Retryable = true
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("delete %s matched no objects", action.BackupSetID))
		}
	}
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}
