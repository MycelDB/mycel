package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewAdminBackupCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "backup", Short: "Manage daemon backups"}
	policy := &cobra.Command{Use: "policy", Short: "Manage backup policy"}
	policy.AddCommand(NewAdminBackupPolicyGetCommand(a), NewAdminBackupPolicySetCommand(a))
	cluster := &cobra.Command{Use: "cluster", Short: "Manage coordinated cluster backup sets"}
	retention := &cobra.Command{Use: "retention", Short: "Preview and apply repository backup retention"}
	retention.AddCommand(NewAdminBackupClusterRetentionPlanCommand(a), NewAdminBackupClusterRetentionApplyCommand(a))
	cluster.AddCommand(NewAdminBackupClusterStartCommand(a), NewAdminBackupClusterStatusCommand(a), NewAdminBackupClusterObservabilityCommand(a), NewAdminBackupClusterCancelCommand(a), NewAdminBackupClusterListCommand(a), NewAdminBackupClusterDeleteCommand(a), NewAdminBackupClusterValidateCommand(a), retention, NewAdminBackupClusterRestorePlanCommand(a), NewAdminBackupClusterRestoreLocalCommand(a), NewAdminBackupClusterRestoreRepositoryLocalCommand(a))
	cmd.AddCommand(policy, cluster, NewAdminBackupTriggerCommand(a), NewAdminBackupStatusCommand(a), NewAdminBackupListCommand(a), NewAdminBackupDeleteCommand(a))
	return cmd
}

func NewAdminBackupPolicyGetCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "get", Short: "Get backup policy", RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.GetBackupPolicy(authCtx, &adminv1.GetBackupPolicyRequest{})
		if err != nil {
			return backupCLIError("get backup policy", err)
		}
		return printBackupPolicy(a, res.GetPolicy())
	}}
}

func NewAdminBackupPolicySetCommand(a *app.App) *cobra.Command {
	var enabled, disabled, includeLogs, allowReads, runMissed, noRunMissed bool
	var backupDir, quiesceTimeout, backupTimeout, retryAfter string
	var scheduleKind, timeOfDay, timezone, archiveFormat string
	var weekdays []string
	var intervalHours, retention, historyLimit int32
	cmd := &cobra.Command{Use: "set", Short: "Update backup policy", RunE: func(cmd *cobra.Command, args []string) error {
		if enabled && disabled {
			return fmt.Errorf("--enabled and --disabled are mutually exclusive")
		}
		if runMissed && noRunMissed {
			return fmt.Errorf("--run-missed and --no-run-missed are mutually exclusive")
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		current, err := client.GetBackupPolicy(authCtx, &adminv1.GetBackupPolicyRequest{})
		if err != nil {
			return backupCLIError("get backup policy", err)
		}
		policy := current.GetPolicy()
		if policy == nil {
			policy = &adminv1.BackupPolicy{Compression: "zip"}
		}
		if enabled {
			policy.Enabled = true
		}
		if disabled {
			policy.Enabled = false
		}
		if cmd.Flags().Changed("dir") {
			policy.BackupDir = backupDir
		}
		if cmd.Flags().Changed("interval-hours") {
			if intervalHours <= 0 {
				return fmt.Errorf("invalid --interval-hours %d: must be positive", intervalHours)
			}
			policy.IntervalHours = intervalHours
		}
		if cmd.Flags().Changed("keep") {
			policy.RetentionCount = retention
		}
		if cmd.Flags().Changed("archive-format") {
			format, err := parseBackupArchiveFormat(archiveFormat)
			if err != nil {
				return err
			}
			policy.ArchiveFormat = format
			policy.Compression = formatBackupArchiveFormat(format, "")
		}
		if cmd.Flags().Changed("include-logs") {
			policy.IncludeLogs = includeLogs
		}
		if cmd.Flags().Changed("allow-reads-during-backup") {
			policy.AllowReadsDuringBackup = allowReads
		}
		if cmd.Flags().Changed("quiesce-timeout") {
			seconds, err := parseDurationSeconds(quiesceTimeout)
			if err != nil {
				return fmt.Errorf("invalid --quiesce-timeout: %w", err)
			}
			policy.QuiesceDrainTimeoutSeconds = seconds
		}
		if cmd.Flags().Changed("backup-timeout") {
			seconds, err := parseDurationSeconds(backupTimeout)
			if err != nil {
				return fmt.Errorf("invalid --backup-timeout: %w", err)
			}
			policy.BackupTimeoutSeconds = seconds
		}
		if cmd.Flags().Changed("retry-after") {
			seconds, err := parseDurationSeconds(retryAfter)
			if err != nil {
				return fmt.Errorf("invalid --retry-after: %w", err)
			}
			policy.RetryAfterSeconds = seconds
		}
		if cmd.Flags().Changed("history-limit") {
			policy.StatusHistoryLimit = historyLimit
		}
		if cmd.Flags().Changed("schedule") {
			policy.ScheduleKind = strings.ToLower(strings.TrimSpace(scheduleKind))
		}
		if cmd.Flags().Changed("time-of-day") {
			policy.TimeOfDay = strings.TrimSpace(timeOfDay)
		}
		if cmd.Flags().Changed("timezone") {
			policy.Timezone = strings.TrimSpace(timezone)
		}
		if cmd.Flags().Changed("weekday") {
			parsed, err := parseBackupWeekdays(weekdays)
			if err != nil {
				return err
			}
			policy.Weekdays = parsed
		}
		if runMissed {
			policy.RunMissed = true
		}
		if noRunMissed {
			policy.RunMissed = false
		}
		if strings.TrimSpace(policy.Compression) == "" && policy.GetArchiveFormat() == adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_UNSPECIFIED {
			policy.Compression = "zip"
			policy.ArchiveFormat = adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP
		}
		res, err := client.UpdateBackupPolicy(authCtx, &adminv1.UpdateBackupPolicyRequest{Policy: policy})
		if err != nil {
			return backupCLIError("update backup policy", err)
		}
		return printBackupPolicy(a, res.GetPolicy())
	}}
	cmd.Flags().BoolVar(&enabled, "enabled", false, "enable scheduled backups")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "disable scheduled backups")
	cmd.Flags().StringVar(&backupDir, "dir", "", "backup directory")
	cmd.Flags().Int32Var(&intervalHours, "interval-hours", 0, "backup interval in whole hours")
	cmd.Flags().Int32Var(&retention, "keep", 0, "number of backups to retain")
	cmd.Flags().StringVar(&archiveFormat, "archive-format", "", "backup archive format: zip, tar, tar.gz, or tar.zst")
	cmd.Flags().BoolVar(&includeLogs, "include-logs", false, "include daemon logs in backups")
	cmd.Flags().BoolVar(&allowReads, "allow-reads-during-backup", false, "allow proven-safe reads during backup")
	cmd.Flags().StringVar(&quiesceTimeout, "quiesce-timeout", "", "quiesce drain timeout duration")
	cmd.Flags().StringVar(&backupTimeout, "backup-timeout", "", "backup timeout duration")
	cmd.Flags().StringVar(&retryAfter, "retry-after", "", "retry delay after scheduled backup failure")
	cmd.Flags().Int32Var(&historyLimit, "history-limit", 0, "backup status history limit")
	cmd.Flags().StringVar(&scheduleKind, "schedule", "", "backup schedule kind: interval, daily, or weekly")
	cmd.Flags().StringVar(&timeOfDay, "time-of-day", "", "wall-clock backup time in HH:MM format for daily/weekly schedules")
	cmd.Flags().StringVar(&timezone, "timezone", "", "IANA timezone for daily/weekly schedules, e.g. UTC or America/Toronto")
	cmd.Flags().StringArrayVar(&weekdays, "weekday", nil, "weekday for weekly schedules; repeat for multiple values (sun..sat or 0..6)")
	cmd.Flags().BoolVar(&runMissed, "run-missed", false, "run a missed calendar backup after daemon restart")
	cmd.Flags().BoolVar(&noRunMissed, "no-run-missed", false, "disable running missed calendar backups after daemon restart")
	return cmd
}

func NewAdminBackupTriggerCommand(a *app.App) *cobra.Command {
	var reason string
	cmd := &cobra.Command{Use: "trigger", Short: "Trigger a backup", RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.TriggerBackup(authCtx, &adminv1.TriggerBackupRequest{Reason: reason})
		if err != nil {
			return backupCLIError("trigger backup", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		return a.Print(res, fmt.Sprintf("backup triggered: %s\nstate: %s\n", res.GetBackup().GetBackupId(), res.GetStatus().GetState()))
	}}
	cmd.Flags().StringVar(&reason, "reason", "", "backup reason")
	return cmd
}

func NewAdminBackupStatusCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "status", Short: "Show backup status", RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.GetBackupStatus(authCtx, &adminv1.GetBackupStatusRequest{})
		if err != nil {
			return backupCLIError("get backup status", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetStatus()
		return a.Print(res, fmt.Sprintf("backup status: %s\nbackup_id: %s\nlast_success_at: %s\nnext_run_at: %s\n", st.GetState(), st.GetBackupId(), st.GetLastSuccessAt(), st.GetNextRunAt()))
	}}
}

func NewAdminBackupListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken string
	cmd := &cobra.Command{Use: "list", Aliases: []string{"ls"}, Short: "List backups", RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.ListBackups(authCtx, &adminv1.ListBackupsRequest{PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return backupCLIError("list backups", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		var b strings.Builder
		for _, backup := range res.GetBackups() {
			fmt.Fprintf(&b, "%s\t%s\t%d bytes\n", backup.GetBackupId(), backup.GetCompletedAt(), backup.GetSizeBytes())
		}
		return a.Print(res.GetBackups(), b.String())
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 0, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	return cmd
}

func NewAdminBackupDeleteCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "delete BACKUP_ID", Aliases: []string{"del", "rm"}, Short: "Delete a backup", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.DeleteBackup(authCtx, &adminv1.DeleteBackupRequest{BackupId: args[0]})
		if err != nil {
			return backupCLIError("delete backup", err)
		}
		return a.Print(res, fmt.Sprintf("backup deleted: %s\n", res.GetBackupId()))
	}}
	return cmd
}

func NewAdminBackupClusterStartCommand(a *app.App) *cobra.Command {
	var reason, outputDir, archiveFormat, idempotencyKey string
	var repositoryID, captureProvider, quiesceReadProfile string
	var convergenceTimeout time.Duration
	var wait bool
	var waitTimeout time.Duration
	cmd := &cobra.Command{Use: "start", Short: "Start a coordinated cluster backup operation", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(outputDir) == "" && strings.TrimSpace(repositoryID) == "" {
			return fmt.Errorf("--output-dir is required unless --repository-id is set")
		}
		format, err := parseBackupArchiveFormat(archiveFormat)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		provider, err := parseBackupCaptureProvider(captureProvider)
		if err != nil {
			return err
		}
		readProfile, err := parseBackupQuiesceReadProfile(quiesceReadProfile)
		if err != nil {
			return err
		}
		req := &adminv1.StartClusterBackupRequest{Reason: reason, OutputDir: outputDir, ArchiveFormat: format, IdempotencyKey: idempotencyKey, RepositoryId: repositoryID, CaptureProvider: provider, QuiesceReadProfile: readProfile}
		if convergenceTimeout > 0 {
			req.ConvergenceTimeoutSeconds = int64(convergenceTimeout.Round(time.Second) / time.Second)
		}
		res, err := client.StartClusterBackup(authCtx, req)
		if err != nil {
			return backupCLIError("start cluster backup", err)
		}
		if wait {
			statusRes, err := waitForClusterBackup(cmd.Context(), client, authCtx, res.GetStatus().GetBackupSetId(), waitTimeout)
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(statusRes, "")
			}
			st := statusRes.GetStatus()
			return a.Print(statusRes, fmt.Sprintf("cluster backup completed: %s\nstate: %s\nmanifest: %s\nerror: %s\n", st.GetBackupSetId(), st.GetState(), st.GetManifestUri(), st.GetError()))
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetStatus()
		return a.Print(res, fmt.Sprintf("cluster backup started: %s\nstate: %s\n", st.GetBackupSetId(), st.GetState()))
	}}
	cmd.Flags().StringVar(&reason, "reason", "", "backup reason")
	cmd.Flags().StringVar(&outputDir, "output-dir", "", "cluster backup output directory mounted on each pod")
	cmd.Flags().StringVar(&archiveFormat, "archive-format", "tar.zst", "backup archive format: zip, tar, tar.gz, or tar.zst")
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "optional idempotency key for start retries")
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID for repository-backed production backup sets")
	cmd.Flags().StringVar(&captureProvider, "capture-provider", "auto", "capture provider: auto or filesystem_immutable_copy")
	cmd.Flags().StringVar(&quiesceReadProfile, "quiesce-read-profile", "control_only", "backup quiesce read profile: control_only or strict_read_only")
	cmd.Flags().DurationVar(&convergenceTimeout, "convergence-timeout", 0, "maximum time to wait for cluster convergence before failing the operation")
	cmd.Flags().BoolVar(&wait, "wait", false, "wait for the cluster backup operation to finish")
	cmd.Flags().DurationVar(&waitTimeout, "timeout", 10*time.Minute, "maximum time to wait with --wait")
	return cmd
}

func NewAdminBackupClusterCancelCommand(a *app.App) *cobra.Command {
	var reason string
	cmd := &cobra.Command{Use: "cancel [BACKUP_SET_ID]", Short: "Cancel a cluster backup operation", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		backupSetID := ""
		if len(args) > 0 {
			backupSetID = args[0]
		}
		res, err := client.CancelClusterBackup(authCtx, &adminv1.CancelClusterBackupRequest{BackupSetId: backupSetID, Reason: reason})
		if err != nil {
			return backupCLIError("cancel cluster backup", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetStatus()
		return a.Print(res, fmt.Sprintf("cluster backup cancel requested: %s\nstate: %s\n", st.GetBackupSetId(), st.GetState()))
	}}
	cmd.Flags().StringVar(&reason, "reason", "", "cancellation reason")
	return cmd
}

func waitForClusterBackup(ctx context.Context, client adminv1.AdminBackupServiceClient, authCtx context.Context, backupSetID string, timeout time.Duration) (*adminv1.GetClusterBackupStatusResponse, error) {
	if strings.TrimSpace(backupSetID) == "" {
		return nil, fmt.Errorf("backup_set_id is required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			return nil, backupCLIError("wait for cluster backup", waitCtx.Err())
		default:
		}
		res, err := client.GetClusterBackupStatus(authCtx, &adminv1.GetClusterBackupStatusRequest{BackupSetId: backupSetID})
		if err != nil {
			return nil, backupCLIError("get cluster backup status", err)
		}
		switch res.GetStatus().GetStateCode() {
		case adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_SUCCEEDED:
			return res, nil
		case adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_FAILED, adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_CANCELED:
			return res, fmt.Errorf("cluster backup %s finished with state %s: %s", backupSetID, res.GetStatus().GetState(), res.GetStatus().GetError())
		}
		select {
		case <-waitCtx.Done():
			return nil, backupCLIError("wait for cluster backup", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func NewAdminBackupClusterStatusCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "status [BACKUP_SET_ID]", Short: "Show cluster backup status", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		backupSetID := ""
		if len(args) > 0 {
			backupSetID = args[0]
		}
		res, err := client.GetClusterBackupStatus(authCtx, &adminv1.GetClusterBackupStatusRequest{BackupSetId: backupSetID})
		if err != nil {
			return backupCLIError("get cluster backup status", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetStatus()
		return a.Print(res, fmt.Sprintf("cluster backup status: %s\nbackup_set_id: %s\ncluster_id: %s\nmanifest: %s\nerror: %s\n", st.GetState(), st.GetBackupSetId(), st.GetClusterId(), st.GetManifestUri(), st.GetError()))
	}}
}

func NewAdminBackupClusterObservabilityCommand(a *app.App) *cobra.Command {
	var repositoryID string
	var includeRetentionProbe bool
	var staleOperationAfter, backupFreshnessAfter, incompleteBackupAfter time.Duration
	cmd := &cobra.Command{Use: "observability", Aliases: []string{"metrics"}, Short: "Show backup/restore observability metrics and alert conditions", RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		req := &adminv1.GetBackupObservabilityRequest{RepositoryId: repositoryID, IncludeRetentionProbe: includeRetentionProbe}
		if staleOperationAfter > 0 {
			req.StaleOperationAfterSeconds = int64(staleOperationAfter.Round(time.Second) / time.Second)
		}
		if backupFreshnessAfter > 0 {
			req.BackupFreshnessAfterSeconds = int64(backupFreshnessAfter.Round(time.Second) / time.Second)
		}
		if incompleteBackupAfter > 0 {
			req.IncompleteBackupAfterSeconds = int64(incompleteBackupAfter.Round(time.Second) / time.Second)
		}
		res, err := client.GetBackupObservability(authCtx, req)
		if err != nil {
			return backupCLIError("get backup observability", err)
		}
		return printBackupObservability(a, res.GetSnapshot())
	}}
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID for retention/incomplete-backup probes")
	cmd.Flags().BoolVar(&includeRetentionProbe, "include-retention-probe", false, "include a non-mutating repository retention probe")
	cmd.Flags().DurationVar(&staleOperationAfter, "stale-operation-after", 0, "alert threshold for active backup/restore operation age")
	cmd.Flags().DurationVar(&backupFreshnessAfter, "backup-freshness-after", 0, "alert threshold for latest successful cluster backup age")
	cmd.Flags().DurationVar(&incompleteBackupAfter, "incomplete-backup-after", 0, "alert threshold for incomplete repository backup prefixes")
	return cmd
}

func NewAdminBackupClusterListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken string
	var repositoryID string
	cmd := &cobra.Command{Use: "list", Aliases: []string{"ls"}, Short: "List cluster backup sets", RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		if strings.TrimSpace(repositoryID) != "" {
			res, err := client.ListClusterBackupSets(authCtx, &adminv1.ListClusterBackupSetsRequest{RepositoryId: repositoryID, PageSize: pageSize, PageToken: pageToken})
			if err != nil {
				return backupCLIError("list repository cluster backup sets", err)
			}
			if a.Output == "json" {
				return a.Print(res, "")
			}
			return a.Print(res.GetBackupSets(), formatClusterBackupSetRows(res.GetBackupSets()))
		}
		res, err := client.ListClusterBackups(authCtx, &adminv1.ListClusterBackupsRequest{PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return backupCLIError("list cluster backups", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		return a.Print(res.GetBackupSets(), formatClusterBackupSetRows(res.GetBackupSets()))
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 0, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID to list repository-truth backup sets")
	return cmd
}

func NewAdminBackupClusterDeleteCommand(a *app.App) *cobra.Command {
	var repositoryID, reason string
	cmd := &cobra.Command{Use: "delete BACKUP_SET_ID", Aliases: []string{"rm"}, Short: "Delete a repository-backed cluster backup set", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(repositoryID) == "" {
			return fmt.Errorf("--repository-id is required")
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.DeleteClusterBackupSet(authCtx, &adminv1.DeleteClusterBackupSetRequest{RepositoryId: repositoryID, BackupSetId: args[0], Reason: reason})
		if err != nil {
			return backupCLIError("delete cluster backup set", err)
		}
		return a.Print(res, fmt.Sprintf("cluster backup set deleted: %s\nrepository: %s\nstate: %s\n", res.GetBackupSetId(), res.GetRepositoryId(), res.GetState()))
	}}
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID containing the backup set")
	cmd.Flags().StringVar(&reason, "reason", "", "operator reason for deletion")
	return cmd
}

func NewAdminBackupClusterRetentionPlanCommand(a *app.App) *cobra.Command {
	opts := retentionCLIOptions{}
	cmd := &cobra.Command{Use: "plan", Short: "Preview repository backup retention pruning", RunE: func(cmd *cobra.Command, args []string) error {
		policy, err := retentionPolicyFromFlags(cmd, opts)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.PlanRepositoryRetention(authCtx, &adminv1.PlanRepositoryRetentionRequest{RepositoryId: opts.repositoryID, RetentionPolicy: policy, Now: opts.now})
		if err != nil {
			return backupCLIError("plan repository retention", err)
		}
		return printRepositoryRetentionPlan(a, res.GetPlan())
	}}
	bindRetentionFlags(cmd, &opts, false)
	return cmd
}

func NewAdminBackupClusterRetentionApplyCommand(a *app.App) *cobra.Command {
	opts := retentionCLIOptions{}
	cmd := &cobra.Command{Use: "apply", Short: "Apply repository backup retention pruning", RunE: func(cmd *cobra.Command, args []string) error {
		policy, err := retentionPolicyFromFlags(cmd, opts)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.ApplyRepositoryRetention(authCtx, &adminv1.ApplyRepositoryRetentionRequest{RepositoryId: opts.repositoryID, RetentionPolicy: policy, Reason: opts.reason, Now: opts.now})
		if err != nil {
			return backupCLIError("apply repository retention", err)
		}
		return printRepositoryRetentionPlan(a, res.GetPlan())
	}}
	bindRetentionFlags(cmd, &opts, true)
	return cmd
}

func NewAdminBackupClusterValidateCommand(a *app.App) *cobra.Command {
	var backupSetPath, repositoryID, backupSetID string
	cmd := &cobra.Command{Use: "validate", Short: "Validate a cluster backup set", RunE: func(cmd *cobra.Command, args []string) error {
		repositoryMode := strings.TrimSpace(repositoryID) != "" || strings.TrimSpace(backupSetID) != ""
		if repositoryMode && (strings.TrimSpace(repositoryID) == "" || strings.TrimSpace(backupSetID) == "") {
			return fmt.Errorf("--repository-id and --backup-set-id are required together")
		}
		if !repositoryMode && strings.TrimSpace(backupSetPath) == "" {
			return fmt.Errorf("--backup-set is required unless --repository-id and --backup-set-id are set")
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.ValidateClusterBackupSet(authCtx, &adminv1.ValidateClusterBackupSetRequest{BackupSetPath: backupSetPath, RepositoryId: repositoryID, BackupSetId: backupSetID})
		if err != nil {
			return backupCLIError("validate cluster backup set", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		if !res.GetValid() {
			return a.Print(res, fmt.Sprintf("cluster backup set invalid: %s\n", strings.Join(res.GetErrors(), "; ")))
		}
		return a.Print(res, fmt.Sprintf("cluster backup set valid: %s\n", res.GetBackupSet().GetBackupSetId()))
	}}
	cmd.Flags().StringVar(&backupSetPath, "backup-set", "", "backup-set directory or backup-set.json path")
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID for repository-truth validation")
	cmd.Flags().StringVar(&backupSetID, "backup-set-id", "", "backup set ID for repository-truth validation")
	return cmd
}

func NewAdminBackupClusterRestorePlanCommand(a *app.App) *cobra.Command {
	var backupSetPath, repositoryID, backupSetID, targetMode, identityMode, authSessionPolicy string
	cmd := &cobra.Command{Use: "restore-plan", Short: "Plan a cluster backup restore", RunE: func(cmd *cobra.Command, args []string) error {
		repositoryMode := strings.TrimSpace(repositoryID) != "" || strings.TrimSpace(backupSetID) != ""
		if repositoryMode {
			if strings.TrimSpace(repositoryID) == "" || strings.TrimSpace(backupSetID) == "" {
				return fmt.Errorf("--repository-id and --backup-set-id are required together")
			}
			target := parseRestoreTargetMode(targetMode)
			if target == adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_UNSPECIFIED {
				return fmt.Errorf("invalid --target-mode %q", targetMode)
			}
			identity := parseRestoreIdentityMode(identityMode)
			if identity == adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_UNSPECIFIED {
				return fmt.Errorf("invalid --identity-mode %q", identityMode)
			}
			authPolicy := parseRestoreAuthSessionPolicy(authSessionPolicy)
			if strings.TrimSpace(authSessionPolicy) != "" && authPolicy == adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_UNSPECIFIED {
				return fmt.Errorf("invalid --auth-session-policy %q", authSessionPolicy)
			}
			client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer closeConn()
			res, err := client.PlanClusterRestore(authCtx, &adminv1.PlanClusterRestoreRequest{RepositoryId: repositoryID, BackupSetId: backupSetID, TargetMode: target, IdentityMode: identity, AuthSessionPolicy: authPolicy})
			if err != nil {
				return backupCLIError("plan repository cluster restore", err)
			}
			if a.Output == "json" {
				return a.Print(res, "")
			}
			return a.Print(res, formatAdminClusterRestorePlan(res.GetPlan()))
		}
		if strings.TrimSpace(backupSetPath) == "" {
			return fmt.Errorf("--backup-set is required unless --repository-id and --backup-set-id are set")
		}
		plan, _, err := clusterbackup.BuildRestorePlan(cmd.Context(), backupSetPath)
		if err != nil {
			return err
		}
		plan = clusterbackup.SortRestorePlan(plan)
		if a.Output == "json" {
			return a.Print(plan, "")
		}
		return a.Print(plan, formatClusterRestorePlan(plan))
	}}
	cmd.Flags().StringVar(&backupSetPath, "backup-set", "", "backup-set directory or backup-set.json path")
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID for repository-truth restore planning")
	cmd.Flags().StringVar(&backupSetID, "backup-set-id", "", "backup set ID for repository-truth restore planning")
	cmd.Flags().StringVar(&targetMode, "target-mode", "new-cluster-bootstrap", "restore target mode: new-cluster-bootstrap or existing-cluster-replace")
	cmd.Flags().StringVar(&identityMode, "identity-mode", "preserve-backup-id", "restore identity mode: preserve-backup-id, adopt-target-id, or generate-new-id")
	cmd.Flags().StringVar(&authSessionPolicy, "auth-session-policy", "", "auth session policy: preserve-restored-sessions or revoke-all-sessions")
	return cmd
}

const restoreLocalDataDirFlag = "data" + "-dir"

func NewAdminBackupClusterRestoreLocalCommand(a *app.App) *cobra.Command {
	var backupSetPath, dataDir string
	var ordinal int
	cmd := &cobra.Command{Use: "restore-local", Short: "Restore one cluster backup ordinal into an offline local data dir", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(backupSetPath) == "" {
			return fmt.Errorf("--backup-set is required")
		}
		if strings.TrimSpace(dataDir) == "" {
			return fmt.Errorf("--data-dir is required")
		}
		result, err := clusterbackup.RestoreLocal(cmd.Context(), clusterbackup.RestoreLocalInput{BackupSet: backupSetPath, Ordinal: ordinal, DataDir: dataDir})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(result, "")
		}
		text := fmt.Sprintf("cluster backup ordinal restored: %s\nordinal: %d\npod: %s\ndata_dir: %s\narchive: %s\n", result.BackupSetID, result.Ordinal, result.PodName, result.DataDir, result.ArchivePath)
		return a.Print(result, text)
	}}
	cmd.Flags().StringVar(&backupSetPath, "backup-set", "", "backup-set directory or backup-set.json path")
	cmd.Flags().IntVar(&ordinal, "ordinal", -1, "cluster node ordinal to restore")
	cmd.Flags().StringVar(&dataDir, restoreLocalDataDirFlag, "", "empty local data directory to restore into")
	_ = cmd.MarkFlagRequired("ordinal")
	return cmd
}

func formatAdminClusterRestorePlan(plan *adminv1.RestorePlanSummary) string {
	if plan == nil {
		return "cluster backup restore plan: <nil>\n"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "cluster backup restore plan: %s\n", plan.GetRestorePlanId())
	fmt.Fprintf(&b, "repository: %s\nbackup_set_id: %s\ntarget_mode: %s\nidentity_mode: %s\nauth_session_policy: %s\nrestorable: %t\n", plan.GetRepositoryId(), plan.GetBackupSetId(), formatRestoreTargetMode(plan.GetTargetMode()), formatRestoreIdentityMode(plan.GetIdentityMode()), formatRestoreAuthSessionPolicy(plan.GetAuthSessionPolicy()), plan.GetRestorable())
	if validation := plan.GetValidation(); validation != nil {
		fmt.Fprintf(&b, "validation: %s\n", validation.GetState().String())
		if validation.GetTotalArtifactBytes() > 0 {
			fmt.Fprintf(&b, "total_artifact_bytes: %d\n", validation.GetTotalArtifactBytes())
		}
	}
	if capacity := plan.GetCapacity(); capacity != nil && capacity.GetRequiredBytes() > 0 {
		fmt.Fprintf(&b, "required_bytes: %d\n", capacity.GetRequiredBytes())
	}
	if len(plan.GetDerivedActions()) > 0 {
		b.WriteString("derived_actions:\n")
		for _, action := range plan.GetDerivedActions() {
			fmt.Fprintf(&b, "  - subsystem: %s\n    mode: %s\n", action.GetSubsystem().String(), action.GetMode().String())
		}
	}
	if len(plan.GetBlockers()) > 0 {
		b.WriteString("blockers:\n")
		for _, blocker := range plan.GetBlockers() {
			fmt.Fprintf(&b, "  - reason: %s\n    detail: %s\n", blocker.GetReason(), blocker.GetDetail())
		}
	}
	return b.String()
}

func formatClusterRestorePlan(plan clusterbackup.RestorePlan) string {
	var b strings.Builder
	fmt.Fprintf(&b, "cluster backup restore plan: %s\n", plan.BackupSetID)
	fmt.Fprintf(&b, "cluster_id: %s\nstate: %s\nmanifest: %s\nexpected_nodes: %d\narchive_format: %s\n", plan.ClusterID, plan.State, plan.ManifestPath, plan.ExpectedNodes, plan.ArchiveFormat)
	if len(plan.Warnings) > 0 {
		b.WriteString("warnings:\n")
		for _, warning := range plan.Warnings {
			fmt.Fprintf(&b, "  - %s\n", warning)
		}
	}
	b.WriteString("nodes:\n")
	for _, node := range plan.Nodes {
		fmt.Fprintf(&b, "  - ordinal: %d\n    pod: %s\n    node_id: %s\n", node.Ordinal, node.PodName, node.NodeID)
		if node.RaftNodeID > 0 {
			fmt.Fprintf(&b, "    raft_node_id: %d\n", node.RaftNodeID)
		}
		fmt.Fprintf(&b, "    archive: %s\n    manifest: %s\n    checksum_sha256: %s\n", firstNonEmptyBackup(node.ArchivePath, node.ArchiveURI, node.ArchiveName), firstNonEmptyBackup(node.ManifestPath, node.ManifestURI, node.ManifestName), node.ChecksumSHA256)
	}
	return b.String()
}

func parseRestoreTargetMode(value string) adminv1.ClusterRestoreTargetMode {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", "-")) {
	case "", "new-cluster-bootstrap", "new":
		return adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_NEW_CLUSTER_BOOTSTRAP
	case "existing-cluster-replace", "replace":
		return adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_EXISTING_CLUSTER_REPLACE
	default:
		return adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_UNSPECIFIED
	}
}

func parseRestoreIdentityMode(value string) adminv1.ClusterRestoreIdentityMode {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", "-")) {
	case "", "preserve-backup-id", "preserve":
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID
	case "adopt-target-id", "adopt":
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_ADOPT_TARGET_ID
	case "generate-new-id", "generate":
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_GENERATE_NEW_ID
	default:
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_UNSPECIFIED
	}
}

func parseRestoreAuthSessionPolicy(value string) adminv1.ClusterRestoreAuthSessionPolicy {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", "-")) {
	case "":
		return adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_UNSPECIFIED
	case "preserve-restored-sessions", "preserve":
		return adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS
	case "revoke-all-sessions", "revoke":
		return adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_REVOKE_ALL_SESSIONS
	default:
		return adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_UNSPECIFIED
	}
}

func formatRestoreTargetMode(mode adminv1.ClusterRestoreTargetMode) string {
	switch mode {
	case adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_NEW_CLUSTER_BOOTSTRAP:
		return "NEW_CLUSTER_BOOTSTRAP"
	case adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_EXISTING_CLUSTER_REPLACE:
		return "EXISTING_CLUSTER_REPLACE"
	default:
		return "UNSPECIFIED"
	}
}

func formatRestoreIdentityMode(mode adminv1.ClusterRestoreIdentityMode) string {
	switch mode {
	case adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID:
		return "PRESERVE_BACKUP_ID"
	case adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_ADOPT_TARGET_ID:
		return "ADOPT_TARGET_ID"
	case adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_GENERATE_NEW_ID:
		return "GENERATE_NEW_ID"
	default:
		return "UNSPECIFIED"
	}
}

func formatRestoreAuthSessionPolicy(policy adminv1.ClusterRestoreAuthSessionPolicy) string {
	switch policy {
	case adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS:
		return "PRESERVE_RESTORED_SESSIONS"
	case adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_REVOKE_ALL_SESSIONS:
		return "REVOKE_ALL_SESSIONS"
	default:
		return "UNSPECIFIED"
	}
}

type retentionCLIOptions struct {
	repositoryID          string
	keepLastSuccessful    int32
	keepFor               string
	deleteIncompleteAfter string
	now                   string
	reason                string
}

func bindRetentionFlags(cmd *cobra.Command, opts *retentionCLIOptions, includeReason bool) {
	cmd.Flags().StringVar(&opts.repositoryID, "repository-id", "", "backup repository ID; defaults to policy default_repository_id")
	cmd.Flags().Int32Var(&opts.keepLastSuccessful, "keep-last-successful", 0, "override number of newest successful backups to keep")
	cmd.Flags().StringVar(&opts.keepFor, "keep-for", "", "override age duration for successful backups to keep, for example 336h or 14d")
	cmd.Flags().StringVar(&opts.deleteIncompleteAfter, "delete-incomplete-after", "", "override age duration before incomplete backup prefixes are deleted")
	cmd.Flags().StringVar(&opts.now, "now", "", "RFC3339 planning time for deterministic previews")
	if includeReason {
		cmd.Flags().StringVar(&opts.reason, "reason", "", "operator reason for applying retention")
	}
}

func retentionPolicyFromFlags(cmd *cobra.Command, opts retentionCLIOptions) (*adminv1.RetentionPolicy, error) {
	changed := cmd.Flags().Changed("keep-last-successful") || cmd.Flags().Changed("keep-for") || cmd.Flags().Changed("delete-incomplete-after")
	if !changed {
		return nil, nil
	}
	policy := &adminv1.RetentionPolicy{}
	if cmd.Flags().Changed("keep-last-successful") {
		if opts.keepLastSuccessful < 0 {
			return nil, fmt.Errorf("--keep-last-successful must not be negative")
		}
		policy.KeepLastSuccessful = opts.keepLastSuccessful
	}
	if cmd.Flags().Changed("keep-for") {
		seconds, err := parseRetentionDurationSeconds(opts.keepFor)
		if err != nil {
			return nil, fmt.Errorf("invalid --keep-for: %w", err)
		}
		policy.KeepForSeconds = seconds
	}
	if cmd.Flags().Changed("delete-incomplete-after") {
		seconds, err := parseRetentionDurationSeconds(opts.deleteIncompleteAfter)
		if err != nil {
			return nil, fmt.Errorf("invalid --delete-incomplete-after: %w", err)
		}
		policy.DeleteIncompleteAfterSeconds = seconds
	}
	return policy, nil
}

func parseRetentionDurationSeconds(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if strings.HasSuffix(value, "d") && len(value) > 1 {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 64)
		if err != nil {
			return 0, err
		}
		if days < 0 {
			return 0, fmt.Errorf("duration must not be negative")
		}
		return int64((time.Duration(days) * 24 * time.Hour).Seconds()), nil
	}
	seconds, err := parseDurationSeconds(value)
	if err != nil {
		return 0, err
	}
	if seconds < 0 {
		return 0, fmt.Errorf("duration must not be negative")
	}
	return seconds, nil
}

func printRepositoryRetentionPlan(a *app.App, plan *adminv1.RepositoryRetentionPlan) error {
	if a.Output == "json" {
		return a.Print(plan, "")
	}
	if plan == nil {
		return a.Print(plan, "repository retention plan: <nil>\n")
	}
	var b strings.Builder
	mode := "apply"
	if plan.GetDryRun() {
		mode = "dry-run"
	}
	fmt.Fprintf(&b, "repository retention plan: %s\n", mode)
	fmt.Fprintf(&b, "  repository: %s\n", plan.GetRepositoryId())
	fmt.Fprintf(&b, "  planned_at: %s\n", plan.GetPlannedAt())
	if policy := plan.GetRetentionPolicy(); policy != nil {
		fmt.Fprintf(&b, "  keep_last_successful: %d\n", policy.GetKeepLastSuccessful())
		fmt.Fprintf(&b, "  keep_for: %s\n", formatSeconds(policy.GetKeepForSeconds()))
		fmt.Fprintf(&b, "  delete_incomplete_after: %s\n", formatSeconds(policy.GetDeleteIncompleteAfterSeconds()))
	}
	fmt.Fprintf(&b, "  deleted_objects: %d\n", plan.GetDeletedObjects())
	for _, warning := range plan.GetWarnings() {
		fmt.Fprintf(&b, "  warning: %s\n", warning)
	}
	for _, action := range plan.GetActions() {
		line := fmt.Sprintf("  - %s %s reason=%s", action.GetAction(), action.GetBackupSetId(), action.GetReason())
		if action.GetDeletedObjects() > 0 {
			line += fmt.Sprintf(" deleted_objects=%d", action.GetDeletedObjects())
		}
		if action.GetBlocked() {
			line += " blocked=true"
		}
		if action.GetError() != "" {
			line += " error=" + action.GetError()
		}
		fmt.Fprintln(&b, line)
	}
	return a.Print(plan, b.String())
}

func printBackupObservability(a *app.App, snapshot *adminv1.BackupObservabilitySnapshot) error {
	if a.Output == "json" {
		return a.Print(snapshot, "")
	}
	if snapshot == nil {
		return a.Print(snapshot, "backup observability: <nil>\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "backup observability: %s\n", firstNonEmptyBackup(snapshot.GetSummaryState(), "unknown"))
	fmt.Fprintf(&b, "  observed_at: %s\n", snapshot.GetObservedAt())
	if snapshot.GetRepositoryId() != "" {
		fmt.Fprintf(&b, "  repository: %s\n", snapshot.GetRepositoryId())
	}
	firing := 0
	for _, alert := range snapshot.GetAlerts() {
		if alert.GetFiring() {
			firing++
		}
	}
	fmt.Fprintf(&b, "  metrics: %d\n", len(snapshot.GetMetrics()))
	fmt.Fprintf(&b, "  firing_alerts: %d\n", firing)
	if firing == 0 {
		fmt.Fprintln(&b, "  alerts: none firing")
	} else {
		fmt.Fprintln(&b, "  alerts:")
		for _, alert := range snapshot.GetAlerts() {
			if !alert.GetFiring() {
				continue
			}
			fmt.Fprintf(&b, "  - [%s] %s reason=%s\n", alert.GetSeverity(), alert.GetName(), alert.GetReason())
			for _, evidence := range alert.GetEvidence() {
				fmt.Fprintf(&b, "      evidence: %s\n", evidence)
			}
		}
	}
	return a.Print(snapshot, b.String())
}

func adminBackupClient(ctx context.Context, a *app.App) (adminv1.AdminBackupServiceClient, context.Context, func(), error) {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return nil, nil, nil, err
	}
	return adminv1.NewAdminBackupServiceClient(conn), authCtx, func() { _ = conn.Close() }, nil
}

func printBackupPolicy(a *app.App, policy *adminv1.BackupPolicy) error {
	if a.Output == "json" {
		return a.Print(policy, "")
	}
	if policy == nil {
		return a.Print(policy, "backup policy: <nil>\n")
	}
	text := fmt.Sprintf("backup policy:\n  enabled: %t\n  dir: %s\n  schedule: %s\n  interval_hours: %d\n  time_of_day: %s\n  timezone: %s\n  weekdays: %s\n  run_missed: %t\n  keep: %d\n  include_logs: %t\n  archive_format: %s\n  quiesce_timeout: %s\n  backup_timeout: %s\n  retry_after: %s\n  history_limit: %d\n  allow_reads_during_backup: %t\n", policy.GetEnabled(), policy.GetBackupDir(), firstNonEmptyBackup(policy.GetScheduleKind(), "interval"), policy.GetIntervalHours(), policy.GetTimeOfDay(), policy.GetTimezone(), formatBackupWeekdays(policy.GetWeekdays()), policy.GetRunMissed(), policy.GetRetentionCount(), policy.GetIncludeLogs(), formatBackupArchiveFormat(policy.GetArchiveFormat(), policy.GetCompression()), formatSeconds(policy.GetQuiesceDrainTimeoutSeconds()), formatSeconds(policy.GetBackupTimeoutSeconds()), formatSeconds(policy.GetRetryAfterSeconds()), policy.GetStatusHistoryLimit(), policy.GetAllowReadsDuringBackup())
	return a.Print(policy, text)
}

func firstNonEmptyBackup(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func formatClusterBackupSetRows(backups []*adminv1.ClusterBackupSetSummary) string {
	var b strings.Builder
	for _, backup := range backups {
		repository := ""
		if backup.GetRepositoryId() != "" {
			repository = "\t" + backup.GetRepositoryId()
		}
		fmt.Fprintf(&b, "%s\t%s\t%d nodes%s\n", backup.GetBackupSetId(), backup.GetState(), backup.GetExpectedNodes(), repository)
	}
	return b.String()
}

func parseBackupCaptureProvider(value string) (adminv1.BackupCaptureProvider, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_AUTO, nil
	case "filesystem_immutable_copy", "filesystem", "fs-copy":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY, nil
	default:
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_UNSPECIFIED, fmt.Errorf("invalid --capture-provider %q: use auto or filesystem_immutable_copy", value)
	}
}

func parseBackupQuiesceReadProfile(value string) (adminv1.BackupQuiesceReadProfile, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "control_only", "control-only":
		return adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY, nil
	case "strict_read_only", "strict-read-only":
		return adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_STRICT_READ_ONLY, nil
	default:
		return adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_UNSPECIFIED, fmt.Errorf("invalid --quiesce-read-profile %q: use control_only or strict_read_only", value)
	}
}

func parseBackupArchiveFormat(value string) (adminv1.BackupArchiveFormat, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "zip":
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP, nil
	case "tar":
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR, nil
	case "tar.gz", "tgz":
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ, nil
	case "tar.zst", "tzst":
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST, nil
	default:
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_UNSPECIFIED, fmt.Errorf("invalid --archive-format %q: use zip, tar, tar.gz, or tar.zst", value)
	}
}

func formatBackupArchiveFormat(format adminv1.BackupArchiveFormat, legacyCompression string) string {
	switch format {
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP:
		return "zip"
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR:
		return "tar"
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ:
		return "tar.gz"
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST:
		return "tar.zst"
	default:
		return firstNonEmptyBackup(legacyCompression, "zip")
	}
}

func parseBackupWeekdays(values []string) ([]int32, error) {
	out := []int32{}
	seen := map[int32]bool{}
	for _, raw := range values {
		value := strings.ToLower(strings.TrimSpace(raw))
		if value == "" {
			return nil, fmt.Errorf("invalid --weekday: value is required")
		}
		weekday, ok := backupWeekdayValue(value)
		if !ok {
			return nil, fmt.Errorf("invalid --weekday %q: use sun..sat or 0..6", raw)
		}
		if !seen[weekday] {
			seen[weekday] = true
			out = append(out, weekday)
		}
	}
	return out, nil
}

func backupWeekdayValue(value string) (int32, bool) {
	switch value {
	case "0", "sun", "sunday":
		return 0, true
	case "1", "mon", "monday":
		return 1, true
	case "2", "tue", "tues", "tuesday":
		return 2, true
	case "3", "wed", "wednesday":
		return 3, true
	case "4", "thu", "thur", "thurs", "thursday":
		return 4, true
	case "5", "fri", "friday":
		return 5, true
	case "6", "sat", "saturday":
		return 6, true
	default:
		return 0, false
	}
}

func formatBackupWeekdays(values []int32) string {
	if len(values) == 0 {
		return ""
	}
	names := make([]string, 0, len(values))
	for _, value := range values {
		switch value {
		case 0:
			names = append(names, "Sunday")
		case 1:
			names = append(names, "Monday")
		case 2:
			names = append(names, "Tuesday")
		case 3:
			names = append(names, "Wednesday")
		case 4:
			names = append(names, "Thursday")
		case 5:
			names = append(names, "Friday")
		case 6:
			names = append(names, "Saturday")
		default:
			names = append(names, strconv.Itoa(int(value)))
		}
	}
	return strings.Join(names, ",")
}

func parseDurationSeconds(value string) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("duration is required")
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		return seconds, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	return int64(d.Seconds()), nil
}

func formatSeconds(seconds int64) string {
	if seconds <= 0 {
		return "0s"
	}
	return (time.Duration(seconds) * time.Second).String()
}

func backupCLIError(action string, err error) error {
	if status.Code(err) == codes.Unavailable {
		return fmt.Errorf("%s: backup/quiesce temporarily unavailable: %w", action, err)
	}
	return fmt.Errorf("%s: %w", action, err)
}
