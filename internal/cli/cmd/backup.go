package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	cmd.AddCommand(policy, NewAdminBackupTriggerCommand(a), NewAdminBackupStatusCommand(a), NewAdminBackupListCommand(a), NewAdminBackupDeleteCommand(a))
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
	var backupDir, interval, quiesceTimeout, backupTimeout, retryAfter string
	var scheduleKind, timeOfDay, timezone, archiveFormat string
	var weekdays []string
	var retention, historyLimit int32
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
		if cmd.Flags().Changed("interval") {
			seconds, err := parseDurationSeconds(interval)
			if err != nil {
				return fmt.Errorf("invalid --interval: %w", err)
			}
			policy.IntervalSeconds = seconds
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
	cmd.Flags().StringVar(&interval, "interval", "", "backup interval duration, e.g. 24h")
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
	text := fmt.Sprintf("backup policy:\n  enabled: %t\n  dir: %s\n  schedule: %s\n  interval: %s\n  time_of_day: %s\n  timezone: %s\n  weekdays: %s\n  run_missed: %t\n  keep: %d\n  include_logs: %t\n  archive_format: %s\n  quiesce_timeout: %s\n  backup_timeout: %s\n  retry_after: %s\n  history_limit: %d\n  allow_reads_during_backup: %t\n", policy.GetEnabled(), policy.GetBackupDir(), firstNonEmptyBackup(policy.GetScheduleKind(), "interval"), formatSeconds(policy.GetIntervalSeconds()), policy.GetTimeOfDay(), policy.GetTimezone(), formatBackupWeekdays(policy.GetWeekdays()), policy.GetRunMissed(), policy.GetRetentionCount(), policy.GetIncludeLogs(), formatBackupArchiveFormat(policy.GetArchiveFormat(), policy.GetCompression()), formatSeconds(policy.GetQuiesceDrainTimeoutSeconds()), formatSeconds(policy.GetBackupTimeoutSeconds()), formatSeconds(policy.GetRetryAfterSeconds()), policy.GetStatusHistoryLimit(), policy.GetAllowReadsDuringBackup())
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
