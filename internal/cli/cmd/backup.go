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
	cmd := &cobra.Command{Use: "backup", Short: "Manage repository-backed deployment backups"}
	policy := &cobra.Command{Use: "policy", Short: "Manage repository-backed backup policy"}
	policy.AddCommand(NewAdminBackupPolicyGetCommand(a), NewAdminBackupPolicySetCommand(a))
	newRetention := func() *cobra.Command {
		retention := &cobra.Command{Use: "retention", Short: "Preview and apply repository backup retention"}
		retention.AddCommand(NewAdminBackupClusterRetentionPlanCommand(a), NewAdminBackupClusterRetentionApplyCommand(a))
		return retention
	}
	cmd.AddCommand(policy, NewAdminBackupClusterStartCommand(a), NewAdminBackupClusterStatusCommand(a), NewAdminBackupClusterObservabilityCommand(a), NewAdminBackupClusterCancelCommand(a), NewAdminBackupClusterListCommand(a), NewAdminBackupClusterDeleteCommand(a), NewAdminBackupClusterValidateCommand(a), newRetention(), NewAdminBackupClusterRestorePlanCommand(a), NewAdminBackupClusterRestoreRepositoryLocalCommand(a))
	cluster := &cobra.Command{Use: "cluster", Hidden: true, Short: "Deprecated alias for repository-backed backup commands"}
	cluster.AddCommand(NewAdminBackupClusterStartCommand(a), NewAdminBackupClusterStatusCommand(a), NewAdminBackupClusterObservabilityCommand(a), NewAdminBackupClusterCancelCommand(a), NewAdminBackupClusterListCommand(a), NewAdminBackupClusterDeleteCommand(a), NewAdminBackupClusterValidateCommand(a), newRetention(), NewAdminBackupClusterRestorePlanCommand(a), NewAdminBackupClusterRestoreRepositoryLocalCommand(a))
	cmd.AddCommand(cluster)
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
	var repositoryID, captureProvider, quiesceReadProfile string
	var allowReadOnly bool
	var keepLastSuccessful int32
	var keepFor, deleteIncompleteAfter string
	var includeArtifactChecks, includeExternalDependencyChecks, includeCapacityEstimates, requireCompleteMarker bool
	cmd := &cobra.Command{Use: "set", Short: "Update repository-backed backup policy", RunE: func(cmd *cobra.Command, args []string) error {
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
			policy = &adminv1.BackupPolicy{}
		}
		if cmd.Flags().Changed("default-repository-id") {
			policy.DefaultRepositoryId = strings.TrimSpace(repositoryID)
		}
		if cmd.Flags().Changed("capture-provider") {
			provider, err := parseBackupCaptureProvider(captureProvider)
			if err != nil {
				return err
			}
			policy.CaptureProvider = provider
		}
		if cmd.Flags().Changed("quiesce-read-profile") {
			profile, err := parseBackupQuiesceReadProfile(quiesceReadProfile)
			if err != nil {
				return err
			}
			policy.QuiesceReadProfile = profile
		}
		if cmd.Flags().Changed("allow-read-only-during-quiesce") {
			policy.AllowReadOnlyDuringQuiesce = allowReadOnly
		}
		if policy.RetentionPolicy == nil && (cmd.Flags().Changed("keep-last-successful") || cmd.Flags().Changed("keep-for") || cmd.Flags().Changed("delete-incomplete-after")) {
			policy.RetentionPolicy = &adminv1.RetentionPolicy{}
		}
		if cmd.Flags().Changed("keep-last-successful") {
			if keepLastSuccessful < 0 {
				return fmt.Errorf("--keep-last-successful must not be negative")
			}
			policy.RetentionPolicy.KeepLastSuccessful = keepLastSuccessful
		}
		if cmd.Flags().Changed("keep-for") {
			seconds, err := parseRetentionDurationSeconds(keepFor)
			if err != nil {
				return fmt.Errorf("invalid --keep-for: %w", err)
			}
			policy.RetentionPolicy.KeepForSeconds = seconds
		}
		if cmd.Flags().Changed("delete-incomplete-after") {
			seconds, err := parseRetentionDurationSeconds(deleteIncompleteAfter)
			if err != nil {
				return fmt.Errorf("invalid --delete-incomplete-after: %w", err)
			}
			policy.RetentionPolicy.DeleteIncompleteAfterSeconds = seconds
		}
		if policy.ValidationOptions == nil && (cmd.Flags().Changed("include-artifact-checks") || cmd.Flags().Changed("include-external-dependency-checks") || cmd.Flags().Changed("include-capacity-estimates") || cmd.Flags().Changed("require-complete-marker")) {
			policy.ValidationOptions = &adminv1.ValidationOptions{}
		}
		if cmd.Flags().Changed("include-artifact-checks") {
			policy.ValidationOptions.IncludeArtifactChecks = includeArtifactChecks
		}
		if cmd.Flags().Changed("include-external-dependency-checks") {
			policy.ValidationOptions.IncludeExternalDependencyChecks = includeExternalDependencyChecks
		}
		if cmd.Flags().Changed("include-capacity-estimates") {
			policy.ValidationOptions.IncludeCapacityEstimates = includeCapacityEstimates
		}
		if cmd.Flags().Changed("require-complete-marker") {
			policy.ValidationOptions.RequireCompleteMarker = requireCompleteMarker
		}
		res, err := client.UpdateBackupPolicy(authCtx, &adminv1.UpdateBackupPolicyRequest{Policy: policy})
		if err != nil {
			return backupCLIError("update backup policy", err)
		}
		return printBackupPolicy(a, res.GetPolicy())
	}}
	cmd.Flags().StringVar(&repositoryID, "default-repository-id", "", "default repository ID used when backup commands omit --repository-id")
	cmd.Flags().StringVar(&captureProvider, "capture-provider", "auto", "capture provider: auto, filesystem_immutable_copy, csi_volume_snapshot, or aws_ebs_snapshot")
	cmd.Flags().StringVar(&quiesceReadProfile, "quiesce-read-profile", "control_only", "backup quiesce read profile: control_only or strict_read_only")
	cmd.Flags().BoolVar(&allowReadOnly, "allow-read-only-during-quiesce", false, "allow configured read-only RPCs while backup quiesce is held")
	cmd.Flags().Int32Var(&keepLastSuccessful, "keep-last-successful", 0, "number of newest successful backup sets to retain")
	cmd.Flags().StringVar(&keepFor, "keep-for", "", "age duration for successful backup sets to retain, for example 336h or 14d")
	cmd.Flags().StringVar(&deleteIncompleteAfter, "delete-incomplete-after", "", "age duration before incomplete repository prefixes are deleted")
	cmd.Flags().BoolVar(&includeArtifactChecks, "include-artifact-checks", false, "include artifact byte/checksum checks during validation")
	cmd.Flags().BoolVar(&includeExternalDependencyChecks, "include-external-dependency-checks", false, "include external dependency checks during validation")
	cmd.Flags().BoolVar(&includeCapacityEstimates, "include-capacity-estimates", false, "include restore capacity estimates during validation")
	cmd.Flags().BoolVar(&requireCompleteMarker, "require-complete-marker", false, "require repository COMPLETE marker during validation")
	return cmd
}

func NewAdminBackupClusterStartCommand(a *app.App) *cobra.Command {
	var reason, archiveFormat, idempotencyKey string
	var repositoryID, captureProvider, quiesceReadProfile string
	var convergenceTimeout time.Duration
	var wait bool
	var waitTimeout time.Duration
	cmd := &cobra.Command{Use: "start", Short: "Start a repository-backed deployment backup operation", RunE: func(cmd *cobra.Command, args []string) error {
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
		req := &adminv1.StartBackupRequest{Reason: reason, ArchiveFormat: format, IdempotencyKey: idempotencyKey, RepositoryId: repositoryID, CaptureProvider: provider, QuiesceReadProfile: readProfile}
		if convergenceTimeout > 0 {
			req.ConvergenceTimeoutSeconds = int64(convergenceTimeout.Round(time.Second) / time.Second)
		}
		res, err := client.StartBackup(authCtx, req)
		if err != nil {
			return backupCLIError("start backup", err)
		}
		if wait {
			statusRes, err := waitForClusterBackup(cmd.Context(), client, authCtx, res.GetOperation().GetBackupSetId(), waitTimeout)
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(statusRes, "")
			}
			st := statusRes.GetOperation()
			return a.Print(statusRes, fmt.Sprintf("backup completed: %s\nstate: %s\nmanifest: %s\nerror: %s\n", st.GetBackupSetId(), st.GetState(), st.GetManifestUri(), st.GetError()))
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetOperation()
		return a.Print(res, fmt.Sprintf("backup started: %s\nstate: %s\n", st.GetBackupSetId(), st.GetState()))
	}}
	cmd.Flags().StringVar(&reason, "reason", "", "backup reason")
	cmd.Flags().StringVar(&archiveFormat, "archive-format", "tar.zst", "backup archive format: zip, tar, tar.gz, or tar.zst")
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "optional idempotency key for start retries")
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID for repository-backed production backup sets")
	cmd.Flags().StringVar(&captureProvider, "capture-provider", "auto", "capture provider: auto, filesystem_immutable_copy, csi_volume_snapshot, or aws_ebs_snapshot")
	cmd.Flags().StringVar(&quiesceReadProfile, "quiesce-read-profile", "control_only", "backup quiesce read profile: control_only or strict_read_only")
	cmd.Flags().DurationVar(&convergenceTimeout, "convergence-timeout", 0, "maximum time to wait for cluster convergence before failing the operation")
	cmd.Flags().BoolVar(&wait, "wait", false, "wait for the cluster backup operation to finish")
	cmd.Flags().DurationVar(&waitTimeout, "timeout", 10*time.Minute, "maximum time to wait with --wait")
	return cmd
}

func NewAdminBackupClusterCancelCommand(a *app.App) *cobra.Command {
	var reason string
	cmd := &cobra.Command{Use: "cancel [BACKUP_SET_ID]", Short: "Cancel a backup operation", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		backupSetID := ""
		if len(args) > 0 {
			backupSetID = args[0]
		}
		res, err := client.CancelBackupOperation(authCtx, &adminv1.CancelBackupOperationRequest{BackupSetId: backupSetID, Reason: reason})
		if err != nil {
			return backupCLIError("cancel backup", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetOperation()
		return a.Print(res, fmt.Sprintf("backup cancel requested: %s\nstate: %s\n", st.GetBackupSetId(), st.GetState()))
	}}
	cmd.Flags().StringVar(&reason, "reason", "", "cancellation reason")
	return cmd
}

func waitForClusterBackup(ctx context.Context, client adminv1.AdminBackupServiceClient, authCtx context.Context, backupSetID string, timeout time.Duration) (*adminv1.GetBackupOperationResponse, error) {
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
			return nil, backupCLIError("wait for backup", waitCtx.Err())
		default:
		}
		res, err := client.GetBackupOperation(authCtx, &adminv1.GetBackupOperationRequest{BackupSetId: backupSetID})
		if err != nil {
			return nil, backupCLIError("get backup operation", err)
		}
		switch res.GetOperation().GetState() {
		case adminv1.BackupOperationState_BACKUP_OPERATION_STATE_SUCCEEDED:
			return res, nil
		case adminv1.BackupOperationState_BACKUP_OPERATION_STATE_FAILED, adminv1.BackupOperationState_BACKUP_OPERATION_STATE_CANCELED:
			return res, fmt.Errorf("backup %s finished with state %s: %s", backupSetID, res.GetOperation().GetState(), res.GetOperation().GetError())
		}
		select {
		case <-waitCtx.Done():
			return nil, backupCLIError("wait for backup", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func NewAdminBackupClusterStatusCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "status [BACKUP_SET_ID]", Short: "Show backup operation status", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		backupSetID := ""
		if len(args) > 0 {
			backupSetID = args[0]
		}
		res, err := client.GetBackupOperation(authCtx, &adminv1.GetBackupOperationRequest{BackupSetId: backupSetID})
		if err != nil {
			return backupCLIError("get backup operation", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetOperation()
		return a.Print(res, fmt.Sprintf("backup status: %s\nbackup_set_id: %s\ndeployment_id: %s\nmanifest: %s\nerror: %s\n", st.GetState(), st.GetBackupSetId(), st.GetDeploymentId(), st.GetManifestUri(), st.GetError()))
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
	cmd := &cobra.Command{Use: "list", Aliases: []string{"ls"}, Short: "List repository-backed backup sets", RunE: func(cmd *cobra.Command, args []string) error {
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.ListBackupSets(authCtx, &adminv1.ListBackupSetsRequest{RepositoryId: repositoryID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return backupCLIError("list backup sets", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		return a.Print(res.GetBackupSets(), formatClusterBackupSetRows(res.GetBackupSets()))
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 0, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID; defaults to policy default_repository_id")
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
		res, err := client.DeleteBackupSet(authCtx, &adminv1.DeleteBackupSetRequest{RepositoryId: repositoryID, BackupSetId: args[0], Reason: reason})
		if err != nil {
			return backupCLIError("delete cluster backup set", err)
		}
		return a.Print(res, fmt.Sprintf("backup set deleted: %s\nrepository: %s\nstate: %s\n", res.GetBackupSetId(), res.GetRepositoryId(), res.GetState()))
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
	var repositoryID, backupSetID string
	cmd := &cobra.Command{Use: "validate", Short: "Validate a repository-backed backup set", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(backupSetID) == "" {
			return fmt.Errorf("--backup-set-id is required")
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.ValidateBackupSet(authCtx, &adminv1.ValidateBackupSetRequest{RepositoryId: repositoryID, BackupSetId: backupSetID})
		if err != nil {
			return backupCLIError("validate backup set", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		if !res.GetValid() {
			return a.Print(res, fmt.Sprintf("backup set invalid: %s\n", strings.Join(res.GetErrors(), "; ")))
		}
		return a.Print(res, fmt.Sprintf("backup set valid: %s\n", res.GetBackupSet().GetBackupSetId()))
	}}
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID; defaults to policy default_repository_id")
	cmd.Flags().StringVar(&backupSetID, "backup-set-id", "", "backup set ID")
	return cmd
}

func NewAdminBackupClusterRestorePlanCommand(a *app.App) *cobra.Command {
	var repositoryID, backupSetID, targetMode, identityMode, authSessionPolicy string
	cmd := &cobra.Command{Use: "restore-plan", Short: "Plan a repository-backed backup restore", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(backupSetID) == "" {
			return fmt.Errorf("--backup-set-id is required")
		}
		target := parseRestoreTargetMode(targetMode)
		if target == adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_UNSPECIFIED {
			return fmt.Errorf("invalid --target-mode %q", targetMode)
		}
		identity := parseRestoreIdentityMode(identityMode)
		if identity == adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_UNSPECIFIED {
			return fmt.Errorf("invalid --identity-mode %q", identityMode)
		}
		authPolicy := parseRestoreAuthSessionPolicy(authSessionPolicy)
		if strings.TrimSpace(authSessionPolicy) != "" && authPolicy == adminv1.RestoreAuthSessionPolicy_RESTORE_AUTH_SESSION_POLICY_UNSPECIFIED {
			return fmt.Errorf("invalid --auth-session-policy %q", authSessionPolicy)
		}
		client, authCtx, closeConn, err := adminBackupClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.PlanRestore(authCtx, &adminv1.PlanRestoreRequest{RepositoryId: repositoryID, BackupSetId: backupSetID, TargetMode: target, IdentityMode: identity, AuthSessionPolicy: authPolicy})
		if err != nil {
			return backupCLIError("plan repository restore", err)
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		return a.Print(res, formatAdminClusterRestorePlan(res.GetPlan()))
	}}
	cmd.Flags().StringVar(&repositoryID, "repository-id", "", "backup repository ID; defaults to policy default_repository_id")
	cmd.Flags().StringVar(&backupSetID, "backup-set-id", "", "backup set ID for repository-truth restore planning")
	cmd.Flags().StringVar(&targetMode, "target-mode", "new-deployment-bootstrap", "restore target mode: new-deployment-bootstrap or existing-deployment-replace")
	cmd.Flags().StringVar(&identityMode, "identity-mode", "preserve-backup-id", "restore identity mode: preserve-backup-id, adopt-target-id, or generate-new-id")
	cmd.Flags().StringVar(&authSessionPolicy, "auth-session-policy", "", "auth session policy: preserve-restored-sessions or revoke-all-sessions")
	return cmd
}

const restoreLocalDataDirFlag = "data" + "-dir"

func formatAdminClusterRestorePlan(plan *adminv1.RestorePlanSummary) string {
	if plan == nil {
		return "backup restore plan: <nil>\n"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "backup restore plan: %s\n", plan.GetRestorePlanId())
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

func parseRestoreTargetMode(value string) adminv1.RestoreTargetMode {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", "-")) {
	case "", "new-deployment-bootstrap", "new-cluster-bootstrap", "new":
		return adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_NEW_DEPLOYMENT_BOOTSTRAP
	case "existing-deployment-replace", "existing-cluster-replace", "replace":
		return adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_EXISTING_DEPLOYMENT_REPLACE
	default:
		return adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_UNSPECIFIED
	}
}

func parseRestoreIdentityMode(value string) adminv1.RestoreIdentityMode {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", "-")) {
	case "", "preserve-backup-id", "preserve":
		return adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID
	case "adopt-target-id", "adopt":
		return adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_ADOPT_TARGET_ID
	case "generate-new-id", "generate":
		return adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_GENERATE_NEW_ID
	default:
		return adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_UNSPECIFIED
	}
}

func parseRestoreAuthSessionPolicy(value string) adminv1.RestoreAuthSessionPolicy {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", "-")) {
	case "":
		return adminv1.RestoreAuthSessionPolicy_RESTORE_AUTH_SESSION_POLICY_UNSPECIFIED
	case "preserve-restored-sessions", "preserve":
		return adminv1.RestoreAuthSessionPolicy_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS
	case "revoke-all-sessions", "revoke":
		return adminv1.RestoreAuthSessionPolicy_RESTORE_AUTH_SESSION_POLICY_REVOKE_ALL_SESSIONS
	default:
		return adminv1.RestoreAuthSessionPolicy_RESTORE_AUTH_SESSION_POLICY_UNSPECIFIED
	}
}

func formatRestoreTargetMode(mode adminv1.RestoreTargetMode) string {
	switch mode {
	case adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_NEW_DEPLOYMENT_BOOTSTRAP:
		return "NEW_DEPLOYMENT_BOOTSTRAP"
	case adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_EXISTING_DEPLOYMENT_REPLACE:
		return "EXISTING_DEPLOYMENT_REPLACE"
	default:
		return "UNSPECIFIED"
	}
}

func formatRestoreIdentityMode(mode adminv1.RestoreIdentityMode) string {
	switch mode {
	case adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID:
		return "PRESERVE_BACKUP_ID"
	case adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_ADOPT_TARGET_ID:
		return "ADOPT_TARGET_ID"
	case adminv1.RestoreIdentityMode_RESTORE_IDENTITY_MODE_GENERATE_NEW_ID:
		return "GENERATE_NEW_ID"
	default:
		return "UNSPECIFIED"
	}
}

func formatRestoreAuthSessionPolicy(policy adminv1.RestoreAuthSessionPolicy) string {
	switch policy {
	case adminv1.RestoreAuthSessionPolicy_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS:
		return "PRESERVE_RESTORED_SESSIONS"
	case adminv1.RestoreAuthSessionPolicy_RESTORE_AUTH_SESSION_POLICY_REVOKE_ALL_SESSIONS:
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
	var b strings.Builder
	fmt.Fprintf(&b, "backup policy:\n")
	fmt.Fprintf(&b, "  default_repository_id: %s\n", policy.GetDefaultRepositoryId())
	fmt.Fprintf(&b, "  capture_provider: %s\n", policy.GetCaptureProvider().String())
	fmt.Fprintf(&b, "  quiesce_read_profile: %s\n", policy.GetQuiesceReadProfile().String())
	fmt.Fprintf(&b, "  allow_read_only_during_quiesce: %t\n", policy.GetAllowReadOnlyDuringQuiesce())
	if retention := policy.GetRetentionPolicy(); retention != nil {
		fmt.Fprintf(&b, "  keep_last_successful: %d\n", retention.GetKeepLastSuccessful())
		fmt.Fprintf(&b, "  keep_for: %s\n", formatSeconds(retention.GetKeepForSeconds()))
		fmt.Fprintf(&b, "  delete_incomplete_after: %s\n", formatSeconds(retention.GetDeleteIncompleteAfterSeconds()))
	}
	if validation := policy.GetValidationOptions(); validation != nil {
		fmt.Fprintf(&b, "  include_artifact_checks: %t\n", validation.GetIncludeArtifactChecks())
		fmt.Fprintf(&b, "  include_external_dependency_checks: %t\n", validation.GetIncludeExternalDependencyChecks())
		fmt.Fprintf(&b, "  include_capacity_estimates: %t\n", validation.GetIncludeCapacityEstimates())
		fmt.Fprintf(&b, "  require_complete_marker: %t\n", validation.GetRequireCompleteMarker())
	}
	return a.Print(policy, b.String())
}

func firstNonEmptyBackup(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func formatClusterBackupSetRows(backups []*adminv1.BackupSetSummary) string {
	var b strings.Builder
	for _, backup := range backups {
		repository := ""
		if backup.GetRepositoryId() != "" {
			repository = "\t" + backup.GetRepositoryId()
		}
		fmt.Fprintf(&b, "%s\t%s\t%d nodes%s\n", backup.GetBackupSetId(), backup.GetState(), backup.GetExpectedNodeCount(), repository)
	}
	return b.String()
}

func parseBackupCaptureProvider(value string) (adminv1.BackupCaptureProvider, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_AUTO, nil
	case "filesystem_immutable_copy", "filesystem", "fs-copy":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY, nil
	case "csi_volume_snapshot", "csi", "volumesnapshot", "volume_snapshot":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_CSI_VOLUME_SNAPSHOT, nil
	case "aws_ebs_snapshot", "ebs", "aws-ebs", "ebs_snapshot":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_AWS_EBS_SNAPSHOT, nil
	default:
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_UNSPECIFIED, fmt.Errorf("invalid --capture-provider %q: use auto, filesystem_immutable_copy, csi_volume_snapshot, or aws_ebs_snapshot", value)
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
