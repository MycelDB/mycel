package admin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	activitymodel "github.com/myceldb/mycel/internal/activity/model"
	activityservice "github.com/myceldb/mycel/internal/activity/service"
	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	daemonbackup "github.com/myceldb/mycel/internal/backup/service"
	"github.com/myceldb/mycel/internal/clustering"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminBackupService struct {
	adminv1.UnimplementedAdminBackupServiceServer
	manager       daemonbackup.Manager
	quiesce       *quiesce.Coordinator
	cluster       *clustering.Manager
	clusterConfig daemonconfig.ClusterConfig
	authorizer    OperatorAuthorizer
	activity      activityservice.Manager
}

func NewAdminBackupService(manager daemonbackup.Manager, quiesce *quiesce.Coordinator, authorizer OperatorAuthorizer, activity ...activityservice.Manager) *AdminBackupService {
	svc := &AdminBackupService{manager: manager, quiesce: quiesce, authorizer: authorizer}
	if len(activity) > 0 {
		svc.activity = activity[0]
	}
	return svc
}

func (s *AdminBackupService) emit(ctx context.Context, severity, category, eventType, message string, mutate func(*activitymodel.Event)) {
	if s.activity == nil {
		return
	}
	_ = s.activity.Emit(ctx, severity, category, eventType, message, mutate)
}

func (s *AdminBackupService) WithClusterRuntime(cluster *clustering.Manager, cfg daemonconfig.ClusterConfig) *AdminBackupService {
	s.cluster = cluster
	s.clusterConfig = cfg
	return s
}

func (s *AdminBackupService) GetBackupPolicy(ctx context.Context, req *adminv1.GetBackupPolicyRequest) (*adminv1.GetBackupPolicyResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	return &adminv1.GetBackupPolicyResponse{Policy: mapBackupPolicy(s.manager.Policy())}, nil
}

func (s *AdminBackupService) UpdateBackupPolicy(ctx context.Context, req *adminv1.UpdateBackupPolicyRequest) (*adminv1.UpdateBackupPolicyResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	if req.GetPolicy() == nil {
		return nil, status.Error(codes.InvalidArgument, "policy is required")
	}
	policy, err := s.manager.UpdatePolicy(ctx, backupPolicyFromProto(req.GetPolicy()))
	if err != nil {
		return nil, mapBackupError(err, "update backup policy")
	}
	return &adminv1.UpdateBackupPolicyResponse{Policy: mapBackupPolicy(policy)}, nil
}

func (s *AdminBackupService) ListBackupRepositories(ctx context.Context, req *adminv1.ListBackupRepositoriesRequest) (*adminv1.ListBackupRepositoriesResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	repositoryManager, ok := s.manager.(daemonbackup.BackupRepositoryManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "backup repository catalog is not configured")
	}
	items := make([]*adminv1.BackupRepository, 0)
	for _, repo := range repositoryManager.ListBackupRepositories() {
		items = append(items, mapBackupRepository(repo))
	}
	pageSize := normalizePageSize(req.GetPageSize())
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if offset > len(items) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the backup repository list")
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListBackupRepositoriesResponse{Repositories: items[offset:end], NextPageToken: next}, nil
}

func (s *AdminBackupService) GetBackupRepository(ctx context.Context, req *adminv1.GetBackupRepositoryRequest) (*adminv1.GetBackupRepositoryResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	repositoryManager, ok := s.manager.(daemonbackup.BackupRepositoryManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "backup repository catalog is not configured")
	}
	repo, err := repositoryManager.GetBackupRepository(req.GetRepositoryId())
	if err != nil {
		return nil, mapBackupError(err, "get backup repository")
	}
	return &adminv1.GetBackupRepositoryResponse{Repository: mapBackupRepository(repo)}, nil
}

func (s *AdminBackupService) CreateBackupRepository(ctx context.Context, req *adminv1.CreateBackupRepositoryRequest) (*adminv1.CreateBackupRepositoryResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	if req.GetRepository() == nil {
		return nil, status.Error(codes.InvalidArgument, "repository is required")
	}
	repositoryManager, ok := s.manager.(daemonbackup.BackupRepositoryManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "backup repository catalog is not configured")
	}
	repo, err := repositoryManager.CreateBackupRepository(ctx, backupRepositoryFromProto(req.GetRepository()))
	if err != nil {
		return nil, mapBackupError(err, "create backup repository")
	}
	return &adminv1.CreateBackupRepositoryResponse{Repository: mapBackupRepository(repo)}, nil
}

func (s *AdminBackupService) UpdateBackupRepository(ctx context.Context, req *adminv1.UpdateBackupRepositoryRequest) (*adminv1.UpdateBackupRepositoryResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	if req.GetRepository() == nil {
		return nil, status.Error(codes.InvalidArgument, "repository is required")
	}
	repositoryManager, ok := s.manager.(daemonbackup.BackupRepositoryManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "backup repository catalog is not configured")
	}
	repo, err := repositoryManager.UpdateBackupRepository(ctx, backupRepositoryFromProto(req.GetRepository()))
	if err != nil {
		return nil, mapBackupError(err, "update backup repository")
	}
	return &adminv1.UpdateBackupRepositoryResponse{Repository: mapBackupRepository(repo)}, nil
}

func (s *AdminBackupService) ValidateBackupRepository(ctx context.Context, req *adminv1.ValidateBackupRepositoryRequest) (*adminv1.ValidateBackupRepositoryResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	repositoryManager, ok := s.manager.(daemonbackup.BackupRepositoryManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "backup repository catalog is not configured")
	}
	validation, err := repositoryManager.ValidateBackupRepository(ctx, req.GetRepositoryId())
	if err != nil {
		return nil, mapBackupError(err, "validate backup repository")
	}
	repo, err := repositoryManager.GetBackupRepository(req.GetRepositoryId())
	if err != nil {
		return nil, mapBackupError(err, "get backup repository")
	}
	return &adminv1.ValidateBackupRepositoryResponse{Repository: mapBackupRepository(repo), Validation: mapRepositoryValidation(validation)}, nil
}

func (s *AdminBackupService) DeleteBackupRepository(ctx context.Context, req *adminv1.DeleteBackupRepositoryRequest) (*adminv1.DeleteBackupRepositoryResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	repositoryManager, ok := s.manager.(daemonbackup.BackupRepositoryManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "backup repository catalog is not configured")
	}
	if err := repositoryManager.DeleteBackupRepository(ctx, req.GetRepositoryId()); err != nil {
		return nil, mapBackupError(err, "delete backup repository")
	}
	return &adminv1.DeleteBackupRepositoryResponse{RepositoryId: req.GetRepositoryId(), State: adminv1.BackupDeletionState_BACKUP_DELETION_STATE_DELETED}, nil
}

func (s *AdminBackupService) TriggerBackup(ctx context.Context, req *adminv1.TriggerBackupRequest) (*adminv1.TriggerBackupResponse, error) {
	principal, err := s.requireBackupManage(ctx)
	if err != nil {
		return nil, err
	}
	reason := firstNonEmptyAdmin(req.GetReason(), "manual backup")
	s.emit(ctx, activitymodel.SeverityInfo, activitymodel.CategoryBackup, "backup.started", "Backup started", func(event *activitymodel.Event) {
		event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		event.IdempotencyKey = "backup.started:" + reason
	})
	result, err := s.manager.Trigger(ctx, backupcore.TriggerInput{Source: principal.PrincipalID, Reason: reason})
	if err != nil {
		s.emit(ctx, activitymodel.SeverityError, activitymodel.CategoryBackup, "backup.failed", "Backup failed", func(event *activitymodel.Event) {
			event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		})
		return nil, mapBackupError(err, "trigger backup")
	}
	statusValue := s.manager.RunStatus()
	s.emit(ctx, activitymodel.SeverityInfo, activitymodel.CategoryBackup, "backup.completed", "Backup completed", func(event *activitymodel.Event) {
		event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		event.Resource = activitymodel.Resource{Kind: "backup", ID: result.Manifest.BackupID, Name: result.Manifest.ArchiveName}
	})
	return &adminv1.TriggerBackupResponse{Status: s.mapStatus(statusValue), Backup: mapBackupSummary(result.Manifest)}, nil
}

func (s *AdminBackupService) GetBackupStatus(ctx context.Context, req *adminv1.GetBackupStatusRequest) (*adminv1.GetBackupStatusResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	quiesceStatus := s.mapQuiesceStatus()
	return &adminv1.GetBackupStatusResponse{Status: s.mapStatus(s.manager.RunStatus()), Quiesce: quiesceStatus}, nil
}

func (s *AdminBackupService) ListBackups(ctx context.Context, req *adminv1.ListBackupsRequest) (*adminv1.ListBackupsResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	manifests, err := s.manager.ListBackups(ctx)
	if err != nil {
		return nil, mapBackupError(err, "list backups")
	}
	items := make([]*adminv1.BackupSummary, 0, len(manifests))
	for _, manifest := range manifests {
		items = append(items, mapBackupSummary(manifest))
	}
	pageSize := normalizePageSize(req.GetPageSize())
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if offset > len(items) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the backup list")
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListBackupsResponse{Backups: items[offset:end], NextPageToken: next}, nil
}

func (s *AdminBackupService) DeleteBackup(ctx context.Context, req *adminv1.DeleteBackupRequest) (*adminv1.DeleteBackupResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	if err := s.manager.DeleteBackup(ctx, req.GetBackupId()); err != nil {
		return nil, mapBackupError(err, "delete backup")
	}
	return &adminv1.DeleteBackupResponse{BackupId: req.GetBackupId()}, nil
}

func (s *AdminBackupService) StartClusterBackup(ctx context.Context, req *adminv1.StartClusterBackupRequest) (*adminv1.StartClusterBackupResponse, error) {
	principal, err := s.requireBackupManage(ctx)
	if err != nil {
		return nil, err
	}
	nodes, clusterID, err := s.clusterBackupNodes()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	clusterManager, ok := s.manager.(daemonbackup.ClusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	input := daemonbackup.StartClusterBackupInput{Reason: firstNonEmptyAdmin(req.GetReason(), "cluster system backup by "+principal.PrincipalID), OutputDir: req.GetOutputDir(), ArchiveFormat: archiveFormatFromProto(req.GetArchiveFormat()), ClusterID: clusterID, ClusterName: s.clusterConfig.Name, StatefulSet: "myceld", Nodes: nodes, IdempotencyKey: req.GetIdempotencyKey(), RepositoryID: req.GetRepositoryId(), CaptureProvider: captureProviderFromProto(req.GetCaptureProvider()), QuiesceReadProfile: quiesceReadProfileFromProto(req.GetQuiesceReadProfile()), Validation: validationOptionsFromProto(req.GetValidationOptions())}
	if req.GetConvergenceTimeoutSeconds() > 0 {
		input.ConvergenceTimeout = time.Duration(req.GetConvergenceTimeoutSeconds()) * time.Second
	}
	st, err := clusterManager.StartClusterBackup(ctx, input)
	if err != nil {
		return nil, mapBackupError(err, "start cluster backup")
	}
	return &adminv1.StartClusterBackupResponse{Status: mapClusterBackupStatus(st), BackupSet: mapClusterBackupSetSummary(st)}, nil
}

func (s *AdminBackupService) GetClusterBackupStatus(ctx context.Context, req *adminv1.GetClusterBackupStatusRequest) (*adminv1.GetClusterBackupStatusResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	clusterManager, ok := s.manager.(daemonbackup.ClusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	backupSetID := firstNonEmptyAdmin(req.GetBackupSetId(), req.GetBackupOperationId())
	st, err := clusterManager.ClusterBackupStatus(backupSetID)
	if err != nil {
		return nil, mapBackupError(err, "get cluster backup status")
	}
	return &adminv1.GetClusterBackupStatusResponse{Status: mapClusterBackupStatus(st)}, nil
}

func (s *AdminBackupService) CancelClusterBackup(ctx context.Context, req *adminv1.CancelClusterBackupRequest) (*adminv1.CancelClusterBackupResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	clusterManager, ok := s.manager.(daemonbackup.ClusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	backupSetID := firstNonEmptyAdmin(req.GetBackupSetId(), req.GetBackupOperationId())
	st, err := clusterManager.CancelClusterBackup(ctx, backupSetID, req.GetReason())
	if err != nil {
		return nil, mapBackupError(err, "cancel cluster backup")
	}
	return &adminv1.CancelClusterBackupResponse{Status: mapClusterBackupStatus(st)}, nil
}

func (s *AdminBackupService) ListClusterBackups(ctx context.Context, req *adminv1.ListClusterBackupsRequest) (*adminv1.ListClusterBackupsResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	clusterManager, ok := s.manager.(daemonbackup.ClusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	statuses := clusterManager.ListClusterBackups()
	items := make([]*adminv1.ClusterBackupSetSummary, 0, len(statuses))
	for _, st := range statuses {
		items = append(items, mapClusterBackupSetSummary(st))
	}
	pageSize := normalizePageSize(req.GetPageSize())
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if offset > len(items) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the cluster backup list")
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListClusterBackupsResponse{BackupSets: items[offset:end], NextPageToken: next}, nil
}

func (s *AdminBackupService) ListClusterBackupOperations(ctx context.Context, req *adminv1.ListClusterBackupOperationsRequest) (*adminv1.ListClusterBackupOperationsResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	clusterManager, ok := s.manager.(daemonbackup.ClusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	statuses := clusterManager.ListClusterBackups()
	items := make([]*adminv1.ClusterBackupStatus, 0, len(statuses))
	for _, st := range statuses {
		items = append(items, mapClusterBackupStatus(st))
	}
	pageSize := normalizePageSize(req.GetPageSize())
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if offset > len(items) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the cluster backup operation list")
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListClusterBackupOperationsResponse{Operations: items[offset:end], NextPageToken: next}, nil
}

func (s *AdminBackupService) ListClusterBackupSets(ctx context.Context, req *adminv1.ListClusterBackupSetsRequest) (*adminv1.ListClusterBackupSetsResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetRepositoryId()) != "" {
		repositoryManager, ok := s.manager.(daemonbackup.RepositoryClusterBackupSetManager)
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "repository-backed backup-set catalog is not configured")
		}
		sets, err := repositoryManager.ListRepositoryClusterBackupSets(ctx, req.GetRepositoryId())
		if err != nil {
			return nil, mapBackupError(err, "list repository backup sets")
		}
		items := make([]*adminv1.ClusterBackupSetSummary, 0, len(sets))
		for _, set := range sets {
			summary := mapClusterManifestSummary(set.Manifest)
			if set.ProductionManifest != nil {
				applyProductionBackupSetSummary(summary, *set.ProductionManifest, set.ProductionMarker, set.ProductionReport)
			}
			items = append(items, summary)
		}
		return paginateClusterBackupSetSummaries(items, req.GetPageSize(), req.GetPageToken())
	}
	clusterManager, ok := s.manager.(daemonbackup.ClusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	statuses := clusterManager.ListClusterBackups()
	items := make([]*adminv1.ClusterBackupSetSummary, 0, len(statuses))
	for _, st := range statuses {
		items = append(items, mapClusterBackupSetSummary(st))
	}
	pageSize := normalizePageSize(req.GetPageSize())
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if offset > len(items) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the cluster backup set list")
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListClusterBackupSetsResponse{BackupSets: items[offset:end], NextPageToken: next}, nil
}

func (s *AdminBackupService) GetClusterBackupSet(ctx context.Context, req *adminv1.GetClusterBackupSetRequest) (*adminv1.GetClusterBackupSetResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetRepositoryId()) != "" {
		repositoryManager, ok := s.manager.(daemonbackup.RepositoryClusterBackupSetManager)
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "repository-backed backup-set catalog is not configured")
		}
		set, err := repositoryManager.GetRepositoryClusterBackupSet(ctx, req.GetRepositoryId(), req.GetBackupSetId())
		if err != nil {
			return nil, mapBackupError(err, "get repository backup set")
		}
		summary := mapClusterManifestSummary(set.Manifest)
		if set.ProductionManifest != nil {
			applyProductionBackupSetSummary(summary, *set.ProductionManifest, set.ProductionMarker, set.ProductionReport)
		}
		return &adminv1.GetClusterBackupSetResponse{BackupSet: summary}, nil
	}
	clusterManager, ok := s.manager.(daemonbackup.ClusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	for _, st := range clusterManager.ListClusterBackups() {
		if st.BackupSetID == req.GetBackupSetId() {
			return &adminv1.GetClusterBackupSetResponse{BackupSet: mapClusterBackupSetSummary(st)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "cluster backup set not found")
}

func (s *AdminBackupService) DeleteClusterBackupSet(ctx context.Context, req *adminv1.DeleteClusterBackupSetRequest) (*adminv1.DeleteClusterBackupSetResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	repositoryManager, ok := s.manager.(daemonbackup.RepositoryClusterBackupSetManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "repository backup-set catalog is not configured")
	}
	if strings.TrimSpace(req.GetRepositoryId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "repository_id is required")
	}
	if strings.TrimSpace(req.GetBackupSetId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "backup_set_id is required")
	}
	if _, err := repositoryManager.DeleteRepositoryClusterBackupSet(ctx, req.GetRepositoryId(), req.GetBackupSetId()); err != nil {
		return nil, mapBackupError(err, "delete repository backup set")
	}
	return &adminv1.DeleteClusterBackupSetResponse{RepositoryId: req.GetRepositoryId(), BackupSetId: req.GetBackupSetId(), State: adminv1.BackupDeletionState_BACKUP_DELETION_STATE_DELETED}, nil
}

func (s *AdminBackupService) PlanRepositoryRetention(ctx context.Context, req *adminv1.PlanRepositoryRetentionRequest) (*adminv1.PlanRepositoryRetentionResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	manager, ok := s.manager.(daemonbackup.RepositoryRetentionManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "repository retention planner is not configured")
	}
	now, err := parseOptionalBackupTime(req.GetNow(), "now")
	if err != nil {
		return nil, err
	}
	plan, err := manager.PlanRepositoryRetention(ctx, daemonbackup.RepositoryRetentionPlanOptions{RepositoryID: req.GetRepositoryId(), Policy: retentionPolicyFromProto(req.GetRetentionPolicy()), Now: now})
	if err != nil {
		return nil, mapBackupError(err, "plan repository retention")
	}
	return &adminv1.PlanRepositoryRetentionResponse{Plan: mapRepositoryRetentionPlan(plan)}, nil
}

func (s *AdminBackupService) ApplyRepositoryRetention(ctx context.Context, req *adminv1.ApplyRepositoryRetentionRequest) (*adminv1.ApplyRepositoryRetentionResponse, error) {
	principal, err := s.requireBackupManage(ctx)
	if err != nil {
		return nil, err
	}
	manager, ok := s.manager.(daemonbackup.RepositoryRetentionManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "repository retention manager is not configured")
	}
	now, err := parseOptionalBackupTime(req.GetNow(), "now")
	if err != nil {
		return nil, err
	}
	reason := firstNonEmptyAdmin(req.GetReason(), "repository retention requested by "+principal.PrincipalID)
	s.emit(ctx, activitymodel.SeverityInfo, activitymodel.CategoryBackup, "backup.repository_retention.started", "Repository backup retention started", func(event *activitymodel.Event) {
		event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		event.Resource = activitymodel.Resource{Kind: "backup_repository", ID: req.GetRepositoryId()}
		event.IdempotencyKey = "backup.repository_retention:" + req.GetRepositoryId() + ":" + reason
	})
	plan, err := manager.ApplyRepositoryRetention(ctx, daemonbackup.RepositoryRetentionPlanOptions{RepositoryID: req.GetRepositoryId(), Policy: retentionPolicyFromProto(req.GetRetentionPolicy()), Now: now})
	if err != nil {
		s.emit(ctx, activitymodel.SeverityError, activitymodel.CategoryBackup, "backup.repository_retention.failed", "Repository backup retention failed", func(event *activitymodel.Event) {
			event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
			event.Resource = activitymodel.Resource{Kind: "backup_repository", ID: req.GetRepositoryId()}
		})
		return nil, mapBackupError(err, "apply repository retention")
	}
	s.emit(ctx, activitymodel.SeverityInfo, activitymodel.CategoryBackup, "backup.repository_retention.completed", "Repository backup retention completed", func(event *activitymodel.Event) {
		event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		event.Resource = activitymodel.Resource{Kind: "backup_repository", ID: plan.RepositoryID}
	})
	return &adminv1.ApplyRepositoryRetentionResponse{Plan: mapRepositoryRetentionPlan(plan)}, nil
}

func (s *AdminBackupService) PlanClusterRestore(ctx context.Context, req *adminv1.PlanClusterRestoreRequest) (*adminv1.PlanClusterRestoreResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	planner, ok := s.manager.(daemonbackup.ClusterRestorePlanner)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster restore planner is not configured")
	}
	plan, err := planner.PlanClusterRestore(ctx, daemonbackup.PlanClusterRestoreInput{RepositoryID: req.GetRepositoryId(), BackupSetID: req.GetBackupSetId(), TargetMode: restoreTargetModeFromProto(req.GetTargetMode()), IdentityMode: restoreIdentityModeFromProto(req.GetIdentityMode()), AuthSessionPolicy: restoreAuthSessionPolicyFromProto(req.GetAuthSessionPolicy()), ValidationOptions: validationOptionsForBackupSet(req.GetValidationOptions()), Target: restoreTargetFromProto(req.GetTarget()), Staging: restoreStagingFromProto(req.GetStaging())})
	if err != nil {
		return nil, mapBackupError(err, "plan cluster restore")
	}
	return &adminv1.PlanClusterRestoreResponse{Plan: mapRestorePlanSummary(plan)}, nil
}

func (s *AdminBackupService) StartClusterRestore(ctx context.Context, req *adminv1.StartClusterRestoreRequest) (*adminv1.StartClusterRestoreResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	manager, ok := s.manager.(daemonbackup.ClusterRestoreManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster restore manager is not configured")
	}
	op, err := manager.StartClusterRestore(ctx, daemonbackup.StartClusterRestoreInput{RestorePlanID: req.GetRestorePlanId(), RepositoryID: req.GetRepositoryId(), BackupSetID: req.GetBackupSetId(), TargetMode: restoreTargetModeFromProto(req.GetTargetMode()), IdentityMode: restoreIdentityModeFromProto(req.GetIdentityMode()), AuthSessionPolicy: restoreAuthSessionPolicyFromProto(req.GetAuthSessionPolicy()), IdempotencyKey: req.GetIdempotencyKey(), DestructiveConfirmation: req.GetDestructiveConfirmation(), Target: restoreTargetFromProto(req.GetTarget()), Staging: restoreStagingFromProto(req.GetStaging()), Preconditions: restorePreconditionsFromProto(req.GetPreconditions()), PlanFreshnessToken: req.GetPlanFreshnessToken()})
	if err != nil {
		return nil, mapBackupError(err, "start cluster restore")
	}
	return &adminv1.StartClusterRestoreResponse{Operation: mapClusterRestoreOperation(op)}, nil
}

func (s *AdminBackupService) GetClusterRestoreStatus(ctx context.Context, req *adminv1.GetClusterRestoreStatusRequest) (*adminv1.GetClusterRestoreStatusResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	manager, ok := s.manager.(daemonbackup.ClusterRestoreManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster restore manager is not configured")
	}
	op, err := manager.ClusterRestoreStatus(req.GetRestoreOperationId())
	if err != nil {
		return nil, mapBackupError(err, "get cluster restore status")
	}
	return &adminv1.GetClusterRestoreStatusResponse{Operation: mapClusterRestoreOperation(op)}, nil
}

func (s *AdminBackupService) ListClusterRestores(ctx context.Context, req *adminv1.ListClusterRestoresRequest) (*adminv1.ListClusterRestoresResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	manager, ok := s.manager.(daemonbackup.ClusterRestoreManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster restore manager is not configured")
	}
	operations := manager.ListClusterRestores()
	items := make([]*adminv1.ClusterRestoreOperation, 0, len(operations))
	for _, op := range operations {
		items = append(items, mapClusterRestoreOperation(op))
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	if offset > len(items) {
		offset = len(items)
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListClusterRestoresResponse{Operations: items[offset:end], NextPageToken: next}, nil
}

func (s *AdminBackupService) CancelClusterRestore(ctx context.Context, req *adminv1.CancelClusterRestoreRequest) (*adminv1.CancelClusterRestoreResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	manager, ok := s.manager.(daemonbackup.ClusterRestoreManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster restore manager is not configured")
	}
	op, err := manager.CancelClusterRestore(ctx, req.GetRestoreOperationId(), req.GetReason())
	if err != nil {
		return nil, mapBackupError(err, "cancel cluster restore")
	}
	return &adminv1.CancelClusterRestoreResponse{Operation: mapClusterRestoreOperation(op)}, nil
}

func (s *AdminBackupService) ValidateClusterBackupSet(ctx context.Context, req *adminv1.ValidateClusterBackupSetRequest) (*adminv1.ValidateClusterBackupSetResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	var result daemonbackup.ClusterBackupSetValidationResult
	var err error
	if strings.TrimSpace(req.GetRepositoryId()) != "" || strings.TrimSpace(req.GetBackupSetId()) != "" {
		if strings.TrimSpace(req.GetRepositoryId()) == "" || strings.TrimSpace(req.GetBackupSetId()) == "" {
			return nil, status.Error(codes.InvalidArgument, "repository_id and backup_set_id are required together")
		}
		repositoryManager, ok := s.manager.(daemonbackup.RepositoryClusterBackupSetManager)
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "repository-backed backup-set catalog is not configured")
		}
		result, err = repositoryManager.ValidateRepositoryClusterBackupSet(ctx, req.GetRepositoryId(), req.GetBackupSetId(), validationOptionsForBackupSet(req.GetValidationOptions()))
	} else {
		result, err = daemonbackup.ValidateClusterBackupSetDetailed(ctx, req.GetBackupSetPath())
	}
	res := &adminv1.ValidateClusterBackupSetResponse{Valid: err == nil}
	if result.Manifest.BackupSetID != "" {
		res.BackupSet = mapClusterManifestSummary(result.Manifest)
		if result.ProductionManifest != nil {
			applyProductionBackupSetSummary(res.BackupSet, *result.ProductionManifest, result.ProductionMarker, result.ProductionReport)
		}
	}
	if result.ProductionReport != nil {
		res.Validation = mapBackupSetValidationReport(*result.ProductionReport)
		if result.ProductionManifest != nil {
			enrichValidationSummaryFromManifest(res.Validation, *result.ProductionManifest)
		}
	}
	if err != nil {
		res.Errors = []string{err.Error()}
		return res, nil
	}
	return res, nil
}

func (s *AdminBackupService) requireBackupManage(ctx context.Context) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	if s.manager == nil {
		return daemonauth.Principal{}, status.Error(codes.FailedPrecondition, "backup service is not configured")
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, commonv1.Capability_CAPABILITY_SYSTEM_BACKUP_SPACE.String())
	if err != nil {
		return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required backup capability")
	}
	return principal, nil
}

func (s *AdminBackupService) mapStatus(in backupcore.RunStatus) *adminv1.BackupStatus {
	out := &adminv1.BackupStatus{BackupId: in.BackupID, State: string(in.State), StartedAt: formatBackupTime(in.StartedAt), CompletedAt: formatBackupTime(in.CompletedAt), ArchivePath: in.ArchivePath, ManifestPath: in.ManifestPath, Error: in.Error, LastSuccessAt: formatBackupTime(in.LastSuccessAt), NextRunAt: formatBackupTime(in.NextRunAt)}
	if s.quiesce != nil {
		for _, participant := range s.quiesce.Status().Participants {
			out.Participants = append(out.Participants, mapQuiesceParticipant(participant))
		}
	}
	return out
}

func (s *AdminBackupService) mapQuiesceStatus() *adminv1.QuiesceStatus {
	out := &adminv1.QuiesceStatus{}
	if s.quiesce == nil {
		return out
	}
	for _, participant := range s.quiesce.Status().Participants {
		out.Participants = append(out.Participants, mapQuiesceParticipant(participant))
	}
	return out
}

func mapBackupRepository(repo daemonbackup.BackupRepository) *adminv1.BackupRepository {
	return &adminv1.BackupRepository{
		RepositoryId:  repo.RepositoryID,
		DisplayName:   repo.DisplayName,
		Provider:      backupRepositoryProviderToProto(repo.Provider),
		S3:            mapS3RepositoryConfig(repo.S3),
		CredentialRef: mapSecretReference(repo.CredentialRef),
		Encryption:    mapEncryptionPolicy(repo.Encryption),
		TlsCaRef:      mapSecretReference(repo.TLSCARef),
		DevOnly:       repo.DevOnly,
		CreateTime:    formatBackupTime(repo.CreateTime),
		UpdateTime:    formatBackupTime(repo.UpdateTime),
		Validation:    mapRepositoryValidation(repo.Validation),
	}
}

func backupRepositoryFromProto(repo *adminv1.BackupRepository) daemonbackup.BackupRepository {
	if repo == nil {
		return daemonbackup.BackupRepository{}
	}
	return daemonbackup.BackupRepository{
		RepositoryID:  repo.GetRepositoryId(),
		DisplayName:   repo.GetDisplayName(),
		Provider:      backupRepositoryProviderFromProto(repo.GetProvider()),
		S3:            s3RepositoryConfigFromProto(repo.GetS3()),
		CredentialRef: secretReferenceFromProto(repo.GetCredentialRef()),
		Encryption:    encryptionPolicyFromProto(repo.GetEncryption()),
		TLSCARef:      secretReferenceFromProto(repo.GetTlsCaRef()),
		DevOnly:       repo.GetDevOnly(),
	}
}

func mapS3RepositoryConfig(cfg daemonbackup.S3CompatibleRepositoryConfig) *adminv1.S3CompatibleRepositoryConfig {
	return &adminv1.S3CompatibleRepositoryConfig{Bucket: cfg.Bucket, Prefix: cfg.Prefix, Region: cfg.Region, EndpointUri: cfg.EndpointURI, ForcePathStyle: cfg.ForcePathStyle, DisableTls: cfg.DisableTLS}
}

func s3RepositoryConfigFromProto(cfg *adminv1.S3CompatibleRepositoryConfig) daemonbackup.S3CompatibleRepositoryConfig {
	if cfg == nil {
		return daemonbackup.S3CompatibleRepositoryConfig{}
	}
	return daemonbackup.S3CompatibleRepositoryConfig{Bucket: cfg.GetBucket(), Prefix: cfg.GetPrefix(), Region: cfg.GetRegion(), EndpointURI: cfg.GetEndpointUri(), ForcePathStyle: cfg.GetForcePathStyle(), DisableTLS: cfg.GetDisableTls()}
}

func mapSecretReference(ref daemonbackup.SecretReference) *adminv1.SecretReference {
	return &adminv1.SecretReference{Scheme: secretReferenceSchemeToProto(ref.Scheme), Name: ref.Name, Namespace: ref.Namespace, Path: ref.Path, Keys: cloneStringMap(ref.Keys), Description: ref.Description}
}

func secretReferenceFromProto(ref *adminv1.SecretReference) daemonbackup.SecretReference {
	if ref == nil {
		return daemonbackup.SecretReference{}
	}
	return daemonbackup.SecretReference{Scheme: secretReferenceSchemeFromProto(ref.GetScheme()), Name: ref.GetName(), Namespace: ref.GetNamespace(), Path: ref.GetPath(), Keys: cloneStringMap(ref.GetKeys()), Description: ref.GetDescription()}
}

func mapEncryptionPolicy(policy daemonbackup.EncryptionPolicy) *adminv1.EncryptionPolicy {
	return &adminv1.EncryptionPolicy{Mode: encryptionModeToProto(policy.Mode), KeyRef: mapSecretReference(policy.KeyRef), KmsKeyId: policy.KMSKeyID, RequireBucketDefaultEncryption: policy.RequireBucketDefaultEncryption}
}

func encryptionPolicyFromProto(policy *adminv1.EncryptionPolicy) daemonbackup.EncryptionPolicy {
	if policy == nil {
		return daemonbackup.EncryptionPolicy{}
	}
	return daemonbackup.EncryptionPolicy{Mode: encryptionModeFromProto(policy.GetMode()), KeyRef: secretReferenceFromProto(policy.GetKeyRef()), KMSKeyID: policy.GetKmsKeyId(), RequireBucketDefaultEncryption: policy.GetRequireBucketDefaultEncryption()}
}

func mapRepositoryValidation(validation daemonbackup.BackupRepositoryValidationSummary) *adminv1.BackupRepositoryValidationSummary {
	out := &adminv1.BackupRepositoryValidationSummary{State: validationStateToProto(validation.State), CheckedAt: formatBackupTime(validation.CheckedAt), SanitizedError: validation.SanitizedError}
	for _, probe := range validation.Probes {
		out.Probes = append(out.Probes, &adminv1.ValidationProbeResult{Name: probe.Name, State: validationStateToProto(probe.State), Detail: probe.Detail, CheckedAt: formatBackupTime(probe.CheckedAt)})
	}
	for _, blocker := range validation.Blockers {
		out.Blockers = append(out.Blockers, mapValidationBlocker(blocker))
	}
	return out
}

func mapValidationBlocker(blocker daemonbackup.ValidationBlocker) *adminv1.ValidationBlocker {
	return &adminv1.ValidationBlocker{Reason: blocker.Reason, Detail: blocker.Detail, Retryable: blocker.Retryable, FieldPath: blocker.FieldPath}
}

func validationOptionsForBackupSet(options *adminv1.ValidationOptions) clusterbackup.BackupSetValidationOptions {
	if options == nil {
		return clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true, AllowLocalArtifactURIs: true}
	}
	return clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: options.GetRequireCompleteMarker(), ValidateLocalArtifactBytes: options.GetIncludeArtifactChecks(), AllowS3ArtifactURIs: true, AllowLocalArtifactURIs: true}
}

func paginateClusterBackupSetSummaries(items []*adminv1.ClusterBackupSetSummary, pageSizeValue int32, pageToken string) (*adminv1.ListClusterBackupSetsResponse, error) {
	pageSize := normalizePageSize(pageSizeValue)
	offset, err := parsePageToken(pageToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if offset > len(items) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the cluster backup set list")
	}
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListClusterBackupSetsResponse{BackupSets: items[offset:end], NextPageToken: next}, nil
}

func mapBackupSetValidationReport(report clusterbackup.BackupSetValidationReport) *adminv1.ValidationSummary {
	state := adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_VALID
	if !report.Valid() {
		state = adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_INVALID
	}
	out := &adminv1.ValidationSummary{State: state, CheckedAt: formatBackupTime(report.CheckedAt), TotalArtifactBytes: report.TotalArtifactBytes}
	for _, issue := range report.Errors {
		out.Blockers = append(out.Blockers, &adminv1.ValidationBlocker{Reason: issue.Reason, Detail: issue.Detail, Retryable: issue.Retryable, FieldPath: issue.FieldPath})
		out.Errors = append(out.Errors, issue.Detail)
	}
	for _, issue := range report.Warnings {
		out.Warnings = append(out.Warnings, issue.Detail)
	}
	return out
}

func mapRestorePlanSummary(plan daemonbackup.ClusterRestorePlan) *adminv1.RestorePlanSummary {
	out := &adminv1.RestorePlanSummary{RestorePlanId: plan.RestorePlanID, RepositoryId: plan.RepositoryID, BackupSetId: plan.BackupSetID, TargetMode: restoreTargetModeToProto(plan.TargetMode), IdentityMode: restoreIdentityModeToProto(plan.IdentityMode), AuthSessionPolicy: restoreAuthSessionPolicyToProto(plan.AuthSessionPolicy), Restorable: plan.Restorable, PlannedAt: formatBackupTime(plan.PlannedAt), Target: restoreTargetToProto(plan.Target), Staging: restoreStagingToProto(plan.Staging), RequiredPreconditions: restorePreconditionsToProto(plan.Preconditions)}
	if plan.Validation != nil {
		out.Validation = mapBackupSetValidationReport(*plan.Validation)
		if plan.ProductionManifest != nil {
			enrichValidationSummaryFromManifest(out.Validation, *plan.ProductionManifest)
		}
	}
	out.Compatibility = &adminv1.CompatibilitySummary{State: adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_VALID, RequiredFeatures: append([]string(nil), plan.Compatibility.RequiredFeatures...), StorageFormatVersions: copyStringMap(plan.Compatibility.StorageFormatVersions), MinRestoreMycelVersion: plan.Compatibility.MinRestoreMycelVersion, MaxRestoreMycelVersion: plan.Compatibility.MaxRestoreMycelVersion, SupportedRestoreTargetModes: append([]string(nil), plan.Compatibility.SupportedRestoreTargetModes...), SupportedIdentityModes: append([]string(nil), plan.Compatibility.SupportedIdentityModes...), RequiresSameTopology: plan.Compatibility.RequiresSameTopology, RequiresSourceFencingForPreservedIdentity: plan.Compatibility.RequiresSourceFencingForPreservedIdentity}
	if plan.ProductionManifest != nil {
		out.Compatibility.SourceVersion = plan.ProductionManifest.Source.MycelVersion
	}
	out.Capacity = &adminv1.CapacityEstimate{RequiredBytes: plan.Capacity.RequiredBytes, AvailableBytes: plan.Capacity.AvailableBytes}
	for _, blocker := range plan.Blockers {
		out.Blockers = append(out.Blockers, mapValidationBlocker(blocker))
	}
	if len(out.Blockers) > 0 {
		if out.Validation == nil {
			out.Validation = &adminv1.ValidationSummary{State: adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_INVALID, CheckedAt: formatBackupTime(plan.PlannedAt)}
		}
		out.Validation.Blockers = append(out.Validation.Blockers, out.Blockers...)
		out.Compatibility.State = adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_INVALID
	} else if out.Validation == nil {
		out.Validation = &adminv1.ValidationSummary{State: adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_VALID, CheckedAt: formatBackupTime(plan.PlannedAt)}
	}
	for _, action := range plan.DerivedActions {
		out.DerivedActions = append(out.DerivedActions, &adminv1.DerivedSubsystemPolicy{Subsystem: derivedSubsystemToProto(action.Subsystem), Mode: derivedPolicyModeToProto(action.Mode)})
	}
	return out
}

func restoreTargetFromProto(in *adminv1.ClusterRestoreTarget) daemonbackup.ClusterRestoreTarget {
	if in == nil {
		return daemonbackup.ClusterRestoreTarget{}
	}
	out := daemonbackup.ClusterRestoreTarget{TargetClusterID: in.GetTargetClusterId(), TargetClusterName: in.GetTargetClusterName(), Namespace: in.GetNamespace(), StatefulSet: in.GetStatefulSet(), ExpectedNodeCount: int(in.GetExpectedNodeCount()), Labels: copyStringMap(in.GetLabels()), Annotations: copyStringMap(in.GetAnnotations())}
	for _, node := range in.GetNodes() {
		out.Nodes = append(out.Nodes, restoreNodeMappingFromProto(node))
	}
	return out
}

func restoreTargetToProto(in daemonbackup.ClusterRestoreTarget) *adminv1.ClusterRestoreTarget {
	if in.TargetClusterID == "" && in.TargetClusterName == "" && in.Namespace == "" && in.StatefulSet == "" && in.ExpectedNodeCount == 0 && len(in.Nodes) == 0 && len(in.Labels) == 0 && len(in.Annotations) == 0 {
		return nil
	}
	out := &adminv1.ClusterRestoreTarget{TargetClusterId: in.TargetClusterID, TargetClusterName: in.TargetClusterName, Namespace: in.Namespace, StatefulSet: in.StatefulSet, ExpectedNodeCount: int32(in.ExpectedNodeCount), Labels: copyStringMap(in.Labels), Annotations: copyStringMap(in.Annotations)}
	for _, node := range in.Nodes {
		out.Nodes = append(out.Nodes, &adminv1.ClusterRestoreNodeMapping{Ordinal: int32(node.Ordinal), SourcePodName: node.SourcePodName, SourceNodeId: node.SourceNodeID, SourceArchiveUri: node.SourceArchiveURI, TargetPodName: node.TargetPodName, TargetNodeName: node.TargetNodeName, TargetPvcName: node.TargetPVCName, TargetDataDir: node.TargetDataDir, StagingPath: node.StagingPath})
	}
	return out
}

func restoreStagingFromProto(in *adminv1.ClusterRestoreStagingLocation) daemonbackup.ClusterRestoreStagingLocation {
	if in == nil {
		return daemonbackup.ClusterRestoreStagingLocation{}
	}
	out := daemonbackup.ClusterRestoreStagingLocation{StagingURI: in.GetStagingUri(), StorageClass: in.GetStorageClass(), RequiredBytes: in.GetRequiredBytes(), AvailableBytes: in.GetAvailableBytes(), CleanBeforeUse: in.GetCleanBeforeUse()}
	for _, blocker := range in.GetBlockers() {
		out.Blockers = append(out.Blockers, validationBlockerFromProto(blocker))
	}
	return out
}

func restoreStagingToProto(in daemonbackup.ClusterRestoreStagingLocation) *adminv1.ClusterRestoreStagingLocation {
	if in.StagingURI == "" && in.StorageClass == "" && in.RequiredBytes == 0 && in.AvailableBytes == 0 && !in.CleanBeforeUse && len(in.Blockers) == 0 {
		return nil
	}
	out := &adminv1.ClusterRestoreStagingLocation{StagingUri: in.StagingURI, StorageClass: in.StorageClass, RequiredBytes: in.RequiredBytes, AvailableBytes: in.AvailableBytes, CleanBeforeUse: in.CleanBeforeUse}
	for _, blocker := range in.Blockers {
		out.Blockers = append(out.Blockers, mapValidationBlocker(blocker))
	}
	return out
}

func restorePreconditionsFromProto(in *adminv1.ClusterRestorePreconditionEvidence) daemonbackup.ClusterRestorePreconditionEvidence {
	if in == nil {
		return daemonbackup.ClusterRestorePreconditionEvidence{}
	}
	out := daemonbackup.ClusterRestorePreconditionEvidence{CleanTarget: restoreCleanTargetProofFromProto(in.GetCleanTarget()), SourceFencing: restoreSourceFencingFromProto(in.GetSourceFencing())}
	for _, blocker := range in.GetBlockers() {
		out.Blockers = append(out.Blockers, validationBlockerFromProto(blocker))
	}
	return out
}

func restoreCleanTargetProofFromProto(in *adminv1.ClusterRestoreCleanTargetProof) daemonbackup.ClusterRestoreCleanTargetProof {
	if in == nil {
		return daemonbackup.ClusterRestoreCleanTargetProof{}
	}
	out := daemonbackup.ClusterRestoreCleanTargetProof{ProofID: in.GetProofId(), TargetEmpty: in.GetTargetEmpty(), OperatorAttestation: in.GetOperatorAttestation()}
	for _, node := range in.GetCheckedNodes() {
		out.CheckedNodes = append(out.CheckedNodes, restoreNodeMappingFromProto(node))
	}
	for _, blocker := range in.GetBlockers() {
		out.Blockers = append(out.Blockers, validationBlockerFromProto(blocker))
	}
	return out
}

func restoreSourceFencingFromProto(in *adminv1.ClusterRestoreSourceFencingEvidence) daemonbackup.ClusterRestoreSourceFencingEvidence {
	if in == nil {
		return daemonbackup.ClusterRestoreSourceFencingEvidence{}
	}
	out := daemonbackup.ClusterRestoreSourceFencingEvidence{SourceClusterFenced: in.GetSourceClusterFenced(), FenceTokenRef: in.GetFenceTokenRef(), FencedEndpoints: append([]string(nil), in.GetFencedEndpoints()...)}
	for _, blocker := range in.GetBlockers() {
		out.Blockers = append(out.Blockers, validationBlockerFromProto(blocker))
	}
	return out
}

func restoreNodeMappingFromProto(node *adminv1.ClusterRestoreNodeMapping) daemonbackup.ClusterRestoreNodeMapping {
	if node == nil {
		return daemonbackup.ClusterRestoreNodeMapping{}
	}
	return daemonbackup.ClusterRestoreNodeMapping{Ordinal: int(node.GetOrdinal()), SourcePodName: node.GetSourcePodName(), SourceNodeID: node.GetSourceNodeId(), SourceArchiveURI: node.GetSourceArchiveUri(), TargetPodName: node.GetTargetPodName(), TargetNodeName: node.GetTargetNodeName(), TargetPVCName: node.GetTargetPvcName(), TargetDataDir: node.GetTargetDataDir(), StagingPath: node.GetStagingPath()}
}

func restorePreconditionsToProto(in daemonbackup.ClusterRestorePreconditionEvidence) *adminv1.ClusterRestorePreconditionEvidence {
	if len(in.Blockers) == 0 && len(in.CleanTarget.Blockers) == 0 && len(in.CleanTarget.CheckedNodes) == 0 && !in.CleanTarget.TargetEmpty && len(in.SourceFencing.Blockers) == 0 && !in.SourceFencing.SourceClusterFenced {
		return nil
	}
	out := &adminv1.ClusterRestorePreconditionEvidence{CleanTarget: restoreCleanTargetProofToProto(in.CleanTarget), SourceFencing: restoreSourceFencingToProto(in.SourceFencing)}
	for _, blocker := range in.Blockers {
		out.Blockers = append(out.Blockers, mapValidationBlocker(blocker))
	}
	return out
}

func restoreCleanTargetProofToProto(in daemonbackup.ClusterRestoreCleanTargetProof) *adminv1.ClusterRestoreCleanTargetProof {
	if in.ProofID == "" && in.CheckedAt.IsZero() && !in.TargetEmpty && len(in.CheckedNodes) == 0 && len(in.Blockers) == 0 && in.OperatorAttestation == "" {
		return nil
	}
	out := &adminv1.ClusterRestoreCleanTargetProof{ProofId: in.ProofID, CheckedAt: formatBackupTime(in.CheckedAt), TargetEmpty: in.TargetEmpty, OperatorAttestation: in.OperatorAttestation}
	for _, node := range in.CheckedNodes {
		out.CheckedNodes = append(out.CheckedNodes, &adminv1.ClusterRestoreNodeMapping{Ordinal: int32(node.Ordinal), SourcePodName: node.SourcePodName, SourceNodeId: node.SourceNodeID, SourceArchiveUri: node.SourceArchiveURI, TargetPodName: node.TargetPodName, TargetNodeName: node.TargetNodeName, TargetPvcName: node.TargetPVCName, TargetDataDir: node.TargetDataDir, StagingPath: node.StagingPath})
	}
	for _, blocker := range in.Blockers {
		out.Blockers = append(out.Blockers, mapValidationBlocker(blocker))
	}
	return out
}

func restoreSourceFencingToProto(in daemonbackup.ClusterRestoreSourceFencingEvidence) *adminv1.ClusterRestoreSourceFencingEvidence {
	if !in.SourceClusterFenced && in.FencedAt.IsZero() && in.FenceTokenRef == "" && len(in.FencedEndpoints) == 0 && len(in.Blockers) == 0 {
		return nil
	}
	out := &adminv1.ClusterRestoreSourceFencingEvidence{SourceClusterFenced: in.SourceClusterFenced, FencedAt: formatBackupTime(in.FencedAt), FenceTokenRef: in.FenceTokenRef, FencedEndpoints: append([]string(nil), in.FencedEndpoints...)}
	for _, blocker := range in.Blockers {
		out.Blockers = append(out.Blockers, mapValidationBlocker(blocker))
	}
	return out
}

func validationBlockerFromProto(in *adminv1.ValidationBlocker) daemonbackup.ValidationBlocker {
	if in == nil {
		return daemonbackup.ValidationBlocker{}
	}
	return daemonbackup.ValidationBlocker{Reason: in.GetReason(), Detail: in.GetDetail(), Retryable: in.GetRetryable(), FieldPath: in.GetFieldPath()}
}

func mapClusterRestoreOperation(op daemonbackup.ClusterRestoreOperation) *adminv1.ClusterRestoreOperation {
	out := &adminv1.ClusterRestoreOperation{RestoreOperationId: op.RestoreOperationID, RestorePlanId: op.RestorePlanID, RepositoryId: op.RepositoryID, BackupSetId: op.BackupSetID, State: restoreStateToProto(op.State), TargetMode: restoreTargetModeToProto(op.TargetMode), IdentityMode: restoreIdentityModeToProto(op.IdentityMode), AuthSessionPolicy: restoreAuthSessionPolicyToProto(op.AuthSessionPolicy), CreatedAt: formatBackupTime(op.CreatedAt), UpdatedAt: formatBackupTime(op.UpdatedAt), CompletedAt: formatBackupTime(op.CompletedAt), Error: op.Error, Target: restoreTargetToProto(op.Target), Staging: restoreStagingToProto(op.Staging), Preconditions: restorePreconditionsToProto(op.Preconditions), Progress: restoreProgressToProto(op.Progress)}
	for _, blocker := range op.Blockers {
		out.Blockers = append(out.Blockers, mapValidationBlocker(blocker))
	}
	for _, task := range op.CleanupTasks {
		out.CleanupTasks = append(out.CleanupTasks, &adminv1.CleanupTask{TaskId: task.TaskID, ResourceKind: task.ResourceKind, ResourceUri: task.ResourceURI, State: task.State, LastError: task.LastError})
	}
	return out
}

func restoreStateToProto(state string) adminv1.ClusterRestoreState {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "PENDING":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_PENDING
	case "PLANNING":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_PLANNING
	case "VALIDATING_BACKUP_SET":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_VALIDATING_BACKUP_SET
	case "VALIDATING_TARGET":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_VALIDATING_TARGET
	case "ACQUIRING_RESTORE_LOCK":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_ACQUIRING_RESTORE_LOCK
	case "STAGING_ARTIFACTS":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_STAGING_ARTIFACTS
	case "READY_TO_APPLY":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_READY_TO_APPLY
	case "PREPARING_TARGET":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_PREPARING_TARGET
	case "APPLYING_ARTIFACTS":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_APPLYING_ARTIFACTS
	case "NORMALIZING_RESTORED_STATE":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_NORMALIZING_RESTORED_STATE
	case "POST_RESTORE_VALIDATING":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_POST_RESTORE_VALIDATING
	case "RESUMING_SERVICE":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_RESUMING_SERVICE
	case "SUCCEEDED":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_SUCCEEDED
	case "CANCELING":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_CANCELING
	case "CANCELED":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_CANCELED
	case "FAILED":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_FAILED
	case "FAILED_REQUIRES_OPERATOR":
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_FAILED_REQUIRES_OPERATOR
	default:
		return adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_UNSPECIFIED
	}
}

func restoreProgressToProto(in daemonbackup.ClusterRestoreProgress) *adminv1.ClusterRestoreProgress {
	if in.TotalNodes == 0 && in.StagedNodes == 0 && in.AppliedNodes == 0 && in.VerifiedNodes == 0 && in.BytesTotal == 0 && in.BytesCompleted == 0 && in.CurrentPhase == "" && len(in.Nodes) == 0 {
		return nil
	}
	out := &adminv1.ClusterRestoreProgress{TotalNodes: int32(in.TotalNodes), StagedNodes: int32(in.StagedNodes), AppliedNodes: int32(in.AppliedNodes), VerifiedNodes: int32(in.VerifiedNodes), BytesTotal: in.BytesTotal, BytesCompleted: in.BytesCompleted, CurrentPhase: in.CurrentPhase, CurrentNode: in.CurrentNode}
	for _, node := range in.Nodes {
		out.Nodes = append(out.Nodes, &adminv1.ClusterRestoreNodeProgress{Ordinal: int32(node.Ordinal), State: node.State, SourceArchiveUri: node.SourceArchiveURI, StagingUri: node.StagingURI, BytesTotal: node.BytesTotal, BytesCompleted: node.BytesCompleted, ChecksumSha256: node.ChecksumSHA256, LastError: node.LastError, Evidence: copyStringMap(node.Evidence)})
	}
	return out
}

func applyProductionBackupSetSummary(out *adminv1.ClusterBackupSetSummary, manifest clusterbackup.BackupSetManifestV1, marker *clusterbackup.CompleteMarkerV1, report *clusterbackup.BackupSetValidationReport) {
	if out == nil {
		return
	}
	out.RepositoryId = manifest.Repository.RepositoryID
	out.Compatibility = &adminv1.CompatibilitySummary{State: adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_VALID, SourceVersion: manifest.Source.MycelVersion, RequiredFeatures: append([]string(nil), manifest.Compatibility.RequiredFeatures...), StorageFormatVersions: copyStringMap(manifest.Compatibility.StorageFormatVersions), MinRestoreMycelVersion: manifest.Compatibility.MinRestoreMycelVersion, MaxRestoreMycelVersion: manifest.Compatibility.MaxRestoreMycelVersion, SupportedRestoreTargetModes: append([]string(nil), manifest.Compatibility.SupportedRestoreTargetModes...), SupportedIdentityModes: append([]string(nil), manifest.Compatibility.SupportedIdentityModes...), RequiresSameTopology: manifest.Compatibility.RequiresSameTopology, RequiresSourceFencingForPreservedIdentity: manifest.Compatibility.RequiresSourceFencingForPreservedIdentity}
	if report != nil {
		out.Validation = mapBackupSetValidationReport(*report)
		enrichValidationSummaryFromManifest(out.Validation, manifest)
		if !report.Valid() && out.Compatibility != nil {
			for _, issue := range report.Errors {
				if strings.HasPrefix(issue.FieldPath, "compatibility") {
					out.Compatibility.State = adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_INVALID
					out.Compatibility.Blockers = append(out.Compatibility.Blockers, &adminv1.ValidationBlocker{Reason: issue.Reason, Detail: issue.Detail, Retryable: issue.Retryable, FieldPath: issue.FieldPath})
				}
			}
		}
	}
	manifestSummary := &adminv1.BackupSetManifestSummary{ManifestVersion: int32(manifest.ManifestVersion), ManifestUri: out.ManifestUri, TotalArtifactBytes: parseSignedDecimalString(manifest.Validation.TotalArtifactBytes), ArtifactCount: int32(len(manifest.Artifacts.Nodes) + len(manifest.Artifacts.AdditionalManifests))}
	if marker != nil {
		manifestSummary.ManifestUri = marker.ManifestURI
		manifestSummary.ManifestChecksumSha256 = marker.ManifestChecksumSHA256
		manifestSummary.ManifestSizeBytes = parseSignedDecimalString(marker.ManifestSizeBytes)
		manifestSummary.TotalArtifactBytes = parseSignedDecimalString(marker.TotalArtifactBytes)
		manifestSummary.ArtifactCount = int32(marker.ArtifactCount)
	}
	out.Manifest = manifestSummary
}

func enrichValidationSummaryFromManifest(out *adminv1.ValidationSummary, manifest clusterbackup.BackupSetManifestV1) {
	if out == nil {
		return
	}
	out.ValidatorVersion = manifest.Validation.ValidatorVersion
	out.ExpectedNodeCount = int32(manifest.Validation.ExpectedNodeCount)
	out.NodeArtifactCount = int32(manifest.Validation.NodeArtifactCount)
	out.RequiredArtifactCount = int32(manifest.Validation.RequiredArtifactCount)
	out.ValidatedArtifactCount = int32(manifest.Validation.ValidatedArtifactCount)
	if out.TotalArtifactBytes == 0 {
		out.TotalArtifactBytes = parseSignedDecimalString(manifest.Validation.TotalArtifactBytes)
	}
	out.ChecksumAlgorithm = manifest.Validation.ChecksumAlgorithm
	out.Errors = append(out.Errors, manifest.Validation.Errors...)
	out.Warnings = append(out.Warnings, manifest.Validation.Warnings...)
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func parseSignedDecimalString(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func backupRepositoryProviderToProto(provider string) adminv1.BackupRepositoryProvider {
	switch provider {
	case daemonbackup.RepositoryProviderS3Compatible, "":
		return adminv1.BackupRepositoryProvider_BACKUP_REPOSITORY_PROVIDER_S3_COMPATIBLE
	default:
		return adminv1.BackupRepositoryProvider_BACKUP_REPOSITORY_PROVIDER_UNSPECIFIED
	}
}

func backupRepositoryProviderFromProto(provider adminv1.BackupRepositoryProvider) string {
	switch provider {
	case adminv1.BackupRepositoryProvider_BACKUP_REPOSITORY_PROVIDER_UNSPECIFIED, adminv1.BackupRepositoryProvider_BACKUP_REPOSITORY_PROVIDER_S3_COMPATIBLE:
		return daemonbackup.RepositoryProviderS3Compatible
	default:
		return ""
	}
}

func secretReferenceSchemeToProto(scheme string) adminv1.SecretReferenceScheme {
	switch scheme {
	case daemonbackup.SecretSchemeAWSIRSA:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_AWS_IRSA
	case daemonbackup.SecretSchemeK8sSecret:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_K8S_SECRET
	case daemonbackup.SecretSchemeExternalSecret:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_EXTERNAL_SECRET
	case daemonbackup.SecretSchemeVault:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_VAULT
	case daemonbackup.SecretSchemeKMSKey:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_KMS_KEY
	case daemonbackup.SecretSchemeTLSCA:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_TLS_CA
	case daemonbackup.SecretSchemeEnv:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_ENV
	case daemonbackup.SecretSchemeFileRef:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_FILE_REF
	default:
		return adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_UNSPECIFIED
	}
}

func secretReferenceSchemeFromProto(scheme adminv1.SecretReferenceScheme) string {
	switch scheme {
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_AWS_IRSA:
		return daemonbackup.SecretSchemeAWSIRSA
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_K8S_SECRET:
		return daemonbackup.SecretSchemeK8sSecret
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_EXTERNAL_SECRET:
		return daemonbackup.SecretSchemeExternalSecret
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_VAULT:
		return daemonbackup.SecretSchemeVault
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_KMS_KEY:
		return daemonbackup.SecretSchemeKMSKey
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_TLS_CA:
		return daemonbackup.SecretSchemeTLSCA
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_ENV:
		return daemonbackup.SecretSchemeEnv
	case adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_FILE_REF:
		return daemonbackup.SecretSchemeFileRef
	default:
		return ""
	}
}

func encryptionModeToProto(mode string) adminv1.EncryptionMode {
	switch mode {
	case daemonbackup.EncryptionModeSSEKMS:
		return adminv1.EncryptionMode_ENCRYPTION_MODE_SSE_KMS
	case daemonbackup.EncryptionModeSSES3:
		return adminv1.EncryptionMode_ENCRYPTION_MODE_SSE_S3
	case daemonbackup.EncryptionModeProviderManaged, "":
		return adminv1.EncryptionMode_ENCRYPTION_MODE_PROVIDER_MANAGED
	case daemonbackup.EncryptionModeClientEnvelope:
		return adminv1.EncryptionMode_ENCRYPTION_MODE_CLIENT_ENVELOPE
	case daemonbackup.EncryptionModeNone:
		return adminv1.EncryptionMode_ENCRYPTION_MODE_NONE
	default:
		return adminv1.EncryptionMode_ENCRYPTION_MODE_UNSPECIFIED
	}
}

func encryptionModeFromProto(mode adminv1.EncryptionMode) string {
	switch mode {
	case adminv1.EncryptionMode_ENCRYPTION_MODE_SSE_KMS:
		return daemonbackup.EncryptionModeSSEKMS
	case adminv1.EncryptionMode_ENCRYPTION_MODE_SSE_S3:
		return daemonbackup.EncryptionModeSSES3
	case adminv1.EncryptionMode_ENCRYPTION_MODE_CLIENT_ENVELOPE:
		return daemonbackup.EncryptionModeClientEnvelope
	case adminv1.EncryptionMode_ENCRYPTION_MODE_NONE:
		return daemonbackup.EncryptionModeNone
	case adminv1.EncryptionMode_ENCRYPTION_MODE_UNSPECIFIED, adminv1.EncryptionMode_ENCRYPTION_MODE_PROVIDER_MANAGED:
		return daemonbackup.EncryptionModeProviderManaged
	default:
		return ""
	}
}

func validationStateToProto(state string) adminv1.BackupValidationState {
	switch state {
	case daemonbackup.ValidationStateNotValidated, "":
		return adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_NOT_VALIDATED
	case daemonbackup.ValidationStateValidating:
		return adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_VALIDATING
	case daemonbackup.ValidationStateValid:
		return adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_VALID
	case daemonbackup.ValidationStateWarning:
		return adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_WARNING
	case daemonbackup.ValidationStateInvalid:
		return adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_INVALID
	case daemonbackup.ValidationStateUnsupported:
		return adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_UNSUPPORTED
	default:
		return adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_UNSPECIFIED
	}
}

func captureProviderToProto(provider string) adminv1.BackupCaptureProvider {
	switch provider {
	case "", backupcore.DefaultProductionCaptureProvider:
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_AUTO
	case "filesystem_immutable_copy":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY
	case "csi_volume_snapshot":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_CSI_VOLUME_SNAPSHOT
	case "aws_ebs_snapshot":
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_AWS_EBS_SNAPSHOT
	default:
		return adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_UNSPECIFIED
	}
}

func captureProviderFromProto(provider adminv1.BackupCaptureProvider) string {
	switch provider {
	case adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY:
		return "filesystem_immutable_copy"
	case adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_CSI_VOLUME_SNAPSHOT:
		return "csi_volume_snapshot"
	case adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_AWS_EBS_SNAPSHOT:
		return "aws_ebs_snapshot"
	case adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_UNSPECIFIED, adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_AUTO:
		return backupcore.DefaultProductionCaptureProvider
	default:
		return ""
	}
}

func quiesceReadProfileToProto(profile string) adminv1.BackupQuiesceReadProfile {
	switch profile {
	case "", backupcore.DefaultProductionQuiesceReadProfile:
		return adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY
	case "strict_read_only":
		return adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_STRICT_READ_ONLY
	default:
		return adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_UNSPECIFIED
	}
}

func quiesceReadProfileFromProto(profile adminv1.BackupQuiesceReadProfile) string {
	switch profile {
	case adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_STRICT_READ_ONLY:
		return "strict_read_only"
	case adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_UNSPECIFIED, adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY:
		return backupcore.DefaultProductionQuiesceReadProfile
	default:
		return ""
	}
}

func validationOptionsFromProto(options *adminv1.ValidationOptions) backupcore.ValidationOptions {
	if options == nil {
		return backupcore.ValidationOptions{}
	}
	return backupcore.ValidationOptions{IncludeArtifactChecks: options.GetIncludeArtifactChecks(), IncludeExternalDependencyChecks: options.GetIncludeExternalDependencyChecks(), IncludeCapacityEstimates: options.GetIncludeCapacityEstimates(), RequireCompleteMarker: options.GetRequireCompleteMarker()}
}

func restoreTargetModeToProto(mode string) adminv1.ClusterRestoreTargetMode {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case daemonbackup.ClusterRestoreTargetModeNewClusterBootstrap:
		return adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_NEW_CLUSTER_BOOTSTRAP
	case daemonbackup.ClusterRestoreTargetModeExistingClusterReplace:
		return adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_EXISTING_CLUSTER_REPLACE
	default:
		return adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_UNSPECIFIED
	}
}

func restoreTargetModeFromProto(mode adminv1.ClusterRestoreTargetMode) string {
	switch mode {
	case adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_NEW_CLUSTER_BOOTSTRAP:
		return daemonbackup.ClusterRestoreTargetModeNewClusterBootstrap
	case adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_EXISTING_CLUSTER_REPLACE:
		return daemonbackup.ClusterRestoreTargetModeExistingClusterReplace
	default:
		return ""
	}
}

func restoreIdentityModeToProto(mode string) adminv1.ClusterRestoreIdentityMode {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case daemonbackup.ClusterRestoreIdentityModePreserveBackupID:
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID
	case daemonbackup.ClusterRestoreIdentityModeAdoptTargetID:
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_ADOPT_TARGET_ID
	case daemonbackup.ClusterRestoreIdentityModeGenerateNewID:
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_GENERATE_NEW_ID
	default:
		return adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_UNSPECIFIED
	}
}

func restoreIdentityModeFromProto(mode adminv1.ClusterRestoreIdentityMode) string {
	switch mode {
	case adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID:
		return daemonbackup.ClusterRestoreIdentityModePreserveBackupID
	case adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_ADOPT_TARGET_ID:
		return daemonbackup.ClusterRestoreIdentityModeAdoptTargetID
	case adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_GENERATE_NEW_ID:
		return daemonbackup.ClusterRestoreIdentityModeGenerateNewID
	default:
		return ""
	}
}

func restoreAuthSessionPolicyToProto(policy string) adminv1.ClusterRestoreAuthSessionPolicy {
	switch strings.ToUpper(strings.TrimSpace(policy)) {
	case daemonbackup.ClusterRestoreAuthSessionPolicyPreserveRestoredSessions:
		return adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS
	case daemonbackup.ClusterRestoreAuthSessionPolicyRevokeAllSessions:
		return adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_REVOKE_ALL_SESSIONS
	default:
		return adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_UNSPECIFIED
	}
}

func restoreAuthSessionPolicyFromProto(policy adminv1.ClusterRestoreAuthSessionPolicy) string {
	switch policy {
	case adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS:
		return daemonbackup.ClusterRestoreAuthSessionPolicyPreserveRestoredSessions
	case adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_REVOKE_ALL_SESSIONS:
		return daemonbackup.ClusterRestoreAuthSessionPolicyRevokeAllSessions
	default:
		return ""
	}
}

func derivedSubsystemToProto(subsystem string) adminv1.BackupDerivedSubsystem {
	switch subsystem {
	case "semantic_vector_search":
		return adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_SEMANTIC_VECTOR_SEARCH
	case "lexical_search":
		return adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_LEXICAL_SEARCH
	case "graph_checkpoint":
		return adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_GRAPH_CHECKPOINT
	case "persistent_graph_query_index":
		return adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_PERSISTENT_GRAPH_QUERY_INDEX
	case "metadata_adjacency_memory_index":
		return adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_METADATA_ADJACENCY_MEMORY_INDEX
	default:
		return adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_UNSPECIFIED
	}
}

func derivedSubsystemFromProto(subsystem adminv1.BackupDerivedSubsystem) string {
	switch subsystem {
	case adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_SEMANTIC_VECTOR_SEARCH:
		return "semantic_vector_search"
	case adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_LEXICAL_SEARCH:
		return "lexical_search"
	case adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_GRAPH_CHECKPOINT:
		return "graph_checkpoint"
	case adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_PERSISTENT_GRAPH_QUERY_INDEX:
		return "persistent_graph_query_index"
	case adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_METADATA_ADJACENCY_MEMORY_INDEX:
		return "metadata_adjacency_memory_index"
	default:
		return ""
	}
}

func derivedPolicyModeToProto(mode string) adminv1.BackupDerivedPolicyMode {
	switch mode {
	case "include_required":
		return adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_INCLUDE_REQUIRED
	case "include_optional":
		return adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_INCLUDE_OPTIONAL
	case "rebuild_allowed":
		return adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_REBUILD_ALLOWED
	case "discard":
		return adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_DISCARD
	default:
		return adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_UNSPECIFIED
	}
}

func derivedPolicyModeFromProto(mode adminv1.BackupDerivedPolicyMode) string {
	switch mode {
	case adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_INCLUDE_REQUIRED:
		return "include_required"
	case adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_INCLUDE_OPTIONAL:
		return "include_optional"
	case adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_REBUILD_ALLOWED:
		return "rebuild_allowed"
	case adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_DISCARD:
		return "discard"
	default:
		return ""
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mapBackupPolicy(policy backupcore.Policy) *adminv1.BackupPolicy {
	policy = backupcore.EffectivePolicy("", policy)
	weekdays := make([]int32, 0, len(policy.Weekdays))
	for _, weekday := range policy.Weekdays {
		weekdays = append(weekdays, int32(weekday))
	}
	derived := make([]*adminv1.DerivedSubsystemPolicy, 0, len(policy.DerivedSubsystems))
	for _, row := range policy.DerivedSubsystems {
		derived = append(derived, &adminv1.DerivedSubsystemPolicy{Subsystem: derivedSubsystemToProto(row.Subsystem), Mode: derivedPolicyModeToProto(row.Mode)})
	}
	return &adminv1.BackupPolicy{
		Enabled:                    policy.Enabled,
		BackupDir:                  policy.BackupDir,
		IntervalHours:              int32(policy.Interval / time.Hour),
		RetentionCount:             int32(policy.RetentionCount),
		IncludeLogs:                policy.IncludeLogs,
		Compression:                policy.Compression,
		ArchiveFormat:              archiveFormatToProto(policy.ArchiveFormat),
		QuiesceDrainTimeoutSeconds: int64(policy.QuiesceDrainTimeout.Seconds()),
		BackupTimeoutSeconds:       int64(policy.BackupTimeout.Seconds()),
		RetryAfterSeconds:          int64(policy.RetryAfter.Seconds()),
		StatusHistoryLimit:         int32(policy.StatusHistoryLimit),
		AllowReadsDuringBackup:     policy.AllowReadsDuringBackup,
		ScheduleKind:               policy.ScheduleKind,
		TimeOfDay:                  policy.TimeOfDay,
		Timezone:                   policy.Timezone,
		Weekdays:                   weekdays,
		RunMissed:                  policy.RunMissed,
		DefaultRepositoryId:        policy.DefaultRepositoryID,
		CaptureProvider:            captureProviderToProto(policy.CaptureProvider),
		QuiesceReadProfile:         quiesceReadProfileToProto(policy.QuiesceReadProfile),
		AllowReadOnlyDuringQuiesce: policy.AllowReadOnlyDuringQuiesce,
		DerivedSubsystems:          derived,
		RetentionPolicy:            retentionPolicyToProto(policy.Retention),
		ValidationOptions:          &adminv1.ValidationOptions{IncludeArtifactChecks: policy.Validation.IncludeArtifactChecks, IncludeExternalDependencyChecks: policy.Validation.IncludeExternalDependencyChecks, IncludeCapacityEstimates: policy.Validation.IncludeCapacityEstimates, RequireCompleteMarker: policy.Validation.RequireCompleteMarker},
	}
}

func retentionPolicyToProto(policy backupcore.RetentionPolicy) *adminv1.RetentionPolicy {
	return &adminv1.RetentionPolicy{KeepLastSuccessful: int32(policy.KeepLastSuccessful), KeepForSeconds: int64(policy.KeepFor.Seconds()), DeleteIncompleteAfterSeconds: int64(policy.DeleteIncompleteAfter.Seconds())}
}

func retentionPolicyFromProto(policy *adminv1.RetentionPolicy) backupcore.RetentionPolicy {
	if policy == nil {
		return backupcore.RetentionPolicy{}
	}
	return backupcore.RetentionPolicy{KeepLastSuccessful: int(policy.GetKeepLastSuccessful()), KeepFor: time.Duration(policy.GetKeepForSeconds()) * time.Second, DeleteIncompleteAfter: time.Duration(policy.GetDeleteIncompleteAfterSeconds()) * time.Second}
}

func mapRepositoryRetentionPlan(plan daemonbackup.RepositoryRetentionPlan) *adminv1.RepositoryRetentionPlan {
	out := &adminv1.RepositoryRetentionPlan{RepositoryId: plan.RepositoryID, PlannedAt: formatBackupTime(plan.PlannedAt), RetentionPolicy: retentionPolicyToProto(plan.Policy), DryRun: plan.DryRun, Warnings: append([]string(nil), plan.Warnings...), DeletedObjects: int32(plan.DeletedObjects)}
	for _, action := range plan.Actions {
		out.Actions = append(out.Actions, &adminv1.RepositoryRetentionAction{BackupSetId: action.BackupSetID, Prefix: action.Prefix, Action: action.Action, Reason: action.Reason, CreatedAt: formatBackupTime(action.CreatedAt), CompletedAt: formatBackupTime(action.CompletedAt), ObjectCount: int32(action.ObjectCount), DeletedObjects: int32(action.DeletedObjects), Retryable: action.Retryable, Blocked: action.Blocked, Error: action.Error, RetentionCutoff: formatBackupTime(action.RetentionCutoff)})
	}
	return out
}

func backupPolicyFromProto(policy *adminv1.BackupPolicy) backupcore.Policy {
	if policy == nil {
		return backupcore.Policy{}
	}
	weekdays := make([]int, 0, len(policy.GetWeekdays()))
	for _, weekday := range policy.GetWeekdays() {
		weekdays = append(weekdays, int(weekday))
	}
	derived := make([]backupcore.DerivedSubsystemPolicy, 0, len(policy.GetDerivedSubsystems()))
	for _, row := range policy.GetDerivedSubsystems() {
		derived = append(derived, backupcore.DerivedSubsystemPolicy{Subsystem: derivedSubsystemFromProto(row.GetSubsystem()), Mode: derivedPolicyModeFromProto(row.GetMode())})
	}
	retention := retentionPolicyFromProto(policy.GetRetentionPolicy())
	validation := backupcore.ValidationOptions{}
	if policy.GetValidationOptions() != nil {
		validation = backupcore.ValidationOptions{IncludeArtifactChecks: policy.GetValidationOptions().GetIncludeArtifactChecks(), IncludeExternalDependencyChecks: policy.GetValidationOptions().GetIncludeExternalDependencyChecks(), IncludeCapacityEstimates: policy.GetValidationOptions().GetIncludeCapacityEstimates(), RequireCompleteMarker: policy.GetValidationOptions().GetRequireCompleteMarker()}
	}
	return backupcore.Policy{Enabled: policy.GetEnabled(), BackupDir: policy.GetBackupDir(), Interval: time.Duration(policy.GetIntervalHours()) * time.Hour, RetentionCount: int(policy.GetRetentionCount()), IncludeLogs: policy.GetIncludeLogs(), Compression: policy.GetCompression(), ArchiveFormat: archiveFormatFromProto(policy.GetArchiveFormat()), QuiesceDrainTimeout: time.Duration(policy.GetQuiesceDrainTimeoutSeconds()) * time.Second, BackupTimeout: time.Duration(policy.GetBackupTimeoutSeconds()) * time.Second, RetryAfter: time.Duration(policy.GetRetryAfterSeconds()) * time.Second, StatusHistoryLimit: int(policy.GetStatusHistoryLimit()), AllowReadsDuringBackup: policy.GetAllowReadsDuringBackup(), ScheduleKind: policy.GetScheduleKind(), TimeOfDay: policy.GetTimeOfDay(), Timezone: policy.GetTimezone(), Weekdays: weekdays, RunMissed: policy.GetRunMissed(), DefaultRepositoryID: policy.GetDefaultRepositoryId(), CaptureProvider: captureProviderFromProto(policy.GetCaptureProvider()), QuiesceReadProfile: quiesceReadProfileFromProto(policy.GetQuiesceReadProfile()), AllowReadOnlyDuringQuiesce: policy.GetAllowReadOnlyDuringQuiesce(), DerivedSubsystems: derived, Retention: retention, Validation: validation}
}

func mapBackupSummary(manifest backupcore.Manifest) *adminv1.BackupSummary {
	return &adminv1.BackupSummary{BackupId: manifest.BackupID, ArchiveName: manifest.ArchiveName, CreatedAt: formatBackupTime(manifest.CreatedAt), CompletedAt: formatBackupTime(manifest.CompletedAt), SizeBytes: manifest.SizeBytes, ChecksumSha256: manifest.ChecksumSHA256, Compression: manifest.Policy.Compression, ArchiveFormat: archiveFormatToProto(backupcore.ArchiveFormat(manifest.Policy.ArchiveFormat)), IncludeLogs: manifest.Policy.IncludeLogs}
}

func (s *AdminBackupService) clusterBackupNodes() ([]daemonbackup.ClusterBackupNode, string, error) {
	if s.cluster == nil {
		return nil, "", fmt.Errorf("clustering manager is not configured")
	}
	meta := s.cluster.SystemMetadata()
	clusterID := firstNonEmptyAdmin(meta.ClusterID, s.cluster.Identity().ClusterID)
	if strings.TrimSpace(clusterID) == "" {
		return nil, "", fmt.Errorf("cluster_id is not available")
	}
	nodes := make([]daemonbackup.ClusterBackupNode, 0, len(meta.Nodes))
	for _, node := range meta.Nodes {
		podName := podNameFromBackendAdvertiseAddr(node.BackendAdvertiseAddr)
		if podName == "" {
			podName = node.NodeName
		}
		ordinal := ordinalFromPodName(podName)
		if ordinal < 0 {
			ordinal = len(nodes)
		}
		nodes = append(nodes, daemonbackup.ClusterBackupNode{PodName: podName, NodeID: node.NodeID, Ordinal: ordinal, RaftNodeID: node.RaftNodeID, BackendAdvertiseAddr: node.BackendAdvertiseAddr})
	}
	if len(nodes) == 0 {
		return nil, "", fmt.Errorf("authoritative cluster membership is not available")
	}
	if s.clusterConfig.RaftNodeCount > 0 && len(nodes) != s.clusterConfig.RaftNodeCount {
		return nil, "", fmt.Errorf("authoritative cluster membership has %d nodes, expected %d", len(nodes), s.clusterConfig.RaftNodeCount)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Ordinal < nodes[j].Ordinal })
	return nodes, clusterID, nil
}

var podOrdinalPattern = regexp.MustCompile(`-(\d+)$`)

func podNameFromBackendAdvertiseAddr(addr string) string {
	host := strings.TrimSpace(addr)
	if i := strings.LastIndex(host, "@"); i >= 0 {
		host = host[i+1:]
	}
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	if i := strings.Index(host, "."); i >= 0 {
		host = host[:i]
	}
	return strings.TrimSpace(host)
}

func ordinalFromPodName(name string) int {
	match := podOrdinalPattern.FindStringSubmatch(strings.TrimSpace(name))
	if len(match) != 2 {
		return -1
	}
	ordinal, err := strconv.Atoi(match[1])
	if err != nil {
		return -1
	}
	return ordinal
}

func mapClusterBackupStatus(st daemonbackup.ClusterBackupRunStatus) *adminv1.ClusterBackupStatus {
	out := &adminv1.ClusterBackupStatus{BackupSetId: st.BackupSetID, State: st.Phase, StateCode: mapClusterBackupStateCode(st.Phase), ClusterId: st.ClusterID, Reason: st.Reason, CreatedAt: formatBackupTime(st.CreatedAt), UpdatedAt: formatBackupTime(st.UpdatedAt), CompletedAt: formatBackupTime(st.CompletedAt), ExpectedNodes: int32(len(st.Expected)), ManifestUri: st.ManifestURI, Nodes: mapClusterBackupNodeArtifacts(st.Nodes), FailedPhase: st.FailurePhase, Error: st.Error, RaftBarriers: st.Barriers, CancelRequested: st.Cancel, CurrentPhase: st.Phase, RepositoryId: st.RepositoryID, BackupOperationId: st.BackupSetID}
	if st.Phase == "waiting_for_cluster_convergence" {
		out.RetryAfterSeconds = 2
	}
	for _, blocker := range st.Blockers {
		out.Blockers = append(out.Blockers, &adminv1.ClusterBackupBlocker{NodeName: blocker.NodeName, NodeId: blocker.NodeID, RaftNodeId: blocker.RaftNodeID, RaftGroup: blocker.RaftGroup, Reason: blocker.Reason, AppliedIndex: blocker.AppliedIndex, CommitIndex: blocker.CommitIndex, Detail: blocker.Detail})
	}
	return out
}

func mapClusterBackupStateCode(phase string) adminv1.ClusterBackupState {
	switch phase {
	case "pending", "requested":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_PENDING
	case "waiting_for_cluster_convergence", "prechecking":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_WAITING_FOR_CLUSTER_CONVERGENCE
	case "ready":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_READY
	case "quiescing":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_QUIESCING
	case "capturing", "archiving", "barrier_wait", "uploading", "committing_manifest":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_CAPTURING
	case "validating":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_VALIDATING
	case "succeeded":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_SUCCEEDED
	case "failed", "aborted":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_FAILED
	case "canceling":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_CANCELING
	case "canceled":
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_CANCELED
	default:
		return adminv1.ClusterBackupState_CLUSTER_BACKUP_STATE_UNSPECIFIED
	}
}

func mapClusterBackupSetSummary(st daemonbackup.ClusterBackupRunStatus) *adminv1.ClusterBackupSetSummary {
	return &adminv1.ClusterBackupSetSummary{BackupSetId: st.BackupSetID, State: st.Phase, ClusterId: st.ClusterID, CreatedAt: formatBackupTime(st.CreatedAt), CompletedAt: formatBackupTime(st.CompletedAt), ExpectedNodes: int32(len(st.Expected)), ManifestUri: st.ManifestURI, Nodes: mapClusterBackupNodeArtifacts(st.Nodes), RepositoryId: st.RepositoryID}
}

func mapClusterManifestSummary(manifest clusterbackup.Manifest) *adminv1.ClusterBackupSetSummary {
	return &adminv1.ClusterBackupSetSummary{BackupSetId: manifest.BackupSetID, State: manifest.State, ClusterId: manifest.ClusterID, CreatedAt: formatBackupTime(manifest.CreatedAt), CompletedAt: formatBackupTime(manifest.CompletedAt), ExpectedNodes: int32(manifest.ExpectedNodes), ManifestUri: manifest.ManifestURI, Nodes: mapClusterBackupNodeArtifacts(manifest.Nodes)}
}

func mapClusterBackupNodeArtifacts(nodes []clusterbackup.NodeArtifact) []*adminv1.ClusterBackupNodeArtifact {
	out := make([]*adminv1.ClusterBackupNodeArtifact, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, &adminv1.ClusterBackupNodeArtifact{PodName: node.PodName, NodeId: node.NodeID, Ordinal: int32(node.Ordinal), RaftNodeId: node.RaftNodeID, ArchiveName: node.ArchiveName, ArchiveUri: node.ArchiveURI, ManifestName: node.ManifestName, ManifestUri: node.ManifestURI, SizeBytes: node.SizeBytes, ChecksumSha256: node.ChecksumSHA256, AppliedIndexes: node.AppliedIndexes})
	}
	return out
}

func archiveFormatToProto(format backupcore.ArchiveFormat) adminv1.BackupArchiveFormat {
	switch format {
	case "", backupcore.ArchiveFormatZip:
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP
	case backupcore.ArchiveFormatTar:
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR
	case backupcore.ArchiveFormatTarGz:
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ
	case backupcore.ArchiveFormatTarZst:
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST
	default:
		return adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_UNSPECIFIED
	}
}

func archiveFormatFromProto(format adminv1.BackupArchiveFormat) backupcore.ArchiveFormat {
	switch format {
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_UNSPECIFIED:
		return ""
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP:
		return backupcore.ArchiveFormatZip
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR:
		return backupcore.ArchiveFormatTar
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ:
		return backupcore.ArchiveFormatTarGz
	case adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST:
		return backupcore.ArchiveFormatTarZst
	default:
		return backupcore.ArchiveFormat(fmt.Sprintf("unknown:%d", format.Number()))
	}
}

func mapQuiesceParticipant(in quiesce.ParticipantStatus) *adminv1.QuiesceParticipantStatus {
	return &adminv1.QuiesceParticipantStatus{Name: in.Name, Quiesced: in.Quiesced, Active: int32(in.Active), Reason: in.Reason, Mode: string(in.Mode), Source: in.Source, Since: formatBackupTime(in.Since), LastError: in.LastError, ActiveReadOnly: int32(in.ActiveReadOnly), ActiveWrites: int32(in.ActiveWrites), ReadAllowlistProfile: in.ReadAllowlistProfile, Deadline: formatBackupTime(in.Deadline), AcceptingReadOnly: in.AcceptingReadOnly, AcceptingWrites: in.AcceptingWrites}
}

func parseOptionalBackupTime(value, field string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, status.Errorf(codes.InvalidArgument, "%s must be RFC3339", field)
	}
	return parsed.UTC(), nil
}

func formatBackupTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func mapBackupError(err error, action string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, backupcore.ErrBackupRunning) {
		return status.Error(codes.Aborted, "backup already running")
	}
	if errors.Is(err, backupcore.ErrBackupNotFound) {
		return status.Error(codes.NotFound, "backup not found")
	}
	if errors.Is(err, daemonbackup.ErrBackupRepositoryNotFound) {
		return status.Error(codes.NotFound, "backup repository not found")
	}
	if errors.Is(err, daemonbackup.ErrBackupRepositoryAlreadyExists) {
		return status.Error(codes.AlreadyExists, "backup repository already exists")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	msg := err.Error()
	if strings.Contains(msg, "not known") {
		return status.Error(codes.NotFound, msg)
	}
	if strings.Contains(msg, "already active") || strings.Contains(msg, "already running") {
		return status.Error(codes.Aborted, msg)
	}
	if strings.Contains(msg, "required") || strings.Contains(msg, "must") || strings.Contains(msg, "unsupported") || strings.Contains(msg, "invalid") || strings.Contains(msg, "unknown time zone") || strings.Contains(msg, "out of range") {
		return status.Error(codes.InvalidArgument, msg)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
