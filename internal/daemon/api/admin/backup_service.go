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

type clusterBackupManager interface {
	TriggerClusterBackup(context.Context, daemonbackup.TriggerClusterBackupInput) (daemonbackup.ClusterBackupRunStatus, error)
	ClusterBackupStatus(string) (daemonbackup.ClusterBackupRunStatus, error)
	ListClusterBackups() []daemonbackup.ClusterBackupRunStatus
}

type AdminBackupService struct {
	adminv1.UnimplementedAdminBackupServiceServer
	manager       daemonbackup.Manager
	quiesce       *quiesce.Coordinator
	cluster       *clustering.Manager
	clusterConfig daemonconfig.ClusterConfig
	authorizer    OperatorAuthorizer
}

func NewAdminBackupService(manager daemonbackup.Manager, quiesce *quiesce.Coordinator, authorizer OperatorAuthorizer) *AdminBackupService {
	return &AdminBackupService{manager: manager, quiesce: quiesce, authorizer: authorizer}
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

func (s *AdminBackupService) TriggerBackup(ctx context.Context, req *adminv1.TriggerBackupRequest) (*adminv1.TriggerBackupResponse, error) {
	principal, err := s.requireBackupManage(ctx)
	if err != nil {
		return nil, err
	}
	result, err := s.manager.Trigger(ctx, backupcore.TriggerInput{Source: principal.OperatorID, Reason: firstNonEmptyAdmin(req.GetReason(), "manual backup")})
	if err != nil {
		return nil, mapBackupError(err, "trigger backup")
	}
	statusValue := s.manager.RunStatus()
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

func (s *AdminBackupService) TriggerClusterBackup(ctx context.Context, req *adminv1.TriggerClusterBackupRequest) (*adminv1.TriggerClusterBackupResponse, error) {
	principal, err := s.requireBackupManage(ctx)
	if err != nil {
		return nil, err
	}
	nodes, clusterID, err := s.clusterBackupNodes()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	clusterManager, ok := s.manager.(clusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	st, err := clusterManager.TriggerClusterBackup(ctx, daemonbackup.TriggerClusterBackupInput{Reason: firstNonEmptyAdmin(req.GetReason(), "cluster system backup by "+principal.OperatorID), OutputDir: req.GetOutputDir(), ArchiveFormat: archiveFormatFromProto(req.GetArchiveFormat()), ClusterID: clusterID, Nodes: nodes})
	if err != nil {
		return nil, mapBackupError(err, "trigger cluster backup")
	}
	return &adminv1.TriggerClusterBackupResponse{Status: mapClusterBackupStatus(st), BackupSet: mapClusterBackupSetSummary(st)}, nil
}

func (s *AdminBackupService) GetClusterBackupStatus(ctx context.Context, req *adminv1.GetClusterBackupStatusRequest) (*adminv1.GetClusterBackupStatusResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	clusterManager, ok := s.manager.(clusterBackupManager)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup coordinator is not configured")
	}
	st, err := clusterManager.ClusterBackupStatus(req.GetBackupSetId())
	if err != nil {
		return nil, mapBackupError(err, "get cluster backup status")
	}
	return &adminv1.GetClusterBackupStatusResponse{Status: mapClusterBackupStatus(st)}, nil
}

func (s *AdminBackupService) ListClusterBackups(ctx context.Context, req *adminv1.ListClusterBackupsRequest) (*adminv1.ListClusterBackupsResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	clusterManager, ok := s.manager.(clusterBackupManager)
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

func (s *AdminBackupService) ValidateClusterBackupSet(ctx context.Context, req *adminv1.ValidateClusterBackupSetRequest) (*adminv1.ValidateClusterBackupSetResponse, error) {
	if _, err := s.requireBackupManage(ctx); err != nil {
		return nil, err
	}
	manifest, err := daemonbackup.ValidateClusterBackupSet(ctx, req.GetBackupSetPath())
	if err != nil {
		return &adminv1.ValidateClusterBackupSetResponse{Valid: false, Errors: []string{err.Error()}}, nil
	}
	return &adminv1.ValidateClusterBackupSetResponse{Valid: true, BackupSet: mapClusterManifestSummary(manifest)}, nil
}

func (s *AdminBackupService) requireBackupManage(ctx context.Context) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	if s.manager == nil {
		return daemonauth.Principal{}, status.Error(codes.FailedPrecondition, "backup service is not configured")
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_SYSTEM_BACKUP_SPACE.String())
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

func mapBackupPolicy(policy backupcore.Policy) *adminv1.BackupPolicy {
	policy = backupcore.EffectivePolicy("", policy)
	weekdays := make([]int32, 0, len(policy.Weekdays))
	for _, weekday := range policy.Weekdays {
		weekdays = append(weekdays, int32(weekday))
	}
	return &adminv1.BackupPolicy{Enabled: policy.Enabled, BackupDir: policy.BackupDir, IntervalHours: int32(policy.Interval / time.Hour), RetentionCount: int32(policy.RetentionCount), IncludeLogs: policy.IncludeLogs, Compression: policy.Compression, ArchiveFormat: archiveFormatToProto(policy.ArchiveFormat), QuiesceDrainTimeoutSeconds: int64(policy.QuiesceDrainTimeout.Seconds()), BackupTimeoutSeconds: int64(policy.BackupTimeout.Seconds()), RetryAfterSeconds: int64(policy.RetryAfter.Seconds()), StatusHistoryLimit: int32(policy.StatusHistoryLimit), AllowReadsDuringBackup: policy.AllowReadsDuringBackup, ScheduleKind: policy.ScheduleKind, TimeOfDay: policy.TimeOfDay, Timezone: policy.Timezone, Weekdays: weekdays, RunMissed: policy.RunMissed}
}

func backupPolicyFromProto(policy *adminv1.BackupPolicy) backupcore.Policy {
	if policy == nil {
		return backupcore.Policy{}
	}
	weekdays := make([]int, 0, len(policy.GetWeekdays()))
	for _, weekday := range policy.GetWeekdays() {
		weekdays = append(weekdays, int(weekday))
	}
	return backupcore.Policy{Enabled: policy.GetEnabled(), BackupDir: policy.GetBackupDir(), Interval: time.Duration(policy.GetIntervalHours()) * time.Hour, RetentionCount: int(policy.GetRetentionCount()), IncludeLogs: policy.GetIncludeLogs(), Compression: policy.GetCompression(), ArchiveFormat: archiveFormatFromProto(policy.GetArchiveFormat()), QuiesceDrainTimeout: time.Duration(policy.GetQuiesceDrainTimeoutSeconds()) * time.Second, BackupTimeout: time.Duration(policy.GetBackupTimeoutSeconds()) * time.Second, RetryAfter: time.Duration(policy.GetRetryAfterSeconds()) * time.Second, StatusHistoryLimit: int(policy.GetStatusHistoryLimit()), AllowReadsDuringBackup: policy.GetAllowReadsDuringBackup(), ScheduleKind: policy.GetScheduleKind(), TimeOfDay: policy.GetTimeOfDay(), Timezone: policy.GetTimezone(), Weekdays: weekdays, RunMissed: policy.GetRunMissed()}
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
	return &adminv1.ClusterBackupStatus{BackupSetId: st.BackupSetID, State: st.Phase, ClusterId: st.ClusterID, Reason: st.Reason, CreatedAt: formatBackupTime(st.CreatedAt), UpdatedAt: formatBackupTime(st.UpdatedAt), CompletedAt: formatBackupTime(st.CompletedAt), ExpectedNodes: int32(len(st.Expected)), ManifestUri: st.ManifestURI, Nodes: mapClusterBackupNodeArtifacts(st.Nodes), FailedPhase: st.FailurePhase, Error: st.Error, RaftBarriers: st.Barriers}
}

func mapClusterBackupSetSummary(st daemonbackup.ClusterBackupRunStatus) *adminv1.ClusterBackupSetSummary {
	return &adminv1.ClusterBackupSetSummary{BackupSetId: st.BackupSetID, State: st.Phase, ClusterId: st.ClusterID, CreatedAt: formatBackupTime(st.CreatedAt), CompletedAt: formatBackupTime(st.CompletedAt), ExpectedNodes: int32(len(st.Expected)), ManifestUri: st.ManifestURI, Nodes: mapClusterBackupNodeArtifacts(st.Nodes)}
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
	return &adminv1.QuiesceParticipantStatus{Name: in.Name, Quiesced: in.Quiesced, Active: int32(in.Active), Reason: in.Reason, Mode: string(in.Mode), Source: in.Source, Since: formatBackupTime(in.Since), LastError: in.LastError}
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
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	msg := err.Error()
	if strings.Contains(msg, "required") || strings.Contains(msg, "must") || strings.Contains(msg, "unsupported") || strings.Contains(msg, "invalid") || strings.Contains(msg, "unknown time zone") || strings.Contains(msg, "out of range") {
		return status.Error(codes.InvalidArgument, msg)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
