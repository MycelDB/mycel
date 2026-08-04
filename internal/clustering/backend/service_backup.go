package backend

import (
	"context"
	"fmt"
	"strings"
	"time"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BackupRaftBarrier struct {
	GroupID string
	Index   uint64
}

type BackupRaftFreezeGroup struct {
	GroupID       string
	BarrierIndex  uint64
	AppliedIndex  uint64
	CommitIndex   uint64
	Term          uint64
	LastIndex     uint64
	SnapshotIndex uint64
	Leader        uint64
}

type BackupRaftFreeze struct {
	LeaseID    string
	AcquiredAt time.Time
	ReleasedAt time.Time
	ExpiresAt  time.Time
	Groups     map[string]BackupRaftFreezeGroup
}

type CreateLocalBackupArchiveInput struct {
	ClusterID       string
	RequesterNodeID uint64
	BackupSetID     string
	Reason          string
	PodName         string
	NodeID          string
	RaftNodeID      uint64
	Ordinal         int
	OutputDir       string
	ArchiveFormat   string
	UTCTimestamp    time.Time
	Barriers        []BackupRaftBarrier
	FreezeLeaseID   string
	TTL             time.Duration
}

type CreateLocalBackupArchiveResult struct {
	ClusterID      string
	PodName        string
	NodeID         string
	RaftNodeID     uint64
	Ordinal        int
	ArchiveName    string
	ArchiveURI     string
	ManifestName   string
	ManifestURI    string
	SizeBytes      int64
	ChecksumSHA256 string
	AppliedIndexes map[string]uint64
	RaftFreeze     BackupRaftFreeze
}

type ClusterBackupProvider interface {
	CheckLocalClusterBackupReadiness(ctx context.Context, in CreateLocalBackupArchiveInput) (map[string]uint64, map[string]uint64, error)
	AcquireLocalClusterBackupQuiesce(ctx context.Context, in CreateLocalBackupArchiveInput) error
	ReleaseLocalClusterBackupQuiesce(ctx context.Context, in CreateLocalBackupArchiveInput) error
	AcquireLocalRaftBackupFreeze(ctx context.Context, in CreateLocalBackupArchiveInput) (BackupRaftFreeze, error)
	ReleaseLocalRaftBackupFreeze(ctx context.Context, in CreateLocalBackupArchiveInput) error
	CreateLocalClusterBackupArchive(ctx context.Context, in CreateLocalBackupArchiveInput) (CreateLocalBackupArchiveResult, error)
}

func (s *Service) WithClusterBackupProvider(provider ClusterBackupProvider) *Service {
	s.ClusterBackupProvider = provider
	return s
}

func (s *Service) CheckLocalBackupReadiness(ctx context.Context, req *clusterpb.CheckLocalBackupReadinessRequest) (*clusterpb.CheckLocalBackupReadinessResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	in, err := s.validateClusterBackupControlRequest(req.GetClusterId(), req.GetBackupSetId(), req.GetReason(), req.GetPodName(), req.GetNodeId(), req.GetRequesterNodeId(), req.GetRaftNodeId(), int(req.GetOrdinal()))
	if err != nil {
		return nil, err
	}
	in.OutputDir = strings.TrimSpace(req.GetOutputDir())
	in.ArchiveFormat = strings.TrimSpace(req.GetArchiveFormat())
	applied, commits, err := s.ClusterBackupProvider.CheckLocalClusterBackupReadiness(ctx, in)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &clusterpb.CheckLocalBackupReadinessResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: in.ClusterID, BackupSetId: in.BackupSetID, PodName: in.PodName, AppliedIndexes: applied, CommitIndexes: commits}, nil
}

func (s *Service) AcquireLocalBackupQuiesce(ctx context.Context, req *clusterpb.AcquireLocalBackupQuiesceRequest) (*clusterpb.AcquireLocalBackupQuiesceResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	in, err := s.validateClusterBackupControlRequest(req.GetClusterId(), req.GetBackupSetId(), req.GetReason(), req.GetPodName(), req.GetNodeId(), req.GetRequesterNodeId(), req.GetRaftNodeId(), int(req.GetOrdinal()))
	if err != nil {
		return nil, err
	}
	if err := s.ClusterBackupProvider.AcquireLocalClusterBackupQuiesce(ctx, in); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &clusterpb.AcquireLocalBackupQuiesceResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: in.ClusterID, BackupSetId: in.BackupSetID, PodName: in.PodName}, nil
}

func (s *Service) ReleaseLocalBackupQuiesce(ctx context.Context, req *clusterpb.ReleaseLocalBackupQuiesceRequest) (*clusterpb.ReleaseLocalBackupQuiesceResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	in, err := s.validateClusterBackupControlRequest(req.GetClusterId(), req.GetBackupSetId(), "", req.GetPodName(), req.GetNodeId(), req.GetRequesterNodeId(), req.GetRaftNodeId(), int(req.GetOrdinal()))
	if err != nil {
		return nil, err
	}
	if err := s.ClusterBackupProvider.ReleaseLocalClusterBackupQuiesce(ctx, in); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &clusterpb.ReleaseLocalBackupQuiesceResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: in.ClusterID, BackupSetId: in.BackupSetID, PodName: in.PodName}, nil
}

func (s *Service) AcquireLocalRaftBackupFreeze(ctx context.Context, req *clusterpb.AcquireLocalRaftBackupFreezeRequest) (*clusterpb.AcquireLocalRaftBackupFreezeResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	in, err := s.validateClusterBackupControlRequest(req.GetClusterId(), req.GetBackupSetId(), req.GetReason(), req.GetPodName(), req.GetNodeId(), req.GetRequesterNodeId(), req.GetRaftNodeId(), int(req.GetOrdinal()))
	if err != nil {
		return nil, err
	}
	in.Barriers = protoBarriersToBackend(req.GetBarriers())
	in.TTL = time.Duration(req.GetTtlSeconds()) * time.Second
	freeze, err := s.ClusterBackupProvider.AcquireLocalRaftBackupFreeze(ctx, in)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &clusterpb.AcquireLocalRaftBackupFreezeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: in.ClusterID, BackupSetId: in.BackupSetID, PodName: in.PodName, LeaseId: freeze.LeaseID, AcquiredAt: formatBackendTime(freeze.AcquiredAt), ExpiresAt: formatBackendTime(freeze.ExpiresAt), Groups: freezeGroupsToProto(freeze.Groups)}, nil
}

func (s *Service) ReleaseLocalRaftBackupFreeze(ctx context.Context, req *clusterpb.ReleaseLocalRaftBackupFreezeRequest) (*clusterpb.ReleaseLocalRaftBackupFreezeResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	in, err := s.validateClusterBackupControlRequest(req.GetClusterId(), req.GetBackupSetId(), "", req.GetPodName(), req.GetNodeId(), req.GetRequesterNodeId(), req.GetRaftNodeId(), int(req.GetOrdinal()))
	if err != nil {
		return nil, err
	}
	in.FreezeLeaseID = strings.TrimSpace(req.GetLeaseId())
	if err := s.ClusterBackupProvider.ReleaseLocalRaftBackupFreeze(ctx, in); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &clusterpb.ReleaseLocalRaftBackupFreezeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: in.ClusterID, BackupSetId: in.BackupSetID, PodName: in.PodName}, nil
}

func (s *Service) CreateLocalBackupArchive(ctx context.Context, req *clusterpb.CreateLocalBackupArchiveRequest) (*clusterpb.CreateLocalBackupArchiveResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	base, err := s.validateClusterBackupControlRequest(req.GetClusterId(), req.GetBackupSetId(), req.GetReason(), req.GetPodName(), req.GetNodeId(), req.GetRequesterNodeId(), req.GetRaftNodeId(), int(req.GetOrdinal()))
	if err != nil {
		return nil, err
	}
	timestamp, err := time.Parse("20060102T150405Z", strings.TrimSpace(req.GetUtcTimestamp()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("utc_timestamp must use YYYYMMDDThhmmssZ format: %v", err))
	}
	barriers := protoBarriersToBackend(req.GetBarriers())
	base.OutputDir = strings.TrimSpace(req.GetOutputDir())
	base.ArchiveFormat = strings.TrimSpace(req.GetArchiveFormat())
	base.UTCTimestamp = timestamp.UTC()
	base.Barriers = barriers
	base.FreezeLeaseID = strings.TrimSpace(req.GetRaftFreezeLeaseId())
	result, err := s.ClusterBackupProvider.CreateLocalClusterBackupArchive(ctx, base)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &clusterpb.CreateLocalBackupArchiveResponse{
		ProtocolVersion:      clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:            result.ClusterID,
		PodName:              result.PodName,
		NodeId:               result.NodeID,
		RaftNodeId:           result.RaftNodeID,
		Ordinal:              int32(result.Ordinal),
		ArchiveName:          result.ArchiveName,
		ArchiveUri:           result.ArchiveURI,
		ManifestName:         result.ManifestName,
		ManifestUri:          result.ManifestURI,
		SizeBytes:            result.SizeBytes,
		ChecksumSha256:       result.ChecksumSHA256,
		AppliedIndexes:       result.AppliedIndexes,
		RaftFreezeLeaseId:    result.RaftFreeze.LeaseID,
		RaftFreezeAcquiredAt: formatBackendTime(result.RaftFreeze.AcquiredAt),
		RaftFreezeReleasedAt: formatBackendTime(result.RaftFreeze.ReleasedAt),
		RaftFreezeExpiresAt:  formatBackendTime(result.RaftFreeze.ExpiresAt),
		RaftFreezeGroups:     freezeGroupsToProto(result.RaftFreeze.Groups),
	}, nil
}

func protoBarriersToBackend(input []*clusterpb.BackupRaftBarrier) []BackupRaftBarrier {
	out := make([]BackupRaftBarrier, 0, len(input))
	for _, barrier := range input {
		out = append(out, BackupRaftBarrier{GroupID: strings.TrimSpace(barrier.GetGroupId()), Index: barrier.GetIndex()})
	}
	return out
}

func freezeGroupsToProto(input map[string]BackupRaftFreezeGroup) map[string]*clusterpb.BackupRaftFreezeGroup {
	out := make(map[string]*clusterpb.BackupRaftFreezeGroup, len(input))
	for key, group := range input {
		out[key] = &clusterpb.BackupRaftFreezeGroup{GroupId: group.GroupID, BarrierIndex: group.BarrierIndex, AppliedIndex: group.AppliedIndex, CommitIndex: group.CommitIndex, Term: group.Term, LastIndex: group.LastIndex, SnapshotIndex: group.SnapshotIndex, Leader: group.Leader}
	}
	return out
}

func formatBackendTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func (s *Service) validateClusterBackupControlRequest(clusterID, backupSetID, reason, podName, nodeID string, requesterNodeID, raftNodeID uint64, ordinal int) (CreateLocalBackupArchiveInput, error) {
	if s.ClusterBackupProvider == nil {
		return CreateLocalBackupArchiveInput{}, status.Error(codes.FailedPrecondition, "cluster backup provider is not configured")
	}
	if !s.Identity.ClusterAdmitted {
		return CreateLocalBackupArchiveInput{}, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if strings.TrimSpace(clusterID) == "" || strings.TrimSpace(clusterID) != s.Identity.ClusterID {
		return CreateLocalBackupArchiveInput{}, status.Error(codes.FailedPrecondition, "cluster_id does not match local node")
	}
	return CreateLocalBackupArchiveInput{ClusterID: strings.TrimSpace(clusterID), RequesterNodeID: requesterNodeID, BackupSetID: strings.TrimSpace(backupSetID), Reason: reason, PodName: strings.TrimSpace(podName), NodeID: strings.TrimSpace(nodeID), RaftNodeID: raftNodeID, Ordinal: ordinal}, nil
}
