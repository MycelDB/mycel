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
}

type ClusterBackupProvider interface {
	CreateLocalClusterBackupArchive(ctx context.Context, in CreateLocalBackupArchiveInput) (CreateLocalBackupArchiveResult, error)
}

func (s *Service) WithClusterBackupProvider(provider ClusterBackupProvider) *Service {
	s.ClusterBackupProvider = provider
	return s
}

func (s *Service) CreateLocalBackupArchive(ctx context.Context, req *clusterpb.CreateLocalBackupArchiveRequest) (*clusterpb.CreateLocalBackupArchiveResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if s.ClusterBackupProvider == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster backup provider is not configured")
	}
	if !s.Identity.ClusterAdmitted {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetClusterId()) != s.Identity.ClusterID {
		return nil, status.Error(codes.FailedPrecondition, "cluster_id does not match local node")
	}
	timestamp, err := time.Parse("20060102T150405Z", strings.TrimSpace(req.GetUtcTimestamp()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("utc_timestamp must use YYYYMMDDThhmmssZ format: %v", err))
	}
	barriers := make([]BackupRaftBarrier, 0, len(req.GetBarriers()))
	for _, barrier := range req.GetBarriers() {
		barriers = append(barriers, BackupRaftBarrier{GroupID: strings.TrimSpace(barrier.GetGroupId()), Index: barrier.GetIndex()})
	}
	result, err := s.ClusterBackupProvider.CreateLocalClusterBackupArchive(ctx, CreateLocalBackupArchiveInput{
		ClusterID:       strings.TrimSpace(req.GetClusterId()),
		RequesterNodeID: req.GetRequesterNodeId(),
		BackupSetID:     strings.TrimSpace(req.GetBackupSetId()),
		Reason:          req.GetReason(),
		PodName:         strings.TrimSpace(req.GetPodName()),
		NodeID:          strings.TrimSpace(req.GetNodeId()),
		RaftNodeID:      req.GetRaftNodeId(),
		Ordinal:         int(req.GetOrdinal()),
		OutputDir:       strings.TrimSpace(req.GetOutputDir()),
		ArchiveFormat:   strings.TrimSpace(req.GetArchiveFormat()),
		UTCTimestamp:    timestamp.UTC(),
		Barriers:        barriers,
	})
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &clusterpb.CreateLocalBackupArchiveResponse{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       result.ClusterID,
		PodName:         result.PodName,
		NodeId:          result.NodeID,
		RaftNodeId:      result.RaftNodeID,
		Ordinal:         int32(result.Ordinal),
		ArchiveName:     result.ArchiveName,
		ArchiveUri:      result.ArchiveURI,
		ManifestName:    result.ManifestName,
		ManifestUri:     result.ManifestURI,
		SizeBytes:       result.SizeBytes,
		ChecksumSha256:  result.ChecksumSHA256,
		AppliedIndexes:  result.AppliedIndexes,
	}, nil
}
