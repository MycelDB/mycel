package admin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *AdminClusterService) GetLocalGraphForensicExport(ctx context.Context, req *adminv1.GetLocalGraphForensicExportRequest) (*adminv1.GetLocalGraphForensicExportResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.graphForensicExport == nil {
		return nil, status.Error(codes.FailedPrecondition, "graph forensic export is not configured")
	}
	export, err := s.graphForensicExport.LocalGraphForensicExport(ctx, req.GetSpaceId(), req.GetDomainId(), graphservice.LocalGraphForensicExportOptions{PageSize: int(req.GetPageSize()), PageToken: req.GetPageToken(), SourceLabel: req.GetSourceLabel()})
	if err != nil {
		if errors.Is(err, graphservice.ErrInvalidInput) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, err
	}
	if s.cluster == nil {
		return nil, status.Error(codes.Unavailable, "clustering manager is not available")
	}
	identity := s.cluster.Identity()
	collectedAt := export.Stats.CollectedAt
	if collectedAt.IsZero() {
		collectedAt = time.Now().UTC()
	}
	sourceLabel := strings.TrimSpace(req.GetSourceLabel())
	if sourceLabel == "" {
		sourceLabel = "local_daemon"
	}
	manifest := &adminv1.GraphForensicExportManifest{ReportId: uuid.NewString(), SourceNodeId: identity.NodeID, SourceNodeName: identity.NodeName, SourceClusterId: identity.ClusterID, SourceLabel: sourceLabel, CollectedAt: formatClusterTime(collectedAt), MycelVersion: mycelBuildVersion(), ImageTag: strings.TrimSpace(os.Getenv("MYCEL_IMAGE_TAG"))}
	out := &adminv1.GetLocalGraphForensicExportResponse{Manifest: manifest, Stats: localGraphConsistencyStatsToProto(export.Stats), NextPageToken: export.NextPageToken, Truncated: export.Truncated, Warnings: append([]string(nil), export.Warnings...)}
	for _, node := range export.Nodes {
		out.Nodes = append(out.Nodes, forensicGraphEntityToProto(node))
	}
	for _, edge := range export.Edges {
		out.Edges = append(out.Edges, forensicGraphEntityToProto(edge))
	}
	return out, nil
}

func forensicGraphEntityToProto(entity graphservice.ForensicGraphEntity) *adminv1.GraphForensicEntity {
	return &adminv1.GraphForensicEntity{Id: entity.ID, Checksum: entity.Checksum, CanonicalJson: entity.CanonicalJSON}
}

func mycelBuildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || strings.TrimSpace(info.Main.Version) == "" {
		return "unknown"
	}
	if info.Main.Version == "(devel)" {
		return fmt.Sprintf("%s %s", info.Main.Path, info.Main.Version)
	}
	return info.Main.Version
}
