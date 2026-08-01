package backend

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/topology"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	clusterpb.UnimplementedClusterBackendServiceServer
	Identity               model.NodeIdentity
	State                  model.NodeState
	Topology               *topology.Registry
	Membership             *membership.FileStore
	BlobPayloadProvider    BlobPayloadProvider
	RaftRouter             consensus.MessageSender
	SpaceReader            SpaceReader
	GraphReader            any
	SemanticReader         any
	ClientRequestForwarder ForwardedClientRequestHandler

	forwardMu          sync.Mutex
	forwardDiagnostics ForwardClientDiagnostics
}

func NewService(identity model.NodeIdentity, state model.NodeState, registry *topology.Registry) *Service {
	return &Service{Identity: identity, State: state, Topology: registry}
}

func (s *Service) WithMembership(store *membership.FileStore) *Service {
	s.Membership = store
	return s
}

func (s *Service) WithRaftRouter(router consensus.MessageSender) *Service {
	s.RaftRouter = router
	return s
}

func (s *Service) clusterView() *clusterpb.ClusterView {
	return SnapshotToProto(s.Topology.Snapshot(), s.Identity, s.State)
}

func (s *Service) RegisterNode(ctx context.Context, req *clusterpb.RegisterNodeRequest) (*clusterpb.RegisterNodeResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	id, err := IdentityFromProto(req.GetIdentity())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateIdentity(id); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !s.Identity.ClusterAdmitted || s.Membership == nil {
		return &clusterpb.RegisterNodeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Accepted: false, Reason: "local node is not admitted to a cluster", ClusterView: s.clusterView()}, nil
	}
	if s.Identity.ClusterName != "" && id.ClusterName != "" && s.Identity.ClusterName != id.ClusterName {
		return &clusterpb.RegisterNodeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Accepted: false, Reason: "cluster name mismatch", ClusterView: s.clusterView()}, nil
	}
	now := time.Now().UTC()
	seen := now
	peer := model.Peer{NodeID: id.NodeID, NodeName: id.NodeName, ClusterID: s.Identity.ClusterID, ClusterName: id.ClusterName, BackendAdvertiseAddr: id.BackendAdvertiseAddr, State: model.PeerStateActive, Source: model.PeerSourceDiscovered, LastSeenAt: &seen}
	if err := s.Topology.Upsert(ctx, peer); err != nil {
		return nil, err
	}
	for _, protoPeer := range req.GetKnownPeers() {
		known, err := PeerFromProto(protoPeer)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		// The registering node's local self peer may still carry its pre-admission
		// temporary cluster_id. The authoritative peer record was written above
		// using this cluster's ID, so do not let the caller's self snapshot
		// overwrite it.
		if known.NodeID == id.NodeID {
			continue
		}
		if known.State == model.PeerStateSelf {
			known.State = model.PeerStateActive
		}
		if known.Source == model.PeerSourceSelf || known.Source == "" {
			known.Source = model.PeerSourceDiscovered
		}
		_ = s.Topology.Upsert(ctx, known)
	}
	return &clusterpb.RegisterNodeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Accepted: true, ClusterView: s.clusterView()}, nil
}

func (s *Service) GetClusterView(ctx context.Context, req *clusterpb.GetClusterViewRequest) (*clusterpb.GetClusterViewResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	return &clusterpb.GetClusterViewResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterView: s.clusterView()}, nil
}

func (s *Service) UpdateNodeStatus(ctx context.Context, req *clusterpb.UpdateNodeStatusRequest) (*clusterpb.UpdateNodeStatusResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	id, err := IdentityFromProto(req.GetIdentity())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateIdentity(id); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	observed, _ := parseOptionalTime(req.GetObservedAt())
	if observed.IsZero() {
		observed = time.Now().UTC()
	}
	peerState := model.PeerStateActive
	if NodeStateFromProto(req.GetState()) == model.NodeStateStopped || NodeStateFromProto(req.GetState()) == model.NodeStateFailed {
		peerState = model.PeerStateUnreachable
	}
	seen := observed.UTC()
	if err := s.Topology.Upsert(ctx, model.Peer{NodeID: id.NodeID, NodeName: id.NodeName, ClusterID: id.ClusterID, ClusterName: id.ClusterName, BackendAdvertiseAddr: id.BackendAdvertiseAddr, State: peerState, Source: model.PeerSourceDiscovered, LastSeenAt: &seen}); err != nil {
		return nil, err
	}
	return &clusterpb.UpdateNodeStatusResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Accepted: true}, nil
}

func (s *Service) WatchClusterUpdates(req *clusterpb.WatchClusterUpdatesRequest, stream clusterpb.ClusterBackendService_WatchClusterUpdatesServer) error {
	return status.Error(codes.Unimplemented, "cluster update watch is not implemented")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (s *Service) ListClusterMembers(ctx context.Context, req *clusterpb.ListClusterMembersRequest) (*clusterpb.ListClusterMembersResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if !s.Identity.ClusterAdmitted || s.Membership == nil {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	data, err := s.Membership.Load(ctx)
	if err != nil {
		return nil, err
	}
	members := make([]*clusterpb.ClusterMember, 0, len(data.Members))
	for _, member := range data.Members {
		m := &clusterpb.ClusterMember{
			NodeName:                 member.NodeName,
			NodeId:                   member.NodeID,
			State:                    string(member.State),
			BackendAdvertiseAddr:     member.BackendAdvertiseAddr,
			Role:                     member.Role,
			ClusterBootstrap:         member.ClusterBootstrap,
			NodePublicKeyFingerprint: member.NodePublicKeyFingerprint,
			CreatedAt:                formatTime(member.CreatedAt),
			UpdatedAt:                formatTime(member.UpdatedAt),
		}
		if member.JoinedAt != nil {
			m.JoinedAt = formatTime(*member.JoinedAt)
		}
		members = append(members, m)
	}
	return &clusterpb.ListClusterMembersResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: data.ClusterID, ClusterName: data.ClusterName, Members: members}, nil
}

func validateIdentity(id model.NodeIdentity) error {
	if id.Version != model.NodeIdentityVersion {
		return fmt.Errorf("unsupported clustering node identity version %d", id.Version)
	}
	if strings.TrimSpace(id.NodeID) == "" {
		return fmt.Errorf("clustering node_id is required")
	}
	if strings.TrimSpace(id.ClusterID) == "" {
		return fmt.Errorf("clustering cluster_id is required")
	}
	if id.CreatedAt.IsZero() {
		return fmt.Errorf("clustering created_at is required")
	}
	if id.UpdatedAt.IsZero() {
		return fmt.Errorf("clustering updated_at is required")
	}
	if strings.TrimSpace(id.BackendAdvertiseAddr) == "" {
		return fmt.Errorf("clustering backend_advertise_addr is required")
	}
	return nil
}

func validateProtocol(v clusterpb.ClusterProtocolVersion) error {
	if v != clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1 {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("unsupported clustering protocol version %d", v))
	}
	return nil
}
