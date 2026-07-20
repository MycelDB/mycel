package backend

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/topology"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	clusterpb.UnimplementedClusterBackendServiceServer
	Identity            model.NodeIdentity
	State               model.NodeState
	Topology            *topology.Registry
	Membership          *membership.FileStore
	Authority           *clusterpb.ClusterAuthority
	WAL                 WALReader
	Checkpoint          CheckpointProvider
	SnapshotInstaller   SnapshotInstaller
	ReplicationStatus   ReplicationStatusProvider
	AuthorityInstaller  AuthorityInstaller
	BlobPayloadProvider BlobPayloadProvider
	RaftRouter          consensus.MessageSender
	SpaceReader         SpaceReader
	GraphReader         any
	SemanticReader      any
}

func NewService(identity model.NodeIdentity, state model.NodeState, registry *topology.Registry) *Service {
	return &Service{Identity: identity, State: state, Topology: registry}
}

func (s *Service) WithMembership(store *membership.FileStore) *Service {
	s.Membership = store
	return s
}

func (s *Service) WithAuthority(authority *clusterpb.ClusterAuthority) *Service {
	s.Authority = authority
	return s
}

func (s *Service) WithRaftRouter(router consensus.MessageSender) *Service {
	s.RaftRouter = router
	return s
}

func (s *Service) clusterView() *clusterpb.ClusterView {
	return SnapshotToProtoWithAuthority(s.Topology.Snapshot(), s.Identity, s.State, s.Authority)
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
	if req.GetJoinToken() != "" {
		if ok, reason, err := s.admitWithToken(ctx, id, req.GetJoinToken(), req.GetNodePublicKeyFingerprint(), now); err != nil {
			return nil, err
		} else if !ok {
			return &clusterpb.RegisterNodeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Accepted: false, Reason: reason, ClusterView: s.clusterView()}, nil
		}
	} else if ok, reason, err := s.validateReturningMember(ctx, id); err != nil {
		return nil, err
	} else if !ok {
		return &clusterpb.RegisterNodeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Accepted: false, Reason: reason, ClusterView: s.clusterView()}, nil
	}
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

func (s *Service) admitWithToken(ctx context.Context, id model.NodeIdentity, token string, fingerprint string, now time.Time) (bool, string, error) {
	member, ok, err := s.Membership.FindByNodeName(ctx, id.NodeName)
	if err != nil {
		return false, "", err
	}
	if !ok || member.State != membership.MemberStatePending || member.JoinToken == nil {
		return false, "no pending membership", nil
	}
	if member.JoinToken.ConsumedAt != nil {
		return false, "join token already consumed", nil
	}
	if member.JoinToken.RevokedAt != nil {
		return false, "join token revoked", nil
	}
	if !member.JoinToken.ExpiresAt.IsZero() && now.After(member.JoinToken.ExpiresAt) {
		return false, "join token expired", nil
	}
	if !membership.VerifyToken(member.JoinToken.Hash, token) {
		return false, "invalid join token", nil
	}
	consumed := now
	member.NodeID = id.NodeID
	member.State = membership.MemberStateActive
	member.BackendAdvertiseAddr = id.BackendAdvertiseAddr
	member.NodePublicKeyFingerprint = firstNonEmpty(fingerprint, id.NodePublicKeyFingerprint)
	member.JoinedAt = &consumed
	member.UpdatedAt = now
	member.JoinToken.Hash = ""
	member.JoinToken.ConsumedAt = &consumed
	if err := s.Membership.UpsertMember(ctx, member); err != nil {
		return false, "", err
	}
	return true, "", nil
}

func (s *Service) validateReturningMember(ctx context.Context, id model.NodeIdentity) (bool, string, error) {
	member, ok, err := s.Membership.FindByNodeID(ctx, id.NodeID)
	if err != nil {
		return false, "", err
	}
	if !ok || member.State != membership.MemberStateActive {
		return false, "node is not an active member", nil
	}
	if member.NodeName != id.NodeName {
		return false, "node name mismatch", nil
	}
	if id.ClusterID != s.Identity.ClusterID {
		return false, "cluster id mismatch", nil
	}
	return true, "", nil
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
		if member.JoinToken != nil {
			m.TokenId = member.JoinToken.TokenID
			m.TokenExpiresAt = formatTime(member.JoinToken.ExpiresAt)
			if member.JoinToken.ConsumedAt != nil {
				m.TokenConsumedAt = formatTime(*member.JoinToken.ConsumedAt)
			}
			if member.JoinToken.RevokedAt != nil {
				m.TokenRevokedAt = formatTime(*member.JoinToken.RevokedAt)
			}
		}
		members = append(members, m)
	}
	return &clusterpb.ListClusterMembersResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: data.ClusterID, ClusterName: data.ClusterName, Members: members}, nil
}

func (s *Service) AddClusterNode(ctx context.Context, req *clusterpb.AddClusterNodeRequest) (*clusterpb.AddClusterNodeResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if !s.Identity.ClusterAdmitted || s.Membership == nil {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if !s.isPrimary() {
		return nil, s.notPrimaryError()
	}
	nodeName := strings.TrimSpace(req.GetNodeName())
	if nodeName == "" {
		return nil, status.Error(codes.InvalidArgument, "node_name is required")
	}
	token, err := membership.GenerateToken()
	if err != nil {
		return nil, err
	}
	ttl := time.Duration(req.GetTokenTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	now := time.Now().UTC()
	expires := now.Add(ttl)
	member := membership.Member{NodeName: nodeName, State: membership.MemberStatePending, Role: "member", JoinToken: &membership.JoinToken{TokenID: "join_tok_" + uuid.NewString(), Hash: membership.HashToken(token), CreatedAt: now, ExpiresAt: expires}, CreatedAt: now, UpdatedAt: now}
	if err := s.Membership.UpsertMember(ctx, member); err != nil {
		return nil, err
	}
	return &clusterpb.AddClusterNodeResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, NodeName: nodeName, State: string(membership.MemberStatePending), Token: token, TokenId: member.JoinToken.TokenID, ExpiresAt: formatTime(expires)}, nil
}

func (s *Service) isPrimary() bool {
	if s == nil || s.Authority == nil || s.Authority.GetPrimary() == nil {
		return false
	}
	return s.Identity.NodeID != "" && s.Identity.NodeID == s.Authority.GetPrimary().GetNodeId()
}

func (s *Service) notPrimaryError() error {
	st := status.New(codes.FailedPrecondition, "node is not cluster primary")
	if s.Authority == nil || s.Authority.GetPrimary() == nil {
		return st.Err()
	}
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: "MYCEL_CLUSTER_NOT_PRIMARY", Domain: "mycel.cluster", Metadata: map[string]string{
		"mycel-primary-node-id":                s.Authority.GetPrimary().GetNodeId(),
		"mycel-primary-node-name":              s.Authority.GetPrimary().GetNodeName(),
		"mycel-primary-backend-advertise-addr": s.Authority.GetPrimary().GetBackendAdvertiseAddr(),
		"mycel-authority-epoch":                fmt.Sprintf("%d", s.Authority.GetAuthorityEpoch()),
	}})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
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
