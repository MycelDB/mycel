package admin

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const clusterManageCapability = "CAPABILITY_MESH_MANAGE"

type AdminClusterService struct {
	adminv1.UnimplementedAdminClusterServiceServer
	cluster    *clustering.Manager
	authorizer OperatorAuthorizer
}

func NewAdminClusterService(cluster *clustering.Manager, authorizer OperatorAuthorizer) *AdminClusterService {
	return &AdminClusterService{cluster: cluster, authorizer: authorizer}
}

func (s *AdminClusterService) GetClusterStatus(ctx context.Context, req *adminv1.GetClusterStatusRequest) (*adminv1.GetClusterStatusResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil {
		return nil, status.Error(codes.Unavailable, "clustering manager is not available")
	}
	identity := s.cluster.Identity()
	peers := []*adminv1.ClusterPeer{}
	if topology := s.cluster.Topology(); topology != nil {
		for _, peer := range topology.List() {
			peers = append(peers, clusterPeerToProto(peer))
		}
	}
	return &adminv1.GetClusterStatusResponse{
		Node: &adminv1.ClusterLocalNode{
			NodeId:                   identity.NodeID,
			NodeName:                 identity.NodeName,
			State:                    nodeStateToAdminProto(s.cluster.State()),
			Admitted:                 identity.ClusterAdmitted,
			Bootstrap:                identity.ClusterBootstrap,
			BackendAdvertiseAddr:     identity.BackendAdvertiseAddr,
			NodePublicKeyFingerprint: identity.NodePublicKeyFingerprint,
		},
		Cluster: &adminv1.ClusterInfo{
			ClusterId:   identity.ClusterID,
			ClusterName: identity.ClusterName,
			Mode:        clusterModeFromNodeState(s.cluster.State()),
		},
		Peers: peers,
	}, nil
}

func (s *AdminClusterService) ListClusterMembers(ctx context.Context, req *adminv1.ListClusterMembersRequest) (*adminv1.ListClusterMembersResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	store, err := s.membershipStore()
	if err != nil {
		return nil, err
	}
	data, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	members := make([]*adminv1.ClusterMember, 0, len(data.Members))
	for _, member := range data.Members {
		members = append(members, clusterMemberToProto(member))
	}
	return &adminv1.ListClusterMembersResponse{ClusterId: data.ClusterID, ClusterName: data.ClusterName, Members: members}, nil
}

func (s *AdminClusterService) AddClusterNode(ctx context.Context, req *adminv1.AddClusterNodeRequest) (*adminv1.AddClusterNodeResponse, error) {
	if _, err := s.requireClusterManage(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil || !s.cluster.IsAdmitted() {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	store, err := s.membershipStore()
	if err != nil {
		return nil, err
	}
	nodeName := strings.TrimSpace(req.GetNodeName())
	if nodeName == "" {
		return nil, status.Error(codes.InvalidArgument, "node_name is required")
	}
	token, err := membership.GenerateToken()
	if err != nil {
		return nil, status.Error(codes.Internal, "generate join token")
	}
	ttl := time.Duration(req.GetTokenTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	now := time.Now().UTC()
	expires := now.Add(ttl)
	member := membership.Member{
		NodeName:  nodeName,
		State:     membership.MemberStatePending,
		Role:      "member",
		JoinToken: &membership.JoinToken{TokenID: "join_tok_" + uuid.NewString(), Hash: membership.HashToken(token), CreatedAt: now, ExpiresAt: expires},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.UpsertMember(ctx, member); err != nil {
		return nil, err
	}
	return &adminv1.AddClusterNodeResponse{NodeName: nodeName, State: adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_PENDING, Token: token, TokenId: member.JoinToken.TokenID, ExpiresAt: formatClusterTime(expires)}, nil
}

func (s *AdminClusterService) membershipStore() (*membership.FileStore, error) {
	if s.cluster == nil {
		return nil, status.Error(codes.Unavailable, "clustering manager is not available")
	}
	store := s.cluster.Membership()
	if store == nil {
		return nil, status.Error(codes.Unavailable, "cluster membership store is not available")
	}
	return store, nil
}

func (s *AdminClusterService) requireClusterManage(ctx context.Context) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	if s.authorizer == nil {
		return principal, nil
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, clusterManageCapability)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "cluster manage capability is required")
	}
	return principal, nil
}

func clusterModeFromNodeState(state model.NodeState) adminv1.ClusterMode {
	if state == model.NodeStateStandalone {
		return adminv1.ClusterMode_CLUSTER_MODE_STANDALONE
	}
	if state == model.NodeStateClustered || state == model.NodeStateInitializing || state == model.NodeStateFailed || state == model.NodeStateStopped {
		return adminv1.ClusterMode_CLUSTER_MODE_CLUSTERED
	}
	return adminv1.ClusterMode_CLUSTER_MODE_UNSPECIFIED
}

func nodeStateToAdminProto(state model.NodeState) adminv1.ClusterNodeState {
	switch state {
	case model.NodeStateInitializing:
		return adminv1.ClusterNodeState_CLUSTER_NODE_STATE_INITIALIZING
	case model.NodeStateStandalone:
		return adminv1.ClusterNodeState_CLUSTER_NODE_STATE_STANDALONE
	case model.NodeStateClustered:
		return adminv1.ClusterNodeState_CLUSTER_NODE_STATE_CLUSTERED
	case model.NodeStateFailed:
		return adminv1.ClusterNodeState_CLUSTER_NODE_STATE_FAILED
	case model.NodeStateStopped:
		return adminv1.ClusterNodeState_CLUSTER_NODE_STATE_STOPPED
	default:
		return adminv1.ClusterNodeState_CLUSTER_NODE_STATE_UNSPECIFIED
	}
}

func peerStateToAdminProto(state model.PeerState) adminv1.ClusterPeerState {
	switch state {
	case model.PeerStateSelf:
		return adminv1.ClusterPeerState_CLUSTER_PEER_STATE_SELF
	case model.PeerStateSeed:
		return adminv1.ClusterPeerState_CLUSTER_PEER_STATE_SEED
	case model.PeerStateActive:
		return adminv1.ClusterPeerState_CLUSTER_PEER_STATE_ACTIVE
	case model.PeerStateUnreachable:
		return adminv1.ClusterPeerState_CLUSTER_PEER_STATE_UNREACHABLE
	default:
		return adminv1.ClusterPeerState_CLUSTER_PEER_STATE_UNSPECIFIED
	}
}

func peerSourceToAdminProto(source model.PeerSource) adminv1.ClusterPeerSource {
	switch source {
	case model.PeerSourceSelf:
		return adminv1.ClusterPeerSource_CLUSTER_PEER_SOURCE_SELF
	case model.PeerSourceSeed:
		return adminv1.ClusterPeerSource_CLUSTER_PEER_SOURCE_SEED
	case model.PeerSourceDiscovered:
		return adminv1.ClusterPeerSource_CLUSTER_PEER_SOURCE_DISCOVERED
	default:
		return adminv1.ClusterPeerSource_CLUSTER_PEER_SOURCE_UNSPECIFIED
	}
}

func memberStateToAdminProto(state membership.MemberState) adminv1.ClusterMemberState {
	switch state {
	case membership.MemberStatePending:
		return adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_PENDING
	case membership.MemberStateActive:
		return adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_ACTIVE
	case membership.MemberStateRejected:
		return adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_REJECTED
	case membership.MemberStateRemoved:
		return adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_REMOVED
	default:
		return adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_UNSPECIFIED
	}
}

func clusterPeerToProto(peer model.Peer) *adminv1.ClusterPeer {
	return &adminv1.ClusterPeer{NodeId: peer.NodeID, NodeName: peer.NodeName, ClusterId: peer.ClusterID, ClusterName: peer.ClusterName, BackendAdvertiseAddr: peer.BackendAdvertiseAddr, State: peerStateToAdminProto(peer.State), Source: peerSourceToAdminProto(peer.Source), LastSeenAt: formatOptionalClusterTime(peer.LastSeenAt)}
}

func clusterMemberToProto(member membership.Member) *adminv1.ClusterMember {
	out := &adminv1.ClusterMember{NodeName: member.NodeName, NodeId: member.NodeID, State: memberStateToAdminProto(member.State), BackendAdvertiseAddr: member.BackendAdvertiseAddr, Role: member.Role, ClusterBootstrap: member.ClusterBootstrap, NodePublicKeyFingerprint: member.NodePublicKeyFingerprint, CreatedAt: formatClusterTime(member.CreatedAt), UpdatedAt: formatClusterTime(member.UpdatedAt), JoinedAt: formatOptionalClusterTime(member.JoinedAt)}
	if member.JoinToken != nil {
		out.TokenId = member.JoinToken.TokenID
		out.TokenExpiresAt = formatClusterTime(member.JoinToken.ExpiresAt)
		out.TokenConsumedAt = formatOptionalClusterTime(member.JoinToken.ConsumedAt)
		out.TokenRevokedAt = formatOptionalClusterTime(member.JoinToken.RevokedAt)
	}
	return out
}

func formatClusterTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func formatOptionalClusterTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatClusterTime(*t)
}
