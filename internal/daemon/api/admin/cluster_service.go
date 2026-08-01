package admin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const clusterManageCapability = "CAPABILITY_MESH_MANAGE"

type WALStatusProvider interface {
	LastCommittedLSN() wal.LSN
	RetainedRange(ctx context.Context) (wal.RetainedRange, error)
}

type LocalGraphConsistencyProvider interface {
	LocalGraphConsistencyStats(ctx context.Context, spaceID string, domainID string) (graphservice.LocalGraphStats, error)
}

type LocalGraphForensicExportProvider interface {
	LocalGraphForensicExport(ctx context.Context, spaceID string, domainID string, opts graphservice.LocalGraphForensicExportOptions) (graphservice.LocalGraphForensicExport, error)
}

type AdminClusterService struct {
	adminv1.UnimplementedAdminClusterServiceServer
	cluster                  *clustering.Manager
	authorizer               OperatorAuthorizer
	walStatus                WALStatusProvider
	checkpointStore          *wal.CheckpointStore
	clusterConfig            daemonconfig.ClusterConfig
	raftGroups               *consensus.MultiGroup
	raftTransportDiagnostics *consensus.TransportDiagnostics
	graphConsistency         LocalGraphConsistencyProvider
	graphForensicExport      LocalGraphForensicExportProvider
	graphPeerCollector       GraphConsistencyPeerCollector
}

func NewAdminClusterService(cluster *clustering.Manager, authorizer OperatorAuthorizer) *AdminClusterService {
	return &AdminClusterService{cluster: cluster, authorizer: authorizer}
}

func (s *AdminClusterService) WithWALStatus(provider WALStatusProvider, checkpoint *wal.CheckpointStore) *AdminClusterService {
	s.walStatus = provider
	s.checkpointStore = checkpoint
	return s
}

func (s *AdminClusterService) WithClusterRuntime(cfg daemonconfig.ClusterConfig, groups *consensus.MultiGroup, diagnostics ...*consensus.TransportDiagnostics) *AdminClusterService {
	s.clusterConfig = cfg
	s.raftGroups = groups
	if len(diagnostics) > 0 {
		s.raftTransportDiagnostics = diagnostics[0]
	}
	return s
}

func (s *AdminClusterService) WithGraphConsistency(provider LocalGraphConsistencyProvider) *AdminClusterService {
	s.graphConsistency = provider
	return s
}

func (s *AdminClusterService) WithGraphForensicExport(provider LocalGraphForensicExportProvider) *AdminClusterService {
	s.graphForensicExport = provider
	return s
}

func (s *AdminClusterService) WithGraphConsistencyPeerCollector(collector GraphConsistencyPeerCollector) *AdminClusterService {
	s.graphPeerCollector = collector
	return s
}

func (s *AdminClusterService) GetClusterRuntimeStatus(ctx context.Context, req *adminv1.GetClusterRuntimeStatusRequest) (*adminv1.GetClusterRuntimeStatusResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	out := &adminv1.GetClusterRuntimeStatusResponse{Engine: adminv1.ClusterEngine_CLUSTER_ENGINE_RAFT, ClusterName: s.clusterConfig.Name, RaftNodeCount: uint32(s.clusterConfig.RaftNodeCount), RaftPartitionCount: uint32(s.clusterConfig.RaftPartitionCount), RaftReplicaFactor: uint32(s.clusterConfig.RaftReplicaFactor), LocalRaftNodeId: uint64(s.clusterConfig.RaftLocalNodeID), RaftNodeAddrs: append([]string(nil), s.clusterConfig.RaftNodeAddrs...)}
	if s.raftGroups != nil {
		statuses := s.raftGroups.Status()
		out.RaftGroupCount = int32(len(statuses))
		for _, st := range statuses {
			if st.Leader != 0 {
				out.RaftGroupsWithLeader++
			}
		}
	}
	out.RaftTransport = raftTransportDiagnosticsToProto(s.raftTransportDiagnostics)
	return out, nil
}

func (s *AdminClusterService) ListRaftGroups(ctx context.Context, req *adminv1.ListRaftGroupsRequest) (*adminv1.ListRaftGroupsResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.raftGroups == nil {
		return &adminv1.ListRaftGroupsResponse{}, nil
	}
	replicas := raftReplicaNodeIDs(s.clusterConfig.RaftNodeCount)
	out := &adminv1.ListRaftGroupsResponse{}
	for _, st := range s.raftGroups.Status() {
		out.Groups = append(out.Groups, raftGroupStatusToProto(st, replicas))
	}
	return out, nil
}

func (s *AdminClusterService) LookupSpaceRoute(ctx context.Context, req *adminv1.LookupSpaceRouteRequest) (*adminv1.LookupSpaceRouteResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	partitionCount := uint32(s.clusterConfig.RaftPartitionCount)
	if partitionCount == 0 {
		return nil, status.Error(codes.FailedPrecondition, "raft partition count is not configured")
	}
	pid, err := partitioning.PartitionForSpace(req.GetSpaceId(), partitionCount)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var leader consensus.NodeID
	if s.raftGroups != nil {
		if g, ok := s.raftGroups.Group(consensus.PartitionGroupID(pid.Uint32())); ok {
			leader = g.Leader()
		}
	}
	return &adminv1.LookupSpaceRouteResponse{SpaceId: strings.TrimSpace(req.GetSpaceId()), PartitionId: pid.Uint32(), LeaderNodeId: uint64(leader), ReplicaNodeIds: raftReplicaNodeIDs(s.clusterConfig.RaftNodeCount)}, nil
}

func (s *AdminClusterService) GetLocalGraphConsistency(ctx context.Context, req *adminv1.GetLocalGraphConsistencyRequest) (*adminv1.GetLocalGraphConsistencyResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.graphConsistency == nil {
		return nil, status.Error(codes.FailedPrecondition, "graph consistency diagnostics are not configured")
	}
	stats, err := s.graphConsistency.LocalGraphConsistencyStats(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	out := &adminv1.GetLocalGraphConsistencyResponse{Stats: localGraphConsistencyStatsToProto(stats)}
	if s.raftGroups != nil && s.clusterConfig.RaftPartitionCount > 0 {
		if st, ok := raftGroupStatusByID(s.raftGroups, consensus.PartitionGroupID(stats.PartitionID)); ok {
			out.RaftGroup = raftGroupStatusToProto(st, raftReplicaNodeIDs(s.clusterConfig.RaftNodeCount))
		} else {
			out.Warnings = append(out.Warnings, fmt.Sprintf("raft partition group %d is not available", stats.PartitionID))
		}
	} else if s.raftGroups == nil {
		out.Warnings = append(out.Warnings, "raft groups are not configured; report is local-only")
	}
	return out, nil
}

func (s *AdminClusterService) GetClusterHealth(ctx context.Context, req *adminv1.GetClusterHealthRequest) (*adminv1.GetClusterHealthResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil {
		return nil, status.Error(codes.Unavailable, "clustering manager is not available")
	}
	warnings := []string{}
	active, pending, unreachable := int32(0), int32(0), int32(0)
	if ms := s.cluster.Membership(); ms != nil {
		if data, err := ms.Load(ctx); err == nil {
			for _, m := range data.Members {
				switch m.State {
				case membership.MemberStateActive:
					active++
				case membership.MemberStatePending:
					pending++
				}
			}
		}
	}
	if top := s.cluster.Topology(); top != nil {
		for _, p := range top.List() {
			if p.State == model.PeerStateUnreachable {
				unreachable++
				warnings = append(warnings, fmt.Sprintf("peer %s is unreachable", firstNonEmptyCluster(p.NodeName, p.BackendAdvertiseAddr)))
			}
		}
	}
	if pending > 0 {
		warnings = append(warnings, fmt.Sprintf("%d pending member(s)", pending))
	}
	readiness := s.cluster.Readiness()
	if readiness.ExpectedMemberCount > 0 && active > 0 && int(active) < readiness.ExpectedMemberCount {
		warnings = append(warnings, fmt.Sprintf("active member count %d is below expected %d", active, readiness.ExpectedMemberCount))
	}
	if s.raftGroups != nil {
		if g, ok := s.raftGroups.Group(consensus.SystemGroupID); !ok || g == nil || g.Leader() == 0 {
			warnings = append(warnings, "system raft group has no leader")
		}
	}
	for _, blocker := range readiness.ReadinessBlockers {
		warnings = append(warnings, blocker)
	}
	if !s.cluster.IsAdmitted() {
		warnings = append(warnings, "local node is not admitted")
	}
	if readiness.MetadataApplied && readiness.AuthoritativeClusterID != "" && readiness.LocalClusterID != "" && readiness.AuthoritativeClusterID != readiness.LocalClusterID {
		warnings = append(warnings, "local cluster_id does not match authoritative system metadata")
	}
	if readiness.MetadataApplied && !readiness.MetadataValidated {
		warnings = append(warnings, "system metadata is not validated")
	}
	if readiness.MetadataValidated && !readiness.PartitionGroupsStarted {
		warnings = append(warnings, "partition groups are not started")
	}
	if !readiness.MetadataApplied {
		warnings = append(warnings, "system metadata is not applied")
	}
	if s.cluster.State() == model.NodeStateInitializing {
		warnings = append(warnings, "local node is initializing")
	}
	health := "healthy"
	if len(warnings) > 0 {
		health = "degraded"
	}
	if !s.cluster.IsAdmitted() || !readiness.ClientReady {
		health = "unhealthy"
	}
	return &adminv1.GetClusterHealthResponse{Status: health, Warnings: warnings, ActiveMembers: active, PendingMembers: pending, UnreachablePeers: unreachable, Readiness: clusterReadinessToProto(readiness)}, nil
}

func clusterReadinessToProto(readiness clustering.ClusterReadiness) *adminv1.ClusterReadiness {
	return &adminv1.ClusterReadiness{ClientReady: readiness.ClientReady, MetadataApplied: readiness.MetadataApplied, MetadataValidated: readiness.MetadataValidated, PartitionGroupsStarted: readiness.PartitionGroupsStarted, AuthoritativeClusterId: readiness.AuthoritativeClusterID, LocalClusterId: readiness.LocalClusterID, ExpectedMemberCount: int32(readiness.ExpectedMemberCount), ReadinessBlockers: append([]string(nil), readiness.ReadinessBlockers...)}
}

func firstNonEmptyCluster(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return "unknown"
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
	readiness := s.cluster.Readiness()
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
		Peers:     peers,
		Readiness: clusterReadinessToProto(readiness),
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

func (s *AdminClusterService) resolveMember(ctx context.Context, target string) (membership.Member, error) {
	if target == "" {
		return membership.Member{}, status.Error(codes.InvalidArgument, "target is required")
	}
	store, err := s.membershipStore()
	if err != nil {
		return membership.Member{}, err
	}
	data, err := store.Load(ctx)
	if err != nil {
		return membership.Member{}, err
	}
	for _, member := range data.Members {
		if member.NodeID == target || strings.EqualFold(member.NodeName, target) {
			return member, nil
		}
	}
	return membership.Member{}, status.Error(codes.NotFound, "cluster member not found")
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

func raftTransportDiagnosticsToProto(diagnostics *consensus.TransportDiagnostics) *adminv1.RaftTransportDiagnostics {
	if diagnostics == nil {
		return nil
	}
	snapshot := diagnostics.Snapshot()
	out := &adminv1.RaftTransportDiagnostics{SendAttempts: snapshot.SendAttempts, SendFailures: snapshot.SendFailures, AuthFailures: snapshot.AuthFailures, MissingSenderFailures: snapshot.MissingSenderFailures, LastErrorAt: formatClusterTime(snapshot.LastErrorAt), LastError: snapshot.LastError, LastFailureReason: snapshot.LastFailureReason, LastGroupId: string(snapshot.LastGroupID), LastSourceNodeId: uint64(snapshot.LastSourceNodeID), LastTargetNodeId: uint64(snapshot.LastTargetNodeID), LastMessageType: snapshot.LastMessageType}
	for _, target := range snapshot.Targets {
		out.Targets = append(out.Targets, &adminv1.RaftTransportTargetDiagnostics{GroupId: string(target.GroupID), TargetNodeId: uint64(target.TargetNodeID), SendAttempts: target.SendAttempts, SendFailures: target.SendFailures, AuthFailures: target.AuthFailures, MissingSenderFailures: target.MissingSenderFailures, LastErrorAt: formatClusterTime(target.LastErrorAt), LastError: target.LastError, LastFailureReason: target.LastFailureReason, LastMessageType: target.LastMessageType})
	}
	return out
}

func raftReplicaNodeIDs(nodeCount int) []uint64 {
	if nodeCount <= 0 {
		return nil
	}
	out := make([]uint64, 0, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		out = append(out, uint64(i))
	}
	return out
}

func raftGroupStatusByID(groups *consensus.MultiGroup, id consensus.GroupID) (consensus.GroupStatus, bool) {
	if groups == nil {
		return consensus.GroupStatus{}, false
	}
	for _, st := range groups.Status() {
		if st.GroupID == id {
			return st, true
		}
	}
	return consensus.GroupStatus{}, false
}

func raftGroupStatusToProto(st consensus.GroupStatus, replicas []uint64) *adminv1.RaftGroupStatus {
	kind := adminv1.RaftGroupKind_RAFT_GROUP_KIND_SYSTEM
	partitionID := uint32(0)
	if st.PartitionID != nil {
		kind = adminv1.RaftGroupKind_RAFT_GROUP_KIND_PARTITION
		partitionID = *st.PartitionID
	}
	health := adminv1.RaftGroupHealth_RAFT_GROUP_HEALTH_HEALTHY
	healthReason := ""
	if st.Leader == 0 {
		health = adminv1.RaftGroupHealth_RAFT_GROUP_HEALTH_NO_LEADER
		healthReason = "raft group has no leader"
	}
	applyLag := uint64(0)
	if st.CommitIndex > st.AppliedIndex {
		applyLag = st.CommitIndex - st.AppliedIndex
	}
	return &adminv1.RaftGroupStatus{GroupId: string(st.GroupID), Kind: kind, PartitionId: partitionID, LocalNodeId: uint64(st.NodeID), LeaderNodeId: uint64(st.Leader), PreferredLeaderNodeId: uint64(st.PreferredLeader), ReplicaNodeIds: append([]uint64(nil), replicas...), Health: health, HealthReason: healthReason, Term: st.Term, CommitIndex: st.CommitIndex, AppliedIndex: st.AppliedIndex, ApplyLag: applyLag, LastIndex: st.LastIndex, SnapshotIndex: st.SnapshotIndex, ReadDiagnostics: raftReadDiagnosticsToProto(st.ReadDiagnostics)}
}

func raftReadDiagnosticsToProto(in consensus.ReadDiagnostics) *adminv1.RaftReadDiagnostics {
	return &adminv1.RaftReadDiagnostics{ReadIndexAttempts: in.ReadIndexAttempts, ReadIndexSuccesses: in.ReadIndexSuccesses, ReadIndexFailures: in.ReadIndexFailures, ReadIndexTimeouts: in.ReadIndexTimeouts, ReadIndexNoLeader: in.ReadIndexNoLeader, ReadIndexNotLeader: in.ReadIndexNotLeader, ApplyWaitFailures: in.ApplyWaitFailures, LastFailureAt: formatClusterTime(in.LastFailureAt), LastFailureReason: in.LastFailureReason, LastReadIndex: in.LastReadIndex, LastAppliedWaitIndex: in.LastAppliedWaitIndex, LastAppliedWaitSuccess: in.LastAppliedWaitSuccess, LastAppliedWaitMillis: in.LastAppliedWaitDuration.Milliseconds()}
}

func localGraphConsistencyStatsToProto(stats graphservice.LocalGraphStats) *adminv1.LocalGraphConsistencyStats {
	return &adminv1.LocalGraphConsistencyStats{SpaceId: stats.SpaceID, DomainId: stats.DomainID, PartitionId: stats.PartitionID, Revision: stats.Revision, NodeCount: uint64(stats.NodeCount), EdgeCount: uint64(stats.EdgeCount), NodeChecksum: stats.NodeChecksum, EdgeChecksum: stats.EdgeChecksum, GraphChecksum: stats.GraphChecksum, ChecksumAlgorithm: stats.ChecksumAlgorithm, CollectedAt: formatClusterTime(stats.CollectedAt), Source: stats.Source}
}
