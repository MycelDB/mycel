package admin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/replerror"
	"github.com/myceldb/mycel/internal/clustering/replication"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const clusterManageCapability = "CAPABILITY_MESH_MANAGE"

func notPrimaryClusterError(cluster *clustering.Manager) error {
	st := status.New(codes.FailedPrecondition, "node is not cluster primary")
	if cluster == nil {
		return st.Err()
	}
	authority, ok := cluster.Authority()
	if !ok {
		return st.Err()
	}
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: daemonruntime.NotPrimaryReason, Domain: "mycel.cluster", Metadata: map[string]string{
		daemonruntime.PrimaryNodeIDKey:   authority.Primary.NodeID,
		daemonruntime.PrimaryNodeNameKey: authority.Primary.NodeName,
		daemonruntime.PrimaryBackendKey:  authority.Primary.BackendAdvertiseAddr,
		daemonruntime.AuthorityEpochKey:  fmt.Sprintf("%d", authority.AuthorityEpoch),
	}})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

type WALStatusProvider interface {
	LastCommittedLSN() wal.LSN
	RetainedRange(ctx context.Context) (wal.RetainedRange, error)
}

type AdminClusterService struct {
	adminv1.UnimplementedAdminClusterServiceServer
	cluster               *clustering.Manager
	authorizer            OperatorAuthorizer
	replicationProgress   *replication.ProgressStore
	replicationFollower   *replication.Follower
	walStatus             WALStatusProvider
	checkpointStore       *wal.CheckpointStore
	resyncCoordinator     *replication.ResyncCoordinator
	switchoverCoordinator *replication.SwitchoverCoordinator
	failoverCoordinator   *replication.FailoverCoordinator
}

func NewAdminClusterService(cluster *clustering.Manager, authorizer OperatorAuthorizer) *AdminClusterService {
	return &AdminClusterService{cluster: cluster, authorizer: authorizer}
}

func (s *AdminClusterService) WithReplication(progress *replication.ProgressStore, follower *replication.Follower) *AdminClusterService {
	s.replicationProgress = progress
	s.replicationFollower = follower
	return s
}

func (s *AdminClusterService) WithWALStatus(provider WALStatusProvider, checkpoint *wal.CheckpointStore) *AdminClusterService {
	s.walStatus = provider
	s.checkpointStore = checkpoint
	return s
}

func (s *AdminClusterService) WithResync(coordinator *replication.ResyncCoordinator) *AdminClusterService {
	s.resyncCoordinator = coordinator
	return s
}

func (s *AdminClusterService) WithSwitchover(coordinator *replication.SwitchoverCoordinator) *AdminClusterService {
	s.switchoverCoordinator = coordinator
	return s
}

func (s *AdminClusterService) WithFailover(coordinator *replication.FailoverCoordinator) *AdminClusterService {
	s.failoverCoordinator = coordinator
	return s
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
	repl := s.replicationStatusToProto()
	if repl != nil && repl.GetSnapshotRequired() != nil {
		warnings = append(warnings, "replication requires snapshot resync")
	}
	if repl != nil && repl.GetLastError() != "" {
		warnings = append(warnings, "replication error: "+repl.GetLastError())
	}
	if pending > 0 {
		warnings = append(warnings, fmt.Sprintf("%d pending member(s)", pending))
	}
	if !s.cluster.IsAdmitted() {
		warnings = append(warnings, "local node is not admitted")
	}
	health := "healthy"
	if len(warnings) > 0 {
		health = "degraded"
	}
	if !s.cluster.IsAdmitted() {
		health = "unhealthy"
	}
	a, _ := s.cluster.Authority()
	out := &adminv1.GetClusterHealthResponse{Status: health, Warnings: warnings, LocalRole: string(s.cluster.LocalRole()), PrimaryNodeId: a.Primary.NodeID, PrimaryNodeName: a.Primary.NodeName, AuthorityEpoch: a.AuthorityEpoch, ActiveMembers: active, PendingMembers: pending, UnreachablePeers: unreachable}
	if repl != nil {
		out.ReplicationLagRecords = repl.GetLagRecords()
		out.CatchupState = catchupStateText(repl.GetCatchupState())
		out.SnapshotRequired = repl.GetSnapshotRequired() != nil
	}
	return out, nil
}

func firstNonEmptyCluster(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return "unknown"
}

func catchupStateText(state adminv1.ClusterReplicationCatchupState) string {
	switch state {
	case adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_STREAMING:
		return "streaming"
	case adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_CAUGHT_UP:
		return "caught_up"
	case adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_RETRYING:
		return "retrying"
	case adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_SNAPSHOT_REQUIRED:
		return "snapshot_required"
	case adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_ERROR:
		return "error"
	default:
		return "unknown"
	}
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
			Role:                     nodeRoleToAdminProto(s.cluster.LocalRole()),
		},
		Cluster: &adminv1.ClusterInfo{
			ClusterId:   identity.ClusterID,
			ClusterName: identity.ClusterName,
			Mode:        clusterModeFromNodeState(s.cluster.State()),
		},
		Peers:       peers,
		Authority:   clusterAuthorityToAdminProto(s.cluster.Authority()),
		Replication: s.replicationStatusToProto(),
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

func (s *AdminClusterService) RemoveClusterNode(ctx context.Context, req *adminv1.RemoveClusterNodeRequest) (*adminv1.RemoveClusterNodeResponse, error) {
	if _, err := s.requireClusterManage(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil || !s.cluster.IsAdmitted() {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if s.cluster.LocalRole() != clustering.NodeRolePrimary {
		return nil, notPrimaryClusterError(s.cluster)
	}
	store, err := s.membershipStore()
	if err != nil {
		return nil, err
	}
	member, err := s.resolveMember(ctx, strings.TrimSpace(req.GetTarget()))
	if err != nil {
		return nil, err
	}
	a, _ := s.cluster.Authority()
	if member.NodeID == a.Primary.NodeID || member.NodeID == s.cluster.Identity().NodeID {
		return nil, status.Error(codes.FailedPrecondition, "cannot remove cluster primary")
	}
	member.State = membership.MemberStateRemoved
	member.JoinToken = nil
	if err := store.UpsertMember(ctx, member); err != nil {
		return nil, err
	}
	return &adminv1.RemoveClusterNodeResponse{NodeId: member.NodeID, NodeName: member.NodeName, State: adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_REMOVED}, nil
}

func (s *AdminClusterService) RenameClusterNode(ctx context.Context, req *adminv1.RenameClusterNodeRequest) (*adminv1.RenameClusterNodeResponse, error) {
	if _, err := s.requireClusterManage(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil || !s.cluster.IsAdmitted() {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if s.cluster.LocalRole() != clustering.NodeRolePrimary {
		return nil, notPrimaryClusterError(s.cluster)
	}
	newName := strings.TrimSpace(req.GetNewNodeName())
	if newName == "" {
		return nil, status.Error(codes.InvalidArgument, "new_node_name is required")
	}
	store, err := s.membershipStore()
	if err != nil {
		return nil, err
	}
	member, err := s.resolveMember(ctx, strings.TrimSpace(req.GetTarget()))
	if err != nil {
		return nil, err
	}
	member.NodeName = newName
	if err := store.UpsertMember(ctx, member); err != nil {
		return nil, err
	}
	return &adminv1.RenameClusterNodeResponse{NodeId: member.NodeID, NodeName: member.NodeName, State: memberStateToAdminProto(member.State)}, nil
}

func (s *AdminClusterService) AddClusterNode(ctx context.Context, req *adminv1.AddClusterNodeRequest) (*adminv1.AddClusterNodeResponse, error) {
	if _, err := s.requireClusterManage(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil || !s.cluster.IsAdmitted() {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if s.cluster.LocalRole() != clustering.NodeRolePrimary {
		return nil, notPrimaryClusterError(s.cluster)
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

func (s *AdminClusterService) ListClusterResyncOperations(ctx context.Context, req *adminv1.ListClusterResyncOperationsRequest) (*adminv1.ListClusterResyncOperationsResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.resyncCoordinator == nil || s.resyncCoordinator.History == nil {
		return &adminv1.ListClusterResyncOperationsResponse{}, nil
	}
	ops, err := s.resyncCoordinator.History.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*adminv1.ClusterResyncOperation, 0, len(ops))
	for _, op := range ops {
		out = append(out, &adminv1.ClusterResyncOperation{OperationId: op.OperationID, TargetNodeId: op.TargetNodeID, TargetNodeName: op.TargetNodeName, TargetBackendAdvertiseAddr: op.TargetBackendAdvertiseAddr, StartedAt: formatClusterTime(op.StartedAt), CompletedAt: formatClusterTime(op.CompletedAt), Status: string(op.Status), SnapshotBaseLsn: op.SnapshotBaseLSN, TotalBytes: op.TotalBytes, Checksum: op.Checksum, Error: op.Error})
	}
	return &adminv1.ListClusterResyncOperationsResponse{Operations: out}, nil
}

func (s *AdminClusterService) PromoteLocalPrimary(ctx context.Context, req *adminv1.PromoteLocalPrimaryRequest) (*adminv1.PromoteLocalPrimaryResponse, error) {
	if _, err := s.requireClusterManage(ctx); err != nil {
		return nil, err
	}
	if s.failoverCoordinator == nil {
		return nil, status.Error(codes.Unavailable, "cluster failover is not available")
	}
	result, err := s.failoverCoordinator.PromoteLocalPrimary(ctx, req.GetForce(), strings.TrimSpace(req.GetConfirmOldPrimaryFenced()))
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &adminv1.PromoteLocalPrimaryResponse{OperationId: result.OperationID, OldPrimaryNodeId: result.OldPrimaryNodeID, OldPrimaryNodeName: result.OldPrimaryNodeName, NewPrimaryNodeId: result.NewPrimaryNodeID, NewPrimaryNodeName: result.NewPrimaryNodeName, AuthorityEpoch: result.AuthorityEpoch}, nil
}

func (s *AdminClusterService) SwitchClusterPrimary(ctx context.Context, req *adminv1.SwitchClusterPrimaryRequest) (*adminv1.SwitchClusterPrimaryResponse, error) {
	if _, err := s.requireClusterManage(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil || !s.cluster.IsAdmitted() {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if s.cluster.LocalRole() != clustering.NodeRolePrimary {
		return nil, notPrimaryClusterError(s.cluster)
	}
	if s.switchoverCoordinator == nil {
		return nil, status.Error(codes.Unavailable, "cluster switchover is not available")
	}
	target := strings.TrimSpace(req.GetTarget())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target is required")
	}
	if req.GetDryRun() {
		return nil, status.Error(codes.Unimplemented, "dry-run is not implemented")
	}
	if req.GetTimeoutSeconds() > 0 {
		s.switchoverCoordinator.Timeout = time.Duration(req.GetTimeoutSeconds()) * time.Second
	}
	result, err := s.switchoverCoordinator.SwitchPrimary(ctx, target)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &adminv1.SwitchClusterPrimaryResponse{OperationId: result.OperationID, OldPrimaryNodeId: result.OldPrimaryNodeID, OldPrimaryNodeName: result.OldPrimaryNodeName, NewPrimaryNodeId: result.NewPrimaryNodeID, NewPrimaryNodeName: result.NewPrimaryNodeName, AuthorityEpoch: result.AuthorityEpoch, FinalLsn: result.FinalLSN}, nil
}

func (s *AdminClusterService) ResyncClusterNode(ctx context.Context, req *adminv1.ResyncClusterNodeRequest) (*adminv1.ResyncClusterNodeResponse, error) {
	if _, err := s.requireClusterManage(ctx); err != nil {
		return nil, err
	}
	if s.cluster == nil || !s.cluster.IsAdmitted() {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if s.cluster.LocalRole() != clustering.NodeRolePrimary {
		return nil, notPrimaryClusterError(s.cluster)
	}
	if s.resyncCoordinator == nil {
		return nil, status.Error(codes.Unavailable, "cluster resync is not available")
	}
	target := strings.TrimSpace(req.GetTarget())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target is required")
	}
	result, err := s.resyncCoordinator.Resync(ctx, target)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &adminv1.ResyncClusterNodeResponse{OperationId: result.OperationID, TargetNodeId: result.Target.NodeID, TargetNodeName: result.Target.NodeName, SnapshotBaseLsn: result.SnapshotBaseLSN, TotalBytes: result.TotalBytes, Checksum: result.Checksum}, nil
}

func (s *AdminClusterService) replicationStatusToProto() *adminv1.ClusterReplicationStatus {
	if s == nil || s.cluster == nil {
		return nil
	}
	authority, _ := s.cluster.Authority()
	status := &adminv1.ClusterReplicationStatus{PrimaryNodeId: authority.Primary.NodeID, PrimaryNodeName: authority.Primary.NodeName, PrimaryBackendAdvertiseAddr: authority.Primary.BackendAdvertiseAddr, AuthorityEpoch: authority.AuthorityEpoch}
	switch s.cluster.LocalRole() {
	case clustering.NodeRolePrimary:
		status.Role = adminv1.ClusterReplicationRole_CLUSTER_REPLICATION_ROLE_PRIMARY
		status.CatchupState = adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_CAUGHT_UP
		s.populateWALRange(context.Background(), status)
		return status
	case clustering.NodeRoleFollower:
		status.Role = adminv1.ClusterReplicationRole_CLUSTER_REPLICATION_ROLE_FOLLOWER
	default:
		status.Role = adminv1.ClusterReplicationRole_CLUSTER_REPLICATION_ROLE_NOT_APPLICABLE
		return status
	}
	if s.replicationProgress == nil {
		return status
	}
	progress, err := s.replicationProgress.Load(context.Background())
	if err != nil {
		status.LastError = err.Error()
		return status
	}
	s.populateWALRange(context.Background(), status)
	status.ReceivedLsn = uint64(progress.ReceivedLSN)
	status.AppliedLsn = uint64(progress.AppliedLSN)
	status.CatchupState = catchupStateToAdminProto(progress.CatchupState)
	if progress.SnapshotRequired != nil {
		status.SnapshotRequired = snapshotRequiredToAdminProto(*progress.SnapshotRequired)
		status.FirstRetainedLsn = uint64(progress.SnapshotRequired.FirstRetainedLSN)
		status.CheckpointLsn = uint64(progress.SnapshotRequired.CheckpointLSN)
	}
	if status.PrimaryLastLsn >= status.AppliedLsn && status.PrimaryLastLsn > 0 {
		status.LagRecords = status.PrimaryLastLsn - status.AppliedLsn
	} else if progress.ReceivedLSN >= progress.AppliedLSN {
		status.LagRecords = uint64(progress.ReceivedLSN - progress.AppliedLSN)
	}
	status.Connected = false
	status.LastError = progress.LastError
	status.UpdatedAt = formatClusterTime(progress.UpdatedAt)
	if s.replicationFollower != nil {
		status.Connected = s.replicationFollower.Connected()
		if e := s.replicationFollower.LastError(); e != "" {
			status.LastError = e
		}
	}
	return status
}

func (s *AdminClusterService) populateWALRange(ctx context.Context, out *adminv1.ClusterReplicationStatus) {
	if s.walStatus != nil {
		out.PrimaryLastLsn = uint64(s.walStatus.LastCommittedLSN())
		if r, err := s.walStatus.RetainedRange(ctx); err == nil {
			out.FirstRetainedLsn = uint64(r.FirstRetainedLSN)
			out.PrimaryLastLsn = uint64(r.LastCommittedLSN)
		}
	}
	if s.checkpointStore != nil {
		if cp, err := s.checkpointStore.Load(ctx); err == nil {
			out.CheckpointLsn = uint64(cp.LSN)
		}
	}
}

func snapshotRequiredToAdminProto(info replerror.SnapshotRequiredInfo) *adminv1.SnapshotRequiredInfo {
	return &adminv1.SnapshotRequiredInfo{RequestedAfterLsn: uint64(info.RequestedAfterLSN), NextRequestedLsn: uint64(info.NextRequestedLSN), FirstRetainedLsn: uint64(info.FirstRetainedLSN), LastCommittedLsn: uint64(info.LastCommittedLSN), CheckpointLsn: uint64(info.CheckpointLSN), PrimaryNodeId: info.PrimaryNodeID, AuthorityEpoch: info.AuthorityEpoch}
}

func catchupStateToAdminProto(state replication.CatchupState) adminv1.ClusterReplicationCatchupState {
	switch state {
	case replication.CatchupStateStreaming:
		return adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_STREAMING
	case replication.CatchupStateCaughtUp:
		return adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_CAUGHT_UP
	case replication.CatchupStateRetrying:
		return adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_RETRYING
	case replication.CatchupStateSnapshotRequired:
		return adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_SNAPSHOT_REQUIRED
	case replication.CatchupStateError:
		return adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_ERROR
	default:
		return adminv1.ClusterReplicationCatchupState_CLUSTER_REPLICATION_CATCHUP_STATE_UNKNOWN
	}
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

func nodeRoleToAdminProto(role clustering.NodeRole) adminv1.ClusterNodeRole {
	switch role {
	case clustering.NodeRoleNone:
		return adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_NONE
	case clustering.NodeRolePrimary:
		return adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_PRIMARY
	case clustering.NodeRoleFollower:
		return adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_FOLLOWER
	case clustering.NodeRoleCandidate:
		return adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_CANDIDATE
	case clustering.NodeRoleObserver:
		return adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_OBSERVER
	case clustering.NodeRoleLearner:
		return adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_LEARNER
	default:
		return adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_UNSPECIFIED
	}
}

func clusterAuthorityToAdminProto(authority clustering.Authority, ok bool) *adminv1.ClusterAuthority {
	if !ok {
		return nil
	}
	return &adminv1.ClusterAuthority{ClusterId: authority.ClusterID, PrimaryNodeId: authority.Primary.NodeID, PrimaryNodeName: authority.Primary.NodeName, PrimaryBackendAdvertiseAddr: authority.Primary.BackendAdvertiseAddr, AuthorityEpoch: authority.AuthorityEpoch, Term: authority.Term, Source: string(authority.Source), UpdatedAt: formatClusterTime(authority.UpdatedAt)}
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
