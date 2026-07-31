package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/spf13/cobra"
)

func NewClusterCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "cluster", Short: "Inspect and manage daemon cluster topology"}
	cmd.AddCommand(&cobra.Command{Use: "status", Short: "Show local daemon cluster peer status", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterStatus(cmd.Context(), a)
	}})
	cmd.AddCommand(&cobra.Command{Use: "members", Short: "List cluster admission membership", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterMembers(cmd.Context(), a)
	}})
	cmd.AddCommand(&cobra.Command{Use: "health", Short: "Show aggregate cluster health", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterHealth(cmd.Context(), a)
	}})
	cmd.AddCommand(&cobra.Command{Use: "raft-groups", Short: "List local Raft group diagnostics", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterRaftGroups(cmd.Context(), a)
	}})
	return cmd
}

type clusterStatusOutput struct {
	Node      clusterNodeOutput      `json:"node"`
	Cluster   clusterInfoOutput      `json:"cluster"`
	Peers     []clusterPeerOutput    `json:"peers"`
	Readiness clusterReadinessOutput `json:"readiness,omitempty"`
}

type clusterNodeOutput struct {
	NodeID               string `json:"node_id"`
	Name                 string `json:"node_name,omitempty"`
	State                string `json:"state"`
	Admitted             bool   `json:"admitted"`
	Bootstrap            bool   `json:"bootstrap"`
	BackendAdvertiseAddr string `json:"backend_advertise_addr,omitempty"`
}

type clusterInfoOutput struct {
	ClusterID   string `json:"cluster_id"`
	ClusterName string `json:"cluster_name,omitempty"`
	Mode        string `json:"mode"`
}

type clusterPeerOutput struct {
	NodeID               string `json:"node_id,omitempty"`
	NodeName             string `json:"node_name,omitempty"`
	ClusterID            string `json:"cluster_id,omitempty"`
	ClusterName          string `json:"cluster_name,omitempty"`
	BackendAdvertiseAddr string `json:"backend_advertise_addr"`
	State                string `json:"state"`
	Source               string `json:"source"`
	LastSeenAt           string `json:"last_seen_at,omitempty"`
}

type clusterReadinessOutput struct {
	ClientReady            bool     `json:"client_ready"`
	MetadataApplied        bool     `json:"metadata_applied"`
	MetadataValidated      bool     `json:"metadata_validated"`
	PartitionGroupsStarted bool     `json:"partition_groups_started"`
	AuthoritativeClusterID string   `json:"authoritative_cluster_id,omitempty"`
	LocalClusterID         string   `json:"local_cluster_id,omitempty"`
	ExpectedMemberCount    int32    `json:"expected_member_count,omitempty"`
	ReadinessBlockers      []string `json:"readiness_blockers,omitempty"`
}

type clusterMemberOutput struct {
	NodeName                 string `json:"node_name"`
	NodeID                   string `json:"node_id,omitempty"`
	State                    string `json:"state"`
	BackendAdvertiseAddr     string `json:"backend_advertise_addr,omitempty"`
	ClusterBootstrap         bool   `json:"cluster_bootstrap,omitempty"`
	NodePublicKeyFingerprint string `json:"node_public_key_fingerprint,omitempty"`
	TokenID                  string `json:"token_id,omitempty"`
	TokenExpiresAt           string `json:"token_expires_at,omitempty"`
	TokenConsumedAt          string `json:"token_consumed_at,omitempty"`
	TokenRevokedAt           string `json:"token_revoked_at,omitempty"`
	CreatedAt                string `json:"created_at,omitempty"`
	UpdatedAt                string `json:"updated_at,omitempty"`
	JoinedAt                 string `json:"joined_at,omitempty"`
}

type clusterMembersOutput struct {
	ClusterID   string                `json:"cluster_id"`
	ClusterName string                `json:"cluster_name,omitempty"`
	Members     []clusterMemberOutput `json:"members"`
}

type raftGroupOutput struct {
	GroupID               string   `json:"group_id"`
	Kind                  string   `json:"kind"`
	PartitionID           uint32   `json:"partition_id,omitempty"`
	LocalNodeID           uint64   `json:"local_node_id"`
	LeaderNodeID          uint64   `json:"leader_node_id,omitempty"`
	PreferredLeaderNodeID uint64   `json:"preferred_leader_node_id,omitempty"`
	ReplicaNodeIDs        []uint64 `json:"replica_node_ids,omitempty"`
	Health                string   `json:"health"`
	HealthReason          string   `json:"health_reason,omitempty"`
	Term                  uint64   `json:"term"`
	CommitIndex           uint64   `json:"commit_index"`
	AppliedIndex          uint64   `json:"applied_index"`
	ApplyLag              uint64   `json:"apply_lag"`
	LastIndex             uint64   `json:"last_index"`
	SnapshotIndex         uint64   `json:"snapshot_index,omitempty"`
}

type raftGroupsOutput struct {
	Groups []raftGroupOutput `json:"groups"`
}

func runClusterStatus(ctx context.Context, a *app.App) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).GetClusterStatus(authCtx, &adminv1.GetClusterStatusRequest{})
	if err != nil {
		return fmt.Errorf("get cluster status: %w", err)
	}
	node := res.GetNode()
	cluster := res.GetCluster()
	out := clusterStatusOutput{Node: clusterNodeOutput{NodeID: node.GetNodeId(), Name: node.GetNodeName(), State: nodeStateText(node.GetState()), Admitted: node.GetAdmitted(), Bootstrap: node.GetBootstrap(), BackendAdvertiseAddr: node.GetBackendAdvertiseAddr()}, Cluster: clusterInfoOutput{ClusterID: cluster.GetClusterId(), ClusterName: cluster.GetClusterName(), Mode: clusterModeText(cluster.GetMode())}, Readiness: clusterReadinessFromProto(res.GetReadiness())}
	for _, p := range res.GetPeers() {
		out.Peers = append(out.Peers, clusterPeerOutput{NodeID: p.GetNodeId(), NodeName: p.GetNodeName(), ClusterID: p.GetClusterId(), ClusterName: p.GetClusterName(), BackendAdvertiseAddr: p.GetBackendAdvertiseAddr(), State: peerStateText(p.GetState()), Source: peerSourceText(p.GetSource()), LastSeenAt: p.GetLastSeenAt()})
	}
	lines := []string{fmt.Sprintf("node=%s name=%s state=%s cluster=%s mode=%s client_ready=%t metadata_applied=%t metadata_validated=%t partitions_started=%t\n", out.Node.NodeID, out.Node.Name, out.Node.State, out.Cluster.ClusterName, out.Cluster.Mode, out.Readiness.ClientReady, out.Readiness.MetadataApplied, out.Readiness.MetadataValidated, out.Readiness.PartitionGroupsStarted)}
	for _, blocker := range out.Readiness.ReadinessBlockers {
		lines = append(lines, "readiness_blocker: "+blocker+"\n")
	}
	for _, p := range out.Peers {
		lines = append(lines, fmt.Sprintf("%s\t%s\t%s\t%s\n", p.State, p.NodeName, p.BackendAdvertiseAddr, p.Source))
	}
	return a.Print(out, strings.Join(lines, ""))
}

func clusterReadinessFromProto(readiness *adminv1.ClusterReadiness) clusterReadinessOutput {
	if readiness == nil {
		return clusterReadinessOutput{}
	}
	return clusterReadinessOutput{ClientReady: readiness.GetClientReady(), MetadataApplied: readiness.GetMetadataApplied(), MetadataValidated: readiness.GetMetadataValidated(), PartitionGroupsStarted: readiness.GetPartitionGroupsStarted(), AuthoritativeClusterID: readiness.GetAuthoritativeClusterId(), LocalClusterID: readiness.GetLocalClusterId(), ExpectedMemberCount: readiness.GetExpectedMemberCount(), ReadinessBlockers: append([]string(nil), readiness.GetReadinessBlockers()...)}
}

func runClusterHealth(ctx context.Context, a *app.App) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).GetClusterHealth(authCtx, &adminv1.GetClusterHealthRequest{})
	if err != nil {
		return fmt.Errorf("get cluster health: %w", err)
	}
	readiness := res.GetReadiness()
	text := fmt.Sprintf("status=%s active=%d pending=%d unreachable=%d client_ready=%t metadata_applied=%t metadata_validated=%t partitions_started=%t\n", res.GetStatus(), res.GetActiveMembers(), res.GetPendingMembers(), res.GetUnreachablePeers(), readiness.GetClientReady(), readiness.GetMetadataApplied(), readiness.GetMetadataValidated(), readiness.GetPartitionGroupsStarted())
	for _, blocker := range readiness.GetReadinessBlockers() {
		text += "readiness_blocker: " + blocker + "\n"
	}
	for _, warning := range res.GetWarnings() {
		text += "warning: " + warning + "\n"
	}
	return a.Print(res, text)
}

func runClusterRaftGroups(ctx context.Context, a *app.App) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).ListRaftGroups(authCtx, &adminv1.ListRaftGroupsRequest{})
	if err != nil {
		return fmt.Errorf("list raft groups: %w", err)
	}
	out := raftGroupsOutput{Groups: []raftGroupOutput{}}
	lines := []string{}
	for _, group := range res.GetGroups() {
		item := raftGroupOutput{GroupID: group.GetGroupId(), Kind: raftGroupKindText(group.GetKind()), PartitionID: group.GetPartitionId(), LocalNodeID: group.GetLocalNodeId(), LeaderNodeID: group.GetLeaderNodeId(), PreferredLeaderNodeID: group.GetPreferredLeaderNodeId(), ReplicaNodeIDs: append([]uint64(nil), group.GetReplicaNodeIds()...), Health: raftGroupHealthText(group.GetHealth()), HealthReason: group.GetHealthReason(), Term: group.GetTerm(), CommitIndex: group.GetCommitIndex(), AppliedIndex: group.GetAppliedIndex(), ApplyLag: group.GetApplyLag(), LastIndex: group.GetLastIndex(), SnapshotIndex: group.GetSnapshotIndex()}
		out.Groups = append(out.Groups, item)
		line := fmt.Sprintf("%s\t%s\thealth=%s leader=%d term=%d commit=%d applied=%d lag=%d last=%d snapshot=%d", item.Kind, item.GroupID, item.Health, item.LeaderNodeID, item.Term, item.CommitIndex, item.AppliedIndex, item.ApplyLag, item.LastIndex, item.SnapshotIndex)
		if item.HealthReason != "" {
			line += " reason=" + item.HealthReason
		}
		lines = append(lines, line+"\n")
	}
	return a.Print(out, strings.Join(lines, ""))
}

func runClusterMembers(ctx context.Context, a *app.App) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).ListClusterMembers(authCtx, &adminv1.ListClusterMembersRequest{})
	if err != nil {
		return fmt.Errorf("list cluster members: %w", err)
	}
	out := clusterMembersOutput{ClusterID: res.GetClusterId(), ClusterName: res.GetClusterName()}
	lines := []string{}
	for _, m := range res.GetMembers() {
		member := clusterMemberOutput{NodeName: m.GetNodeName(), NodeID: m.GetNodeId(), State: memberStateText(m.GetState()), BackendAdvertiseAddr: m.GetBackendAdvertiseAddr(), ClusterBootstrap: m.GetClusterBootstrap(), NodePublicKeyFingerprint: m.GetNodePublicKeyFingerprint(), TokenID: m.GetTokenId(), TokenExpiresAt: m.GetTokenExpiresAt(), TokenConsumedAt: m.GetTokenConsumedAt(), TokenRevokedAt: m.GetTokenRevokedAt(), CreatedAt: m.GetCreatedAt(), UpdatedAt: m.GetUpdatedAt(), JoinedAt: m.GetJoinedAt()}
		out.Members = append(out.Members, member)
		lines = append(lines, fmt.Sprintf("%s\t%s\t%s\t%s\n", member.State, member.NodeName, member.NodeID, member.BackendAdvertiseAddr))
	}
	return a.Print(out, strings.Join(lines, ""))
}

func nodeStateText(v adminv1.ClusterNodeState) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_node_state_")
}
func clusterModeText(v adminv1.ClusterMode) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_mode_")
}
func peerStateText(v adminv1.ClusterPeerState) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_peer_state_")
}
func peerSourceText(v adminv1.ClusterPeerSource) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_peer_source_")
}
func memberStateText(v adminv1.ClusterMemberState) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_member_state_")
}
func raftGroupKindText(v adminv1.RaftGroupKind) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "raft_group_kind_")
}
func raftGroupHealthText(v adminv1.RaftGroupHealth) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "raft_group_health_")
}
func replicationCatchupText(v adminv1.ClusterReplicationCatchupState) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_replication_catchup_state_")
}
