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
	return cmd
}

type clusterStatusOutput struct {
	Node    clusterNodeOutput   `json:"node"`
	Cluster clusterInfoOutput   `json:"cluster"`
	Peers   []clusterPeerOutput `json:"peers"`
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
	out := clusterStatusOutput{Node: clusterNodeOutput{NodeID: node.GetNodeId(), Name: node.GetNodeName(), State: nodeStateText(node.GetState()), Admitted: node.GetAdmitted(), Bootstrap: node.GetBootstrap(), BackendAdvertiseAddr: node.GetBackendAdvertiseAddr()}, Cluster: clusterInfoOutput{ClusterID: cluster.GetClusterId(), ClusterName: cluster.GetClusterName(), Mode: clusterModeText(cluster.GetMode())}}
	for _, p := range res.GetPeers() {
		out.Peers = append(out.Peers, clusterPeerOutput{NodeID: p.GetNodeId(), NodeName: p.GetNodeName(), ClusterID: p.GetClusterId(), ClusterName: p.GetClusterName(), BackendAdvertiseAddr: p.GetBackendAdvertiseAddr(), State: peerStateText(p.GetState()), Source: peerSourceText(p.GetSource()), LastSeenAt: p.GetLastSeenAt()})
	}
	lines := []string{fmt.Sprintf("node=%s name=%s state=%s cluster=%s mode=%s\n", out.Node.NodeID, out.Node.Name, out.Node.State, out.Cluster.ClusterName, out.Cluster.Mode)}
	for _, p := range out.Peers {
		lines = append(lines, fmt.Sprintf("%s\t%s\t%s\t%s\n", p.State, p.NodeName, p.BackendAdvertiseAddr, p.Source))
	}
	return a.Print(out, strings.Join(lines, ""))
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
	text := fmt.Sprintf("status=%s active=%d pending=%d unreachable=%d\n", res.GetStatus(), res.GetActiveMembers(), res.GetPendingMembers(), res.GetUnreachablePeers())
	for _, warning := range res.GetWarnings() {
		text += "warning: " + warning + "\n"
	}
	return a.Print(res, text)
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
func replicationCatchupText(v adminv1.ClusterReplicationCatchupState) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_replication_catchup_state_")
}
