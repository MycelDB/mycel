package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
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
	readinessCmd := &cobra.Command{Use: "readiness", Short: "Check local daemon cluster readiness"}
	readinessCmd.AddCommand(&cobra.Command{Use: "check", Short: "Exit successfully only when local daemon is client-ready", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterReadinessCheck(cmd.Context(), a)
	}})
	cmd.AddCommand(readinessCmd)
	cmd.AddCommand(&cobra.Command{Use: "raft-groups", Short: "List local Raft group diagnostics", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterRaftGroups(cmd.Context(), a)
	}})
	var consistencySpaceID string
	var consistencyDomainID string
	consistencyCmd := &cobra.Command{Use: "consistency", Short: "Show local graph consistency diagnostics", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterConsistency(cmd.Context(), a, consistencySpaceID, consistencyDomainID)
	}}
	consistencyCmd.Flags().StringVar(&consistencySpaceID, "space-id", "", "space ID")
	consistencyCmd.Flags().StringVar(&consistencyDomainID, "domain-id", "", "domain ID")
	_ = consistencyCmd.MarkFlagRequired("space-id")
	_ = consistencyCmd.MarkFlagRequired("domain-id")
	cmd.AddCommand(consistencyCmd)
	var reportSpaceID string
	var reportDomainID string
	reportCmd := &cobra.Command{Use: "consistency-report", Short: "Show cluster graph consistency report", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterConsistencyReport(cmd.Context(), a, reportSpaceID, reportDomainID)
	}}
	reportCmd.Flags().StringVar(&reportSpaceID, "space-id", "", "space ID")
	reportCmd.Flags().StringVar(&reportDomainID, "domain-id", "", "domain ID")
	_ = reportCmd.MarkFlagRequired("space-id")
	_ = reportCmd.MarkFlagRequired("domain-id")
	cmd.AddCommand(reportCmd)
	var exportSpaceID string
	var exportDomainID string
	var exportPageSize uint32
	var exportPageToken string
	var exportSourceLabel string
	exportCmd := &cobra.Command{Use: "forensic-export", Short: "Export bounded local graph forensic evidence", RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterForensicExport(cmd.Context(), a, exportSpaceID, exportDomainID, exportPageSize, exportPageToken, exportSourceLabel)
	}}
	exportCmd.Flags().StringVar(&exportSpaceID, "space-id", "", "space ID")
	exportCmd.Flags().StringVar(&exportDomainID, "domain-id", "", "domain ID")
	exportCmd.Flags().Uint32Var(&exportPageSize, "page-size", 100, "maximum entities to export in this page")
	exportCmd.Flags().StringVar(&exportPageToken, "page-token", "", "entity offset page token")
	exportCmd.Flags().StringVar(&exportSourceLabel, "source-label", "", "operator label for the source pod/PVC")
	_ = exportCmd.MarkFlagRequired("space-id")
	_ = exportCmd.MarkFlagRequired("domain-id")
	cmd.AddCommand(exportCmd)
	var diffLimit int
	diffCmd := &cobra.Command{Use: "forensic-diff --left FILE --right FILE", Short: "Diff two graph forensic export JSON files", RunE: func(cmd *cobra.Command, args []string) error {
		left, _ := cmd.Flags().GetString("left")
		right, _ := cmd.Flags().GetString("right")
		return runClusterForensicDiff(a, left, right, diffLimit)
	}}
	diffCmd.Flags().String("left", "", "left/source forensic export JSON file")
	diffCmd.Flags().String("right", "", "right/target forensic export JSON file")
	diffCmd.Flags().IntVar(&diffLimit, "limit", 100, "maximum missing/differing IDs per category; 0 means unlimited")
	_ = diffCmd.MarkFlagRequired("left")
	_ = diffCmd.MarkFlagRequired("right")
	cmd.AddCommand(diffCmd)
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
	GroupID               string                    `json:"group_id"`
	Kind                  string                    `json:"kind"`
	PartitionID           uint32                    `json:"partition_id,omitempty"`
	LocalNodeID           uint64                    `json:"local_node_id"`
	LeaderNodeID          uint64                    `json:"leader_node_id,omitempty"`
	PreferredLeaderNodeID uint64                    `json:"preferred_leader_node_id,omitempty"`
	ReplicaNodeIDs        []uint64                  `json:"replica_node_ids,omitempty"`
	Health                string                    `json:"health"`
	HealthReason          string                    `json:"health_reason,omitempty"`
	Term                  uint64                    `json:"term"`
	CommitIndex           uint64                    `json:"commit_index"`
	AppliedIndex          uint64                    `json:"applied_index"`
	ApplyLag              uint64                    `json:"apply_lag"`
	LastIndex             uint64                    `json:"last_index"`
	SnapshotIndex         uint64                    `json:"snapshot_index,omitempty"`
	ReadDiagnostics       raftReadDiagnosticsOutput `json:"read_diagnostics"`
}

type raftReadDiagnosticsOutput struct {
	ReadIndexAttempts      uint64 `json:"read_index_attempts"`
	ReadIndexSuccesses     uint64 `json:"read_index_successes"`
	ReadIndexFailures      uint64 `json:"read_index_failures"`
	ReadIndexTimeouts      uint64 `json:"read_index_timeouts"`
	ReadIndexNoLeader      uint64 `json:"read_index_no_leader"`
	ReadIndexNotLeader     uint64 `json:"read_index_not_leader"`
	ApplyWaitFailures      uint64 `json:"apply_wait_failures"`
	LastFailureAt          string `json:"last_failure_at,omitempty"`
	LastFailureReason      string `json:"last_failure_reason,omitempty"`
	LastReadIndex          uint64 `json:"last_read_index,omitempty"`
	LastAppliedWaitIndex   uint64 `json:"last_applied_wait_index,omitempty"`
	LastAppliedWaitSuccess uint64 `json:"last_applied_wait_success,omitempty"`
	LastAppliedWaitMillis  int64  `json:"last_applied_wait_millis,omitempty"`
}

type raftGroupsOutput struct {
	Groups []raftGroupOutput `json:"groups"`
}

type graphConsistencyStatsOutput struct {
	SpaceID           string `json:"space_id"`
	DomainID          string `json:"domain_id"`
	PartitionID       uint32 `json:"partition_id,omitempty"`
	Revision          uint64 `json:"revision"`
	NodeCount         uint64 `json:"node_count"`
	EdgeCount         uint64 `json:"edge_count"`
	NodeChecksum      string `json:"node_checksum"`
	EdgeChecksum      string `json:"edge_checksum"`
	GraphChecksum     string `json:"graph_checksum"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	CollectedAt       string `json:"collected_at,omitempty"`
	Source            string `json:"source"`
}

type graphConsistencyOutput struct {
	Stats     graphConsistencyStatsOutput `json:"stats"`
	RaftGroup *raftGroupOutput            `json:"raft_group,omitempty"`
	Warnings  []string                    `json:"warnings,omitempty"`
}

type graphConsistencyReportOutput struct {
	Status                 string                              `json:"status"`
	SpaceID                string                              `json:"space_id"`
	DomainID               string                              `json:"domain_id"`
	PartitionID            uint32                              `json:"partition_id,omitempty"`
	LocalNodeID            uint64                              `json:"local_node_id,omitempty"`
	LeaderNodeID           uint64                              `json:"leader_node_id,omitempty"`
	ExpectedReplicaNodeIDs []uint64                            `json:"expected_replica_node_ids,omitempty"`
	ComparisonBasis        string                              `json:"comparison_basis"`
	RaftGroup              *raftGroupOutput                    `json:"raft_group,omitempty"`
	Replicas               []graphConsistencyReplicaOutput     `json:"replicas"`
	Warnings               []graphConsistencyStructuredWarning `json:"warnings,omitempty"`
}

type graphConsistencyReplicaOutput struct {
	RaftNodeID  uint64                       `json:"raft_node_id,omitempty"`
	NodeID      string                       `json:"node_id,omitempty"`
	NodeName    string                       `json:"node_name,omitempty"`
	BackendAddr string                       `json:"backend_addr,omitempty"`
	Local       bool                         `json:"local,omitempty"`
	Reachable   bool                         `json:"reachable"`
	Stats       *graphConsistencyStatsOutput `json:"stats,omitempty"`
	Error       string                       `json:"error,omitempty"`
}

type graphConsistencyStructuredWarning struct {
	Code       string `json:"code"`
	Severity   string `json:"severity"`
	RaftNodeID uint64 `json:"raft_node_id,omitempty"`
	Message    string `json:"message"`
}

type graphForensicExportOutput struct {
	Manifest      graphForensicManifestOutput `json:"manifest"`
	Stats         graphConsistencyStatsOutput `json:"stats"`
	Nodes         []graphForensicEntityOutput `json:"nodes"`
	Edges         []graphForensicEntityOutput `json:"edges"`
	NextPageToken string                      `json:"next_page_token,omitempty"`
	Truncated     bool                        `json:"truncated"`
	Warnings      []string                    `json:"warnings,omitempty"`
}

type graphForensicManifestOutput struct {
	ReportID        string `json:"report_id"`
	SourceNodeID    string `json:"source_node_id,omitempty"`
	SourceNodeName  string `json:"source_node_name,omitempty"`
	SourceClusterID string `json:"source_cluster_id,omitempty"`
	SourceLabel     string `json:"source_label,omitempty"`
	CollectedAt     string `json:"collected_at,omitempty"`
	MycelVersion    string `json:"mycel_version,omitempty"`
	ImageTag        string `json:"image_tag,omitempty"`
}

type graphForensicEntityOutput struct {
	ID            string `json:"id"`
	Checksum      string `json:"checksum"`
	CanonicalJSON string `json:"canonical_json"`
}

type graphForensicDiffOutput struct {
	Status           string                    `json:"status"`
	Left             graphForensicDiffSource   `json:"left"`
	Right            graphForensicDiffSource   `json:"right"`
	NodeSummary      graphForensicDiffSummary  `json:"node_summary"`
	EdgeSummary      graphForensicDiffSummary  `json:"edge_summary"`
	NodesOnlyInLeft  []string                  `json:"nodes_only_in_left,omitempty"`
	NodesOnlyInRight []string                  `json:"nodes_only_in_right,omitempty"`
	EdgesOnlyInLeft  []string                  `json:"edges_only_in_left,omitempty"`
	EdgesOnlyInRight []string                  `json:"edges_only_in_right,omitempty"`
	DifferingNodes   []graphForensicEntityDiff `json:"differing_nodes,omitempty"`
	DifferingEdges   []graphForensicEntityDiff `json:"differing_edges,omitempty"`
	Truncated        bool                      `json:"truncated"`
	Warnings         []string                  `json:"warnings,omitempty"`
}

type graphForensicDiffSource struct {
	ReportID        string `json:"report_id,omitempty"`
	SourceNodeID    string `json:"source_node_id,omitempty"`
	SourceNodeName  string `json:"source_node_name,omitempty"`
	SourceClusterID string `json:"source_cluster_id,omitempty"`
	SourceLabel     string `json:"source_label,omitempty"`
	Revision        uint64 `json:"revision"`
	NodeCount       uint64 `json:"node_count"`
	EdgeCount       uint64 `json:"edge_count"`
	GraphChecksum   string `json:"graph_checksum"`
}

type graphForensicDiffSummary struct {
	OnlyInLeft     int `json:"only_in_left"`
	OnlyInRight    int `json:"only_in_right"`
	Differing      int `json:"differing"`
	ShownOnlyLeft  int `json:"shown_only_in_left"`
	ShownOnlyRight int `json:"shown_only_in_right"`
	ShownDiffering int `json:"shown_differing"`
}

type graphForensicEntityDiff struct {
	ID            string   `json:"id"`
	LeftChecksum  string   `json:"left_checksum"`
	RightChecksum string   `json:"right_checksum"`
	ChangedFields []string `json:"changed_fields,omitempty"`
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

func runClusterReadinessCheck(ctx context.Context, a *app.App) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).GetClusterStatus(authCtx, &adminv1.GetClusterStatusRequest{})
	if err != nil {
		return fmt.Errorf("get cluster status: %w", err)
	}
	readiness := clusterReadinessFromProto(res.GetReadiness())
	if err := validateClusterReadiness(readiness); err != nil {
		return err
	}
	return a.Print(readiness, "cluster ready\n")
}

func validateClusterReadiness(readiness clusterReadinessOutput) error {
	blockers := append([]string(nil), readiness.ReadinessBlockers...)
	if !readiness.ClientReady {
		blockers = append(blockers, "client_ready=false")
	}
	if !readiness.MetadataApplied {
		blockers = append(blockers, "metadata_applied=false")
	}
	if !readiness.MetadataValidated {
		blockers = append(blockers, "metadata_validated=false")
	}
	if !readiness.PartitionGroupsStarted {
		blockers = append(blockers, "partition_groups_started=false")
	}
	if readiness.AuthoritativeClusterID != "" && readiness.LocalClusterID != "" && readiness.AuthoritativeClusterID != readiness.LocalClusterID {
		blockers = append(blockers, fmt.Sprintf("cluster_id_mismatch authoritative=%s local=%s", readiness.AuthoritativeClusterID, readiness.LocalClusterID))
	}
	if len(blockers) > 0 {
		return fmt.Errorf("cluster not ready: %s", strings.Join(blockers, "; "))
	}
	return nil
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
	out, text := buildRaftGroupsOutput(res.GetGroups())
	return a.Print(out, text)
}

func buildRaftGroupsOutput(groups []*adminv1.RaftGroupStatus) (raftGroupsOutput, string) {
	out := raftGroupsOutput{Groups: []raftGroupOutput{}}
	lines := []string{}
	for _, group := range groups {
		read := raftReadDiagnosticsFromProto(group.GetReadDiagnostics())
		item := raftGroupOutput{GroupID: group.GetGroupId(), Kind: raftGroupKindText(group.GetKind()), PartitionID: group.GetPartitionId(), LocalNodeID: group.GetLocalNodeId(), LeaderNodeID: group.GetLeaderNodeId(), PreferredLeaderNodeID: group.GetPreferredLeaderNodeId(), ReplicaNodeIDs: append([]uint64(nil), group.GetReplicaNodeIds()...), Health: raftGroupHealthText(group.GetHealth()), HealthReason: group.GetHealthReason(), Term: group.GetTerm(), CommitIndex: group.GetCommitIndex(), AppliedIndex: group.GetAppliedIndex(), ApplyLag: group.GetApplyLag(), LastIndex: group.GetLastIndex(), SnapshotIndex: group.GetSnapshotIndex(), ReadDiagnostics: read}
		out.Groups = append(out.Groups, item)
		line := fmt.Sprintf("%s\t%s\thealth=%s leader=%d term=%d commit=%d applied=%d lag=%d last=%d snapshot=%d read_attempts=%d read_ok=%d read_fail=%d", item.Kind, item.GroupID, item.Health, item.LeaderNodeID, item.Term, item.CommitIndex, item.AppliedIndex, item.ApplyLag, item.LastIndex, item.SnapshotIndex, read.ReadIndexAttempts, read.ReadIndexSuccesses, read.ReadIndexFailures)
		if item.HealthReason != "" {
			line += " reason=" + item.HealthReason
		}
		if read.LastFailureReason != "" {
			line += " read_failure=" + read.LastFailureReason
		}
		lines = append(lines, line+"\n")
	}
	return out, strings.Join(lines, "")
}

func raftReadDiagnosticsFromProto(in *adminv1.RaftReadDiagnostics) raftReadDiagnosticsOutput {
	if in == nil {
		return raftReadDiagnosticsOutput{}
	}
	return raftReadDiagnosticsOutput{ReadIndexAttempts: in.GetReadIndexAttempts(), ReadIndexSuccesses: in.GetReadIndexSuccesses(), ReadIndexFailures: in.GetReadIndexFailures(), ReadIndexTimeouts: in.GetReadIndexTimeouts(), ReadIndexNoLeader: in.GetReadIndexNoLeader(), ReadIndexNotLeader: in.GetReadIndexNotLeader(), ApplyWaitFailures: in.GetApplyWaitFailures(), LastFailureAt: in.GetLastFailureAt(), LastFailureReason: in.GetLastFailureReason(), LastReadIndex: in.GetLastReadIndex(), LastAppliedWaitIndex: in.GetLastAppliedWaitIndex(), LastAppliedWaitSuccess: in.GetLastAppliedWaitSuccess(), LastAppliedWaitMillis: in.GetLastAppliedWaitMillis()}
}

func runClusterConsistency(ctx context.Context, a *app.App, spaceID string, domainID string) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).GetLocalGraphConsistency(authCtx, &adminv1.GetLocalGraphConsistencyRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		return fmt.Errorf("get local graph consistency: %w", err)
	}
	out := buildGraphConsistencyOutput(res)
	text := graphConsistencyText(out)
	return a.Print(out, text)
}

func runClusterConsistencyReport(ctx context.Context, a *app.App, spaceID string, domainID string) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).GetGraphConsistencyReport(authCtx, &adminv1.GetGraphConsistencyReportRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		return fmt.Errorf("get graph consistency report: %w", err)
	}
	out := buildGraphConsistencyReportOutput(res)
	return a.Print(out, graphConsistencyReportText(out))
}

func runClusterForensicExport(ctx context.Context, a *app.App, spaceID string, domainID string, pageSize uint32, pageToken string, sourceLabel string) error {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := adminv1.NewAdminClusterServiceClient(conn).GetLocalGraphForensicExport(authCtx, &adminv1.GetLocalGraphForensicExportRequest{SpaceId: spaceID, DomainId: domainID, PageSize: pageSize, PageToken: pageToken, SourceLabel: sourceLabel})
	if err != nil {
		return fmt.Errorf("get local graph forensic export: %w", err)
	}
	out := buildGraphForensicExportOutput(res)
	return a.Print(out, graphForensicExportText(out))
}

func runClusterForensicDiff(a *app.App, leftPath string, rightPath string, limit int) error {
	left, err := readGraphForensicExportFile(leftPath)
	if err != nil {
		return fmt.Errorf("read left forensic export: %w", err)
	}
	right, err := readGraphForensicExportFile(rightPath)
	if err != nil {
		return fmt.Errorf("read right forensic export: %w", err)
	}
	out := diffGraphForensicExports(left, right, limit)
	return a.Print(out, graphForensicDiffText(out))
}

func buildGraphConsistencyOutput(res *adminv1.GetLocalGraphConsistencyResponse) graphConsistencyOutput {
	stats := res.GetStats()
	out := graphConsistencyOutput{Stats: graphConsistencyStatsOutput{SpaceID: stats.GetSpaceId(), DomainID: stats.GetDomainId(), PartitionID: stats.GetPartitionId(), Revision: stats.GetRevision(), NodeCount: stats.GetNodeCount(), EdgeCount: stats.GetEdgeCount(), NodeChecksum: stats.GetNodeChecksum(), EdgeChecksum: stats.GetEdgeChecksum(), GraphChecksum: stats.GetGraphChecksum(), ChecksumAlgorithm: stats.GetChecksumAlgorithm(), CollectedAt: stats.GetCollectedAt(), Source: stats.GetSource()}, Warnings: append([]string(nil), res.GetWarnings()...)}
	if group := res.GetRaftGroup(); group != nil {
		read := raftReadDiagnosticsFromProto(group.GetReadDiagnostics())
		item := raftGroupOutput{GroupID: group.GetGroupId(), Kind: raftGroupKindText(group.GetKind()), PartitionID: group.GetPartitionId(), LocalNodeID: group.GetLocalNodeId(), LeaderNodeID: group.GetLeaderNodeId(), PreferredLeaderNodeID: group.GetPreferredLeaderNodeId(), ReplicaNodeIDs: append([]uint64(nil), group.GetReplicaNodeIds()...), Health: raftGroupHealthText(group.GetHealth()), HealthReason: group.GetHealthReason(), Term: group.GetTerm(), CommitIndex: group.GetCommitIndex(), AppliedIndex: group.GetAppliedIndex(), ApplyLag: group.GetApplyLag(), LastIndex: group.GetLastIndex(), SnapshotIndex: group.GetSnapshotIndex(), ReadDiagnostics: read}
		out.RaftGroup = &item
	}
	return out
}

func graphConsistencyText(out graphConsistencyOutput) string {
	stats := out.Stats
	text := fmt.Sprintf("space=%s domain=%s partition=%d revision=%d nodes=%d edges=%d checksum=%s algorithm=%s source=%s\n", stats.SpaceID, stats.DomainID, stats.PartitionID, stats.Revision, stats.NodeCount, stats.EdgeCount, stats.GraphChecksum, stats.ChecksumAlgorithm, stats.Source)
	if out.RaftGroup != nil {
		g := out.RaftGroup
		text += fmt.Sprintf("raft_group=%s health=%s leader=%d term=%d commit=%d applied=%d lag=%d\n", g.GroupID, g.Health, g.LeaderNodeID, g.Term, g.CommitIndex, g.AppliedIndex, g.ApplyLag)
	}
	for _, warning := range out.Warnings {
		text += "warning: " + warning + "\n"
	}
	return text
}

func buildGraphConsistencyReportOutput(res *adminv1.GetGraphConsistencyReportResponse) graphConsistencyReportOutput {
	out := graphConsistencyReportOutput{Status: graphConsistencyStatusText(res.GetStatus()), SpaceID: res.GetSpaceId(), DomainID: res.GetDomainId(), PartitionID: res.GetPartitionId(), LocalNodeID: res.GetLocalNodeId(), LeaderNodeID: res.GetLeaderNodeId(), ExpectedReplicaNodeIDs: append([]uint64(nil), res.GetExpectedReplicaNodeIds()...), ComparisonBasis: res.GetComparisonBasis(), Replicas: []graphConsistencyReplicaOutput{}}
	if group := res.GetRaftGroup(); group != nil {
		read := raftReadDiagnosticsFromProto(group.GetReadDiagnostics())
		out.RaftGroup = &raftGroupOutput{GroupID: group.GetGroupId(), Kind: raftGroupKindText(group.GetKind()), PartitionID: group.GetPartitionId(), LocalNodeID: group.GetLocalNodeId(), LeaderNodeID: group.GetLeaderNodeId(), PreferredLeaderNodeID: group.GetPreferredLeaderNodeId(), ReplicaNodeIDs: append([]uint64(nil), group.GetReplicaNodeIds()...), Health: raftGroupHealthText(group.GetHealth()), HealthReason: group.GetHealthReason(), Term: group.GetTerm(), CommitIndex: group.GetCommitIndex(), AppliedIndex: group.GetAppliedIndex(), ApplyLag: group.GetApplyLag(), LastIndex: group.GetLastIndex(), SnapshotIndex: group.GetSnapshotIndex(), ReadDiagnostics: read}
	}
	for _, replica := range res.GetReplicas() {
		item := graphConsistencyReplicaOutput{RaftNodeID: replica.GetRaftNodeId(), NodeID: replica.GetNodeId(), NodeName: replica.GetNodeName(), BackendAddr: replica.GetBackendAddr(), Local: replica.GetLocal(), Reachable: replica.GetReachable(), Error: replica.GetError()}
		if stats := replica.GetStats(); stats != nil {
			mapped := graphConsistencyStatsFromProto(stats)
			item.Stats = &mapped
		}
		out.Replicas = append(out.Replicas, item)
	}
	for _, warning := range res.GetWarnings() {
		out.Warnings = append(out.Warnings, graphConsistencyStructuredWarning{Code: warning.GetCode(), Severity: graphConsistencyWarningSeverityText(warning.GetSeverity()), RaftNodeID: warning.GetRaftNodeId(), Message: warning.GetMessage()})
	}
	return out
}

func graphConsistencyStatsFromProto(stats *adminv1.LocalGraphConsistencyStats) graphConsistencyStatsOutput {
	if stats == nil {
		return graphConsistencyStatsOutput{}
	}
	return graphConsistencyStatsOutput{SpaceID: stats.GetSpaceId(), DomainID: stats.GetDomainId(), PartitionID: stats.GetPartitionId(), Revision: stats.GetRevision(), NodeCount: stats.GetNodeCount(), EdgeCount: stats.GetEdgeCount(), NodeChecksum: stats.GetNodeChecksum(), EdgeChecksum: stats.GetEdgeChecksum(), GraphChecksum: stats.GetGraphChecksum(), ChecksumAlgorithm: stats.GetChecksumAlgorithm(), CollectedAt: stats.GetCollectedAt(), Source: stats.GetSource()}
}

func graphConsistencyReportText(out graphConsistencyReportOutput) string {
	text := fmt.Sprintf("status=%s space=%s domain=%s partition=%d replicas=%d/%d basis=%s\n", out.Status, out.SpaceID, out.DomainID, out.PartitionID, reachableGraphConsistencyReplicas(out.Replicas), len(out.ExpectedReplicaNodeIDs), out.ComparisonBasis)
	if out.RaftGroup != nil {
		g := out.RaftGroup
		text += fmt.Sprintf("raft_group=%s health=%s leader=%d term=%d commit=%d applied=%d lag=%d\n", g.GroupID, g.Health, g.LeaderNodeID, g.Term, g.CommitIndex, g.AppliedIndex, g.ApplyLag)
	}
	for _, replica := range out.Replicas {
		line := fmt.Sprintf("replica=%d reachable=%t", replica.RaftNodeID, replica.Reachable)
		if replica.NodeName != "" {
			line += " name=" + replica.NodeName
		}
		if replica.Local {
			line += " local=true"
		}
		if replica.Stats != nil {
			line += fmt.Sprintf(" revision=%d nodes=%d edges=%d checksum=%s", replica.Stats.Revision, replica.Stats.NodeCount, replica.Stats.EdgeCount, replica.Stats.GraphChecksum)
		}
		if replica.Error != "" {
			line += " error=" + replica.Error
		}
		text += line + "\n"
	}
	for _, warning := range out.Warnings {
		text += fmt.Sprintf("warning: code=%s severity=%s", warning.Code, warning.Severity)
		if warning.RaftNodeID != 0 {
			text += fmt.Sprintf(" replica=%d", warning.RaftNodeID)
		}
		text += " " + warning.Message + "\n"
	}
	return text
}

func reachableGraphConsistencyReplicas(replicas []graphConsistencyReplicaOutput) int {
	count := 0
	for _, replica := range replicas {
		if replica.Reachable {
			count++
		}
	}
	return count
}

func buildGraphForensicExportOutput(res *adminv1.GetLocalGraphForensicExportResponse) graphForensicExportOutput {
	manifest := res.GetManifest()
	out := graphForensicExportOutput{Manifest: graphForensicManifestOutput{ReportID: manifest.GetReportId(), SourceNodeID: manifest.GetSourceNodeId(), SourceNodeName: manifest.GetSourceNodeName(), SourceClusterID: manifest.GetSourceClusterId(), SourceLabel: manifest.GetSourceLabel(), CollectedAt: manifest.GetCollectedAt(), MycelVersion: manifest.GetMycelVersion(), ImageTag: manifest.GetImageTag()}, Stats: graphConsistencyStatsFromProto(res.GetStats()), NextPageToken: res.GetNextPageToken(), Truncated: res.GetTruncated(), Warnings: append([]string(nil), res.GetWarnings()...)}
	for _, node := range res.GetNodes() {
		out.Nodes = append(out.Nodes, graphForensicEntityOutput{ID: node.GetId(), Checksum: node.GetChecksum(), CanonicalJSON: node.GetCanonicalJson()})
	}
	for _, edge := range res.GetEdges() {
		out.Edges = append(out.Edges, graphForensicEntityOutput{ID: edge.GetId(), Checksum: edge.GetChecksum(), CanonicalJSON: edge.GetCanonicalJson()})
	}
	return out
}

func graphForensicExportText(out graphForensicExportOutput) string {
	text := fmt.Sprintf("report=%s source=%s cluster=%s space=%s domain=%s revision=%d nodes=%d/%d edges=%d/%d checksum=%s truncated=%t next_page_token=%s\n", out.Manifest.ReportID, firstNonEmptyText(out.Manifest.SourceLabel, out.Manifest.SourceNodeName, out.Manifest.SourceNodeID), out.Manifest.SourceClusterID, out.Stats.SpaceID, out.Stats.DomainID, out.Stats.Revision, len(out.Nodes), out.Stats.NodeCount, len(out.Edges), out.Stats.EdgeCount, out.Stats.GraphChecksum, out.Truncated, out.NextPageToken)
	for _, warning := range out.Warnings {
		text += "warning: " + warning + "\n"
	}
	return text
}

func readGraphForensicExportFile(path string) (graphForensicExportOutput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return graphForensicExportOutput{}, err
	}
	var out graphForensicExportOutput
	if err := json.Unmarshal(data, &out); err != nil {
		return graphForensicExportOutput{}, err
	}
	return out, nil
}

func diffGraphForensicExports(left graphForensicExportOutput, right graphForensicExportOutput, limit int) graphForensicDiffOutput {
	out := graphForensicDiffOutput{Status: "identical", Left: graphForensicDiffSourceFromExport(left), Right: graphForensicDiffSourceFromExport(right)}
	if left.Truncated || right.Truncated {
		out.Warnings = append(out.Warnings, "one or both exports are truncated; diff only covers included entities")
	}
	leftNodes := forensicEntityMap(left.Nodes)
	rightNodes := forensicEntityMap(right.Nodes)
	out.NodesOnlyInLeft, out.NodesOnlyInRight, out.DifferingNodes, out.NodeSummary = diffForensicEntityMaps(leftNodes, rightNodes, limit)
	leftEdges := forensicEntityMap(left.Edges)
	rightEdges := forensicEntityMap(right.Edges)
	out.EdgesOnlyInLeft, out.EdgesOnlyInRight, out.DifferingEdges, out.EdgeSummary = diffForensicEntityMaps(leftEdges, rightEdges, limit)
	if out.NodeSummary.OnlyInLeft > 0 || out.EdgeSummary.OnlyInLeft > 0 || out.NodeSummary.OnlyInRight > 0 || out.EdgeSummary.OnlyInRight > 0 || out.NodeSummary.Differing > 0 || out.EdgeSummary.Differing > 0 {
		out.Status = "different"
	}
	out.Truncated = out.NodeSummary.OnlyInLeft > out.NodeSummary.ShownOnlyLeft || out.NodeSummary.OnlyInRight > out.NodeSummary.ShownOnlyRight || out.NodeSummary.Differing > out.NodeSummary.ShownDiffering || out.EdgeSummary.OnlyInLeft > out.EdgeSummary.ShownOnlyLeft || out.EdgeSummary.OnlyInRight > out.EdgeSummary.ShownOnlyRight || out.EdgeSummary.Differing > out.EdgeSummary.ShownDiffering
	return out
}

func graphForensicDiffSourceFromExport(in graphForensicExportOutput) graphForensicDiffSource {
	return graphForensicDiffSource{ReportID: in.Manifest.ReportID, SourceNodeID: in.Manifest.SourceNodeID, SourceNodeName: in.Manifest.SourceNodeName, SourceClusterID: in.Manifest.SourceClusterID, SourceLabel: in.Manifest.SourceLabel, Revision: in.Stats.Revision, NodeCount: in.Stats.NodeCount, EdgeCount: in.Stats.EdgeCount, GraphChecksum: in.Stats.GraphChecksum}
}

func forensicEntityMap(in []graphForensicEntityOutput) map[string]graphForensicEntityOutput {
	out := make(map[string]graphForensicEntityOutput, len(in))
	for _, entity := range in {
		out[entity.ID] = entity
	}
	return out
}

func diffForensicEntityMaps(left map[string]graphForensicEntityOutput, right map[string]graphForensicEntityOutput, limit int) ([]string, []string, []graphForensicEntityDiff, graphForensicDiffSummary) {
	leftIDs := sortedForensicEntityIDs(left)
	rightIDs := sortedForensicEntityIDs(right)
	leftOnlyAll := []string{}
	rightOnlyAll := []string{}
	differingAll := []graphForensicEntityDiff{}
	for _, id := range leftIDs {
		leftEntity := left[id]
		rightEntity, ok := right[id]
		if !ok {
			leftOnlyAll = append(leftOnlyAll, id)
			continue
		}
		if leftEntity.Checksum != rightEntity.Checksum || leftEntity.CanonicalJSON != rightEntity.CanonicalJSON {
			differingAll = append(differingAll, graphForensicEntityDiff{ID: id, LeftChecksum: leftEntity.Checksum, RightChecksum: rightEntity.Checksum, ChangedFields: changedCanonicalFields(leftEntity.CanonicalJSON, rightEntity.CanonicalJSON)})
		}
	}
	for _, id := range rightIDs {
		if _, ok := left[id]; !ok {
			rightOnlyAll = append(rightOnlyAll, id)
		}
	}
	leftOnly := limitStrings(leftOnlyAll, limit)
	rightOnly := limitStrings(rightOnlyAll, limit)
	differing := limitEntityDiffs(differingAll, limit)
	return leftOnly, rightOnly, differing, graphForensicDiffSummary{OnlyInLeft: len(leftOnlyAll), OnlyInRight: len(rightOnlyAll), Differing: len(differingAll), ShownOnlyLeft: len(leftOnly), ShownOnlyRight: len(rightOnly), ShownDiffering: len(differing)}
}

func sortedForensicEntityIDs(in map[string]graphForensicEntityOutput) []string {
	out := make([]string, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func changedCanonicalFields(leftRaw string, rightRaw string) []string {
	var left map[string]any
	var right map[string]any
	if json.Unmarshal([]byte(leftRaw), &left) != nil || json.Unmarshal([]byte(rightRaw), &right) != nil {
		return nil
	}
	fields := map[string]bool{}
	for key := range left {
		fields[key] = true
	}
	for key := range right {
		fields[key] = true
	}
	out := make([]string, 0, len(fields))
	for key := range fields {
		if !reflect.DeepEqual(left[key], right[key]) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func limitStrings(in []string, limit int) []string {
	if limit > 0 && len(in) > limit {
		return append([]string(nil), in[:limit]...)
	}
	return append([]string(nil), in...)
}

func limitEntityDiffs(in []graphForensicEntityDiff, limit int) []graphForensicEntityDiff {
	if limit > 0 && len(in) > limit {
		return append([]graphForensicEntityDiff(nil), in[:limit]...)
	}
	return append([]graphForensicEntityDiff(nil), in...)
}

func graphForensicDiffText(out graphForensicDiffOutput) string {
	text := fmt.Sprintf("status=%s nodes(left_only=%d right_only=%d differing=%d) edges(left_only=%d right_only=%d differing=%d) truncated=%t\n", out.Status, out.NodeSummary.OnlyInLeft, out.NodeSummary.OnlyInRight, out.NodeSummary.Differing, out.EdgeSummary.OnlyInLeft, out.EdgeSummary.OnlyInRight, out.EdgeSummary.Differing, out.Truncated)
	for _, id := range out.NodesOnlyInLeft {
		text += "node_only_in_left: " + id + "\n"
	}
	for _, id := range out.NodesOnlyInRight {
		text += "node_only_in_right: " + id + "\n"
	}
	for _, diff := range out.DifferingNodes {
		text += fmt.Sprintf("node_diff: %s fields=%s\n", diff.ID, strings.Join(diff.ChangedFields, ","))
	}
	for _, id := range out.EdgesOnlyInLeft {
		text += "edge_only_in_left: " + id + "\n"
	}
	for _, id := range out.EdgesOnlyInRight {
		text += "edge_only_in_right: " + id + "\n"
	}
	for _, diff := range out.DifferingEdges {
		text += fmt.Sprintf("edge_diff: %s fields=%s\n", diff.ID, strings.Join(diff.ChangedFields, ","))
	}
	for _, warning := range out.Warnings {
		text += "warning: " + warning + "\n"
	}
	return text
}

func firstNonEmptyText(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return "unknown"
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
func graphConsistencyStatusText(v adminv1.GraphConsistencyStatus) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "graph_consistency_status_")
}
func graphConsistencyWarningSeverityText(v adminv1.GraphConsistencyWarningSeverity) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "graph_consistency_warning_severity_")
}
func replicationCatchupText(v adminv1.ClusterReplicationCatchupState) string {
	return strings.TrimPrefix(strings.ToLower(v.String()), "cluster_replication_catchup_state_")
}
