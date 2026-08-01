package admin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clusterAuthz struct{ allow bool }

type fakeLocalGraphConsistencyProvider struct {
	stats graphservice.LocalGraphStats
	err   error
}

type fakeGraphConsistencyPeerCollector struct {
	results []backend.PeerLocalGraphConsistencyResult
}

type fakeLocalGraphForensicExportProvider struct {
	export graphservice.LocalGraphForensicExport
	err    error
}

func (p fakeLocalGraphForensicExportProvider) LocalGraphForensicExport(ctx context.Context, spaceID string, domainID string, opts graphservice.LocalGraphForensicExportOptions) (graphservice.LocalGraphForensicExport, error) {
	if p.err != nil {
		return graphservice.LocalGraphForensicExport{}, p.err
	}
	export := p.export
	export.Stats.SpaceID = spaceID
	export.Stats.DomainID = domainID
	return export, nil
}

func (c fakeGraphConsistencyPeerCollector) CollectLocalGraphConsistency(ctx context.Context, addrs map[consensus.NodeID]string, in backend.LocalGraphConsistencyInput) []backend.PeerLocalGraphConsistencyResult {
	return append([]backend.PeerLocalGraphConsistencyResult(nil), c.results...)
}

func (p fakeLocalGraphConsistencyProvider) LocalGraphConsistencyStats(ctx context.Context, spaceID string, domainID string) (graphservice.LocalGraphStats, error) {
	if p.err != nil {
		return graphservice.LocalGraphStats{}, p.err
	}
	stats := p.stats
	stats.SpaceID = spaceID
	stats.DomainID = domainID
	return stats, nil
}

func (a clusterAuthz) HasCapability(ctx context.Context, operatorID string, capability string) (bool, error) {
	return a.allow, nil
}

func authenticatedClusterContext() context.Context {
	return daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindOperator, OperatorID: "op_1", Username: "admin"})
}

func newBootstrapClusterManager(t *testing.T) *clustering.Manager {
	t.Helper()
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}, nil)
	if err != nil {
		t.Fatalf("new cluster manager: %v", err)
	}
	return mgr
}

func TestAdminClusterServiceGetStatusRequiresAuth(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true})
	_, err := svc.GetClusterStatus(context.Background(), &adminv1.GetClusterStatusRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestAdminClusterServiceGetStatus(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true})
	res, err := svc.GetClusterStatus(authenticatedClusterContext(), &adminv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("get cluster status: %v", err)
	}
	if res.GetNode().GetNodeName() != "node-a" || !res.GetNode().GetAdmitted() || !res.GetNode().GetBootstrap() {
		t.Fatalf("unexpected node status: %#v", res.GetNode())
	}
	if res.GetCluster().GetClusterName() != "dev" || res.GetCluster().GetMode() != adminv1.ClusterMode_CLUSTER_MODE_CLUSTERED {
		t.Fatalf("unexpected cluster status: %#v", res.GetCluster())
	}
	if len(res.GetPeers()) == 0 || res.GetPeers()[0].GetState() != adminv1.ClusterPeerState_CLUSTER_PEER_STATE_SELF {
		t.Fatalf("expected self peer, got %#v", res.GetPeers())
	}
	if !res.GetReadiness().GetClientReady() || !res.GetReadiness().GetMetadataApplied() || !res.GetReadiness().GetMetadataValidated() || !res.GetReadiness().GetPartitionGroupsStarted() {
		t.Fatalf("expected standalone manager to be client-ready, got %#v", res.GetReadiness())
	}
}

func TestAdminClusterServiceHealthUsesMetadataReadiness(t *testing.T) {
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", RaftMode: true, RaftLocalNodeID: 1, RaftNodeCount: 3}, nil)
	if err != nil {
		t.Fatalf("new raft manager: %v", err)
	}
	svc := NewAdminClusterService(mgr, clusterAuthz{allow: true})
	res, err := svc.GetClusterHealth(authenticatedClusterContext(), &adminv1.GetClusterHealthRequest{})
	if err != nil {
		t.Fatalf("GetClusterHealth() error = %v", err)
	}
	if res.GetStatus() != "unhealthy" || !containsString(res.GetWarnings(), "system metadata not applied") {
		t.Fatalf("expected metadata readiness unhealthy warning, got %#v", res)
	}
	if res.GetReadiness().GetClientReady() || res.GetReadiness().GetMetadataApplied() || !containsString(res.GetReadiness().GetReadinessBlockers(), "system metadata not applied") {
		t.Fatalf("expected readiness payload to expose metadata blocker, got %#v", res.GetReadiness())
	}
	meta := consensus.SystemMetadata{ClusterID: "cluster_authoritative", ClusterName: "dev", NodeCount: 3, PartitionCount: 16, ReplicaFactor: 3, Nodes: map[string]consensus.SystemNode{"node_1": {NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a", BackendAdvertiseAddr: "127.0.0.1:9093"}, "node_2": {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b", BackendAdvertiseAddr: "127.0.0.1:9094"}, "node_3": {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c", BackendAdvertiseAddr: "127.0.0.1:9095"}}, Placement: map[uint32]consensus.PartitionPlacement{}}
	if err := mgr.ApplySystemMetadata(context.Background(), meta, 1); err != nil {
		t.Fatalf("ApplySystemMetadata() error = %v", err)
	}
	if err := mgr.MarkPartitionGroupsStarted(16, 16); err != nil {
		t.Fatalf("MarkPartitionGroupsStarted() error = %v", err)
	}
	res, err = svc.GetClusterHealth(authenticatedClusterContext(), &adminv1.GetClusterHealthRequest{})
	if err != nil {
		t.Fatalf("GetClusterHealth() after apply error = %v", err)
	}
	if res.GetStatus() != "healthy" {
		t.Fatalf("expected healthy after metadata apply and partition groups started, got %#v", res)
	}
	if !res.GetReadiness().GetClientReady() || !res.GetReadiness().GetMetadataApplied() || !res.GetReadiness().GetMetadataValidated() || !res.GetReadiness().GetPartitionGroupsStarted() {
		t.Fatalf("expected healthy readiness payload, got %#v", res.GetReadiness())
	}
	if res.GetReadiness().GetAuthoritativeClusterId() != "cluster_authoritative" || res.GetReadiness().GetLocalClusterId() != "cluster_authoritative" || res.GetReadiness().GetExpectedMemberCount() != 3 {
		t.Fatalf("unexpected readiness identity/count fields: %#v", res.GetReadiness())
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestRaftGroupStatusToProtoIncludesStorageDiagnostics(t *testing.T) {
	partitionID := uint32(7)
	out := raftGroupStatusToProto(consensus.GroupStatus{GroupID: consensus.PartitionGroupID(partitionID), NodeID: 2, Leader: 0, PreferredLeader: 1, PartitionID: &partitionID, Term: 5, CommitIndex: 11, AppliedIndex: 8, LastIndex: 13, SnapshotIndex: 3, ReadDiagnostics: consensus.ReadDiagnostics{ReadIndexAttempts: 4, ReadIndexSuccesses: 2, ReadIndexFailures: 1, ReadIndexTimeouts: 1, ReadIndexNoLeader: 1, LastFailureAt: time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC), LastFailureReason: "no_leader", LastReadIndex: 12, LastAppliedWaitSuccess: 12, LastAppliedWaitDuration: 25 * time.Millisecond}}, []uint64{1, 2, 3})
	if out.GetKind() != adminv1.RaftGroupKind_RAFT_GROUP_KIND_PARTITION || out.GetPartitionId() != partitionID {
		t.Fatalf("unexpected group kind/partition: %#v", out)
	}
	if out.GetHealth() != adminv1.RaftGroupHealth_RAFT_GROUP_HEALTH_NO_LEADER || out.GetHealthReason() == "" {
		t.Fatalf("expected no-leader health reason, got %#v", out)
	}
	if out.GetApplyLag() != 3 || out.GetLastIndex() != 13 || out.GetSnapshotIndex() != 3 {
		t.Fatalf("unexpected raft diagnostics: %#v", out)
	}
	if out.GetReadDiagnostics().GetReadIndexAttempts() != 4 || out.GetReadDiagnostics().GetReadIndexSuccesses() != 2 || out.GetReadDiagnostics().GetReadIndexFailures() != 1 || out.GetReadDiagnostics().GetReadIndexNoLeader() != 1 || out.GetReadDiagnostics().GetLastFailureReason() != "no_leader" || out.GetReadDiagnostics().GetLastReadIndex() != 12 || out.GetReadDiagnostics().GetLastAppliedWaitSuccess() != 12 || out.GetReadDiagnostics().GetLastAppliedWaitMillis() != 25 {
		t.Fatalf("unexpected read diagnostics: %#v", out.GetReadDiagnostics())
	}
}

func TestAdminClusterServiceLocalGraphConsistencyRequiresAuth(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithGraphConsistency(fakeLocalGraphConsistencyProvider{})
	_, err := svc.GetLocalGraphConsistency(context.Background(), &adminv1.GetLocalGraphConsistencyRequest{SpaceId: "space", DomainId: "domain"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestAdminClusterServiceLocalGraphConsistencyMapsStats(t *testing.T) {
	collectedAt := time.Date(2026, 8, 1, 1, 2, 3, 4, time.UTC)
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{RaftNodeCount: 3}, nil).WithGraphConsistency(fakeLocalGraphConsistencyProvider{stats: graphservice.LocalGraphStats{PartitionID: 7, Revision: 12, NodeCount: 3, EdgeCount: 2, NodeChecksum: "nodes", EdgeChecksum: "edges", GraphChecksum: "graph", ChecksumAlgorithm: graphservice.GraphChecksumAlgorithmV1, CollectedAt: collectedAt, Source: "local_latest"}})
	res, err := svc.GetLocalGraphConsistency(authenticatedClusterContext(), &adminv1.GetLocalGraphConsistencyRequest{SpaceId: "490851b9-0038-4afc-b1f0-d1bd9e829bc8", DomainId: "11111111-1111-1111-1111-111111111111"})
	if err != nil {
		t.Fatalf("GetLocalGraphConsistency() error = %v", err)
	}
	stats := res.GetStats()
	if stats.GetSpaceId() != "490851b9-0038-4afc-b1f0-d1bd9e829bc8" || stats.GetDomainId() != "11111111-1111-1111-1111-111111111111" || stats.GetPartitionId() != 7 || stats.GetRevision() != 12 || stats.GetNodeCount() != 3 || stats.GetEdgeCount() != 2 || stats.GetNodeChecksum() != "nodes" || stats.GetEdgeChecksum() != "edges" || stats.GetGraphChecksum() != "graph" || stats.GetChecksumAlgorithm() != graphservice.GraphChecksumAlgorithmV1 || stats.GetCollectedAt() != formatClusterTime(collectedAt) || stats.GetSource() != "local_latest" {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if len(res.GetWarnings()) != 1 || !strings.Contains(res.GetWarnings()[0], "raft groups are not configured") {
		t.Fatalf("expected local-only warning, got %#v", res.GetWarnings())
	}
}

func TestAdminClusterServiceGraphConsistencyReportConsistent(t *testing.T) {
	base := graphservice.LocalGraphStats{PartitionID: 2, Revision: 7, NodeCount: 3, EdgeCount: 2, NodeChecksum: "nodes", EdgeChecksum: "edges", GraphChecksum: "graph", ChecksumAlgorithm: graphservice.GraphChecksumAlgorithmV1, CollectedAt: time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC), Source: "local_latest"}
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{RaftNodeCount: 3, RaftPartitionCount: 16, RaftReplicaFactor: 3, RaftLocalNodeID: 1, RaftNodeAddrs: []string{"node-a:9091", "node-b:9091", "node-c:9091"}}, nil).WithGraphConsistency(fakeLocalGraphConsistencyProvider{stats: base}).WithGraphConsistencyPeerCollector(fakeGraphConsistencyPeerCollector{results: []backend.PeerLocalGraphConsistencyResult{
		{TargetNode: 2, Addr: "node-b:9091", Result: backend.LocalGraphConsistencyResult{ClusterID: "cluster", NodeID: "node_2", NodeName: "node-b", RaftNodeID: 2, Payload: mustGraphStatsPayload(t, withGraphStatsSpaceDomain(base, "space-1", "domain-1"))}},
		{TargetNode: 3, Addr: "node-c:9091", Result: backend.LocalGraphConsistencyResult{ClusterID: "cluster", NodeID: "node_3", NodeName: "node-c", RaftNodeID: 3, Payload: mustGraphStatsPayload(t, withGraphStatsSpaceDomain(base, "space-1", "domain-1"))}},
	}})
	res, err := svc.GetGraphConsistencyReport(authenticatedClusterContext(), &adminv1.GetGraphConsistencyReportRequest{SpaceId: "space-1", DomainId: "domain-1"})
	if err != nil {
		t.Fatalf("GetGraphConsistencyReport() error = %v", err)
	}
	if res.GetStatus() != adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_CONSISTENT || len(res.GetReplicas()) != 3 || len(res.GetExpectedReplicaNodeIds()) != 3 {
		t.Fatalf("unexpected consistent report: %#v", res)
	}
}

func TestAdminClusterServiceGraphConsistencyReportUsesMetadataReplicaPlacement(t *testing.T) {
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "node-a:9091", RaftMode: true, RaftLocalNodeID: 1, RaftNodeCount: 5}, nil)
	if err != nil {
		t.Fatalf("new raft manager: %v", err)
	}
	meta := consensus.SystemMetadata{ClusterID: "cluster_authoritative", ClusterName: "dev", NodeCount: 5, PartitionCount: 16, ReplicaFactor: 3, Nodes: map[string]consensus.SystemNode{"node_1": {NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a", BackendAdvertiseAddr: "node-a:9091"}, "node_2": {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b", BackendAdvertiseAddr: "node-b:9091"}, "node_3": {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c", BackendAdvertiseAddr: "node-c:9091"}, "node_4": {NodeID: "node_4", RaftNodeID: 4, NodeName: "node-d", BackendAdvertiseAddr: "node-d:9091"}, "node_5": {NodeID: "node_5", RaftNodeID: 5, NodeName: "node-e", BackendAdvertiseAddr: "node-e:9091"}}, Placement: map[uint32]consensus.PartitionPlacement{2: {PartitionID: 2, ReplicaNodeIDs: []string{"node_1", "node_3", "node_5"}, PreferredLeader: "node_1"}}}
	if err := mgr.ApplySystemMetadata(context.Background(), meta, 1); err != nil {
		t.Fatalf("ApplySystemMetadata() error = %v", err)
	}
	base := graphservice.LocalGraphStats{PartitionID: 2, Revision: 7, NodeCount: 3, EdgeCount: 2, NodeChecksum: "nodes", EdgeChecksum: "edges", GraphChecksum: "graph", ChecksumAlgorithm: graphservice.GraphChecksumAlgorithmV1, CollectedAt: time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC), Source: "local_latest"}
	svc := NewAdminClusterService(mgr, clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{RaftNodeCount: 5, RaftPartitionCount: 16, RaftReplicaFactor: 3, RaftLocalNodeID: 1, RaftNodeAddrs: []string{"node-a:9091", "node-b:9091", "node-c:9091", "node-d:9091", "node-e:9091"}}, nil).WithGraphConsistency(fakeLocalGraphConsistencyProvider{stats: base}).WithGraphConsistencyPeerCollector(fakeGraphConsistencyPeerCollector{results: []backend.PeerLocalGraphConsistencyResult{
		{TargetNode: 3, Addr: "node-c:9091", Result: backend.LocalGraphConsistencyResult{ClusterID: "cluster_authoritative", NodeID: "node_3", NodeName: "node-c", RaftNodeID: 3, Payload: mustGraphStatsPayload(t, withGraphStatsSpaceDomain(base, "space-1", "domain-1"))}},
		{TargetNode: 5, Addr: "node-e:9091", Result: backend.LocalGraphConsistencyResult{ClusterID: "cluster_authoritative", NodeID: "node_5", NodeName: "node-e", RaftNodeID: 5, Payload: mustGraphStatsPayload(t, withGraphStatsSpaceDomain(base, "space-1", "domain-1"))}},
	}})
	res, err := svc.GetGraphConsistencyReport(authenticatedClusterContext(), &adminv1.GetGraphConsistencyReportRequest{SpaceId: "space-1", DomainId: "domain-1"})
	if err != nil {
		t.Fatalf("GetGraphConsistencyReport() error = %v", err)
	}
	if got := res.GetExpectedReplicaNodeIds(); len(got) != 3 || got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Fatalf("expected metadata replica placement [1 3 5], got %#v", got)
	}
	if res.GetStatus() != adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_CONSISTENT {
		t.Fatalf("expected placement-scoped consistent report, got %#v", res)
	}
}

func TestAdminClusterServiceGraphConsistencyReportDivergent(t *testing.T) {
	base := graphservice.LocalGraphStats{PartitionID: 2, Revision: 7, NodeCount: 3, EdgeCount: 2, NodeChecksum: "nodes", EdgeChecksum: "edges", GraphChecksum: "graph", ChecksumAlgorithm: graphservice.GraphChecksumAlgorithmV1, CollectedAt: time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC), Source: "local_latest"}
	peer := base
	peer.GraphChecksum = "different"
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{RaftNodeCount: 2, RaftPartitionCount: 16, RaftReplicaFactor: 2, RaftLocalNodeID: 1, RaftNodeAddrs: []string{"node-a:9091", "node-b:9091"}}, nil).WithGraphConsistency(fakeLocalGraphConsistencyProvider{stats: base}).WithGraphConsistencyPeerCollector(fakeGraphConsistencyPeerCollector{results: []backend.PeerLocalGraphConsistencyResult{{TargetNode: 2, Addr: "node-b:9091", Result: backend.LocalGraphConsistencyResult{ClusterID: "cluster", NodeID: "node_2", NodeName: "node-b", RaftNodeID: 2, Payload: mustGraphStatsPayload(t, withGraphStatsSpaceDomain(peer, "space-1", "domain-1"))}}}})
	res, err := svc.GetGraphConsistencyReport(authenticatedClusterContext(), &adminv1.GetGraphConsistencyReportRequest{SpaceId: "space-1", DomainId: "domain-1"})
	if err != nil {
		t.Fatalf("GetGraphConsistencyReport() error = %v", err)
	}
	if res.GetStatus() != adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_DIVERGENT || !hasGraphConsistencyWarning(res.GetWarnings(), "checksum_mismatch") {
		t.Fatalf("expected divergent checksum report, got %#v", res)
	}
}

func TestAdminClusterServiceGraphConsistencyReportUnreachableIsDegraded(t *testing.T) {
	base := graphservice.LocalGraphStats{PartitionID: 2, Revision: 7, NodeCount: 3, EdgeCount: 2, NodeChecksum: "nodes", EdgeChecksum: "edges", GraphChecksum: "graph", ChecksumAlgorithm: graphservice.GraphChecksumAlgorithmV1, CollectedAt: time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC), Source: "local_latest"}
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{RaftNodeCount: 2, RaftPartitionCount: 16, RaftReplicaFactor: 2, RaftLocalNodeID: 1, RaftNodeAddrs: []string{"node-a:9091", "node-b:9091"}}, nil).WithGraphConsistency(fakeLocalGraphConsistencyProvider{stats: base}).WithGraphConsistencyPeerCollector(fakeGraphConsistencyPeerCollector{results: []backend.PeerLocalGraphConsistencyResult{{TargetNode: 2, Addr: "node-b:9091", Err: status.Error(codes.Unavailable, "peer unavailable")}}})
	res, err := svc.GetGraphConsistencyReport(authenticatedClusterContext(), &adminv1.GetGraphConsistencyReportRequest{SpaceId: "space-1", DomainId: "domain-1"})
	if err != nil {
		t.Fatalf("GetGraphConsistencyReport() error = %v", err)
	}
	if res.GetStatus() != adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_DEGRADED || !hasGraphConsistencyWarning(res.GetWarnings(), "replica_unreachable") {
		t.Fatalf("expected degraded unreachable report, got %#v", res)
	}
}

func mustGraphStatsPayload(t *testing.T, stats graphservice.LocalGraphStats) []byte {
	t.Helper()
	payload, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}
	return payload
}

func withGraphStatsSpaceDomain(stats graphservice.LocalGraphStats, spaceID string, domainID string) graphservice.LocalGraphStats {
	stats.SpaceID = spaceID
	stats.DomainID = domainID
	return stats
}

func hasGraphConsistencyWarning(warnings []*adminv1.GraphConsistencyWarning, code string) bool {
	for _, warning := range warnings {
		if warning.GetCode() == code {
			return true
		}
	}
	return false
}

func TestAdminClusterServiceLocalGraphForensicExportMapsEvidence(t *testing.T) {
	collectedAt := time.Date(2026, 8, 1, 1, 2, 3, 4, time.UTC)
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithGraphForensicExport(fakeLocalGraphForensicExportProvider{export: graphservice.LocalGraphForensicExport{Stats: graphservice.LocalGraphStats{PartitionID: 7, Revision: 12, NodeCount: 1, EdgeCount: 1, NodeChecksum: "nodes", EdgeChecksum: "edges", GraphChecksum: "graph", ChecksumAlgorithm: graphservice.GraphChecksumAlgorithmV1, CollectedAt: collectedAt, Source: "local_latest"}, Nodes: []graphservice.ForensicGraphEntity{{ID: "node-1", Checksum: "node-check", CanonicalJSON: `{"id":"node-1"}`}}, Edges: []graphservice.ForensicGraphEntity{{ID: "edge-1", Checksum: "edge-check", CanonicalJSON: `{"id":"edge-1"}`}}, Warnings: []string{"read-only"}}})
	res, err := svc.GetLocalGraphForensicExport(authenticatedClusterContext(), &adminv1.GetLocalGraphForensicExportRequest{SpaceId: "space-1", DomainId: "domain-1", PageSize: 10, SourceLabel: "pvc-a"})
	if err != nil {
		t.Fatalf("GetLocalGraphForensicExport() error = %v", err)
	}
	if res.GetManifest().GetReportId() == "" || res.GetManifest().GetSourceLabel() != "pvc-a" || res.GetStats().GetRevision() != 12 || len(res.GetNodes()) != 1 || len(res.GetEdges()) != 1 || res.GetNodes()[0].GetChecksum() != "node-check" || len(res.GetWarnings()) != 1 {
		t.Fatalf("unexpected forensic export response: %#v", res)
	}
}

func TestAdminClusterServiceLocalGraphForensicExportRequiresAuth(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithGraphForensicExport(fakeLocalGraphForensicExportProvider{})
	_, err := svc.GetLocalGraphForensicExport(context.Background(), &adminv1.GetLocalGraphForensicExportRequest{SpaceId: "space", DomainId: "domain"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestAdminClusterServiceRuntimeStatusAndSpaceRoute(t *testing.T) {
	diagnostics := consensus.NewTransportDiagnostics(nil)
	transport := consensus.RoutedTransport{Diagnostics: diagnostics, Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}
	transport.Send(context.Background(), "system", 2, []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 2, To: 3}})
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{Name: "dev", RaftNodeCount: 3, RaftPartitionCount: 16, RaftReplicaFactor: 3, RaftLocalNodeID: 2, RaftNodeAddrs: []string{"a:9091", "b:9091", "c:9091"}}, nil, diagnostics)
	ctx := authenticatedClusterContext()
	res, err := svc.GetClusterRuntimeStatus(ctx, &adminv1.GetClusterRuntimeStatusRequest{})
	if err != nil {
		t.Fatalf("runtime status: %v", err)
	}
	if res.GetEngine() != adminv1.ClusterEngine_CLUSTER_ENGINE_RAFT || res.GetRaftPartitionCount() != 16 || res.GetLocalRaftNodeId() != 2 || len(res.GetRaftNodeAddrs()) != 3 {
		t.Fatalf("unexpected runtime status: %#v", res)
	}
	if res.GetRaftTransport().GetSendAttempts() != 1 || res.GetRaftTransport().GetMissingSenderFailures() != 1 || res.GetRaftTransport().GetLastTargetNodeId() != 3 {
		t.Fatalf("unexpected transport diagnostics: %#v", res.GetRaftTransport())
	}
	route, err := svc.LookupSpaceRoute(ctx, &adminv1.LookupSpaceRouteRequest{SpaceId: "490851b9-0038-4afc-b1f0-d1bd9e829bc8"})
	if err != nil {
		t.Fatalf("lookup route: %v", err)
	}
	if route.GetPartitionId() >= 16 || len(route.GetReplicaNodeIds()) != 3 {
		t.Fatalf("unexpected route: %#v", route)
	}
	if _, err := svc.LookupSpaceRoute(ctx, &adminv1.LookupSpaceRouteRequest{SpaceId: "not-a-uuid"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}
