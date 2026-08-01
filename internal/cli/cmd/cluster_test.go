package cmd

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
)

func TestClusterStatusCommandUsesAdminAPI(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "cluster", "status")
	if err != nil {
		t.Fatalf("cluster status failed: %v\n%s", err, out)
	}
	var status clusterStatusOutput
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatalf("decode cluster status: %v output=%s", err, out)
	}
	if status.Node.NodeID == "" || status.Node.Name == "" || status.Cluster.ClusterID == "" || status.Cluster.Mode == "" {
		t.Fatalf("unexpected cluster status: %#v", status)
	}
	if len(status.Peers) == 0 || status.Peers[0].State != "self" {
		t.Fatalf("expected self peer in status: %#v", status.Peers)
	}
	if !status.Readiness.ClientReady || !status.Readiness.MetadataApplied || !status.Readiness.MetadataValidated || !status.Readiness.PartitionGroupsStarted {
		t.Fatalf("expected readiness fields in cluster status: %#v", status.Readiness)
	}
}

func TestClusterRaftGroupsCommandUsesAdminAPI(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "cluster", "raft-groups")
	if err != nil {
		t.Fatalf("cluster raft-groups failed: %v\n%s", err, out)
	}
	var groups raftGroupsOutput
	if err := json.Unmarshal([]byte(out), &groups); err != nil {
		t.Fatalf("decode raft groups: %v output=%s", err, out)
	}
	if groups.Groups == nil {
		t.Fatalf("expected groups array in output: %s", out)
	}
}

func TestClusterRaftGroupsOutputIncludesReadDiagnostics(t *testing.T) {
	out, text := buildRaftGroupsOutput([]*adminv1.RaftGroupStatus{{
		GroupId:      "space-partition-1",
		Kind:         adminv1.RaftGroupKind_RAFT_GROUP_KIND_PARTITION,
		LocalNodeId:  1,
		LeaderNodeId: 1,
		Health:       adminv1.RaftGroupHealth_RAFT_GROUP_HEALTH_HEALTHY,
		Term:         3,
		CommitIndex:  12,
		AppliedIndex: 12,
		LastIndex:    12,
		ReadDiagnostics: &adminv1.RaftReadDiagnostics{
			ReadIndexAttempts:    5,
			ReadIndexSuccesses:   4,
			ReadIndexFailures:    1,
			ReadIndexTimeouts:    1,
			LastFailureReason:    "deadline_exceeded",
			LastReadIndex:        11,
			LastAppliedWaitIndex: 11,
		},
	}})
	if len(out.Groups) != 1 {
		t.Fatalf("groups=%d want 1", len(out.Groups))
	}
	read := out.Groups[0].ReadDiagnostics
	if read.ReadIndexAttempts != 5 || read.ReadIndexSuccesses != 4 || read.ReadIndexFailures != 1 || read.ReadIndexTimeouts != 1 || read.LastFailureReason != "deadline_exceeded" || read.LastReadIndex != 11 || read.LastAppliedWaitIndex != 11 {
		t.Fatalf("unexpected read diagnostics: %#v", read)
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	jsonOut := string(data)
	for _, want := range []string{`"read_diagnostics"`, `"read_index_attempts":5`, `"read_index_successes":4`, `"read_index_failures":1`, `"last_failure_reason":"deadline_exceeded"`} {
		if !strings.Contains(jsonOut, want) {
			t.Fatalf("json output missing %s: %s", want, jsonOut)
		}
	}
	for _, want := range []string{"read_attempts=5", "read_ok=4", "read_fail=1", "read_failure=deadline_exceeded"} {
		if !strings.Contains(text, want) {
			t.Fatalf("text output missing %s: %s", want, text)
		}
	}
}

func TestClusterConsistencyOutputIncludesStatsAndRaftGroup(t *testing.T) {
	res := &adminv1.GetLocalGraphConsistencyResponse{
		Stats:     &adminv1.LocalGraphConsistencyStats{SpaceId: "space-1", DomainId: "domain-1", PartitionId: 2, Revision: 7, NodeCount: 3, EdgeCount: 4, NodeChecksum: "nodes", EdgeChecksum: "edges", GraphChecksum: "graph", ChecksumAlgorithm: "graph-v1-sha256", CollectedAt: "2026-08-01T00:00:00Z", Source: "local_latest"},
		RaftGroup: &adminv1.RaftGroupStatus{GroupId: "space-partition-2", Kind: adminv1.RaftGroupKind_RAFT_GROUP_KIND_PARTITION, PartitionId: 2, LocalNodeId: 1, LeaderNodeId: 1, Health: adminv1.RaftGroupHealth_RAFT_GROUP_HEALTH_HEALTHY, Term: 5, CommitIndex: 9, AppliedIndex: 8, ApplyLag: 1, LastIndex: 9},
		Warnings:  []string{"local-only"},
	}
	out := buildGraphConsistencyOutput(res)
	if out.Stats.GraphChecksum != "graph" || out.Stats.NodeCount != 3 || out.Stats.EdgeCount != 4 || out.Stats.Source != "local_latest" {
		t.Fatalf("unexpected consistency stats: %#v", out.Stats)
	}
	if out.RaftGroup == nil || out.RaftGroup.GroupID != "space-partition-2" || out.RaftGroup.ApplyLag != 1 {
		t.Fatalf("unexpected raft group: %#v", out.RaftGroup)
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	jsonOut := string(data)
	for _, want := range []string{`"graph_checksum":"graph"`, `"checksum_algorithm":"graph-v1-sha256"`, `"raft_group"`, `"warnings":["local-only"]`} {
		if !strings.Contains(jsonOut, want) {
			t.Fatalf("json output missing %s: %s", want, jsonOut)
		}
	}
	text := graphConsistencyText(out)
	for _, want := range []string{"space=space-1", "domain=domain-1", "nodes=3", "edges=4", "checksum=graph", "raft_group=space-partition-2", "warning: local-only"} {
		if !strings.Contains(text, want) {
			t.Fatalf("text output missing %s: %s", want, text)
		}
	}
}

func TestClusterConsistencyReportOutputIncludesReplicasAndWarnings(t *testing.T) {
	res := &adminv1.GetGraphConsistencyReportResponse{
		Status:                 adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_DIVERGENT,
		SpaceId:                "space-1",
		DomainId:               "domain-1",
		PartitionId:            2,
		LocalNodeId:            1,
		LeaderNodeId:           1,
		ExpectedReplicaNodeIds: []uint64{1, 2},
		ComparisonBasis:        "latest_state_graph_v1_sha256_no_historical_compare",
		RaftGroup:              &adminv1.RaftGroupStatus{GroupId: "space-partition-2", Kind: adminv1.RaftGroupKind_RAFT_GROUP_KIND_PARTITION, PartitionId: 2, LocalNodeId: 1, LeaderNodeId: 1, Health: adminv1.RaftGroupHealth_RAFT_GROUP_HEALTH_HEALTHY, Term: 5, CommitIndex: 9, AppliedIndex: 9, LastIndex: 9},
		Replicas: []*adminv1.GraphConsistencyReplica{
			{RaftNodeId: 1, NodeId: "node_1", NodeName: "node-a", Local: true, Reachable: true, Stats: &adminv1.LocalGraphConsistencyStats{SpaceId: "space-1", DomainId: "domain-1", PartitionId: 2, Revision: 7, NodeCount: 3, EdgeCount: 4, GraphChecksum: "graph-a", ChecksumAlgorithm: "graph-v1-sha256", Source: "local_latest"}},
			{RaftNodeId: 2, NodeId: "node_2", NodeName: "node-b", BackendAddr: "node-b:9091", Reachable: true, Stats: &adminv1.LocalGraphConsistencyStats{SpaceId: "space-1", DomainId: "domain-1", PartitionId: 2, Revision: 7, NodeCount: 3, EdgeCount: 4, GraphChecksum: "graph-b", ChecksumAlgorithm: "graph-v1-sha256", Source: "local_latest"}},
		},
		Warnings: []*adminv1.GraphConsistencyWarning{{Code: "checksum_mismatch", Severity: adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_CRITICAL, RaftNodeId: 2, Message: "replica checksum differs from baseline"}},
	}
	out := buildGraphConsistencyReportOutput(res)
	if out.Status != "divergent" || len(out.Replicas) != 2 || out.Replicas[1].Stats == nil || out.Replicas[1].Stats.GraphChecksum != "graph-b" || len(out.Warnings) != 1 || out.Warnings[0].Code != "checksum_mismatch" {
		t.Fatalf("unexpected report output: %#v", out)
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	jsonOut := string(data)
	for _, want := range []string{`"status":"divergent"`, `"expected_replica_node_ids":[1,2]`, `"replicas"`, `"warnings"`, `"checksum_mismatch"`} {
		if !strings.Contains(jsonOut, want) {
			t.Fatalf("json output missing %s: %s", want, jsonOut)
		}
	}
	text := graphConsistencyReportText(out)
	for _, want := range []string{"status=divergent", "replicas=2/2", "replica=1", "replica=2", "checksum=graph-b", "warning: code=checksum_mismatch severity=critical replica=2"} {
		if !strings.Contains(text, want) {
			t.Fatalf("text output missing %s: %s", want, text)
		}
	}
}

func TestClusterForensicExportOutputAndDiff(t *testing.T) {
	export := buildGraphForensicExportOutput(&adminv1.GetLocalGraphForensicExportResponse{Manifest: &adminv1.GraphForensicExportManifest{ReportId: "report-a", SourceNodeId: "node_1", SourceLabel: "pvc-a", SourceClusterId: "cluster-a", CollectedAt: "2026-08-01T00:00:00Z"}, Stats: &adminv1.LocalGraphConsistencyStats{SpaceId: "space-1", DomainId: "domain-1", Revision: 2, NodeCount: 2, EdgeCount: 1, GraphChecksum: "graph", ChecksumAlgorithm: "graph-v1-sha256", Source: "local_latest"}, Nodes: []*adminv1.GraphForensicEntity{{Id: "node-a", Checksum: "node-a-check", CanonicalJson: `{"id":"node-a","labels":["A"],"properties":{"title":"A"}}`}}, Edges: []*adminv1.GraphForensicEntity{{Id: "edge-a", Checksum: "edge-a-check", CanonicalJson: `{"id":"edge-a","from_id":"node-a","to_id":"node-b"}`}}, Warnings: []string{"read-only"}})
	if export.Manifest.ReportID != "report-a" || len(export.Nodes) != 1 || len(export.Edges) != 1 || export.Warnings[0] != "read-only" {
		t.Fatalf("unexpected export output: %#v", export)
	}
	left := export
	right := export
	right.Manifest.ReportID = "report-b"
	right.Nodes = []graphForensicEntityOutput{{ID: "node-a", Checksum: "node-a-different", CanonicalJSON: `{"id":"node-a","labels":["B"],"properties":{"title":"B"}}`}, {ID: "node-b", Checksum: "node-b-check", CanonicalJSON: `{"id":"node-b"}`}}
	right.Edges = nil
	diff := diffGraphForensicExports(left, right, 10)
	if diff.Status != "different" || diff.NodeSummary.Differing != 1 || diff.NodeSummary.OnlyInRight != 1 || diff.EdgeSummary.OnlyInLeft != 1 || len(diff.DifferingNodes) != 1 || !clusterTestContainsString(diff.DifferingNodes[0].ChangedFields, "labels") {
		t.Fatalf("unexpected forensic diff: %#v", diff)
	}
	text := graphForensicDiffText(diff)
	for _, want := range []string{"status=different", "node_only_in_right: node-b", "edge_only_in_left: edge-a", "node_diff: node-a"} {
		if !strings.Contains(text, want) {
			t.Fatalf("diff text missing %s: %s", want, text)
		}
	}
	path := t.TempDir() + "/export.json"
	data, err := json.Marshal(left)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
	readBack, err := readGraphForensicExportFile(path)
	if err != nil || readBack.Manifest.ReportID != left.Manifest.ReportID {
		t.Fatalf("read forensic export = %#v err=%v", readBack, err)
	}
}

func clusterTestContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestClusterCommandRequiresAdminCredentials(t *testing.T) {
	_, addr, _, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "cluster", "status")
	if err == nil {
		t.Fatalf("expected cluster status without credentials to fail, output=%s", out)
	}
	if !strings.Contains(err.Error(), "--username/-u and --password/-p") {
		t.Fatalf("unexpected error: %v output=%s", err, out)
	}
}
