package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const graphConsistencyComparisonBasisV1 = "latest_state_graph_v1_sha256_no_historical_compare"

type GraphConsistencyPeerCollector interface {
	CollectLocalGraphConsistency(ctx context.Context, addrs map[consensus.NodeID]string, in backend.LocalGraphConsistencyInput) []backend.PeerLocalGraphConsistencyResult
}

func (s *AdminClusterService) GetGraphConsistencyReport(ctx context.Context, req *adminv1.GetGraphConsistencyReportRequest) (*adminv1.GetGraphConsistencyReportResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.graphConsistency == nil {
		return nil, status.Error(codes.FailedPrecondition, "graph consistency diagnostics are not configured")
	}
	if s.cluster == nil {
		return nil, status.Error(codes.Unavailable, "clustering manager is not available")
	}
	localStats, err := s.graphConsistency.LocalGraphConsistencyStats(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		if errors.Is(err, graphservice.ErrInvalidInput) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, err
	}
	identity := s.cluster.Identity()
	localNodeID := reportLocalRaftNodeID(s.clusterConfig, identity.NodeID)
	out := &adminv1.GetGraphConsistencyReportResponse{Status: adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_UNKNOWN, SpaceId: localStats.SpaceID, DomainId: localStats.DomainID, PartitionId: localStats.PartitionID, LocalNodeId: uint64(localNodeID), ComparisonBasis: graphConsistencyComparisonBasisV1}
	out.Warnings = append(out.Warnings, graphConsistencyWarning("unsupported_historical_compare", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_INFO, localNodeID, "V1 compares latest-state checksums only; historical/common-revision comparison is not supported"))
	metadata := s.cluster.SystemMetadata()
	expected := expectedGraphConsistencyReplicas(s.clusterConfig, metadata, localStats.PartitionID)
	var localRaftGroup *adminv1.RaftGroupStatus
	if s.raftGroups != nil && s.clusterConfig.RaftPartitionCount > 0 {
		if st, ok := raftGroupStatusByID(s.raftGroups, consensus.PartitionGroupID(localStats.PartitionID)); ok {
			localRaftGroup = raftGroupStatusToProto(st, consensusNodeIDsToUint64(expected))
			out.RaftGroup = localRaftGroup
			out.LeaderNodeId = localRaftGroup.GetLeaderNodeId()
		} else {
			out.Warnings = append(out.Warnings, graphConsistencyWarning("unknown_domain", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_WARNING, localNodeID, fmt.Sprintf("raft partition group %d is not available", localStats.PartitionID)))
		}
	} else if s.raftGroups == nil {
		out.Warnings = append(out.Warnings, graphConsistencyWarning("unknown_domain", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_WARNING, localNodeID, "raft group context is not configured; report uses configured replica set only"))
	}
	if len(expected) == 0 && localNodeID != 0 {
		expected = []consensus.NodeID{localNodeID}
	}
	out.ExpectedReplicaNodeIds = consensusNodeIDsToUint64(expected)
	out.Replicas = append(out.Replicas, &adminv1.GraphConsistencyReplica{RaftNodeId: uint64(localNodeID), NodeId: identity.NodeID, NodeName: identity.NodeName, BackendAddr: identity.BackendAdvertiseAddr, Local: true, Reachable: true, Stats: localGraphConsistencyStatsToProto(localStats)})
	peerAddrs := graphConsistencyPeerAddrs(s.clusterConfig, expected, localNodeID)
	missing := missingGraphConsistencyAddrs(s.clusterConfig, expected, localNodeID)
	for _, nodeID := range missing {
		out.Replicas = append(out.Replicas, &adminv1.GraphConsistencyReplica{RaftNodeId: uint64(nodeID), Reachable: false, Error: "backend address is not configured"})
		out.Warnings = append(out.Warnings, graphConsistencyWarning("replica_unreachable", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_WARNING, nodeID, "backend address is not configured"))
	}
	if len(peerAddrs) > 0 {
		collector := s.graphPeerCollector
		if collector == nil {
			collector = backend.Client{AuthToken: s.clusterConfig.BackendAuthToken}
		}
		collectCtx := ctx
		cancel := func() {}
		if _, ok := ctx.Deadline(); !ok {
			collectCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		}
		results := collector.CollectLocalGraphConsistency(collectCtx, peerAddrs, backend.LocalGraphConsistencyInput{ClusterID: identity.ClusterID, RequesterNode: localNodeID, SpaceID: localStats.SpaceID, DomainID: localStats.DomainID})
		cancel()
		for _, peer := range results {
			replica := &adminv1.GraphConsistencyReplica{RaftNodeId: uint64(peer.TargetNode), BackendAddr: peer.Addr}
			if peer.Err != nil {
				replica.Reachable = false
				replica.Error = peer.Err.Error()
				out.Warnings = append(out.Warnings, warningForPeerCollectionError(peer.TargetNode, peer.Err))
			} else {
				stats, err := decodePeerLocalGraphStats(peer.Result.Payload)
				if err != nil {
					replica.Reachable = false
					replica.Error = err.Error()
					out.Warnings = append(out.Warnings, graphConsistencyWarning("unknown_domain", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_WARNING, peer.TargetNode, "peer returned an unreadable graph consistency payload"))
				} else {
					replica.Reachable = true
					replica.NodeId = peer.Result.NodeID
					replica.NodeName = peer.Result.NodeName
					replica.Stats = localGraphConsistencyStatsToProto(stats)
				}
			}
			out.Replicas = append(out.Replicas, replica)
		}
	}
	out.Status = classifyGraphConsistencyReport(out)
	return out, nil
}

func expectedGraphConsistencyReplicas(cfg daemonconfig.ClusterConfig, meta consensus.SystemMetadata, partitionID uint32) []consensus.NodeID {
	if placement, ok := meta.Placement[partitionID]; ok && len(placement.ReplicaNodeIDs) > 0 && len(meta.Nodes) > 0 {
		out := make([]consensus.NodeID, 0, len(placement.ReplicaNodeIDs))
		for _, nodeID := range placement.ReplicaNodeIDs {
			if node, ok := meta.Nodes[nodeID]; ok && node.RaftNodeID != 0 {
				out = append(out, consensus.NodeID(node.RaftNodeID))
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return dedupeNodeIDs(out)
	}
	nodeCount := cfg.RaftNodeCount
	if nodeCount <= 0 {
		return nil
	}
	replicaFactor := cfg.RaftReplicaFactor
	if replicaFactor <= 0 || replicaFactor > nodeCount {
		replicaFactor = nodeCount
	}
	start := int(partitionID) % nodeCount
	out := make([]consensus.NodeID, 0, replicaFactor)
	for i := 0; i < replicaFactor; i++ {
		out = append(out, consensus.NodeID((start+i)%nodeCount+1))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return dedupeNodeIDs(out)
}

func graphConsistencyPeerAddrs(cfg daemonconfig.ClusterConfig, expected []consensus.NodeID, local consensus.NodeID) map[consensus.NodeID]string {
	out := map[consensus.NodeID]string{}
	for _, nodeID := range expected {
		if nodeID == 0 || nodeID == local {
			continue
		}
		idx := int(nodeID) - 1
		if idx >= 0 && idx < len(cfg.RaftNodeAddrs) && strings.TrimSpace(cfg.RaftNodeAddrs[idx]) != "" {
			out[nodeID] = strings.TrimSpace(cfg.RaftNodeAddrs[idx])
		}
	}
	return out
}

func missingGraphConsistencyAddrs(cfg daemonconfig.ClusterConfig, expected []consensus.NodeID, local consensus.NodeID) []consensus.NodeID {
	missing := []consensus.NodeID{}
	for _, nodeID := range expected {
		if nodeID == 0 || nodeID == local {
			continue
		}
		idx := int(nodeID) - 1
		if idx < 0 || idx >= len(cfg.RaftNodeAddrs) || strings.TrimSpace(cfg.RaftNodeAddrs[idx]) == "" {
			missing = append(missing, nodeID)
		}
	}
	return missing
}

func decodePeerLocalGraphStats(payload []byte) (graphservice.LocalGraphStats, error) {
	var stats graphservice.LocalGraphStats
	if len(payload) == 0 {
		return graphservice.LocalGraphStats{}, fmt.Errorf("empty graph consistency payload")
	}
	if err := json.Unmarshal(payload, &stats); err != nil {
		return graphservice.LocalGraphStats{}, err
	}
	return stats, nil
}

func classifyGraphConsistencyReport(report *adminv1.GetGraphConsistencyReportResponse) adminv1.GraphConsistencyStatus {
	if report == nil || len(report.GetReplicas()) == 0 || len(report.GetExpectedReplicaNodeIds()) == 0 {
		return adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_UNKNOWN
	}
	if len(report.GetReplicas()) < len(report.GetExpectedReplicaNodeIds()) {
		return adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_DEGRADED
	}
	var baseline *adminv1.LocalGraphConsistencyStats
	degraded := false
	divergent := false
	lagging := false
	seen := map[uint64]bool{}
	for _, replica := range report.GetReplicas() {
		if replica.GetRaftNodeId() != 0 {
			seen[replica.GetRaftNodeId()] = true
		}
		if !replica.GetReachable() || replica.GetStats() == nil {
			degraded = true
			continue
		}
		stats := replica.GetStats()
		if baseline == nil {
			baseline = stats
			continue
		}
		if stats.GetSpaceId() != baseline.GetSpaceId() || stats.GetDomainId() != baseline.GetDomainId() || stats.GetPartitionId() != baseline.GetPartitionId() || stats.GetChecksumAlgorithm() != baseline.GetChecksumAlgorithm() || stats.GetSource() != baseline.GetSource() {
			divergent = true
		}
		if stats.GetNodeCount() != baseline.GetNodeCount() || stats.GetEdgeCount() != baseline.GetEdgeCount() {
			divergent = true
		}
		if stats.GetNodeChecksum() != baseline.GetNodeChecksum() || stats.GetEdgeChecksum() != baseline.GetEdgeChecksum() || stats.GetGraphChecksum() != baseline.GetGraphChecksum() {
			divergent = true
		}
		if stats.GetRevision() != baseline.GetRevision() {
			lagging = true
		}
	}
	for _, expected := range report.GetExpectedReplicaNodeIds() {
		if !seen[expected] {
			degraded = true
		}
	}
	appendComparisonWarnings(report, baseline)
	if divergent {
		return adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_DIVERGENT
	}
	if degraded {
		return adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_DEGRADED
	}
	if baseline == nil {
		return adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_UNKNOWN
	}
	if lagging {
		return adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_LAGGING
	}
	return adminv1.GraphConsistencyStatus_GRAPH_CONSISTENCY_STATUS_CONSISTENT
}

func appendComparisonWarnings(report *adminv1.GetGraphConsistencyReportResponse, baseline *adminv1.LocalGraphConsistencyStats) {
	if report == nil || baseline == nil {
		return
	}
	for _, replica := range report.GetReplicas() {
		stats := replica.GetStats()
		if stats == nil {
			continue
		}
		if stats.GetRevision() != baseline.GetRevision() {
			report.Warnings = append(report.Warnings, graphConsistencyWarning("apply_lag", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_WARNING, consensus.NodeID(replica.GetRaftNodeId()), fmt.Sprintf("replica revision %d differs from baseline revision %d", stats.GetRevision(), baseline.GetRevision())))
		}
		if stats.GetNodeCount() != baseline.GetNodeCount() || stats.GetEdgeCount() != baseline.GetEdgeCount() {
			report.Warnings = append(report.Warnings, graphConsistencyWarning("count_mismatch", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_CRITICAL, consensus.NodeID(replica.GetRaftNodeId()), "replica node/edge counts differ from baseline"))
		}
		if stats.GetNodeChecksum() != baseline.GetNodeChecksum() || stats.GetEdgeChecksum() != baseline.GetEdgeChecksum() || stats.GetGraphChecksum() != baseline.GetGraphChecksum() {
			report.Warnings = append(report.Warnings, graphConsistencyWarning("checksum_mismatch", adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_CRITICAL, consensus.NodeID(replica.GetRaftNodeId()), "replica checksum differs from baseline"))
		}
	}
}

func warningForPeerCollectionError(nodeID consensus.NodeID, err error) *adminv1.GraphConsistencyWarning {
	code := "replica_unreachable"
	severity := adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_WARNING
	if status.Code(err) == codes.PermissionDenied {
		code = "cluster_id_mismatch"
		severity = adminv1.GraphConsistencyWarningSeverity_GRAPH_CONSISTENCY_WARNING_SEVERITY_CRITICAL
	}
	return graphConsistencyWarning(code, severity, nodeID, err.Error())
}

func graphConsistencyWarning(code string, severity adminv1.GraphConsistencyWarningSeverity, nodeID consensus.NodeID, message string) *adminv1.GraphConsistencyWarning {
	return &adminv1.GraphConsistencyWarning{Code: code, Severity: severity, RaftNodeId: uint64(nodeID), Message: message}
}

func reportLocalRaftNodeID(cfg daemonconfig.ClusterConfig, nodeID string) consensus.NodeID {
	if cfg.RaftLocalNodeID > 0 {
		return consensus.NodeID(cfg.RaftLocalNodeID)
	}
	if strings.HasPrefix(nodeID, "node_") {
		var parsed uint64
		if _, err := fmt.Sscanf(strings.TrimPrefix(nodeID, "node_"), "%d", &parsed); err == nil && parsed > 0 {
			return consensus.NodeID(parsed)
		}
	}
	return 0
}

func consensusNodeIDsToUint64(in []consensus.NodeID) []uint64 {
	out := make([]uint64, 0, len(in))
	for _, id := range in {
		if id != 0 {
			out = append(out, uint64(id))
		}
	}
	return out
}

func dedupeNodeIDs(in []consensus.NodeID) []consensus.NodeID {
	seen := map[consensus.NodeID]bool{}
	out := make([]consensus.NodeID, 0, len(in))
	for _, id := range in {
		if id == 0 || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}
