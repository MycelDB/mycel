package service

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

type ClusterBackupPrecheckInput struct {
	BackupSetID         string
	ClusterID           string
	ExpectedNodeCount   int
	ActiveBackupSetID   string
	IncompatibleQuiesce bool
	Nodes               []ClusterBackupPrecheckNode
	RaftGroups          []ClusterBackupPrecheckRaftGroup
	Destinations        []ClusterBackupDestinationCheck
}

type ClusterBackupPrecheckNode struct {
	PodName              string
	NodeID               string
	Ordinal              int
	RaftNodeID           uint64
	BackendAdvertiseAddr string
	ClusterID            string
	Reachable            bool
	Ready                bool
	Admitted             bool
	CaughtUp             bool
	ReadinessBlockers    []string
	Error                string
}

type ClusterBackupPrecheckRaftGroup struct {
	GroupID       consensus.GroupID
	Leader        consensus.NodeID
	HasQuorum     bool
	CommitIndex   uint64
	AppliedByNode map[uint64]uint64
}

type ClusterBackupDestinationCheck struct {
	PodName        string
	Path           string
	Writable       bool
	OutsideDataDir bool
	Error          string
}

type ClusterBackupPrecheckResult struct {
	OK       bool
	Failures []string
}

func (r ClusterBackupPrecheckResult) Error() error {
	if r.OK {
		return nil
	}
	return fmt.Errorf("cluster backup precheck failed: %s", strings.Join(r.Failures, "; "))
}

func (m *Module) EvaluateClusterBackupPreconditions(input ClusterBackupPrecheckInput) ClusterBackupPrecheckResult {
	m.mu.Lock()
	if input.ActiveBackupSetID == "" {
		input.ActiveBackupSetID = m.activeClusterBackupID
	}
	m.mu.Unlock()
	return EvaluateClusterBackupPreconditions(input)
}

func EvaluateClusterBackupPreconditions(input ClusterBackupPrecheckInput) ClusterBackupPrecheckResult {
	var failures []string
	if strings.TrimSpace(input.BackupSetID) == "" {
		failures = append(failures, "backup_set_id is required")
	}
	if strings.TrimSpace(input.ClusterID) == "" {
		failures = append(failures, "cluster_id is required")
	}
	if strings.TrimSpace(input.ActiveBackupSetID) != "" && strings.TrimSpace(input.ActiveBackupSetID) != strings.TrimSpace(input.BackupSetID) {
		failures = append(failures, fmt.Sprintf("cluster backup %s is already active", input.ActiveBackupSetID))
	}
	if input.IncompatibleQuiesce {
		failures = append(failures, "an incompatible quiesce/recovery operation is active")
	}
	expected := input.ExpectedNodeCount
	if expected <= 0 {
		failures = append(failures, "expected node count must be positive")
	}
	if expected > 0 && len(input.Nodes) != expected {
		failures = append(failures, fmt.Sprintf("expected %d nodes but found %d", expected, len(input.Nodes)))
	}
	seenPods := map[string]struct{}{}
	seenOrdinals := map[int]struct{}{}
	seenRaftNodeIDs := map[uint64]string{}
	for i, node := range input.Nodes {
		label := nodeLabel(i, node)
		pod := strings.TrimSpace(node.PodName)
		if pod == "" {
			failures = append(failures, label+": pod_name is required")
		} else {
			if _, ok := seenPods[pod]; ok {
				failures = append(failures, "duplicate pod_name "+pod)
			}
			seenPods[pod] = struct{}{}
		}
		if strings.TrimSpace(node.NodeID) == "" {
			failures = append(failures, label+": node_id is required")
		}
		if node.Ordinal < 0 {
			failures = append(failures, label+": ordinal must be non-negative")
		} else {
			if _, ok := seenOrdinals[node.Ordinal]; ok {
				failures = append(failures, fmt.Sprintf("duplicate ordinal %d", node.Ordinal))
			}
			seenOrdinals[node.Ordinal] = struct{}{}
		}
		if node.RaftNodeID == 0 {
			failures = append(failures, label+": raft_node_id is required")
		} else {
			if other, ok := seenRaftNodeIDs[node.RaftNodeID]; ok {
				failures = append(failures, fmt.Sprintf("duplicate raft_node_id %d for %s and %s", node.RaftNodeID, other, label))
			}
			seenRaftNodeIDs[node.RaftNodeID] = label
		}
		if !node.Reachable {
			failures = append(failures, label+": node is unreachable")
		}
		if !node.Ready {
			failures = append(failures, label+": pod/daemon is not ready")
		}
		if !node.Admitted {
			failures = append(failures, label+": node is not admitted")
		}
		if !node.CaughtUp {
			failures = append(failures, label+": node is not caught up")
		}
		if strings.TrimSpace(node.ClusterID) == "" {
			failures = append(failures, label+": cluster_id is missing")
		} else if strings.TrimSpace(input.ClusterID) != "" && strings.TrimSpace(node.ClusterID) != strings.TrimSpace(input.ClusterID) {
			failures = append(failures, fmt.Sprintf("%s: cluster_id %s does not match %s", label, node.ClusterID, input.ClusterID))
		}
		if strings.TrimSpace(node.BackendAdvertiseAddr) == "" {
			failures = append(failures, label+": backend advertise address is missing")
		}
		for _, blocker := range node.ReadinessBlockers {
			if strings.TrimSpace(blocker) != "" {
				failures = append(failures, label+": readiness blocker: "+strings.TrimSpace(blocker))
			}
		}
		if strings.TrimSpace(node.Error) != "" {
			failures = append(failures, label+": "+strings.TrimSpace(node.Error))
		}
	}
	if len(input.RaftGroups) == 0 {
		failures = append(failures, "raft group status is required")
	}
	for _, group := range input.RaftGroups {
		groupID := strings.TrimSpace(string(group.GroupID))
		if groupID == "" {
			groupID = "<unknown>"
		}
		if group.Leader == 0 {
			failures = append(failures, fmt.Sprintf("raft group %s has no leader", groupID))
		}
		if !group.HasQuorum {
			failures = append(failures, fmt.Sprintf("raft group %s has no quorum", groupID))
		}
		if group.CommitIndex > 0 && len(group.AppliedByNode) == 0 {
			failures = append(failures, fmt.Sprintf("raft group %s has no applied index evidence", groupID))
		}
		for _, node := range input.Nodes {
			if node.RaftNodeID == 0 {
				continue
			}
			applied, ok := group.AppliedByNode[node.RaftNodeID]
			if !ok {
				failures = append(failures, fmt.Sprintf("raft group %s missing applied index for %s", groupID, nodeLabelForNode(node)))
				continue
			}
			if applied < group.CommitIndex {
				failures = append(failures, fmt.Sprintf("raft group %s node %s applied index %d is behind commit index %d", groupID, nodeLabelForNode(node), applied, group.CommitIndex))
			}
		}
	}
	destinations := map[string]ClusterBackupDestinationCheck{}
	for _, dest := range input.Destinations {
		pod := strings.TrimSpace(dest.PodName)
		if pod == "" {
			failures = append(failures, "backup destination pod_name is required")
			continue
		}
		if _, ok := destinations[pod]; ok {
			failures = append(failures, "duplicate backup destination for "+pod)
		}
		destinations[pod] = dest
		if strings.TrimSpace(dest.Path) == "" {
			failures = append(failures, pod+": backup destination path is required")
		} else if !filepath.IsAbs(dest.Path) {
			failures = append(failures, pod+": backup destination path must be absolute")
		}
		if !dest.Writable {
			failures = append(failures, pod+": backup destination is not writable")
		}
		if !dest.OutsideDataDir {
			failures = append(failures, pod+": backup destination must be outside data dir")
		}
		if strings.TrimSpace(dest.Error) != "" {
			failures = append(failures, pod+": "+strings.TrimSpace(dest.Error))
		}
	}
	for _, node := range input.Nodes {
		pod := strings.TrimSpace(node.PodName)
		if pod == "" {
			continue
		}
		if _, ok := destinations[pod]; !ok {
			failures = append(failures, pod+": backup destination check is missing")
		}
	}
	return ClusterBackupPrecheckResult{OK: len(failures) == 0, Failures: failures}
}

func nodeLabel(index int, node ClusterBackupPrecheckNode) string {
	if strings.TrimSpace(node.PodName) != "" {
		return node.PodName
	}
	if strings.TrimSpace(node.NodeID) != "" {
		return node.NodeID
	}
	return fmt.Sprintf("nodes[%d]", index)
}

func nodeLabelForNode(node ClusterBackupPrecheckNode) string {
	return nodeLabel(0, node)
}
