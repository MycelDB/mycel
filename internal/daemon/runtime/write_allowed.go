package runtime

import (
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequireLocalWriteAllowed is the daemon-level write gate used by modules before
// local mutation paths. Standalone writes are allowed, and mesh/clustered writes
// are routed/forwarded by module-specific Raft executors. In multi-node modes,
// any subsystem that has not been wired to its Raft executor must fail closed
// rather than mutating local state directly.
func (r *Runtime) RequireLocalWriteAllowed() error {
	if r == nil {
		return nil
	}
	mode := strings.TrimSpace(strings.ToLower(r.Config.Mode))
	raftConfigured := len(r.Config.Cluster.RaftNodeAddrs) > 0 || r.Config.Cluster.RaftNodeCount == 1 || r.RaftGroups != nil
	if (mode == "" || mode == "standalone") && !raftConfigured {
		return nil
	}
	if r.ClusterManager == nil {
		return status.Error(codes.Unavailable, "clustered local write rejected: clustering manager is not available")
	}
	readiness := r.ClusterManager.Readiness()
	if !readiness.ClientReady || !readiness.WriteReady {
		return status.Error(codes.Unavailable, "clustered local write rejected: node is not write-ready")
	}
	if blocker := r.raftWriteReadinessBlocker(raftConfigured); blocker != "" {
		return status.Error(codes.Unavailable, "clustered local write rejected: node is not write-ready: "+blocker)
	}
	return status.Error(codes.Unavailable, "clustered local write rejected: raft executor is not configured for this subsystem")
}

func (r *Runtime) raftWriteReadinessBlocker(raftConfigured bool) string {
	if r == nil || !raftConfigured {
		return ""
	}
	if r.RaftGroups == nil {
		return "raft groups are not configured"
	}
	statuses := r.RaftGroups.Status()
	if len(statuses) == 0 {
		return "raft groups are not started"
	}
	missing := make([]string, 0)
	for _, st := range statuses {
		if st.Leader == 0 {
			missing = append(missing, string(st.GroupID))
		}
	}
	if len(missing) == 0 {
		return ""
	}
	sort.Strings(missing)
	return "raft groups without leaders: " + strings.Join(missing, ", ")
}
