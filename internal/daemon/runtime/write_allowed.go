package runtime

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequireLocalWriteAllowed is the daemon-level write gate used by modules before
// local mutation paths. Standalone writes are allowed, and clustered writes are
// routed/forwarded by module-specific Raft executors. In clustered mode, any
// subsystem that has not been wired to its Raft executor must fail closed rather
// than mutating local state directly.
func (r *Runtime) RequireLocalWriteAllowed() error {
	if r == nil || strings.TrimSpace(r.Config.Mode) != "clustered" {
		return nil
	}
	if r.ClusterManager == nil {
		return status.Error(codes.Unavailable, "clustered local write rejected: clustering manager is not available")
	}
	readiness := r.ClusterManager.Readiness()
	if !readiness.ClientReady {
		return status.Error(codes.Unavailable, "clustered local write rejected: node is not client-ready")
	}
	return status.Error(codes.Unavailable, "clustered local write rejected: raft executor is not configured for this subsystem")
}
