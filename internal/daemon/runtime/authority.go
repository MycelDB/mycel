package runtime

import (
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (r *Runtime) RequireWriteAuthority() error {
	if r == nil || r.ClusterManager == nil {
		return nil
	}
	state := r.ClusterManager.State()
	if state == model.NodeStateStandalone {
		return nil
	}
	if !r.ClusterManager.IsAdmitted() {
		return status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if r.ClusterManager.LocalRole() != clustering.NodeRolePrimary {
		return status.Error(codes.FailedPrecondition, "node is not cluster primary")
	}
	return nil
}
