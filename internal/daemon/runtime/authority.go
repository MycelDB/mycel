package runtime

import (
	"strconv"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/model"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	NotPrimaryReason   = "MYCEL_CLUSTER_NOT_PRIMARY"
	PrimaryNodeIDKey   = "mycel-primary-node-id"
	PrimaryNodeNameKey = "mycel-primary-node-name"
	PrimaryBackendKey  = "mycel-primary-backend-advertise-addr"
	AuthorityEpochKey  = "mycel-authority-epoch"
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
		return r.notPrimaryError()
	}
	return nil
}

func (r *Runtime) notPrimaryError() error {
	st := status.New(codes.FailedPrecondition, "node is not cluster primary")
	metadata := map[string]string{}
	if r != nil && r.ClusterManager != nil {
		if authority, ok := r.ClusterManager.Authority(); ok {
			metadata[PrimaryNodeIDKey] = authority.Primary.NodeID
			metadata[PrimaryNodeNameKey] = authority.Primary.NodeName
			metadata[PrimaryBackendKey] = authority.Primary.BackendAdvertiseAddr
			metadata[AuthorityEpochKey] = strconv.FormatInt(authority.AuthorityEpoch, 10)
		}
	}
	if len(metadata) == 0 {
		return st.Err()
	}
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: NotPrimaryReason, Domain: "mycel.cluster", Metadata: metadata})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}
