package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering"
)

type FailoverCoordinator struct{ Cluster *clustering.Manager }

type FailoverResult struct {
	OperationID        string
	OldPrimaryNodeID   string
	OldPrimaryNodeName string
	NewPrimaryNodeID   string
	NewPrimaryNodeName string
	AuthorityEpoch     int64
}

func (c *FailoverCoordinator) PromoteLocalPrimary(ctx context.Context, force bool, confirmation string) (FailoverResult, error) {
	if c == nil || c.Cluster == nil {
		return FailoverResult{}, fmt.Errorf("failover coordinator is not initialized")
	}
	if !force {
		return FailoverResult{}, fmt.Errorf("force is required for emergency failover")
	}
	if confirmation != "old-primary-fenced" {
		return FailoverResult{}, fmt.Errorf("confirmation must be old-primary-fenced")
	}
	if !c.Cluster.IsAdmitted() {
		return FailoverResult{}, fmt.Errorf("local node is not admitted to a cluster")
	}
	if c.Cluster.LocalRole() == clustering.NodeRolePrimary {
		return FailoverResult{}, fmt.Errorf("local node is already primary")
	}
	authority, ok := c.Cluster.Authority()
	if !ok {
		return FailoverResult{}, fmt.Errorf("cluster authority is not available")
	}
	id := c.Cluster.Identity()
	newAuthority := clustering.Authority{Version: clustering.AuthorityVersion, ClusterID: id.ClusterID, Primary: clustering.AuthorityPrimary{NodeID: id.NodeID, NodeName: id.NodeName, BackendAdvertiseAddr: id.BackendAdvertiseAddr}, AuthorityEpoch: authority.AuthorityEpoch + 1, Source: clustering.AuthoritySourceManual, UpdatedAt: time.Now().UTC()}
	if err := c.Cluster.SetAuthority(ctx, newAuthority); err != nil {
		return FailoverResult{}, err
	}
	return FailoverResult{OperationID: "failover_" + uuid.NewString(), OldPrimaryNodeID: authority.Primary.NodeID, OldPrimaryNodeName: authority.Primary.NodeName, NewPrimaryNodeID: id.NodeID, NewPrimaryNodeName: id.NodeName, AuthorityEpoch: newAuthority.AuthorityEpoch}, nil
}
