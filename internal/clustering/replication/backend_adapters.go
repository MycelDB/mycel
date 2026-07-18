package replication

import (
	"context"
	"fmt"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/backend"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
)

type BackendReplicationStatusProvider struct {
	Progress *ProgressStore
	Cluster  *clustering.Manager
}

func (p BackendReplicationStatusProvider) ReplicationStatus(ctx context.Context) (backend.ReplicationStatus, error) {
	if p.Progress == nil || p.Cluster == nil {
		return backend.ReplicationStatus{}, fmt.Errorf("replication status unavailable")
	}
	prog, err := p.Progress.Load(ctx)
	if err != nil {
		return backend.ReplicationStatus{}, err
	}
	a, _ := p.Cluster.Authority()
	return backend.ReplicationStatus{ClusterID: p.Cluster.Identity().ClusterID, LocalNodeID: p.Cluster.Identity().NodeID, PrimaryNodeID: a.Primary.NodeID, AuthorityEpoch: a.AuthorityEpoch, ReceivedLSN: prog.ReceivedLSN, AppliedLSN: prog.AppliedLSN, CatchupState: string(prog.CatchupState)}, nil
}

type BackendAuthorityInstaller struct {
	Cluster  *clustering.Manager
	Progress *ProgressStore
}

func (i BackendAuthorityInstaller) InstallAuthority(ctx context.Context, proto *clusterpb.ClusterAuthority, finalLSN wal.LSN, operationID string) error {
	if i.Cluster == nil {
		return fmt.Errorf("cluster manager unavailable")
	}
	authority, ok, err := clustering.AuthorityFromProto(proto)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("authority is required")
	}
	if authority.ClusterID != i.Cluster.Identity().ClusterID {
		return fmt.Errorf("cluster_id mismatch")
	}
	current, currentOK := i.Cluster.Authority()
	if currentOK && authority.AuthorityEpoch <= current.AuthorityEpoch {
		return fmt.Errorf("authority epoch is stale")
	}
	if authority.Primary.NodeID == i.Cluster.Identity().NodeID && i.Progress != nil {
		prog, err := i.Progress.Load(ctx)
		if err != nil {
			return err
		}
		if prog.AppliedLSN < finalLSN {
			return fmt.Errorf("target applied_lsn %d is behind final_lsn %d", prog.AppliedLSN, finalLSN)
		}
	}
	return i.Cluster.SetAuthority(ctx, authority)
}
