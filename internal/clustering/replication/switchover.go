package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
)

type AuthorityClient interface {
	GetReplicationStatus(ctx context.Context, addr string, clusterID string, requesterNodeID string) (backend.ReplicationStatus, error)
	InstallAuthority(ctx context.Context, addr string, operationID string, clusterID string, targetNodeID string, authority *clusterpb.ClusterAuthority, finalLSN wal.LSN) error
}

type SwitchoverCoordinator struct {
	Cluster *clustering.Manager
	DataDir string
	WAL     *wal.Manager
	Quiesce *quiesce.Coordinator
	Client  AuthorityClient
	Timeout time.Duration
}

type SwitchoverResult struct {
	OperationID        string
	OldPrimaryNodeID   string
	OldPrimaryNodeName string
	NewPrimaryNodeID   string
	NewPrimaryNodeName string
	AuthorityEpoch     int64
	FinalLSN           uint64
}

func (c *SwitchoverCoordinator) SwitchPrimary(ctx context.Context, target string) (SwitchoverResult, error) {
	if c == nil || c.Cluster == nil || c.WAL == nil || c.Client == nil {
		return SwitchoverResult{}, fmt.Errorf("switchover coordinator is not initialized")
	}
	if !c.Cluster.IsAdmitted() {
		return SwitchoverResult{}, fmt.Errorf("local node is not admitted to a cluster")
	}
	if c.Cluster.LocalRole() != clustering.NodeRolePrimary {
		return SwitchoverResult{}, ErrNotPrimary
	}
	t, err := ResolveFollowerTarget(ctx, c.Cluster, target)
	if err != nil {
		return SwitchoverResult{}, err
	}
	authority, ok := c.Cluster.Authority()
	if !ok {
		return SwitchoverResult{}, fmt.Errorf("cluster authority is not available")
	}
	opID := "switch_" + uuid.NewString()
	var lease *quiesce.CompositeLease
	if c.Quiesce != nil {
		l, err := c.Quiesce.QuiesceAll(ctx, quiesce.Request{Reason: "planned primary switchover", Mode: quiesce.ModeBackup, Source: "cluster-switchover"})
		if err != nil {
			return SwitchoverResult{}, err
		}
		lease = l
		defer lease.Release(context.Background())
	}
	final := c.WAL.LastCommittedLSN()
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		st, err := c.Client.GetReplicationStatus(waitCtx, t.BackendAdvertiseAddr, c.Cluster.Identity().ClusterID, c.Cluster.Identity().NodeID)
		if err == nil && st.AppliedLSN >= final {
			break
		}
		if err := waitCtx.Err(); err != nil {
			return SwitchoverResult{}, fmt.Errorf("target did not catch up to final lsn %d: %w", final, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	newAuthority := clustering.Authority{Version: clustering.AuthorityVersion, ClusterID: authority.ClusterID, Primary: clustering.AuthorityPrimary{NodeID: t.NodeID, NodeName: t.NodeName, BackendAdvertiseAddr: t.BackendAdvertiseAddr}, AuthorityEpoch: authority.AuthorityEpoch + 1, Source: clustering.AuthoritySourceManual, UpdatedAt: time.Now().UTC()}
	intent := clustering.SwitchoverIntent{OperationID: opID, ClusterID: authority.ClusterID, OldPrimaryNodeID: authority.Primary.NodeID, NewPrimaryNodeID: t.NodeID, NewAuthority: newAuthority, FinalLSN: uint64(final), Phase: clustering.SwitchoverIntentTargetInstalling}
	if c.DataDir != "" {
		_ = clustering.SaveSwitchoverIntent(ctx, c.DataDir, intent)
	}
	proto := clustering.AuthorityToProto(newAuthority, true)
	if err := c.Client.InstallAuthority(ctx, t.BackendAdvertiseAddr, opID, c.Cluster.Identity().ClusterID, t.NodeID, proto, final); err != nil {
		intent.Phase = clustering.SwitchoverIntentFailed
		intent.Error = err.Error()
		if c.DataDir != "" {
			_ = clustering.SaveSwitchoverIntent(context.Background(), c.DataDir, intent)
		}
		return SwitchoverResult{}, err
	}
	intent.Phase = clustering.SwitchoverIntentTargetInstalled
	if c.DataDir != "" {
		_ = clustering.SaveSwitchoverIntent(ctx, c.DataDir, intent)
	}
	if err := c.Cluster.SetAuthority(ctx, newAuthority); err != nil {
		return SwitchoverResult{}, err
	}
	c.propagateAuthority(ctx, opID, proto, final, t.NodeID)
	intent.Phase = clustering.SwitchoverIntentLocalInstalled
	if c.DataDir != "" {
		_ = clustering.SaveSwitchoverIntent(ctx, c.DataDir, intent)
		_ = clustering.ClearSwitchoverIntent(ctx, c.DataDir)
	}
	return SwitchoverResult{OperationID: opID, OldPrimaryNodeID: authority.Primary.NodeID, OldPrimaryNodeName: authority.Primary.NodeName, NewPrimaryNodeID: t.NodeID, NewPrimaryNodeName: t.NodeName, AuthorityEpoch: newAuthority.AuthorityEpoch, FinalLSN: uint64(final)}, nil
}

func (c *SwitchoverCoordinator) propagateAuthority(ctx context.Context, opID string, authority *clusterpb.ClusterAuthority, final wal.LSN, targetNodeID string) {
	if c == nil || c.Cluster == nil || c.Client == nil || c.Cluster.Membership() == nil || authority == nil {
		return
	}
	data, err := c.Cluster.Membership().Load(ctx)
	if err != nil {
		return
	}
	localID := c.Cluster.Identity().NodeID
	for _, member := range data.Members {
		if member.NodeID == "" || member.NodeID == localID || member.NodeID == targetNodeID || member.BackendAdvertiseAddr == "" {
			continue
		}
		if member.State != "active" {
			continue
		}
		_ = c.Client.InstallAuthority(ctx, member.BackendAdvertiseAddr, opID, c.Cluster.Identity().ClusterID, member.NodeID, authority, final)
	}
}
