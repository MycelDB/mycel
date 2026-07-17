package replication

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
)

type SnapshotCreateService interface {
	Create(context.Context) (SnapshotResult, error)
}
type SnapshotInstallClient interface {
	InstallSnapshot(ctx context.Context, addr string, desc replsnapshot.SnapshotDescriptor, r io.Reader) (replsnapshot.InstallSnapshotResult, error)
}

type ResyncCoordinator struct {
	Cluster *clustering.Manager
	Creator SnapshotCreateService
	Client  SnapshotInstallClient
	History *ResyncHistoryStore
}

type ResyncResult struct {
	OperationID     string
	Target          replsnapshot.ResyncTarget
	SnapshotBaseLSN uint64
	TotalBytes      uint64
	Checksum        string
}

func (c *ResyncCoordinator) Resync(ctx context.Context, target string) (ResyncResult, error) {
	if c == nil || c.Cluster == nil || c.Creator == nil || c.Client == nil {
		return ResyncResult{}, fmt.Errorf("resync coordinator is not initialized")
	}
	if !c.Cluster.IsAdmitted() {
		return ResyncResult{}, fmt.Errorf("local node is not admitted to a cluster")
	}
	if c.Cluster.LocalRole() != clustering.NodeRolePrimary {
		return ResyncResult{}, ErrNotPrimary
	}
	t, err := ResolveFollowerTarget(ctx, c.Cluster, target)
	if err != nil {
		return ResyncResult{}, err
	}
	started := time.Now().UTC()
	op := ResyncOperation{OperationID: "resync-" + uuid.NewString(), TargetNodeID: t.NodeID, TargetNodeName: t.NodeName, TargetBackendAdvertiseAddr: t.BackendAdvertiseAddr, StartedAt: started, Status: ResyncOperationRunning}
	snap, err := c.Creator.Create(ctx)
	if err != nil {
		op.CompletedAt = time.Now().UTC()
		op.Status = ResyncOperationFailed
		op.Error = err.Error()
		_ = c.saveHistory(ctx, op)
		return ResyncResult{}, err
	}
	if snap.OperationID != "" {
		op.OperationID = snap.OperationID
	}
	op.SnapshotBaseLSN = uint64(snap.BaseLSN)
	op.TotalBytes = snap.TotalBytes
	op.Checksum = snap.Checksum
	_ = c.saveHistory(ctx, op)
	defer func() { _ = os.Remove(snap.ArchivePath) }()
	f, err := os.Open(snap.ArchivePath)
	if err != nil {
		return ResyncResult{}, err
	}
	defer f.Close()
	id := c.Cluster.Identity()
	desc := replsnapshot.SnapshotDescriptor{OperationID: snap.OperationID, ClusterID: id.ClusterID, PrimaryNodeID: id.NodeID, TargetNodeID: t.NodeID, AuthorityEpoch: c.authorityEpoch(), SnapshotBaseLSN: snap.BaseLSN, ManifestJSON: snap.ManifestJSON, TotalBytes: snap.TotalBytes, Checksum: snap.Checksum}
	if _, err := c.Client.InstallSnapshot(ctx, t.BackendAdvertiseAddr, desc, f); err != nil {
		op.CompletedAt = time.Now().UTC()
		op.Status = ResyncOperationFailed
		op.Error = err.Error()
		_ = c.saveHistory(ctx, op)
		return ResyncResult{}, err
	}
	op.CompletedAt = time.Now().UTC()
	op.Status = ResyncOperationSucceeded
	_ = c.saveHistory(ctx, op)
	return ResyncResult{OperationID: snap.OperationID, Target: t, SnapshotBaseLSN: uint64(snap.BaseLSN), TotalBytes: snap.TotalBytes, Checksum: snap.Checksum}, nil
}

func (c *ResyncCoordinator) authorityEpoch() int64 {
	a, ok := c.Cluster.Authority()
	if !ok {
		return 0
	}
	return a.AuthorityEpoch
}

func (c *ResyncCoordinator) saveHistory(ctx context.Context, op ResyncOperation) error {
	if c.History == nil || op.OperationID == "" {
		return nil
	}
	return c.History.Upsert(ctx, op)
}

var ErrNotPrimary = fmt.Errorf("node is not cluster primary")
