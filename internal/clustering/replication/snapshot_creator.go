package replication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	"github.com/myceldb/mycel/internal/wal"
)

type SnapshotResult struct {
	OperationID  string
	BaseLSN      wal.LSN
	ManifestJSON string
	ArchivePath  string
	TotalBytes   uint64
	Checksum     string
}

type SnapshotCreator struct {
	DataDir        string
	SnapshotDir    string
	ClusterID      string
	PrimaryNodeID  string
	AuthorityEpoch int64
	Quiesce        *quiesce.Coordinator
	WAL            *wal.Manager
	Progress       wal.AppliedLSNStore
	Checkpoint     *wal.CheckpointStore
	Logger         *slog.Logger
	Now            func() time.Time
}

func (c *SnapshotCreator) Create(ctx context.Context) (SnapshotResult, error) {
	if c == nil || c.DataDir == "" || c.WAL == nil || c.Progress == nil || c.Checkpoint == nil {
		return SnapshotResult{}, fmt.Errorf("snapshot creator is not initialized")
	}
	now := time.Now().UTC
	if c.Now != nil {
		now = c.Now
	}
	op := "resync-" + uuid.NewString()
	snapDir := c.SnapshotDir
	if snapDir == "" {
		snapDir = filepath.Join(c.DataDir, "..", "resync-snapshots")
	}
	var lease *quiesce.CompositeLease
	var err error
	if c.Quiesce != nil {
		lease, err = c.Quiesce.QuiesceAll(ctx, quiesce.Request{Reason: "snapshot resync", Mode: quiesce.ModeBackup, Source: "snapshot-resync"})
		if err != nil {
			return SnapshotResult{}, err
		}
		defer lease.Release(context.Background())
	}
	last := c.WAL.LastCommittedLSN()
	if err := c.WAL.WaitUntilCommitted(ctx, last); err != nil {
		return SnapshotResult{}, err
	}
	cp, err := wal.CreateCheckpoint(ctx, c.Progress, c.Checkpoint, last)
	if err != nil {
		return SnapshotResult{}, err
	}
	policy := replsnapshot.DefaultResyncSnapshotPathPolicy()
	manifest, err := replsnapshot.BuildManifest(ctx, c.DataDir, replsnapshot.Manifest{ClusterID: c.ClusterID, PrimaryNodeID: c.PrimaryNodeID, AuthorityEpoch: c.AuthorityEpoch, SnapshotBaseLSN: cp.LSN, CreatedAt: now()}, policy)
	if err != nil {
		return SnapshotResult{}, err
	}
	if err := os.MkdirAll(snapDir, 0700); err != nil {
		return SnapshotResult{}, err
	}
	archive := filepath.Join(snapDir, op+".zip")
	if err := replsnapshot.WriteZipSnapshot(ctx, c.DataDir, archive, manifest); err != nil {
		return SnapshotResult{}, err
	}
	checksum, size, err := replsnapshot.FileSHA256(archive)
	if err != nil {
		return SnapshotResult{}, err
	}
	manifestJSON, err := manifest.JSON()
	if err != nil {
		return SnapshotResult{}, err
	}
	return SnapshotResult{OperationID: op, BaseLSN: cp.LSN, ManifestJSON: manifestJSON, ArchivePath: archive, TotalBytes: uint64(size), Checksum: checksum}, nil
}
