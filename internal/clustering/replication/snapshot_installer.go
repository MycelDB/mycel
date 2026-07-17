package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
	"github.com/myceldb/mycel/internal/wal"
)

type SnapshotInstaller struct {
	DataDir            string
	Identity           func() model.NodeIdentity
	Authority          func() (string, int64, bool) // primary node id, epoch, ok
	Progress           *ProgressStore
	ReceiveLog         *ReceiveLog
	ReloadAfterInstall func(ctx context.Context) error
}

func (i *SnapshotInstaller) InstallSnapshot(ctx context.Context, desc replsnapshot.SnapshotDescriptor, r io.Reader) (wal.LSN, error) {
	if i == nil || i.Progress == nil || i.ReceiveLog == nil {
		return 0, fmt.Errorf("%w: installer not initialized", replsnapshot.ErrSnapshotInstallFailed)
	}
	id := model.NodeIdentity{}
	if i.Identity != nil {
		id = i.Identity()
	}
	if desc.ClusterID == "" || desc.ClusterID != id.ClusterID {
		return 0, fmt.Errorf("%w: cluster mismatch", replsnapshot.ErrSnapshotValidationFailed)
	}
	if desc.TargetNodeID == "" || desc.TargetNodeID != id.NodeID {
		return 0, fmt.Errorf("%w: target mismatch", replsnapshot.ErrSnapshotValidationFailed)
	}
	if i.Authority != nil {
		primary, epoch, ok := i.Authority()
		if ok && (desc.PrimaryNodeID != primary || desc.AuthorityEpoch < epoch) {
			return 0, fmt.Errorf("%w: authority mismatch", replsnapshot.ErrSnapshotValidationFailed)
		}
	}
	op := strings.TrimSpace(desc.OperationID)
	if op == "" {
		op = "snapshot"
	}
	staging := filepath.Join(i.DataDir, "meta", "clustering", "replication", "snapshot-staging", op)
	if err := os.RemoveAll(staging); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return 0, err
	}
	archive := filepath.Join(staging, "snapshot.archive")
	f, err := os.OpenFile(archive, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), r)
	closeErr := f.Close()
	if copyErr != nil {
		return 0, copyErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if desc.TotalBytes != 0 && uint64(n) != desc.TotalBytes {
		return 0, fmt.Errorf("%w: byte count mismatch", replsnapshot.ErrSnapshotValidationFailed)
	}
	if desc.Checksum != "" && !strings.EqualFold(desc.Checksum, hex.EncodeToString(h.Sum(nil))) {
		return 0, fmt.Errorf("%w: checksum mismatch", replsnapshot.ErrSnapshotValidationFailed)
	}
	raw, _ := json.MarshalIndent(desc, "", "  ")
	_ = os.WriteFile(filepath.Join(staging, "descriptor.json"), append(raw, '\n'), 0o600)
	unpacked := filepath.Join(staging, "unpacked")
	manifest, err := replsnapshot.ExtractZipSnapshot(ctx, archive, unpacked, replsnapshot.DefaultResyncSnapshotPathPolicy())
	if err != nil {
		return 0, err
	}
	if manifest.ClusterID != desc.ClusterID || manifest.PrimaryNodeID != desc.PrimaryNodeID || manifest.AuthorityEpoch != desc.AuthorityEpoch || manifest.SnapshotBaseLSN != desc.SnapshotBaseLSN {
		return 0, fmt.Errorf("%w: manifest descriptor mismatch", replsnapshot.ErrSnapshotValidationFailed)
	}
	tx, err := installMaterializedSnapshot(ctx, i.DataDir, unpacked, filepath.Join(staging, "rollback"), manifest)
	if err != nil {
		return 0, err
	}
	if i.ReloadAfterInstall != nil {
		if err := i.ReloadAfterInstall(ctx); err != nil {
			_ = tx.Rollback(context.Background())
			return 0, err
		}
	}
	if err := i.ReceiveLog.Clear(ctx); err != nil {
		return 0, err
	}
	p := Progress{Version: ProgressVersion, ClusterID: desc.ClusterID, PrimaryNodeID: desc.PrimaryNodeID, AuthorityEpoch: desc.AuthorityEpoch, ReceivedLSN: desc.SnapshotBaseLSN, AppliedLSN: desc.SnapshotBaseLSN, CatchupState: CatchupStateCaughtUp}
	if err := i.Progress.Save(ctx, p); err != nil {
		return 0, err
	}
	if err := os.RemoveAll(staging); err != nil {
		return 0, err
	}
	return desc.SnapshotBaseLSN, nil
}
