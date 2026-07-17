package replication

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/myceldb/mycel/internal/wal"
)

type Applier struct {
	Log      *ReceiveLog
	Progress *ProgressStore
	Registry *wal.Registry
	Logger   *slog.Logger
}

func (a *Applier) ApplyReceived(ctx context.Context, clusterID, primaryNodeID string, epoch int64, rec Record) error {
	if a.Log == nil || a.Progress == nil || a.Registry == nil {
		return fmt.Errorf("replication applier is not initialized")
	}
	p, err := a.Progress.Load(ctx)
	if err != nil {
		return err
	}
	if p.ClusterID != "" && (p.ClusterID != clusterID || p.PrimaryNodeID != primaryNodeID || p.AuthorityEpoch != epoch) {
		return fmt.Errorf("replication authority changed")
	}
	if rec.LSN <= p.AppliedLSN {
		return nil
	}
	if rec.LSN != p.AppliedLSN.Next() {
		return fmt.Errorf("replication wal gap: got %s want %s", rec.LSN, p.AppliedLSN.Next())
	}
	if err := a.Log.Put(ctx, rec); err != nil {
		return err
	}
	p.Version = ProgressVersion
	p.ClusterID = clusterID
	p.PrimaryNodeID = primaryNodeID
	p.AuthorityEpoch = epoch
	p.ReceivedLSN = rec.LSN
	p.LastRecordAt = rec.Timestamp
	p.LastError = ""
	if err := a.Progress.Save(ctx, p); err != nil {
		return err
	}
	if err := a.Registry.Apply(ctx, rec.WALRecord()); err != nil {
		_ = a.Progress.UpdateError(ctx, err)
		return err
	}
	p.AppliedLSN = rec.LSN
	p.LastError = ""
	return a.Progress.Save(ctx, p)
}

func (a *Applier) Replay(ctx context.Context) error {
	p, err := a.Progress.Load(ctx)
	if err != nil {
		return err
	}
	recs, err := a.Log.ScanAfter(ctx, p.AppliedLSN)
	if err != nil {
		return err
	}
	for _, rec := range recs {
		if err := a.ApplyReceived(ctx, p.ClusterID, p.PrimaryNodeID, p.AuthorityEpoch, rec); err != nil {
			return err
		}
	}
	return nil
}
