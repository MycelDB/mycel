package replication

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/replerror"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

type Streamer interface {
	StreamWal(ctx context.Context, addr string, req *clusterpb.StreamWalRequest, handle func(*clusterpb.WalRecord) error) error
}

type Follower struct {
	Manager   *clustering.Manager
	Streamer  Streamer
	Applier   *Applier
	Progress  *ProgressStore
	Interval  time.Duration
	Logger    *slog.Logger
	mu        sync.Mutex
	connected bool
	lastError string
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func (f *Follower) Start(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.wg.Add(1)
	go func() { defer f.wg.Done(); f.loop(runCtx) }()
	return nil
}
func (f *Follower) Stop(ctx context.Context) error {
	f.mu.Lock()
	cancel := f.cancel
	f.cancel = nil
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() { f.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (f *Follower) Connected() bool   { f.mu.Lock(); defer f.mu.Unlock(); return f.connected }
func (f *Follower) LastError() string { f.mu.Lock(); defer f.mu.Unlock(); return f.lastError }

func (f *Follower) loop(ctx context.Context) {
	interval := f.Interval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		_ = f.runOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *Follower) runOnce(ctx context.Context) error {
	if f.Manager == nil || f.Streamer == nil || f.Applier == nil || f.Progress == nil {
		return nil
	}
	if f.Manager.State() != model.NodeStateClustered || !f.Manager.IsAdmitted() || f.Manager.LocalRole() != clustering.NodeRoleFollower {
		return nil
	}
	authority, ok := f.Manager.Authority()
	if !ok {
		return f.setError(ctx, fmt.Errorf("cluster authority is unknown"))
	}
	addr := authority.Primary.BackendAdvertiseAddr
	if addr == "" && f.Manager.Topology() != nil {
		for _, peer := range f.Manager.Topology().List() {
			if peer.NodeID == authority.Primary.NodeID {
				addr = peer.BackendAdvertiseAddr
				break
			}
		}
	}
	if addr == "" {
		return f.setError(ctx, fmt.Errorf("primary endpoint is unknown"))
	}
	p, err := f.Progress.Load(ctx)
	if err != nil {
		return f.setError(ctx, err)
	}
	req := &clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: f.Manager.Identity().ClusterID, FollowerNodeId: f.Manager.Identity().NodeID, AfterLsn: uint64(p.AppliedLSN), AuthorityEpoch: authority.AuthorityEpoch}
	f.setConnected(true)
	_ = f.Progress.UpdateCatchupState(ctx, CatchupStateStreaming, nil)
	defer f.setConnected(false)
	err = f.Streamer.StreamWal(ctx, addr, req, func(pb *clusterpb.WalRecord) error {
		rec, err := RecordFromProto(pb)
		if err != nil {
			return err
		}
		return f.Applier.ApplyReceived(ctx, authority.ClusterID, authority.Primary.NodeID, authority.AuthorityEpoch, rec)
	})
	if err != nil && err != io.EOF {
		if info, ok := replerror.SnapshotRequiredInfoFromError(err); ok {
			_ = f.Progress.UpdateSnapshotRequired(ctx, info)
			return f.setError(ctx, err)
		}
		return f.setError(ctx, err)
	}
	_ = f.Progress.UpdateCatchupState(ctx, CatchupStateCaughtUp, nil)
	_ = f.Progress.UpdateError(ctx, nil)
	f.setLastError("")
	return nil
}
func (f *Follower) setConnected(v bool)   { f.mu.Lock(); f.connected = v; f.mu.Unlock() }
func (f *Follower) setLastError(s string) { f.mu.Lock(); f.lastError = s; f.mu.Unlock() }
func (f *Follower) setError(ctx context.Context, err error) error {
	f.setLastError(err.Error())
	_ = f.Progress.UpdateError(ctx, err)
	return err
}
