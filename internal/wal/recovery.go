package wal

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Recovery replays committed WAL records through registered appliers and tracks
// applied progress.
type Recovery struct {
	manager  *Manager
	registry *Registry
	progress AppliedLSNStore
	wait     *ApplyWaiter
}

func NewRecovery(manager *Manager, registry *Registry, progress AppliedLSNStore) *Recovery {
	return &Recovery{manager: manager, registry: registry, progress: progress, wait: NewApplyWaiter()}
}

func (r *Recovery) Waiter() *ApplyWaiter { return r.wait }

func (r *Recovery) Recover(ctx context.Context) (LSN, error) {
	applied, err := r.progress.AppliedLSN(ctx)
	if err != nil {
		return 0, err
	}
	r.wait.SetApplied(applied)
	it, err := r.manager.ReadFrom(ctx, applied.Next())
	if err != nil {
		return 0, err
	}
	defer it.Close()
	for {
		rec, ok, err := it.Next()
		if err != nil {
			return applied, err
		}
		if !ok {
			return applied, nil
		}
		if rec.LSN <= applied {
			continue
		}
		applier, ok := r.registry.Applier(rec.Type)
		if !ok {
			return applied, fmt.Errorf("%w: unknown record type %s at lsn %s", ErrInvalidRecord, rec.Type, rec.LSN)
		}
		if err := applier.ApplyWAL(ctx, rec); err != nil {
			return applied, fmt.Errorf("apply wal record %s (%s): %w", rec.LSN, rec.Type, err)
		}
		if err := r.progress.SetAppliedLSN(ctx, rec.LSN); err != nil {
			return applied, err
		}
		applied = rec.LSN
		r.wait.SetApplied(applied)
	}
}

// ApplyWaiter lets callers wait until WAL records are applied through a target LSN.
type ApplyWaiter struct {
	mu      sync.Mutex
	cond    *sync.Cond
	applied LSN
}

func NewApplyWaiter() *ApplyWaiter {
	w := &ApplyWaiter{}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *ApplyWaiter) AppliedLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.applied
}

func (w *ApplyWaiter) SetApplied(lsn LSN) {
	w.mu.Lock()
	if lsn > w.applied {
		w.applied = lsn
		w.cond.Broadcast()
	}
	w.mu.Unlock()
}

func (w *ApplyWaiter) WaitUntilApplied(ctx context.Context, target LSN) error {
	if target == 0 {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		w.mu.Lock()
		applied := w.applied
		w.mu.Unlock()
		if applied >= target {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
