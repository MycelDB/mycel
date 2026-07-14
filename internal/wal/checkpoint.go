package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/filestore"
)

type Checkpoint struct {
	LSN       LSN       `json:"lsn"`
	CreatedAt time.Time `json:"created_at"`
}

type CheckpointStore struct{ path string }

func NewCheckpointStore(path string) *CheckpointStore { return &CheckpointStore{path: path} }

func (s *CheckpointStore) Load(ctx context.Context) (Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	raw, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return Checkpoint{}, nil
	}
	if err != nil {
		return Checkpoint{}, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(raw, &cp); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

func (s *CheckpointStore) Save(ctx context.Context, cp Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	return filestore.WriteFileAtomic(s.path, append(raw, '\n'), 0o600)
}

func CreateCheckpoint(ctx context.Context, progress AppliedLSNStore, store *CheckpointStore, target LSN) (Checkpoint, error) {
	if progress == nil || store == nil {
		return Checkpoint{}, fmt.Errorf("%w: progress and checkpoint store are required", ErrInvalidRecord)
	}
	applied, err := progress.AppliedLSN(ctx)
	if err != nil {
		return Checkpoint{}, err
	}
	if target == 0 || target > applied {
		target = applied
	}
	cp := Checkpoint{LSN: target, CreatedAt: time.Now().UTC()}
	if err := store.Save(ctx, cp); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

func (m *Manager) OldestRetainedLSN() (LSN, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.oldestRetainedLSNLocked()
}

func (m *Manager) oldestRetainedLSNLocked() (LSN, error) {
	segs, err := listSegments(m.dir)
	if err != nil {
		return 0, err
	}
	if len(segs) == 0 {
		return 0, nil
	}
	return segs[0].start, nil
}

func (m *Manager) RetainFrom(ctx context.Context, from LSN) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if from == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	segs, err := listSegments(m.dir)
	if err != nil {
		return err
	}
	for i, seg := range segs {
		if i == len(segs)-1 {
			break
		}
		nextStart := segs[i+1].start
		if nextStart <= from {
			if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}
