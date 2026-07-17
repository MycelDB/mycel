package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/clustering/replerror"
	"github.com/myceldb/mycel/internal/wal"
)

const ProgressVersion = 1

type CatchupState string

const (
	CatchupStateUnknown          CatchupState = "unknown"
	CatchupStateStreaming        CatchupState = "streaming"
	CatchupStateCaughtUp         CatchupState = "caught_up"
	CatchupStateRetrying         CatchupState = "retrying"
	CatchupStateSnapshotRequired CatchupState = "snapshot_required"
	CatchupStateError            CatchupState = "error"
)

type Progress struct {
	Version          int                             `json:"version"`
	ClusterID        string                          `json:"cluster_id"`
	PrimaryNodeID    string                          `json:"primary_node_id"`
	AuthorityEpoch   int64                           `json:"authority_epoch"`
	ReceivedLSN      wal.LSN                         `json:"received_lsn"`
	AppliedLSN       wal.LSN                         `json:"applied_lsn"`
	LastRecordAt     time.Time                       `json:"last_record_at,omitempty"`
	LastError        string                          `json:"last_error,omitempty"`
	CatchupState     CatchupState                    `json:"catchup_state,omitempty"`
	SnapshotRequired *replerror.SnapshotRequiredInfo `json:"snapshot_required,omitempty"`
	UpdatedAt        time.Time                       `json:"updated_at"`
}

type ProgressStore struct{ path string }

func NewProgressStore(path string) *ProgressStore { return &ProgressStore{path: path} }

func (s *ProgressStore) Load(ctx context.Context) (Progress, error) {
	if err := ctx.Err(); err != nil {
		return Progress{}, err
	}
	raw, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return Progress{Version: ProgressVersion}, nil
	}
	if err != nil {
		return Progress{}, err
	}
	var p Progress
	if err := json.Unmarshal(raw, &p); err != nil {
		return Progress{}, err
	}
	if p.Version == 0 {
		p.Version = ProgressVersion
	}
	return p, nil
}
func (s *ProgressStore) Save(ctx context.Context, p Progress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.AppliedLSN > p.ReceivedLSN {
		return fmt.Errorf("applied_lsn %s exceeds received_lsn %s", p.AppliedLSN, p.ReceivedLSN)
	}
	if p.Version == 0 {
		p.Version = ProgressVersion
	}
	p.UpdatedAt = time.Now().UTC()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
func (s *ProgressStore) UpdateError(ctx context.Context, err error) error {
	p, loadErr := s.Load(ctx)
	if loadErr != nil {
		return loadErr
	}
	if err != nil {
		p.LastError = err.Error()
		p.CatchupState = CatchupStateRetrying
	} else {
		p.LastError = ""
	}
	return s.Save(ctx, p)
}

func (s *ProgressStore) UpdateCatchupState(ctx context.Context, state CatchupState, err error) error {
	p, loadErr := s.Load(ctx)
	if loadErr != nil {
		return loadErr
	}
	p.CatchupState = state
	if err != nil {
		p.LastError = err.Error()
	} else {
		p.LastError = ""
	}
	return s.Save(ctx, p)
}

func (s *ProgressStore) UpdateSnapshotRequired(ctx context.Context, info replerror.SnapshotRequiredInfo) error {
	p, err := s.Load(ctx)
	if err != nil {
		return err
	}
	p.CatchupState = CatchupStateSnapshotRequired
	p.SnapshotRequired = &info
	p.LastError = "follower requires snapshot catch-up"
	return s.Save(ctx, p)
}
