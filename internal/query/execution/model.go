// Package execution implements the daemon-independent bounded query operator
// runtime. Operators exchange neutral binding rows and never transport types.
package execution

import (
	"context"
	"io"

	graph "github.com/myceldb/mycel/internal/graph/model"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

// DefaultBatchSize bounds the number of rows exchanged by one pull.
const DefaultBatchSize = 128

// BindingRow contains aliases produced by an access operator. Projected is
// populated by projection or aggregation operators; the source bindings remain
// available for ordering and graph-result assembly.
type BindingRow struct {
	Nodes     map[string]graph.Node
	Edges     map[string]graph.Edge
	Paths     map[string]querylogical.Path
	Trees     map[string]querylogical.Tree
	Projected querylogical.Row
	Sequence  int64
}

// Batch is one bounded transfer between operators. An empty batch with a nil
// error is not end-of-stream; io.EOF is the only completion signal.
type Batch struct {
	Rows []BindingRow
}

// Operator is the pull-based lifecycle shared by every execution stage.
type Operator interface {
	Open(context.Context) error
	Next(context.Context) (Batch, error)
	Close() error
}

// LegacySource adapts already-materialized bindings from the temporary legacy
// access path. It permits neutral shaping to migrate independently of storage
// access selection.
type LegacySource struct {
	rows      []BindingRow
	batchSize int
	tracker   *Tracker
	offset    int
	opened    bool
	closed    bool
}

func NewLegacySource(rows []BindingRow, batchSize int, tracker *Tracker) *LegacySource {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &LegacySource{rows: rows, batchSize: batchSize, tracker: tracker}
}

func (s *LegacySource) Open(ctx context.Context) error {
	if err := canceled(ctx, "open legacy source"); err != nil {
		return err
	}
	if s.opened && !s.closed {
		return invalidState("open legacy source", "operator is already open")
	}
	s.offset = 0
	s.opened = true
	s.closed = false
	return nil
}

func (s *LegacySource) Next(ctx context.Context) (Batch, error) {
	if err := s.ready(ctx, "read legacy source"); err != nil {
		return Batch{}, err
	}
	if s.offset >= len(s.rows) {
		return Batch{}, io.EOF
	}
	end := s.offset + s.batchSize
	if end > len(s.rows) {
		end = len(s.rows)
	}
	start := s.offset
	rows := append([]BindingRow(nil), s.rows[start:end]...)
	for i := range rows {
		rows[i].Sequence = int64(start + i)
	}
	s.offset = end
	if s.tracker != nil {
		if err := s.tracker.ExamineCandidates(int64(len(rows))); err != nil {
			_ = s.Close()
			return Batch{}, err
		}
	}
	return Batch{Rows: rows}, nil
}

func (s *LegacySource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

func (s *LegacySource) ready(ctx context.Context, operation string) error {
	if err := canceled(ctx, operation); err != nil {
		_ = s.Close()
		return err
	}
	if !s.opened {
		return invalidState(operation, "operator is not open")
	}
	if s.closed {
		return io.EOF
	}
	return nil
}
