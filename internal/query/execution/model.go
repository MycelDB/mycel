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
	Nodes      map[string]graph.Node
	Edges      map[string]graph.Edge
	Paths      map[string]querylogical.Path
	Trees      map[string]querylogical.Tree
	GraphNodes []graph.Node
	GraphEdges []graph.Edge
	Projected  querylogical.Row
	// Cursor resumes the access source at this row. Streaming operators retain
	// it so response pagination can continue from its lookahead row.
	Cursor   string
	Sequence int64
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

// SliceSource exposes an already-materialized bounded row slice through the
// common pull-operator lifecycle.
type SliceSource struct {
	rows      []BindingRow
	batchSize int
	tracker   *Tracker
	offset    int
	opened    bool
	closed    bool
}

func NewSliceSource(rows []BindingRow, batchSize int, tracker *Tracker) *SliceSource {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &SliceSource{rows: rows, batchSize: batchSize, tracker: tracker}
}

func (s *SliceSource) Open(ctx context.Context) error {
	if err := canceled(ctx, "open slice source"); err != nil {
		return err
	}
	if s.opened && !s.closed {
		return invalidState("open slice source", "operator is already open")
	}
	s.offset = 0
	s.opened = true
	s.closed = false
	return nil
}

func (s *SliceSource) Next(ctx context.Context) (Batch, error) {
	if err := s.ready(ctx, "read slice source"); err != nil {
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

func (s *SliceSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

func (s *SliceSource) ready(ctx context.Context, operation string) error {
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
