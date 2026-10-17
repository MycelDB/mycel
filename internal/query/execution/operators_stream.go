package execution

import (
	"context"
	"io"
	"time"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

type unaryOperator struct {
	input    Operator
	opened   bool
	closed   bool
	closeErr error
}

func (u *unaryOperator) open(ctx context.Context, operation string) error {
	if err := canceled(ctx, operation); err != nil {
		return err
	}
	if u.opened && !u.closed {
		return invalidState(operation, "operator is already open")
	}
	if err := u.input.Open(ctx); err != nil {
		_ = u.input.Close()
		return err
	}
	u.opened = true
	u.closed = false
	u.closeErr = nil
	return nil
}

func (u *unaryOperator) ready(ctx context.Context, operation string) error {
	if err := canceled(ctx, operation); err != nil {
		_ = u.close()
		return err
	}
	if !u.opened {
		return invalidState(operation, "operator is not open")
	}
	if u.closed {
		return io.EOF
	}
	return nil
}

func (u *unaryOperator) fail(err error) error {
	if err != nil {
		_ = u.close()
	}
	return err
}

func (u *unaryOperator) close() error {
	if u.closed {
		return u.closeErr
	}
	u.closed = true
	u.closeErr = u.input.Close()
	return u.closeErr
}

// FilterOperator applies a residual predicate to source bindings.
type FilterOperator struct {
	unaryOperator
	predicate *querylogical.Predicate
	eval      evaluator
	tracker   *Tracker
}

func NewFilter(input Operator, predicate *querylogical.Predicate, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *FilterOperator {
	return &FilterOperator{unaryOperator: unaryOperator{input: input}, predicate: predicate, eval: newEvaluator(parameters, now), tracker: tracker}
}

func (o *FilterOperator) Open(ctx context.Context) error {
	return o.open(ctx, "open residual filter")
}

func (o *FilterOperator) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "filter query rows"); err != nil {
		return Batch{}, err
	}
	for {
		batch, err := o.input.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return Batch{}, io.EOF
			}
			return Batch{}, o.fail(err)
		}
		rows := make([]BindingRow, 0, len(batch.Rows))
		for _, row := range batch.Rows {
			if err := canceled(ctx, "filter query rows"); err != nil {
				return Batch{}, o.fail(err)
			}
			matched, err := o.eval.predicate(row, o.predicate)
			if err != nil {
				return Batch{}, o.fail(err)
			}
			if matched {
				rows = append(rows, row)
			}
		}
		if len(rows) == 0 {
			continue
		}
		if o.tracker != nil {
			if err := o.tracker.ProduceIntermediate(int64(len(rows))); err != nil {
				return Batch{}, o.fail(err)
			}
		}
		return Batch{Rows: rows}, nil
	}
}

func (o *FilterOperator) Close() error { return o.close() }

// ProjectionOperator converts graph bindings into neutral result cells.
type ProjectionOperator struct {
	unaryOperator
	projections []querylogical.Projection
	eval        evaluator
	tracker     *Tracker
}

func NewProjection(input Operator, projections []querylogical.Projection, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *ProjectionOperator {
	return &ProjectionOperator{unaryOperator: unaryOperator{input: input}, projections: projections, eval: newEvaluator(parameters, now), tracker: tracker}
}

func (o *ProjectionOperator) Open(ctx context.Context) error {
	return o.open(ctx, "open projection")
}

func (o *ProjectionOperator) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "project query rows"); err != nil {
		return Batch{}, err
	}
	batch, err := o.input.Next(ctx)
	if err != nil {
		if err == io.EOF {
			return Batch{}, io.EOF
		}
		return Batch{}, o.fail(err)
	}
	rows := make([]BindingRow, 0, len(batch.Rows))
	for _, row := range batch.Rows {
		if err := canceled(ctx, "project query rows"); err != nil {
			return Batch{}, o.fail(err)
		}
		projected, err := projectRow(row, o.projections, o.eval)
		if err != nil {
			return Batch{}, o.fail(err)
		}
		row.Projected = projected
		rows = append(rows, row)
	}
	if o.tracker != nil {
		if err := o.tracker.ProduceIntermediate(int64(len(rows))); err != nil {
			return Batch{}, o.fail(err)
		}
	}
	return Batch{Rows: rows}, nil
}

func (o *ProjectionOperator) Close() error { return o.close() }

// OffsetOperator discards a fixed number of input rows without materializing
// the remaining stream.
type OffsetOperator struct {
	unaryOperator
	offset    int64
	remaining int64
}

func NewOffset(input Operator, offset int64) *OffsetOperator {
	return &OffsetOperator{unaryOperator: unaryOperator{input: input}, offset: offset, remaining: offset}
}

func (o *OffsetOperator) Open(ctx context.Context) error {
	if o.offset < 0 {
		return executionInvalid("open offset", "offset must be non-negative")
	}
	o.remaining = o.offset
	return o.open(ctx, "open offset")
}

func (o *OffsetOperator) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "offset query rows"); err != nil {
		return Batch{}, err
	}
	for {
		batch, err := o.input.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return Batch{}, io.EOF
			}
			return Batch{}, o.fail(err)
		}
		if o.remaining >= int64(len(batch.Rows)) {
			o.remaining -= int64(len(batch.Rows))
			continue
		}
		if o.remaining > 0 {
			batch.Rows = batch.Rows[o.remaining:]
			o.remaining = 0
		}
		return batch, nil
	}
}

func (o *OffsetOperator) Close() error { return o.close() }

// LimitOperator terminates its input immediately after the configured number
// of rows has been emitted.
type LimitOperator struct {
	unaryOperator
	limit     int64
	remaining int64
}

func NewLimit(input Operator, limit int64) *LimitOperator {
	return &LimitOperator{unaryOperator: unaryOperator{input: input}, limit: limit, remaining: limit}
}

func (o *LimitOperator) Open(ctx context.Context) error {
	if o.limit < 0 {
		return executionInvalid("open limit", "limit must be non-negative")
	}
	o.remaining = o.limit
	return o.open(ctx, "open limit")
}

func (o *LimitOperator) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "limit query rows"); err != nil {
		return Batch{}, err
	}
	if o.limit > 0 && o.remaining == 0 {
		_ = o.close()
		return Batch{}, io.EOF
	}
	batch, err := o.input.Next(ctx)
	if err != nil {
		if err == io.EOF {
			return Batch{}, io.EOF
		}
		return Batch{}, o.fail(err)
	}
	if o.limit == 0 {
		return batch, nil
	}
	if int64(len(batch.Rows)) > o.remaining {
		batch.Rows = batch.Rows[:o.remaining]
	}
	o.remaining -= int64(len(batch.Rows))
	if o.remaining == 0 {
		_ = o.close()
	}
	return batch, nil
}

func (o *LimitOperator) Close() error { return o.close() }

// DistinctOperator preserves first occurrence order while retaining bounded
// canonical projected-row keys.
type DistinctOperator struct {
	unaryOperator
	tracker *Tracker
	seen    map[string]int64
}

func NewDistinct(input Operator, tracker *Tracker) *DistinctOperator {
	return &DistinctOperator{unaryOperator: unaryOperator{input: input}, tracker: tracker}
}

func (o *DistinctOperator) Open(ctx context.Context) error {
	o.seen = map[string]int64{}
	return o.open(ctx, "open distinct")
}

func (o *DistinctOperator) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "deduplicate query rows"); err != nil {
		return Batch{}, err
	}
	for {
		batch, err := o.input.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return Batch{}, io.EOF
			}
			return Batch{}, o.failDistinct(err)
		}
		rows := make([]BindingRow, 0, len(batch.Rows))
		for _, row := range batch.Rows {
			if err := canceled(ctx, "deduplicate query rows"); err != nil {
				return Batch{}, o.failDistinct(err)
			}
			key := canonicalRow(row.Projected)
			if _, exists := o.seen[key]; exists {
				continue
			}
			bytes := int64(len(key)) + 32
			if o.tracker != nil {
				if err := o.tracker.ReserveMemory(bytes); err != nil {
					return Batch{}, o.failDistinct(err)
				}
			}
			o.seen[key] = bytes
			rows = append(rows, row)
		}
		if len(rows) > 0 {
			return Batch{Rows: rows}, nil
		}
	}
}

func (o *DistinctOperator) failDistinct(err error) error {
	_ = o.Close()
	return err
}

func (o *DistinctOperator) Close() error {
	for _, bytes := range o.seen {
		if o.tracker != nil {
			o.tracker.ReleaseMemory(bytes)
		}
	}
	o.seen = nil
	return o.close()
}
