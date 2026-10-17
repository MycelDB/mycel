package execution

import (
	"container/heap"
	"context"
	"io"
	"sort"
	"time"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

type materializedRows struct {
	unaryOperator
	tracker      *Tracker
	rows         []BindingRow
	offset       int
	retained     int64
	materialized bool
}

func (m *materializedRows) nextBatch(ctx context.Context, operation string) (Batch, error) {
	if err := m.ready(ctx, operation); err != nil {
		return Batch{}, err
	}
	if m.offset >= len(m.rows) {
		return Batch{}, io.EOF
	}
	batchSize := DefaultBatchSize
	if m.tracker != nil {
		batchSize = m.tracker.Limits().BatchSize
	}
	end := m.offset + batchSize
	if end > len(m.rows) {
		end = len(m.rows)
	}
	batch := Batch{Rows: append([]BindingRow(nil), m.rows[m.offset:end]...)}
	m.offset = end
	return batch, nil
}

func (m *materializedRows) closeRows() error {
	if m.tracker != nil {
		m.tracker.ReleaseMemory(m.retained)
	}
	m.retained = 0
	m.rows = nil
	m.offset = 0
	m.materialized = false
	return m.close()
}

func (m *materializedRows) reserveRow(row BindingRow) error {
	bytes := estimateBindingRow(row)
	if m.tracker != nil {
		if err := m.tracker.ReserveMemory(bytes); err != nil {
			return err
		}
	}
	m.retained += bytes
	return nil
}

// SortOperator is a blocking stable sort with an explicit retained-memory
// reservation for every input row.
type SortOperator struct {
	materializedRows
	orders []querylogical.Order
	eval   evaluator
}

func NewSort(input Operator, orders []querylogical.Order, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *SortOperator {
	return &SortOperator{materializedRows: materializedRows{unaryOperator: unaryOperator{input: input}, tracker: tracker}, orders: orders, eval: newEvaluator(parameters, now)}
}

func (o *SortOperator) Open(ctx context.Context) error {
	o.rows = nil
	o.offset = 0
	o.retained = 0
	o.materialized = false
	return o.open(ctx, "open sort")
}

func (o *SortOperator) Next(ctx context.Context) (Batch, error) {
	if !o.materialized {
		if err := o.load(ctx); err != nil {
			_ = o.Close()
			return Batch{}, err
		}
	}
	return o.nextBatch(ctx, "read sorted rows")
}

func (o *SortOperator) load(ctx context.Context) error {
	if err := o.ready(ctx, "sort query rows"); err != nil {
		return err
	}
	for {
		batch, err := o.input.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for _, row := range batch.Rows {
			if err := canceled(ctx, "sort query rows"); err != nil {
				return err
			}
			if err := o.reserveRow(row); err != nil {
				return err
			}
			o.rows = append(o.rows, row)
		}
	}
	if o.tracker != nil {
		if err := o.tracker.ProduceIntermediate(int64(len(o.rows))); err != nil {
			return err
		}
	}
	if err := sortRows(o.rows, o.orders, o.eval); err != nil {
		return err
	}
	o.materialized = true
	return nil
}

func (o *SortOperator) Close() error { return o.closeRows() }

// TopNOperator retains at most N rows while examining the complete input.
// It is used only when planner shaping semantics make pre-projection Top-N safe.
type TopNOperator struct {
	materializedRows
	n      int
	orders []querylogical.Order
	eval   evaluator
}

func NewTopN(input Operator, n int, orders []querylogical.Order, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *TopNOperator {
	return &TopNOperator{materializedRows: materializedRows{unaryOperator: unaryOperator{input: input}, tracker: tracker}, n: n, orders: orders, eval: newEvaluator(parameters, now)}
}

func (o *TopNOperator) Open(ctx context.Context) error {
	if o.n <= 0 {
		return executionInvalid("open Top-N", "Top-N row count must be positive")
	}
	o.rows = nil
	o.offset = 0
	o.retained = 0
	o.materialized = false
	return o.open(ctx, "open Top-N")
}

func (o *TopNOperator) Next(ctx context.Context) (Batch, error) {
	if !o.materialized {
		if err := o.load(ctx); err != nil {
			_ = o.Close()
			return Batch{}, err
		}
	}
	return o.nextBatch(ctx, "read Top-N rows")
}

func (o *TopNOperator) load(ctx context.Context) error {
	if err := o.ready(ctx, "select Top-N rows"); err != nil {
		return err
	}
	retained := &topNHeap{orders: o.orders}
	heap.Init(retained)
	for {
		batch, err := o.input.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for _, row := range batch.Rows {
			if err := canceled(ctx, "select Top-N rows"); err != nil {
				return err
			}
			values, err := rowOrderValues(row, o.orders, o.eval)
			if err != nil {
				return err
			}
			candidate := rankedRow{row: row, values: values, retained: estimateBindingRow(row) + estimateValues(values)}
			if retained.Len() < o.n {
				if o.tracker != nil {
					if err := o.tracker.ReserveMemory(candidate.retained); err != nil {
						return err
					}
				}
				o.retained += candidate.retained
				heap.Push(retained, candidate)
				continue
			}
			if compareRanked(candidate, retained.rows[0], o.orders) >= 0 {
				continue
			}
			dropped := heap.Pop(retained).(rankedRow)
			o.retained -= dropped.retained
			if o.tracker != nil {
				o.tracker.ReleaseMemory(dropped.retained)
				if err := o.tracker.ReserveMemory(candidate.retained); err != nil {
					return err
				}
			}
			o.retained += candidate.retained
			heap.Push(retained, candidate)
		}
	}
	o.rows = make([]BindingRow, 0, retained.Len())
	for retained.Len() > 0 {
		o.rows = append(o.rows, heap.Pop(retained).(rankedRow).row)
	}
	if err := sortRows(o.rows, o.orders, o.eval); err != nil {
		return err
	}
	if o.tracker != nil {
		if err := o.tracker.ProduceIntermediate(int64(len(o.rows))); err != nil {
			return err
		}
	}
	o.materialized = true
	return nil
}

func (o *TopNOperator) Close() error { return o.closeRows() }

type rankedRow struct {
	row      BindingRow
	values   []querylogical.Value
	retained int64
}

type topNHeap struct {
	rows   []rankedRow
	orders []querylogical.Order
}

func (h topNHeap) Len() int { return len(h.rows) }
func (h topNHeap) Less(i int, j int) bool {
	// The worst retained row is the root so a better candidate can replace it.
	return compareRanked(h.rows[i], h.rows[j], h.orders) > 0
}
func (h topNHeap) Swap(i int, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *topNHeap) Push(value any)   { h.rows = append(h.rows, value.(rankedRow)) }
func (h *topNHeap) Pop() any {
	last := len(h.rows) - 1
	value := h.rows[last]
	h.rows[last] = rankedRow{}
	h.rows = h.rows[:last]
	return value
}

func compareRanked(left rankedRow, right rankedRow, orders []querylogical.Order) int {
	return compareOrderValues(left.values, left.row.Sequence, right.values, right.row.Sequence, orders)
}

type aggregateState struct {
	count int64
	sum   float64
	value querylogical.Value
	has   bool
}

type aggregateGroup struct {
	key       string
	row       BindingRow
	projected querylogical.Row
	states    []aggregateState
	retained  int64
}

// AggregateOperator performs deterministic canonical-key grouping and reserves
// memory per retained group/state.
type AggregateOperator struct {
	materializedRows
	projections []querylogical.Projection
	aggregates  []querylogical.Aggregate
	eval        evaluator
}

func NewAggregate(input Operator, projections []querylogical.Projection, aggregates []querylogical.Aggregate, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *AggregateOperator {
	return &AggregateOperator{
		materializedRows: materializedRows{unaryOperator: unaryOperator{input: input}, tracker: tracker},
		projections:      projections, aggregates: aggregates, eval: newEvaluator(parameters, now),
	}
}

func (o *AggregateOperator) Open(ctx context.Context) error {
	o.rows = nil
	o.offset = 0
	o.retained = 0
	o.materialized = false
	return o.open(ctx, "open aggregation")
}

func (o *AggregateOperator) Next(ctx context.Context) (Batch, error) {
	if !o.materialized {
		if err := o.load(ctx); err != nil {
			_ = o.Close()
			return Batch{}, err
		}
	}
	return o.nextBatch(ctx, "read aggregate rows")
}

func (o *AggregateOperator) load(ctx context.Context) error {
	if err := o.ready(ctx, "aggregate query rows"); err != nil {
		return err
	}
	groups := map[string]*aggregateGroup{}
	order := make([]string, 0)
	for {
		batch, err := o.input.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for _, row := range batch.Rows {
			if err := canceled(ctx, "aggregate query rows"); err != nil {
				return err
			}
			projected, err := projectRow(row, o.projections, o.eval)
			if err != nil {
				return err
			}
			key := canonicalRow(projected)
			group := groups[key]
			if group == nil {
				group = &aggregateGroup{key: key, row: row, projected: projected, states: make([]aggregateState, len(o.aggregates))}
				bytes := estimateBindingRow(row) + estimateProjectedRow(projected) + int64(len(key)) + int64(64*len(o.aggregates))
				if o.tracker != nil {
					if err := o.tracker.ReserveMemory(bytes); err != nil {
						return err
					}
				}
				group.retained = bytes
				o.retained += bytes
				groups[key] = group
				order = append(order, key)
			}
			if err := o.accumulate(row, group.states); err != nil {
				return err
			}
		}
	}
	if len(groups) == 0 && len(o.projections) == 0 {
		group := &aggregateGroup{key: "{}", projected: querylogical.Row{Fields: map[string]querylogical.Cell{}}, states: make([]aggregateState, len(o.aggregates))}
		bytes := int64(64*len(o.aggregates) + 64)
		if o.tracker != nil {
			if err := o.tracker.ReserveMemory(bytes); err != nil {
				return err
			}
		}
		group.retained = bytes
		o.retained += bytes
		groups[group.key] = group
		order = append(order, group.key)
	}
	sort.Strings(order)
	for _, key := range order {
		group := groups[key]
		fields := make(map[string]querylogical.Cell, len(group.projected.Fields)+len(o.aggregates))
		for name, cell := range group.projected.Fields {
			fields[name] = cell
		}
		for i, aggregate := range o.aggregates {
			value := finishAggregate(aggregate, group.states[i])
			fields[aggregateOutputName(aggregate)] = scalarCell(value)
		}
		group.row.Projected = querylogical.Row{Fields: fields}
		o.rows = append(o.rows, group.row)
	}
	if o.tracker != nil {
		if err := o.tracker.ProduceIntermediate(int64(len(o.rows))); err != nil {
			return err
		}
	}
	o.materialized = true
	return nil
}

func (o *AggregateOperator) accumulate(row BindingRow, states []aggregateState) error {
	for i, aggregate := range o.aggregates {
		state := &states[i]
		if aggregate.Star {
			state.count++
			continue
		}
		if aggregate.Function == querylogical.AggregateCount && aggregate.Argument != nil && aggregate.Argument.Kind == querylogical.ValueBinding {
			alias := aggregate.Argument.Binding
			_, hasNode := row.Nodes[alias]
			_, hasEdge := row.Edges[alias]
			_, hasPath := row.Paths[alias]
			if hasNode || hasEdge || hasPath {
				state.count++
			}
			continue
		}
		value, err := o.eval.value(row, dereferenceValue(aggregate.Argument))
		if err != nil {
			return err
		}
		if value.Kind == querylogical.ValueNull {
			continue
		}
		switch aggregate.Function {
		case querylogical.AggregateCount:
			state.count++
		case querylogical.AggregateSum, querylogical.AggregateAvg:
			number, ok := finiteNumber(value)
			if !ok {
				return executionInvalid("aggregate query rows", string(aggregate.Function)+" aggregate requires numeric values")
			}
			state.sum += number
			state.count++
		case querylogical.AggregateMin:
			if !state.has || compareValues(value, state.value) < 0 {
				state.value, state.has = value, true
			}
		case querylogical.AggregateMax:
			if !state.has || compareValues(value, state.value) > 0 {
				state.value, state.has = value, true
			}
		default:
			return executionInvalid("aggregate query rows", "unsupported aggregate function "+string(aggregate.Function))
		}
	}
	return nil
}

func finishAggregate(aggregate querylogical.Aggregate, state aggregateState) querylogical.Value {
	switch aggregate.Function {
	case querylogical.AggregateCount:
		return querylogical.NativeValue(state.count)
	case querylogical.AggregateSum:
		return querylogical.Value{Kind: querylogical.ValueNumber, Number: state.sum}
	case querylogical.AggregateAvg:
		if state.count == 0 {
			return querylogical.Value{Kind: querylogical.ValueNull}
		}
		return querylogical.Value{Kind: querylogical.ValueNumber, Number: state.sum / float64(state.count)}
	case querylogical.AggregateMin, querylogical.AggregateMax:
		if !state.has {
			return querylogical.Value{Kind: querylogical.ValueNull}
		}
		return state.value
	default:
		return querylogical.Value{Kind: querylogical.ValueNull}
	}
}

func (o *AggregateOperator) Close() error { return o.closeRows() }
