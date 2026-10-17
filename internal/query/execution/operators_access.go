package execution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

// NodePage is one finite page returned by an indexed graph access method.
type NodePage struct {
	Nodes              []graph.Node
	NextCursor         string
	IndexEntries       int64
	NodesLoaded        int64
	SemanticCandidates int64
	// SourceRetainedBytes accounts for source state retained across page reads.
	// A source reports each allocation once; PagedNodeSource releases it on Close.
	SourceRetainedBytes int64
}

// NodePageReader reads at most limit nodes beginning at cursor.
type NodePageReader func(context.Context, int, string) (NodePage, error)

// PageCheckpointEncoder binds a resumable source position to the query plan.
type PageCheckpointEncoder func(pageCursor string, offset int) string

type bufferedNodeRow struct {
	row   BindingRow
	bytes int64
}

// PagedNodeSource adapts a cursor-based graph access method to bounded pulls.
// A checkpoint identifies a row before it is emitted, allowing the lookahead
// row retained by response pagination to become the next page's first row.
type PagedNodeSource struct {
	alias             string
	reader            NodePageReader
	checkpointEncoder PageCheckpointEncoder
	resumeCursor      string
	resumeOffset      int
	batchSize         int
	tracker           *Tracker

	pageCursor     string
	nextCursor     string
	buffer         []bufferedNodeRow
	offset         int
	sequence       int64
	retained       int64
	sourceRetained int64
	opened         bool
	closed         bool
	exhausted      bool
}

func NewPagedNodeSource(alias string, reader NodePageReader, checkpointEncoder PageCheckpointEncoder, resumeCursor string, resumeOffset int, batchSize int, tracker *Tracker) *PagedNodeSource {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &PagedNodeSource{
		alias: alias, reader: reader, checkpointEncoder: checkpointEncoder,
		resumeCursor: resumeCursor, resumeOffset: resumeOffset,
		batchSize: batchSize, tracker: tracker,
	}
}

func (s *PagedNodeSource) Open(ctx context.Context) error {
	if err := canceled(ctx, "open indexed node source"); err != nil {
		return err
	}
	if s.opened && !s.closed {
		return invalidState("open indexed node source", "operator is already open")
	}
	if s.reader == nil {
		return invalidState("open indexed node source", "node page reader is required")
	}
	if s.resumeOffset < 0 || s.resumeOffset >= s.batchSize {
		return executionInvalid("open indexed node source", "source cursor offset is outside the bounded page")
	}
	s.releaseBuffer()
	s.releaseSourceMemory()
	s.pageCursor = s.resumeCursor
	s.nextCursor = ""
	s.buffer = nil
	s.offset = 0
	s.sequence = 0
	s.opened = true
	s.closed = false
	s.exhausted = false
	return nil
}

func (s *PagedNodeSource) Next(ctx context.Context) (Batch, error) {
	if err := s.ready(ctx); err != nil {
		return Batch{}, err
	}
	rows := make([]BindingRow, 0, s.batchSize)
	for len(rows) < s.batchSize {
		if s.offset >= len(s.buffer) {
			if s.exhausted {
				if len(rows) > 0 {
					return Batch{Rows: rows}, nil
				}
				return Batch{}, io.EOF
			}
			if err := s.fetch(ctx); err != nil {
				_ = s.Close()
				return Batch{}, err
			}
			if len(s.buffer) == 0 {
				if len(rows) > 0 {
					return Batch{Rows: rows}, nil
				}
				return Batch{}, io.EOF
			}
		}
		item := &s.buffer[s.offset]
		item.row.Sequence = s.sequence
		s.sequence++
		if s.checkpointEncoder != nil {
			item.row.Cursor = s.checkpointEncoder(s.pageCursor, s.offset)
		}
		rows = append(rows, item.row)
		if s.tracker != nil {
			if err := s.tracker.ExamineCandidates(1); err != nil {
				_ = s.Close()
				return Batch{}, err
			}
			s.tracker.ReleaseMemory(item.bytes)
		}
		s.retained -= item.bytes
		item.bytes = 0
		s.offset++
	}
	return Batch{Rows: rows}, nil
}

func (s *PagedNodeSource) fetch(ctx context.Context) error {
	if err := canceled(ctx, "scan indexed nodes"); err != nil {
		return err
	}
	cursor := s.pageCursor
	if s.buffer != nil {
		cursor = s.nextCursor
	}
	page, err := s.reader(ctx, s.batchSize, cursor)
	if err != nil {
		var queryErr *querylogical.QueryError
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return querylogical.NewError(querylogical.ErrorCanceled, "scan indexed nodes", "query context canceled", err)
		case errors.As(err, &queryErr):
			return err
		default:
			return querylogical.NewError(querylogical.ErrorDependencyFailure, "scan indexed nodes", err.Error(), err)
		}
	}
	if len(page.Nodes) > s.batchSize {
		return invalidState("scan indexed nodes", "access method returned an oversized page")
	}
	if page.SourceRetainedBytes < 0 {
		return invalidState("scan indexed nodes", "access method reported negative retained memory")
	}
	if s.tracker != nil {
		if err := s.tracker.ReserveMemory(page.SourceRetainedBytes); err != nil {
			return err
		}
		s.sourceRetained += page.SourceRetainedBytes
		if err := s.tracker.ScanIndexEntries(page.IndexEntries); err != nil {
			return err
		}
		if err := s.tracker.LoadNodes(page.NodesLoaded); err != nil {
			return err
		}
		if err := s.tracker.UseSemanticCandidates(page.SemanticCandidates); err != nil {
			return err
		}
	}
	if len(page.Nodes) == 0 && page.NextCursor != "" {
		return querylogical.NewError(querylogical.ErrorDependencyFailure, "scan indexed nodes", "access method returned an empty non-terminal page", nil)
	}
	s.releaseBuffer()
	s.pageCursor = cursor
	s.nextCursor = page.NextCursor
	s.offset = 0
	s.buffer = make([]bufferedNodeRow, 0, len(page.Nodes))
	for _, node := range page.Nodes {
		row := BindingRow{Nodes: map[string]graph.Node{s.alias: node}}
		bytes := estimateBindingRow(row)
		if s.tracker != nil {
			if err := s.tracker.ReserveMemory(bytes); err != nil {
				s.releaseBuffer()
				return err
			}
		}
		s.retained += bytes
		s.buffer = append(s.buffer, bufferedNodeRow{row: row, bytes: bytes})
	}
	if s.buffer == nil || len(s.buffer) == 0 || page.NextCursor == "" {
		s.exhausted = page.NextCursor == ""
	}
	if s.resumeOffset > 0 {
		if s.resumeOffset >= len(s.buffer) {
			s.releaseBuffer()
			return querylogical.NewError(querylogical.ErrorStaleCursor, "resume indexed node source", "source cursor offset exceeds the indexed page", nil)
		}
		for i := 0; i < s.resumeOffset; i++ {
			if s.tracker != nil {
				s.tracker.ReleaseMemory(s.buffer[i].bytes)
			}
			s.retained -= s.buffer[i].bytes
			s.buffer[i].bytes = 0
		}
		s.offset = s.resumeOffset
		s.resumeOffset = 0
	}
	return nil
}

func (s *PagedNodeSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.releaseBuffer()
	s.releaseSourceMemory()
	return nil
}

func (s *PagedNodeSource) ready(ctx context.Context) error {
	if err := canceled(ctx, "read indexed node source"); err != nil {
		_ = s.Close()
		return err
	}
	if !s.opened {
		return invalidState("read indexed node source", "operator is not open")
	}
	if s.closed {
		return io.EOF
	}
	return nil
}

func (s *PagedNodeSource) releaseBuffer() {
	if s.tracker != nil && s.retained > 0 {
		s.tracker.ReleaseMemory(s.retained)
	}
	s.retained = 0
	s.buffer = nil
	s.offset = 0
}

func (s *PagedNodeSource) releaseSourceMemory() {
	if s.tracker != nil && s.sourceRetained > 0 {
		s.tracker.ReleaseMemory(s.sourceRetained)
	}
	s.sourceRetained = 0
}

// NodePatternFilter applies start-node labels, IDs, and literal properties.
type NodePatternFilter struct {
	unaryOperator
	pattern querylogical.NodePattern
	eval    evaluator
	tracker *Tracker
}

func NewNodePatternFilter(input Operator, pattern querylogical.NodePattern, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *NodePatternFilter {
	return &NodePatternFilter{unaryOperator: unaryOperator{input: input}, pattern: pattern, eval: newEvaluator(parameters, now), tracker: tracker}
}

func (o *NodePatternFilter) Open(ctx context.Context) error {
	return o.open(ctx, "open node pattern filter")
}

func (o *NodePatternFilter) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "filter start nodes"); err != nil {
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
			matched, err := o.matches(row)
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

func (o *NodePatternFilter) matches(row BindingRow) (bool, error) {
	node, ok := row.Nodes[o.pattern.Alias]
	if !ok {
		return false, executionInvalid("filter start nodes", fmt.Sprintf("alias %q is not bound", o.pattern.Alias))
	}
	if len(o.pattern.NodeIDs) > 0 && !containsString(o.pattern.NodeIDs, node.ID.String()) {
		return false, nil
	}
	for _, label := range o.pattern.Labels {
		if !containsString(node.Labels, label) {
			return false, nil
		}
	}
	for name, expected := range o.pattern.Properties {
		actual, exists := graph.Property(node, name)
		if !exists {
			return false, nil
		}
		resolved, err := o.eval.value(row, expected)
		if err != nil {
			return false, err
		}
		if compareValues(querylogical.NativeValue(actual), resolved) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (o *NodePatternFilter) Close() error { return o.close() }

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// NodeSetMode selects union or intersection over child node sources.
type NodeSetMode string

const (
	NodeSetUnion        NodeSetMode = "union"
	NodeSetIntersection NodeSetMode = "intersection"
)

// NodeSetOperator materializes bounded child sets, deduplicates by node ID,
// and emits a deterministic ID order.
type NodeSetOperator struct {
	children  []Operator
	mode      NodeSetMode
	batchSize int
	tracker   *Tracker

	rows     []BindingRow
	sizes    []int64
	offset   int
	retained int64
	opened   bool
	closed   bool
}

func NewNodeSet(children []Operator, mode NodeSetMode, batchSize int, tracker *Tracker) *NodeSetOperator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &NodeSetOperator{children: append([]Operator(nil), children...), mode: mode, batchSize: batchSize, tracker: tracker}
}

func (o *NodeSetOperator) Open(ctx context.Context) error {
	if err := canceled(ctx, "open indexed node set"); err != nil {
		return err
	}
	if o.opened && !o.closed {
		return invalidState("open indexed node set", "operator is already open")
	}
	if len(o.children) == 0 {
		return executionInvalid("open indexed node set", "at least one child source is required")
	}
	if o.mode != NodeSetUnion && o.mode != NodeSetIntersection {
		return executionInvalid("open indexed node set", "unsupported set mode "+string(o.mode))
	}
	o.releaseRows()
	for index, child := range o.children {
		if child == nil {
			o.closeChildren(index)
			return invalidState("open indexed node set", "child source is required")
		}
		if err := child.Open(ctx); err != nil {
			o.closeChildren(index + 1)
			return err
		}
	}
	o.offset = 0
	o.opened = true
	o.closed = false
	return nil
}

func (o *NodeSetOperator) Next(ctx context.Context) (Batch, error) {
	if err := canceled(ctx, "read indexed node set"); err != nil {
		_ = o.Close()
		return Batch{}, err
	}
	if !o.opened {
		return Batch{}, invalidState("read indexed node set", "operator is not open")
	}
	if o.closed {
		return Batch{}, io.EOF
	}
	if o.rows == nil {
		if err := o.materialize(ctx); err != nil {
			_ = o.Close()
			return Batch{}, err
		}
	}
	if o.offset >= len(o.rows) {
		return Batch{}, io.EOF
	}
	end := o.offset + o.batchSize
	if end > len(o.rows) {
		end = len(o.rows)
	}
	rows := append([]BindingRow(nil), o.rows[o.offset:end]...)
	for index := o.offset; index < end; index++ {
		if o.tracker != nil {
			o.tracker.ReleaseMemory(o.sizes[index])
		}
		o.retained -= o.sizes[index]
		o.sizes[index] = 0
	}
	o.offset = end
	return Batch{Rows: rows}, nil
}

func (o *NodeSetOperator) materialize(ctx context.Context) error {
	var retained map[string]BindingRow
	retainedSizes := map[string]int64{}
	materialized := false
	defer func() {
		if !materialized {
			releaseSizeMap(o.tracker, retainedSizes)
		}
	}()
	for childIndex, child := range o.children {
		current := map[string]BindingRow{}
		currentSizes := map[string]int64{}
		for {
			batch, err := child.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				releaseSizeMap(o.tracker, currentSizes)
				return err
			}
			for _, row := range batch.Rows {
				id, err := bindingNodeID(row)
				if err != nil {
					releaseSizeMap(o.tracker, currentSizes)
					return err
				}
				if _, exists := current[id]; exists {
					continue
				}
				row.Cursor = ""
				bytes := estimateBindingRow(row) + int64(len(id))
				if o.tracker != nil {
					if err := o.tracker.ReserveMemory(bytes); err != nil {
						releaseSizeMap(o.tracker, currentSizes)
						return err
					}
				}
				current[id] = row
				currentSizes[id] = bytes
			}
		}
		if childIndex == 0 {
			retained, retainedSizes = current, currentSizes
			continue
		}
		if o.mode == NodeSetUnion {
			for id, row := range current {
				if _, exists := retained[id]; exists {
					if o.tracker != nil {
						o.tracker.ReleaseMemory(currentSizes[id])
					}
					continue
				}
				retained[id] = row
				retainedSizes[id] = currentSizes[id]
			}
			continue
		}
		for id := range retained {
			if _, ok := current[id]; !ok {
				if o.tracker != nil {
					o.tracker.ReleaseMemory(retainedSizes[id])
				}
				delete(retained, id)
				delete(retainedSizes, id)
			}
		}
		releaseSizeMap(o.tracker, currentSizes)
	}
	ids := make([]string, 0, len(retained))
	for id := range retained {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	o.rows = make([]BindingRow, 0, len(ids))
	o.sizes = make([]int64, 0, len(ids))
	for _, id := range ids {
		o.rows = append(o.rows, retained[id])
		o.sizes = append(o.sizes, retainedSizes[id])
		o.retained += retainedSizes[id]
	}
	materialized = true
	if o.tracker != nil {
		if err := o.tracker.ProduceIntermediate(int64(len(o.rows))); err != nil {
			return err
		}
	}
	return nil
}

func releaseSizeMap(tracker *Tracker, sizes map[string]int64) {
	if tracker == nil {
		return
	}
	for _, bytes := range sizes {
		tracker.ReleaseMemory(bytes)
	}
}

func bindingNodeID(row BindingRow) (string, error) {
	if len(row.Nodes) != 1 {
		return "", executionInvalid("combine indexed node sets", "child row must contain exactly one node binding")
	}
	for _, node := range row.Nodes {
		return node.ID.String(), nil
	}
	return "", executionInvalid("combine indexed node sets", "child row has no node binding")
}

func (o *NodeSetOperator) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	o.releaseRows()
	var first error
	for _, child := range o.children {
		if child != nil {
			if err := child.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func (o *NodeSetOperator) closeChildren(limit int) {
	if limit > len(o.children) {
		limit = len(o.children)
	}
	for index := 0; index < limit; index++ {
		if o.children[index] != nil {
			_ = o.children[index].Close()
		}
	}
}

func (o *NodeSetOperator) releaseRows() {
	if o.tracker != nil && o.retained > 0 {
		o.tracker.ReleaseMemory(o.retained)
	}
	o.retained = 0
	o.rows = nil
	o.sizes = nil
	o.offset = 0
}

// StaticValue resolves a bound literal/parameter/date for a storage boundary.
func StaticValue(value querylogical.Value, parameters map[string]querylogical.Value, now time.Time) (any, error) {
	resolved, err := newEvaluator(parameters, now).value(BindingRow{}, value)
	if err != nil {
		return nil, err
	}
	return nativeStaticValue(resolved), nil
}

func nativeStaticValue(value querylogical.Value) any {
	switch value.Kind {
	case querylogical.ValueNull:
		return nil
	case querylogical.ValueBool:
		return value.Bool
	case querylogical.ValueNumber:
		return value.Number
	case querylogical.ValueInteger:
		return value.Integer
	case querylogical.ValueUnsigned:
		return value.Unsigned
	case querylogical.ValueString, querylogical.ValueDate, querylogical.ValueUnknown:
		return value.String
	case querylogical.ValueList:
		out := make([]any, 0, len(value.List))
		for _, item := range value.List {
			out = append(out, nativeStaticValue(item))
		}
		return out
	case querylogical.ValueObject:
		out := make(map[string]any, len(value.Object))
		for name, item := range value.Object {
			out[name] = nativeStaticValue(item)
		}
		return out
	default:
		return strings.TrimSpace(value.String)
	}
}
