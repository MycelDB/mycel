package execution

import (
	"context"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

// ShapeOptions are execution-only inputs for the neutral shaping pipeline.
type ShapeOptions struct {
	Parameters map[string]querylogical.Value
	Now        time.Time
}

// Matches owns retained binding rows produced by a mutation's read phase.
// Release must be called after the command finishes consuming the bindings.
type Matches struct {
	Rows        []BindingRow
	Diagnostics querylogical.Diagnostics
	retained    int64
	tracker     *Tracker
}

// Release drops the retained-memory charge for matched bindings.
func (m *Matches) Release() {
	if m == nil || m.tracker == nil || m.retained == 0 {
		return
	}
	m.tracker.ReleaseMemory(m.retained)
	m.retained = 0
}

// CollectMatches runs the common residual-filter and mutation-limit operators
// without projection or response pagination. Storage access remains supplied by
// the engine-selected source.
func CollectMatches(ctx context.Context, source Operator, query querylogical.Query, tracker *Tracker, options ShapeOptions) (Matches, error) {
	if source == nil {
		return Matches{}, executionInvalid("match mutation rows", "binding source is required")
	}
	if tracker == nil {
		return Matches{}, invalidState("match mutation rows", "resource tracker is required")
	}
	var root Operator = source
	if query.Predicate != nil {
		root = NewFilter(root, query.Predicate, options.Parameters, options.Now, tracker)
	}
	if query.Page.Offset > 0 {
		root = NewOffset(root, query.Page.Offset)
	}
	if query.Page.Limit > 0 {
		root = NewLimit(root, query.Page.Limit)
	}
	started := time.Now()
	rows, retained, err := drain(ctx, root, tracker, false)
	if err != nil {
		return Matches{}, err
	}
	return Matches{Rows: rows, Diagnostics: runtimeDiagnostics(tracker.Snapshot(), time.Since(started)), retained: retained, tracker: tracker}, nil
}

// EstimateBindingRow reports the retained-memory estimate used by operators.
func EstimateBindingRow(row BindingRow) int64 { return estimateBindingRow(row) }

// ValidatePage rejects pagination state before callers perform side effects.
func ValidatePage(page querylogical.PageSpec) error {
	if page.Offset < 0 || page.Limit < 0 || page.Size < 0 {
		return executionInvalid("paginate query results", "pagination values must be non-negative")
	}
	_, _, err := paginationBounds(page.Size, page.Cursor)
	return err
}

// Shape runs the non-storage operators over a supplied binding source.
func Shape(ctx context.Context, source Operator, query querylogical.Query, tracker *Tracker, options ShapeOptions) (querylogical.Result, error) {
	if source == nil {
		return querylogical.Result{}, executionInvalid("execute query", "binding source is required")
	}
	if tracker == nil {
		return querylogical.Result{}, invalidState("execute query", "resource tracker is required")
	}
	if err := ValidatePage(query.Page); err != nil {
		return querylogical.Result{}, err
	}
	if err := validateAggregates(query.Aggregates); err != nil {
		return querylogical.Result{}, err
	}
	root, err := buildPipeline(source, query, tracker, options)
	if err != nil {
		_ = source.Close()
		return querylogical.Result{}, err
	}
	started := time.Now()
	rows, outputMemory, err := drain(ctx, root, tracker, true)
	if err != nil {
		return querylogical.Result{}, err
	}
	pageRows, nextCursor, err := paginateRows(rows, query.Page.Size, query.Page.Cursor)
	if err != nil {
		tracker.ReleaseMemory(outputMemory)
		return querylogical.Result{}, err
	}
	resultRows := make([]querylogical.Row, 0, len(pageRows))
	for _, row := range pageRows {
		resultRows = append(resultRows, row.Projected)
	}
	if tracker.Snapshot().Truncated {
		nextCursor = ""
	}
	result := querylogical.Result{
		Rows:       resultRows,
		Graph:      resultGraph(pageRows),
		Counters:   querylogical.Counters{RowsReturned: int64(len(resultRows))},
		NextCursor: nextCursor,
	}
	tracker.ReleaseMemory(outputMemory)
	result.Diagnostics = runtimeDiagnostics(tracker.Snapshot(), time.Since(started))
	result.Diagnostics.RowsReturned = int64(len(resultRows))
	return result, nil
}

func paginateRows(rows []BindingRow, requested int32, cursor string) ([]BindingRow, string, error) {
	start, pageSize, err := paginationBounds(requested, cursor)
	if err != nil {
		return nil, "", err
	}
	if len(rows) <= pageSize {
		return rows, "", nil
	}
	next := rows[pageSize].Cursor
	if next == "" {
		next = strconv.Itoa(start + pageSize)
	}
	return rows[:pageSize], next, nil
}

func validateAggregates(aggregates []querylogical.Aggregate) error {
	for _, aggregate := range aggregates {
		switch aggregate.Function {
		case querylogical.AggregateCount:
			if aggregate.Star && aggregate.Argument != nil {
				return executionInvalid("aggregate query rows", "count aggregate cannot contain both star and an argument")
			}
			if !aggregate.Star && aggregate.Argument == nil {
				return executionInvalid("aggregate query rows", "count aggregate requires an argument")
			}
		case querylogical.AggregateSum, querylogical.AggregateAvg, querylogical.AggregateMin, querylogical.AggregateMax:
			if aggregate.Star || aggregate.Argument == nil || aggregate.Argument.Kind == querylogical.ValueBinding {
				return executionInvalid("aggregate query rows", string(aggregate.Function)+" aggregate requires a value argument")
			}
		default:
			return executionInvalid("aggregate query rows", "unsupported aggregate function "+string(aggregate.Function))
		}
	}
	return nil
}

func paginationBounds(requested int32, cursor string) (int, int, error) {
	start := 0
	if strings.TrimSpace(cursor) != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil || parsed < 0 {
			return 0, 0, executionInvalid("paginate query results", "invalid page cursor")
		}
		start = parsed
	}
	const maxPageSize = 500
	pageSize := int(requested)
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return start, pageSize, nil
}

func buildPipeline(source Operator, query querylogical.Query, tracker *Tracker, options ShapeOptions) (Operator, error) {
	var root Operator = source
	if query.Predicate != nil {
		root = NewFilter(root, query.Predicate, options.Parameters, options.Now, tracker)
	}
	if len(query.Aggregates) > 0 {
		root = NewAggregate(root, query.Returns, query.Aggregates, options.Parameters, options.Now, tracker)
		if len(query.OrderBy) > 0 {
			root = NewSort(root, query.OrderBy, options.Parameters, options.Now, tracker)
		}
	} else {
		if len(query.OrderBy) > 0 {
			if topN, ok := safeTopN(query); ok {
				root = NewTopN(root, topN, query.OrderBy, options.Parameters, options.Now, tracker)
			} else {
				root = NewSort(root, query.OrderBy, options.Parameters, options.Now, tracker)
			}
		}
		root = NewProjection(root, query.Returns, options.Parameters, options.Now, tracker)
	}
	if query.Distinct {
		root = NewDistinct(root, tracker)
	}
	if query.Page.Offset > 0 {
		root = NewOffset(root, query.Page.Offset)
	}
	if query.Page.Limit > 0 {
		root = NewLimit(root, query.Page.Limit)
	}
	pageOffset, pageSize, err := paginationBounds(query.Page.Size, query.Page.Cursor)
	if err != nil {
		return nil, err
	}
	if pageOffset > 0 {
		root = NewOffset(root, int64(pageOffset))
	}
	root = NewLimit(root, int64(pageSize+1))
	return root, nil
}

func safeTopN(query querylogical.Query) (int, bool) {
	if query.Page.Limit <= 0 || query.Page.Offset < 0 || query.Page.Offset > int64(math.MaxInt)-query.Page.Limit || query.Distinct || len(query.Aggregates) > 0 {
		return 0, false
	}
	n := query.Page.Offset + query.Page.Limit
	if n <= 0 || n > int64(math.MaxInt) {
		return 0, false
	}
	return int(n), true
}

func drain(ctx context.Context, root Operator, tracker *Tracker, accountOutput bool) ([]BindingRow, int64, error) {
	if err := root.Open(ctx); err != nil {
		_ = root.Close()
		return nil, 0, err
	}
	rows := []BindingRow{}
	var retained int64
	fail := func(err error) ([]BindingRow, int64, error) {
		_ = root.Close()
		tracker.ReleaseMemory(retained)
		return nil, 0, err
	}
	for {
		if err := canceled(ctx, "execute query"); err != nil {
			return fail(err)
		}
		batch, err := root.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fail(err)
		}
		if len(batch.Rows) == 0 {
			continue
		}
		if accountOutput {
			if err := tracker.ProduceOutput(int64(len(batch.Rows))); err != nil {
				return fail(err)
			}
		}
		for _, row := range batch.Rows {
			bytes := estimateBindingRow(row)
			if err := tracker.ReserveMemory(bytes); err != nil {
				return fail(err)
			}
			retained += bytes
			rows = append(rows, row)
		}
	}
	if err := root.Close(); err != nil {
		tracker.ReleaseMemory(retained)
		return nil, 0, querylogical.NewError(querylogical.ErrorDependencyFailure, "close query operators", err.Error(), err)
	}
	return rows, retained, nil
}

func runtimeDiagnostics(usage Usage, elapsed time.Duration) querylogical.Diagnostics {
	return querylogical.Diagnostics{
		IndexEntriesScanned: usage.IndexEntries,
		NodesLoaded:         usage.NodesLoaded,
		EdgesLoaded:         usage.EdgesLoaded,
		RowsScanned:         usage.CandidatesExamined,
		RowsProduced:        usage.IntermediateRows,
		RowsReturned:        usage.OutputRows,
		CandidateCount:      usage.CandidatesExamined,
		AdjacencyScanCalls:  usage.AdjacencyCalls,
		NodeReadCalls:       usage.NodeReadCalls,
		ExecutionMillis:     elapsed.Milliseconds(),
		IntermediateRows:    usage.IntermediateRows,
		PeakMemoryBytes:     usage.PeakRetainedMemory,
		RetainedMemoryBytes: usage.RetainedMemory,
		TraversalDepth:      usage.TraversalDepth,
		RootCount:           usage.StartNodes,
		Truncated:           usage.Truncated,
		TruncationReason:    usage.TruncationReason,
	}
}

func resultGraph(rows []BindingRow) querylogical.ResultGraph {
	graph := querylogical.ResultGraph{}
	nodes := map[string]struct{}{}
	edges := map[string]struct{}{}
	addNode := func(node querylogical.Node) {
		if _, ok := nodes[node.ID]; ok {
			return
		}
		nodes[node.ID] = struct{}{}
		graph.Nodes = append(graph.Nodes, node)
	}
	addEdge := func(edge querylogical.Edge) {
		if _, ok := edges[edge.ID]; ok {
			return
		}
		edges[edge.ID] = struct{}{}
		graph.Edges = append(graph.Edges, edge)
	}
	var addTreeNode func(querylogical.TreeNode)
	addTreeNode = func(tree querylogical.TreeNode) {
		addNode(tree.Node)
		for _, child := range tree.Children {
			addTreeNode(child)
		}
	}
	for _, binding := range rows {
		for _, node := range binding.GraphNodes {
			addNode(neutralNode(node))
		}
		for _, edge := range binding.GraphEdges {
			addEdge(neutralEdge(edge))
		}
		row := binding.Projected
		names := make([]string, 0, len(row.Fields))
		for name := range row.Fields {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			cell := row.Fields[name]
			switch cell.Kind {
			case querylogical.CellNode:
				if cell.Node != nil {
					addNode(*cell.Node)
				}
			case querylogical.CellEdge:
				if cell.Edge != nil {
					addEdge(*cell.Edge)
				}
			case querylogical.CellPath:
				if cell.Path != nil {
					for _, node := range cell.Path.Nodes {
						addNode(node)
					}
					for _, edge := range cell.Path.Edges {
						addEdge(edge)
					}
				}
			case querylogical.CellTree:
				if cell.Tree != nil {
					for _, root := range cell.Tree.Roots {
						addTreeNode(root)
					}
				}
			}
		}
	}
	return graph
}
