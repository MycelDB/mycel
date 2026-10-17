package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	schemacompile "github.com/myceldb/mycel/internal/schema/compile"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func (e *Engine) indexedSource(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, cursor indexedCursor, nextRowsReturned int64) (queryexecution.Operator, error) {
	access := prepared.Physical.Access
	batchSize := tracker.Limits().BatchSize
	if requested := int(prepared.Statement.Read.Page.Size) + 1; prepared.Statement.Read.Page.Size > 0 && requested < batchSize {
		batchSize = requested
	}
	if prepared.Physical.Family == queryphysical.FamilyIndexedSubtree && prepared.Statement.Read.Page.Limit > 0 {
		if requested := int(prepared.Statement.Read.Page.Limit) + 1; requested < batchSize {
			batchSize = requested
		}
	}
	checkpoint := sourceCheckpointEncoder(prepared, cursor, nextRowsReturned)
	resumeCursor, resumeOffset := "", 0
	resuming := false
	if cursor.Mode == cursorModeSource {
		resumeCursor, resumeOffset = cursor.PageCursor, cursor.PageOffset
		resuming = cursor.Version > 0 || cursor.StartIndex > 0 || resumeCursor != "" || resumeOffset > 0 || cursor.RowsReturned > 0
		if resumeOffset >= batchSize {
			return nil, staleCursor("source cursor offset exceeds the indexed page size", nil)
		}
	}

	switch prepared.Physical.Family {
	case queryphysical.FamilyNodeIDLookup:
		reader := e.nodeIDPageReader(execution, access.NodeIDs)
		return queryexecution.NewPagedNodeSource(access.StartAlias, reader, checkpoint, resumeCursor, resumeOffset, resuming, batchSize, tracker), nil
	case queryphysical.FamilyLabelIndex:
		label := access.Labels[0]
		reader := func(ctx context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
			nodes, next, stats, err := e.ports.Indexes.ScanLabel(ctx, execution.Transaction, graphquery.LabelScan{Label: label, Limit: limit, Cursor: cursor})
			return executionNodePage(nodes, next, stats), err
		}
		return queryexecution.NewPagedNodeSource(access.StartAlias, reader, checkpoint, resumeCursor, resumeOffset, resuming, batchSize, tracker), nil
	case queryphysical.FamilyIndexedEquality, queryphysical.FamilyIndexedOrder:
		if err := e.configureIndexes(ctx, execution.Transaction, prepared); err != nil {
			return nil, err
		}
		reader, err := e.orderedPageReader(execution, prepared, access)
		if err != nil {
			return nil, err
		}
		return queryexecution.NewPagedNodeSource(access.StartAlias, reader, checkpoint, resumeCursor, resumeOffset, resuming, batchSize, tracker), nil
	case queryphysical.FamilyTagIndex:
		return e.tagSetSource(execution, prepared, tracker)
	case queryphysical.FamilyIndexedPredicate:
		if err := e.configureIndexes(ctx, execution.Transaction, prepared); err != nil {
			return nil, err
		}
		return e.predicateSetSource(execution, prepared, tracker)
	case queryphysical.FamilySemantic:
		return e.semanticSource(execution, prepared, tracker, checkpoint, resumeCursor, resumeOffset, resuming, batchSize)
	case queryphysical.FamilyIndexedAdjacency:
		return e.adjacencySource(execution, prepared, tracker, cursor, nextRowsReturned, batchSize)
	case queryphysical.FamilyIndexedPath:
		return e.pathSource(ctx, execution, prepared, tracker, batchSize)
	case queryphysical.FamilyIndexedSubtree:
		return e.subtreeSource(ctx, execution, prepared, tracker, checkpoint, resumeCursor, resumeOffset, resuming, batchSize)
	default:
		return nil, unsupportedPlan(prepared)
	}
}

func sourceCheckpointEncoder(prepared PreparedQuery, cursor indexedCursor, nextRowsReturned int64) queryexecution.PageCheckpointEncoder {
	if cursor.Mode == cursorModeOutput || !nativeSourceCursorSupported(prepared) {
		return nil
	}
	return func(pageCursor string, offset int) string {
		return encodeIndexedCursor(prepared, indexedCursor{
			Mode: cursorModeSource, PageCursor: pageCursor, PageOffset: offset,
			RowsReturned: nextRowsReturned,
		})
	}
}

func nativeSourceCursorSupported(prepared PreparedQuery) bool {
	if prepared.Statement.Read == nil || !sourceCursorSafe(*prepared.Statement.Read, prepared.Physical) {
		return false
	}
	switch prepared.Physical.Family {
	case queryphysical.FamilyNodeIDLookup, queryphysical.FamilyLabelIndex,
		queryphysical.FamilyIndexedAdjacency, queryphysical.FamilyIndexedEquality,
		queryphysical.FamilyIndexedOrder, queryphysical.FamilyIndexedSubtree,
		queryphysical.FamilySemantic:
		return true
	default:
		return false
	}
}

func sourceCursorSafe(query querylogical.Query, plan queryphysical.Plan) bool {
	if query.Distinct || len(query.Aggregates) != 0 {
		return false
	}
	return len(query.OrderBy) == 0 || plan.Family == queryphysical.FamilyIndexedOrder || plan.Family == queryphysical.FamilyIndexedSubtree
}

func nativeTraversalFamily(family queryphysical.Family) bool {
	switch family {
	case queryphysical.FamilyIndexedAdjacency, queryphysical.FamilyIndexedPath, queryphysical.FamilyIndexedSubtree:
		return true
	default:
		return false
	}
}

func (e *Engine) adjacencySource(execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, cursor indexedCursor, nextRowsReturned int64, batchSize int) (queryexecution.Operator, error) {
	query := *prepared.Statement.Read
	if len(query.Steps) != 1 || len(prepared.Physical.Access.Steps) != 1 {
		return nil, querylogical.NewError(querylogical.ErrorInternal, "execute adjacency query", "adjacency plan requires one traversal step", nil)
	}
	step := resolvedTraversalStep(query.Steps[0], prepared.Physical.Access.Steps[0])
	edgeReader := e.traversalEdgeReader(execution, step)
	nodeReader := func(ctx context.Context, nodeID string) (graph.Node, error) {
		return e.ports.Entities.GetNode(ctx, execution.Transaction, nodeID)
	}
	var checkpoint queryexecution.AdjacencyCheckpointEncoder
	if cursor.Mode != cursorModeOutput && nativeSourceCursorSupported(prepared) {
		checkpoint = func(startIndex int, pageCursor string, pageOffset int) string {
			return encodeIndexedCursor(prepared, indexedCursor{
				Mode: cursorModeSource, StartIndex: startIndex, PageCursor: pageCursor,
				PageOffset: pageOffset, RowsReturned: nextRowsReturned,
			})
		}
	}
	resuming := cursor.Mode == cursorModeSource && (cursor.Version > 0 || cursor.StartIndex > 0 || cursor.PageCursor != "" || cursor.PageOffset > 0 || cursor.RowsReturned > 0)
	return queryexecution.NewAdjacencySource(
		query.Start.Alias, query.Start, step, prepared.Physical.Access.NodeIDs,
		nodeReader, edgeReader, checkpoint, cursor.StartIndex, cursor.PageCursor,
		cursor.PageOffset, resuming, batchSize, prepared.Planning.Parameters, execution.Now, tracker,
	), nil
}

func (e *Engine) pathSource(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, batchSize int) (queryexecution.Operator, error) {
	query := *prepared.Statement.Read
	if len(query.Steps) == 0 || len(query.Steps) != len(prepared.Physical.Access.Steps) {
		return nil, querylogical.NewError(querylogical.ErrorInternal, "execute indexed path", "path plan has incompatible traversal steps", nil)
	}
	source, err := e.pathStartSource(ctx, execution, prepared, tracker, batchSize)
	if err != nil {
		return nil, err
	}
	source = queryexecution.NewNodePatternFilter(source, query.Start, prepared.Planning.Parameters, execution.Now, tracker)
	source = queryexecution.NewStartNodeLimit(source, tracker)
	currentAlias := query.Start.Alias
	for index, logicalStep := range query.Steps {
		step := resolvedTraversalStep(logicalStep, prepared.Physical.Access.Steps[index])
		edgeReader := e.traversalEdgeReader(execution, step)
		nodeReader := func(ctx context.Context, nodeID string) (graph.Node, error) {
			return e.ports.Entities.GetNode(ctx, execution.Transaction, nodeID)
		}
		source = queryexecution.NewPathExpand(source, currentAlias, step, query.PathAlias, edgeReader, nodeReader, batchSize, prepared.Planning.Parameters, execution.Now, tracker)
		currentAlias = step.Target.Alias
	}
	return source, nil
}

func (e *Engine) pathStartSource(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, batchSize int) (queryexecution.Operator, error) {
	access := prepared.Physical.Access
	switch access.Kind {
	case queryphysical.AccessNodeIDs:
		return queryexecution.NewPagedNodeSource(access.StartAlias, e.nodeIDPageReader(execution, access.NodeIDs), nil, "", 0, false, batchSize, tracker), nil
	case queryphysical.AccessLabel:
		label := access.Labels[0]
		reader := func(ctx context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
			nodes, next, stats, err := e.ports.Indexes.ScanLabel(ctx, execution.Transaction, graphquery.LabelScan{Label: label, Limit: limit, Cursor: cursor})
			return executionNodePage(nodes, next, stats), err
		}
		return queryexecution.NewPagedNodeSource(access.StartAlias, reader, nil, "", 0, false, batchSize, tracker), nil
	case queryphysical.AccessTag:
		return e.tagSetSource(execution, prepared, tracker)
	case queryphysical.AccessCompoundProperty:
		if err := e.configureIndexes(ctx, execution.Transaction, prepared); err != nil {
			return nil, err
		}
		return e.predicateSetSource(execution, prepared, tracker)
	default:
		return nil, querylogical.NewError(querylogical.ErrorInternal, "execute indexed path", "unsupported indexed path start access", nil)
	}
}

func (e *Engine) subtreeSource(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, checkpoint queryexecution.PageCheckpointEncoder, resumeCursor string, resumeOffset int, resuming bool, batchSize int) (queryexecution.Operator, error) {
	query := *prepared.Statement.Read
	if len(query.Steps) != 1 || len(prepared.Physical.Access.Steps) != 1 {
		return nil, querylogical.NewError(querylogical.ErrorInternal, "execute indexed subtree", "subtree plan requires one traversal step", nil)
	}
	if err := e.configureIndexes(ctx, execution.Transaction, prepared); err != nil {
		return nil, err
	}
	reader, err := e.orderedPageReader(execution, prepared, prepared.Physical.Access)
	if err != nil {
		return nil, err
	}
	var source queryexecution.Operator = queryexecution.NewPagedNodeSource(query.Start.Alias, reader, checkpoint, resumeCursor, resumeOffset, resuming, batchSize, tracker)
	source = queryexecution.NewNodePatternFilter(source, query.Start, prepared.Planning.Parameters, execution.Now, tracker)
	source = queryexecution.NewStartNodeLimit(source, tracker)
	step := resolvedTraversalStep(query.Steps[0], prepared.Physical.Access.Steps[0])
	edgeReader := e.traversalEdgeReader(execution, step)
	nodeReader := func(ctx context.Context, nodeID string) (graph.Node, error) {
		return e.ports.Entities.GetNode(ctx, execution.Transaction, nodeID)
	}
	return queryexecution.NewSubtreeExpand(
		source, query.Start.Alias, step, edgeReader, nodeReader, batchSize,
		prepared.Planning.Parameters, execution.Now, tracker,
	), nil
}

func resolvedTraversalStep(step querylogical.Step, access queryphysical.TraversalStep) querylogical.Step {
	step.Depth = querylogical.DepthBounds{Min: access.MinDepth, Max: access.MaxDepth}
	return step
}

func (e *Engine) traversalEdgeReader(execution ExecutionContext, step querylogical.Step) queryexecution.TraversalEdgePageReader {
	direction := graphquery.AdjacencyDirectionOut
	if step.Edge.Direction == querylogical.DirectionIncoming {
		direction = graphquery.AdjacencyDirectionIn
	}
	return func(ctx context.Context, nodeID string, limit int, cursor string) (queryexecution.TraversalEdgePage, error) {
		edges, next, stats, err := e.ports.Traversals.ScanAdjacency(ctx, execution.Transaction, graphquery.AdjacencyScan{
			NodeID: nodeID, Label: step.Edge.Labels[0], Direction: direction, Limit: limit, Cursor: cursor,
		})
		return queryexecution.TraversalEdgePage{
			Edges: edges, NextCursor: next, IndexEntries: int64(stats.IndexEntriesScanned), EdgesLoaded: int64(stats.EdgesLoaded),
		}, err
	}
}

func (e *Engine) nodeIDPageReader(execution ExecutionContext, ids []string) queryexecution.NodePageReader {
	return func(ctx context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
		start, err := boundedPosition(cursor, len(ids))
		if err != nil {
			return queryexecution.NodePage{}, err
		}
		end := start + limit
		if end > len(ids) {
			end = len(ids)
		}
		nodes := make([]graph.Node, 0, end-start)
		for _, id := range ids[start:end] {
			node, err := e.ports.Entities.GetNode(ctx, execution.Transaction, id)
			if err != nil {
				return queryexecution.NodePage{}, err
			}
			nodes = append(nodes, node)
		}
		next := ""
		if end < len(ids) {
			next = strconv.Itoa(end)
		}
		return queryexecution.NodePage{Nodes: nodes, NextCursor: next, NodesLoaded: int64(len(nodes)), NodeReadCalls: int64(len(nodes))}, nil
	}
}

func (e *Engine) orderedPageReader(execution ExecutionContext, prepared PreparedQuery, access queryphysical.AccessPath) (queryexecution.NodePageReader, error) {
	low, hasLow, lowExclusive, err := executionBound(access.Low, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return nil, err
	}
	high, hasHigh, highExclusive, err := executionBound(access.High, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return nil, err
	}
	direction := schemamodel.IndexSortDirectionAsc
	if len(prepared.Statement.Read.OrderBy) > 0 && prepared.Statement.Read.OrderBy[0].Direction == querylogical.SortDescending {
		direction = schemamodel.IndexSortDirectionDesc
	}
	return func(ctx context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
		nodes, next, stats, err := e.ports.Indexes.ScanNodePropertyOrdered(ctx, execution.Transaction, graphquery.OrderedNodePropertyScan{
			IndexName: access.Index, Direction: direction, Limit: limit, Cursor: cursor,
			HasLow: hasLow, Low: low, LowExclusive: lowExclusive,
			HasHigh: hasHigh, High: high, HighExclusive: highExclusive,
		})
		return executionNodePage(nodes, next, stats), err
	}, nil
}

func executionBound(bound *queryphysical.Bound, parameters map[string]querylogical.Value, now time.Time) (any, bool, bool, error) {
	if bound == nil {
		return nil, false, false, nil
	}
	value, err := queryexecution.StaticValue(bound.Value, parameters, now)
	if err != nil {
		return nil, false, false, err
	}
	return value, true, !bound.Inclusive, nil
}

func executionNodePage(nodes []graph.Node, next string, stats graphquery.IndexedReadStats) queryexecution.NodePage {
	return queryexecution.NodePage{
		Nodes: nodes, NextCursor: next,
		IndexEntries: int64(stats.IndexEntriesScanned), NodesLoaded: int64(stats.NodesLoaded), NodeReadCalls: int64(stats.NodeReadCalls),
	}
}

func (e *Engine) configureIndexes(ctx context.Context, tx sessionmodel.GraphTransaction, prepared PreparedQuery) error {
	if prepared.Planning.Schema == nil {
		return querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "configure query indexes", "indexed query requires an active schema", nil)
	}
	if err := e.ports.Indexes.ConfigureIndexes(ctx, tx, schemacompile.Hash(*prepared.Planning.Schema), prepared.Planning.Schema.Indexes); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextQueryError("configure query indexes", contextErr)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return contextQueryError("configure query indexes", err)
		}
		return querylogical.NewError(querylogical.ErrorDependencyFailure, "configure query indexes", err.Error(), err)
	}
	return nil
}

func (e *Engine) tagSetSource(execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker) (queryexecution.Operator, error) {
	branches := make([]queryexecution.Operator, 0, len(prepared.Physical.Access.TagBranches))
	for _, tags := range prepared.Physical.Access.TagBranches {
		children := make([]queryexecution.Operator, 0, len(tags))
		for _, tag := range tags {
			tag := tag
			reader := func(ctx context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
				nodes, next, stats, err := e.ports.Indexes.ScanTag(ctx, execution.Transaction, graphquery.TagScan{Tag: tag, Limit: limit, Cursor: cursor})
				return executionNodePage(nodes, next, stats), err
			}
			children = append(children, queryexecution.NewPagedNodeSource(prepared.Physical.Access.StartAlias, reader, nil, "", 0, false, tracker.Limits().BatchSize, tracker))
		}
		branches = append(branches, queryexecution.NewNodeSet(children, queryexecution.NodeSetIntersection, tracker.Limits().BatchSize, tracker))
	}
	if len(branches) == 1 {
		return branches[0], nil
	}
	return queryexecution.NewNodeSet(branches, queryexecution.NodeSetUnion, tracker.Limits().BatchSize, tracker), nil
}

func (e *Engine) predicateSetSource(execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker) (queryexecution.Operator, error) {
	branches := make([]queryexecution.Operator, 0, len(prepared.Physical.Access.Branches))
	for _, branch := range prepared.Physical.Access.Branches {
		children := make([]queryexecution.Operator, 0, len(branch.Scans))
		for _, scan := range branch.Scans {
			reader, err := e.indexScanReader(execution, prepared, scan)
			if err != nil {
				return nil, err
			}
			children = append(children, queryexecution.NewPagedNodeSource(prepared.Physical.Access.StartAlias, reader, nil, "", 0, false, tracker.Limits().BatchSize, tracker))
		}
		branches = append(branches, queryexecution.NewNodeSet(children, queryexecution.NodeSetIntersection, tracker.Limits().BatchSize, tracker))
	}
	if len(branches) == 1 {
		return branches[0], nil
	}
	return queryexecution.NewNodeSet(branches, queryexecution.NodeSetUnion, tracker.Limits().BatchSize, tracker), nil
}

func (e *Engine) indexScanReader(execution ExecutionContext, prepared PreparedQuery, scan queryphysical.IndexScan) (queryexecution.NodePageReader, error) {
	low, hasLow, lowExclusive, err := executionBound(scan.Low, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return nil, err
	}
	high, hasHigh, highExclusive, err := executionBound(scan.High, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
		nodes, next, stats, err := e.ports.Indexes.ScanNodePropertyOrdered(ctx, execution.Transaction, graphquery.OrderedNodePropertyScan{
			IndexName: scan.Index, Direction: schemamodel.IndexSortDirectionAsc, Limit: limit, Cursor: cursor,
			HasLow: hasLow, Low: low, LowExclusive: lowExclusive,
			HasHigh: hasHigh, High: high, HighExclusive: highExclusive,
		})
		return executionNodePage(nodes, next, stats), err
	}, nil
}

func (e *Engine) semanticSource(execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, checkpoint queryexecution.PageCheckpointEncoder, resumeCursor string, resumeOffset int, resuming bool, batchSize int) (queryexecution.Operator, error) {
	semantic := prepared.Physical.Access.Semantic
	if semantic == nil {
		return nil, querylogical.NewError(querylogical.ErrorInternal, "execute semantic query", "semantic access specification is missing", nil)
	}
	loaded := false
	reported := false
	candidateIDs := []string{}
	candidateCount := int64(0)
	candidateMemory := int64(0)
	reader := func(ctx context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
		if !loaded {
			ruleIDs := []string{}
			if strings.TrimSpace(semantic.RuleRef) != "" {
				ruleIDs = append(ruleIDs, semantic.RuleRef)
			}
			batch, err := e.ports.SemanticCandidates.SearchCandidates(ctx, SemanticCandidateRequest{
				SpaceID: execution.Transaction.SpaceID, DomainID: execution.Transaction.DomainID,
				ActorPrincipalID: execution.Transaction.PrincipalID, SemanticRuleIDs: ruleIDs,
				EmbeddingBindingKey: semantic.EmbeddingBindingKey, Text: semantic.Query,
				Limit: semantic.Limit, MinScore: semantic.MinScore,
			})
			if err != nil {
				return queryexecution.NodePage{}, err
			}
			candidateCount = int64(len(batch.Candidates))
			if candidateCount > tracker.Limits().MaxSemanticCandidates {
				return queryexecution.NodePage{}, querylogical.NewError(
					querylogical.ErrorResourceExhausted,
					"scan semantic candidates",
					fmt.Sprintf("semantic candidate limit exceeded: used %d, limit %d", candidateCount, tracker.Limits().MaxSemanticCandidates),
					nil,
				)
			}
			seen := map[string]struct{}{}
			for _, candidate := range batch.Candidates {
				id := strings.TrimSpace(candidate.NodeID)
				if id == "" {
					id = strings.TrimSpace(candidate.TargetNodeID)
				}
				if id == "" {
					return queryexecution.NodePage{}, fmt.Errorf("semantic candidate has no node identity")
				}
				if _, exists := seen[id]; exists {
					continue
				}
				seen[id] = struct{}{}
				candidateIDs = append(candidateIDs, id)
				candidateMemory += int64(len(id) + 16)
			}
			loaded = true
		}
		start, err := boundedPosition(cursor, len(candidateIDs))
		if err != nil {
			return queryexecution.NodePage{}, err
		}
		end := start + limit
		if end > len(candidateIDs) {
			end = len(candidateIDs)
		}
		nodes := make([]graph.Node, 0, end-start)
		for _, id := range candidateIDs[start:end] {
			node, err := e.ports.Entities.GetNode(ctx, execution.Transaction, id)
			if err != nil {
				return queryexecution.NodePage{}, fmt.Errorf("hydrate semantic candidate %s: %w", id, err)
			}
			nodes = append(nodes, node)
		}
		next := ""
		if end < len(candidateIDs) {
			next = strconv.Itoa(end)
		}
		page := queryexecution.NodePage{Nodes: nodes, NextCursor: next, NodesLoaded: int64(len(nodes)), NodeReadCalls: int64(len(nodes))}
		if !reported {
			page.SemanticCandidates = candidateCount
			page.SourceRetainedBytes = candidateMemory
			reported = true
		}
		return page, nil
	}
	return queryexecution.NewPagedNodeSource(prepared.Physical.Access.StartAlias, reader, checkpoint, resumeCursor, resumeOffset, resuming, batchSize, tracker), nil
}

func boundedPosition(cursor string, length int) (int, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	position, err := strconv.Atoi(cursor)
	if err != nil || position < 0 || position > length {
		return 0, querylogical.NewError(querylogical.ErrorStaleCursor, "resume indexed source", "source cursor position is invalid", err)
	}
	return position, nil
}
