package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func (e *Engine) executeMutation(ctx context.Context, execution ExecutionContext, prepared PreparedQuery) (querylogical.Result, error) {
	if prepared.Statement.Mutation == nil || prepared.Physical.Family != queryphysical.FamilyMutation || prepared.Kind != PlanIndexed {
		return querylogical.Result{}, unsupportedPlan(prepared)
	}
	if execution.Transaction != prepared.Planning.Transaction {
		return querylogical.Result{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "execute mutation", "execution transaction does not match the prepared transaction", nil)
	}
	if execution.Transaction.Mode != sessionmodel.TransactionModeReadWrite {
		return querylogical.Result{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "execute mutation", "mutation requires a read-write transaction", nil)
	}
	if e.ports.Mutations == nil {
		return querylogical.Result{}, querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "execute mutation", "mutation writer is required", nil)
	}
	if err := queryexecution.ValidatePage(prepared.Statement.Mutation.Page); err != nil {
		return querylogical.Result{}, err
	}
	if execution.Now.IsZero() {
		execution.Now = time.Now()
	}
	budget := queryexecution.ResolveBudget(execution.Budget, physicalBudget(prepared.Physical))
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		return querylogical.Result{}, err
	}
	mutationCtx, scope, err := e.ports.Mutations.BeginMutationStatement(ctx, execution.Transaction)
	if err := mutationDependencyError(ctx, "begin mutation statement", err); err != nil {
		return querylogical.Result{}, err
	}
	finish := func(commit bool, statementErr error) (querylogical.Result, error) {
		finishErr := e.ports.Mutations.FinishMutationStatement(context.WithoutCancel(ctx), execution.Transaction, scope, commit)
		if finishErr != nil {
			return querylogical.Result{}, querylogical.NewError(
				querylogical.ErrorDependencyFailure,
				"finish mutation statement",
				fmt.Sprintf("statement error: %v; finish error: %v", statementErr, finishErr),
				finishErr,
			)
		}
		return querylogical.Result{}, statementErr
	}

	mutation := *prepared.Statement.Mutation
	run, err := e.runMutation(mutationCtx, execution, prepared, mutation, tracker)
	if err != nil {
		tracker.ReleaseMemory(tracker.Snapshot().RetainedMemory)
		return finish(false, err)
	}
	releaseRows := func() {
		tracker.ReleaseMemory(run.retained)
		run.retained = 0
	}
	defer releaseRows()
	result, err := shapeMutationResult(mutationCtx, run.rows, mutation, prepared, execution, tracker, budget)
	if err != nil {
		return finish(false, err)
	}
	releaseRows()
	result.Diagnostics.RetainedMemoryBytes = tracker.Snapshot().RetainedMemory
	if _, err := finish(true, nil); err != nil {
		return querylogical.Result{}, err
	}
	result.Counters.NodesInserted = run.counters.NodesInserted
	result.Counters.NodesUpdated = run.counters.NodesUpdated
	result.Counters.NodesDeleted = run.counters.NodesDeleted
	result.Counters.EdgesInserted = run.counters.EdgesInserted
	result.Counters.EdgesUpdated = run.counters.EdgesUpdated
	result.Counters.EdgesDeleted = run.counters.EdgesDeleted
	result.Diagnostics = mergeRuntimeDiagnostics(queryphysical.Explain(prepared.Physical), result.Diagnostics)
	result.Diagnostics.Planner = PlannerName
	result.Diagnostics.PlannerVersion = PlannerVersion
	return result, nil
}

type mutationRunResult struct {
	rows     []queryexecution.BindingRow
	counters querylogical.Counters
	retained int64
}

func (e *Engine) runMutation(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) (mutationRunResult, error) {
	switch mutation.Kind {
	case querylogical.MutationInsertNode:
		return e.insertNode(ctx, execution, prepared, mutation)
	case querylogical.MutationMergeNode:
		return e.mergeNode(ctx, execution, prepared, mutation, tracker)
	case querylogical.MutationMatchSet:
		return e.matchSet(ctx, execution, prepared, mutation, tracker)
	case querylogical.MutationMatchDelete:
		return e.matchDelete(ctx, execution, prepared, mutation, tracker)
	case querylogical.MutationCreateRelationship:
		return e.createRelationship(ctx, execution, prepared, mutation, tracker, false)
	case querylogical.MutationMergeRelationship:
		return e.createRelationship(ctx, execution, prepared, mutation, tracker, true)
	default:
		return mutationRunResult{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "execute mutation", fmt.Sprintf("unsupported mutation kind %q", mutation.Kind), nil)
	}
}

func (e *Engine) insertNode(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation) (mutationRunResult, error) {
	properties, err := resolveMutationValues(mutation.Node.Properties, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return mutationRunResult{}, err
	}
	_, err = e.ports.Mutations.CreateNode(ctx, execution.Transaction, graph.NodeInput{Labels: append([]string(nil), mutation.Node.Labels...), Properties: properties})
	if err := mutationDependencyError(ctx, "insert node", err); err != nil {
		return mutationRunResult{}, err
	}
	return mutationRunResult{counters: querylogical.Counters{NodesInserted: 1}}, nil
}

func (e *Engine) mergeNode(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) (mutationRunResult, error) {
	query := querylogical.Query{
		Source: querylogical.SourceGQL, AccessMode: querylogical.AccessModeReadOnly,
		Start: *mutation.Node, Page: querylogical.PageSpec{Limit: mutation.Limit},
	}
	matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
	if err != nil {
		return mutationRunResult{}, err
	}
	defer matches.Release()
	rows, retained, err := cloneMutationRowsTracked(matches.Rows, tracker)
	if err != nil {
		return mutationRunResult{}, err
	}
	counters := querylogical.Counters{}
	if len(rows) == 0 {
		properties, resolveErr := resolveMutationValues(mutation.Node.Properties, prepared.Planning.Parameters, execution.Now)
		if resolveErr != nil {
			return mutationRunResult{}, resolveErr
		}
		node, createErr := e.ports.Mutations.CreateNode(ctx, execution.Transaction, graph.NodeInput{Labels: append([]string(nil), mutation.Node.Labels...), Properties: properties})
		if err := mutationDependencyError(ctx, "merge node", createErr); err != nil {
			return mutationRunResult{}, err
		}
		rows = []queryexecution.BindingRow{{Nodes: map[string]graph.Node{mutation.Node.Alias: node}}}
		retained = mutationRowsSize(rows)
		if err := tracker.ReserveMemory(retained); err != nil {
			return mutationRunResult{}, err
		}
		counters.NodesInserted = 1
	}
	if err := prepareMutationGraphTracked(rows, mutation.ReturnGraph, tracker, &retained); err != nil {
		return mutationRunResult{}, err
	}
	return mutationRunResult{rows: rows, counters: counters, retained: retained}, nil
}

type nodeSetState struct {
	node        graph.Node
	assignments map[int]struct{}
}

type edgeSetState struct {
	edge        graph.Edge
	assignments map[int]struct{}
}

func prepareSetStates(rows []queryexecution.BindingRow, assignments []querylogical.Assignment, values []any, tracker *queryexecution.Tracker) (map[string]*nodeSetState, map[string]*edgeSetState, int64, error) {
	nodes := map[string]*nodeSetState{}
	edges := map[string]*edgeSetState{}
	var retained int64
	for rowIndex := range rows {
		for assignmentIndex, assignment := range assignments {
			if node, ok := rows[rowIndex].Nodes[assignment.Field.Alias]; ok {
				id := node.ID.String()
				state := nodes[id]
				if state == nil {
					bytes := mutationNodeSize(node)
					if err := tracker.ReserveMemory(bytes); err != nil {
						return nil, nil, retained, err
					}
					retained += bytes
					state = &nodeSetState{node: cloneMutationNode(node), assignments: map[int]struct{}{}}
					nodes[id] = state
				}
				state.assignments[assignmentIndex] = struct{}{}
				continue
			}
			if edge, ok := rows[rowIndex].Edges[assignment.Field.Alias]; ok {
				id := edge.ID.String()
				state := edges[id]
				if state == nil {
					bytes := mutationEdgeSize(edge)
					if err := tracker.ReserveMemory(bytes); err != nil {
						return nil, nil, retained, err
					}
					retained += bytes
					state = &edgeSetState{edge: cloneMutationEdge(edge), assignments: map[int]struct{}{}}
					edges[id] = state
				}
				state.assignments[assignmentIndex] = struct{}{}
				continue
			}
			return nil, nil, retained, querylogical.NewError(querylogical.ErrorInvalidQuery, "set mutation", fmt.Sprintf("alias %q is not bound", assignment.Field.Alias), nil)
		}
	}
	for assignmentIndex, assignment := range assignments {
		for _, state := range nodes {
			if _, ok := state.assignments[assignmentIndex]; ok {
				if err := assignNodeValue(&state.node, assignment.Field, values[assignmentIndex]); err != nil {
					return nil, nil, retained, err
				}
			}
		}
		for _, state := range edges {
			if _, ok := state.assignments[assignmentIndex]; ok {
				if err := assignEdgeValue(&state.edge, assignment.Field, values[assignmentIndex]); err != nil {
					return nil, nil, retained, err
				}
			}
		}
	}
	var after int64
	for _, state := range nodes {
		after += mutationNodeSize(state.node)
	}
	for _, state := range edges {
		after += mutationEdgeSize(state.edge)
	}
	if after > retained {
		growth := after - retained
		if err := tracker.ReserveMemory(growth); err != nil {
			return nil, nil, retained, err
		}
		retained += growth
	}
	return nodes, edges, retained, nil
}

func (e *Engine) matchSet(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) (mutationRunResult, error) {
	query := *mutation.Match
	query.Page.Limit = mutation.Limit
	matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
	if err != nil {
		return mutationRunResult{}, err
	}
	defer matches.Release()
	rows, retained, err := cloneMutationRowsTracked(matches.Rows, tracker)
	if err != nil {
		return mutationRunResult{}, err
	}
	assignmentValues := make([]any, len(mutation.Assignments))
	for index, assignment := range mutation.Assignments {
		value, resolveErr := queryexecution.StaticValue(assignment.Value, prepared.Planning.Parameters, execution.Now)
		if resolveErr != nil {
			return mutationRunResult{}, resolveErr
		}
		assignmentValues[index] = value
	}

	nodes, edges, workspaceRetained, err := prepareSetStates(rows, mutation.Assignments, assignmentValues, tracker)
	defer func() { tracker.ReleaseMemory(workspaceRetained) }()
	if err != nil {
		return mutationRunResult{}, err
	}

	writtenNodes := map[string]graph.Node{}
	nodeIDs := sortedMutationKeys(nodes)
	for _, id := range nodeIDs {
		updated := nodes[id].node
		written, writeErr := e.ports.Mutations.UpdateNode(ctx, execution.Transaction, graph.UpdateNodeInput{
			NodeID: updated.ID.String(), Labels: append([]string(nil), updated.Labels...),
			Properties: cloneAnyMap(updated.Properties), Payload: cloneAnyMap(updated.Payload),
			UpdateMask: []string{"labels", "properties", "payload"},
		})
		if err := mutationDependencyError(ctx, "set node", writeErr); err != nil {
			return mutationRunResult{}, err
		}
		writtenNodes[id] = written
	}
	writtenEdges := map[string]graph.Edge{}
	edgeIDs := sortedMutationKeys(edges)
	for _, id := range edgeIDs {
		updated := edges[id].edge
		written, writeErr := e.ports.Mutations.UpdateEdge(ctx, execution.Transaction, graph.UpdateEdgeInput{
			EdgeID: updated.ID.String(), Labels: append([]string(nil), updated.Labels...),
			Properties: cloneAnyMap(updated.Properties), Payload: cloneAnyMap(updated.Payload),
			UpdateMask: []string{"labels", "properties", "payload"},
		})
		if err := mutationDependencyError(ctx, "set edge", writeErr); err != nil {
			return mutationRunResult{}, err
		}
		writtenEdges[id] = written
	}
	before := mutationRowsSize(rows)
	for rowIndex := range rows {
		for alias, node := range rows[rowIndex].Nodes {
			if written, ok := writtenNodes[node.ID.String()]; ok {
				rows[rowIndex].Nodes[alias] = written
			}
		}
		for alias, edge := range rows[rowIndex].Edges {
			if written, ok := writtenEdges[edge.ID.String()]; ok {
				rows[rowIndex].Edges[alias] = written
			}
		}
	}
	prepareMutationGraph(rows, mutation.ReturnGraph)
	if err := reserveMutationGrowth(tracker, before, rows, &retained); err != nil {
		return mutationRunResult{}, err
	}
	return mutationRunResult{
		rows: rows, retained: retained,
		counters: querylogical.Counters{NodesUpdated: int64(len(writtenNodes)), EdgesUpdated: int64(len(writtenEdges))},
	}, nil
}

func (e *Engine) matchDelete(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) (mutationRunResult, error) {
	query := *mutation.Match
	query.Page.Limit = mutation.Limit
	matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
	if err != nil {
		return mutationRunResult{}, err
	}
	defer matches.Release()
	rows, retained, err := cloneMutationRowsTracked(matches.Rows, tracker)
	if err != nil {
		return mutationRunResult{}, err
	}
	edges := map[string]struct{}{}
	nodes := map[string]struct{}{}
	for _, row := range rows {
		for _, target := range mutation.Targets {
			if edge, ok := row.Edges[target]; ok {
				edges[edge.ID.String()] = struct{}{}
			}
			if node, ok := row.Nodes[target]; ok {
				nodes[node.ID.String()] = struct{}{}
			}
		}
	}
	if err := e.validateNodeDeletes(ctx, execution, nodes, edges, tracker); err != nil {
		return mutationRunResult{}, err
	}
	for edgeID := range edges {
		_, deleteErr := e.ports.Mutations.DeleteEdge(ctx, execution.Transaction, edgeID)
		if err := mutationDependencyError(ctx, "delete edge", deleteErr); err != nil {
			return mutationRunResult{}, err
		}
	}
	for nodeID := range nodes {
		_, _, deleteErr := e.ports.Mutations.DeleteNode(ctx, execution.Transaction, nodeID, false)
		if err := mutationDependencyError(ctx, "delete node", deleteErr); err != nil {
			return mutationRunResult{}, err
		}
	}
	if err := prepareMutationGraphTracked(rows, mutation.ReturnGraph, tracker, &retained); err != nil {
		return mutationRunResult{}, err
	}
	return mutationRunResult{
		rows: rows, retained: retained,
		counters: querylogical.Counters{NodesDeleted: int64(len(nodes)), EdgesDeleted: int64(len(edges))},
	}, nil
}

func (e *Engine) validateNodeDeletes(ctx context.Context, execution ExecutionContext, nodes, deletedEdges map[string]struct{}, tracker *queryexecution.Tracker) error {
	if len(nodes) == 0 {
		return nil
	}
	if e.ports.Traversals == nil {
		return querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "validate node delete", "adjacency access is required", nil)
	}
	limit := tracker.Limits().BatchSize
	for nodeID := range nodes {
		for _, direction := range []graphquery.AdjacencyDirection{graphquery.AdjacencyDirectionOut, graphquery.AdjacencyDirectionIn} {
			cursor := ""
			for {
				if err := tracker.UseAdjacency(1); err != nil {
					return err
				}
				edges, next, stats, scanErr := e.ports.Traversals.ScanAdjacency(ctx, execution.Transaction, graphquery.AdjacencyScan{
					NodeID: nodeID, Direction: direction, Limit: limit, Cursor: cursor,
				})
				if err := mutationDependencyError(ctx, "validate node delete", scanErr); err != nil {
					return err
				}
				if err := tracker.ScanIndexEntries(int64(stats.IndexEntriesScanned)); err != nil {
					return err
				}
				if err := tracker.LoadEdges(int64(stats.EdgesLoaded)); err != nil {
					return err
				}
				for _, edge := range edges {
					if _, ok := deletedEdges[edge.ID.String()]; !ok {
						return querylogical.NewError(querylogical.ErrorInvalidQuery, "delete node", fmt.Sprintf("node %q requires deleting incident edge %q first", nodeID, edge.ID.String()), nil)
					}
				}
				if next == "" {
					break
				}
				if next == cursor {
					return querylogical.NewError(querylogical.ErrorInternal, "validate node delete", "adjacency cursor did not advance", nil)
				}
				cursor = next
			}
		}
	}
	return nil
}

func (e *Engine) createRelationship(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker, merge bool) (mutationRunResult, error) {
	bindings := []queryexecution.BindingRow{{Nodes: map[string]graph.Node{}, Edges: map[string]graph.Edge{}}}
	var retained int64
	for _, pattern := range mutation.Nodes {
		query := querylogical.Query{Source: querylogical.SourceGQL, AccessMode: querylogical.AccessModeReadOnly, Start: pattern}
		matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
		if err != nil {
			return mutationRunResult{}, err
		}
		next := make([]queryexecution.BindingRow, 0, len(bindings)*len(matches.Rows))
		for _, binding := range bindings {
			for _, matched := range matches.Rows {
				node := matched.Nodes[pattern.Alias]
				bytes := queryexecution.EstimateBindingRow(binding) + mutationNodeSize(node)
				if err := tracker.ProduceIntermediate(1); err != nil {
					matches.Release()
					return mutationRunResult{}, err
				}
				if err := tracker.ReserveMemory(bytes); err != nil {
					matches.Release()
					return mutationRunResult{}, err
				}
				retained += bytes
				row := cloneMutationRow(binding)
				row.Nodes[pattern.Alias] = node
				next = append(next, row)
			}
		}
		matches.Release()
		bindings = next
	}
	if merge && len(bindings) > 1 {
		return mutationRunResult{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "merge relationship", fmt.Sprintf("matched %d endpoint bindings; refine endpoint predicates", len(bindings)), nil)
	}
	properties, err := resolveMutationValues(mutation.Relationship.Properties, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return mutationRunResult{}, err
	}
	counters := querylogical.Counters{}
	rows := make([]queryexecution.BindingRow, 0, len(bindings))
	var workspaceRetained int64
	defer func() { tracker.ReleaseMemory(workspaceRetained) }()
	for _, binding := range bindings {
		from, fromOK := binding.Nodes[mutation.Relationship.FromAlias]
		to, toOK := binding.Nodes[mutation.Relationship.ToAlias]
		if !fromOK || !toOK {
			return mutationRunResult{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "create relationship", "relationship endpoint is not bound", nil)
		}
		var edge graph.Edge
		if merge {
			existing, edgeRetained, findErr := e.findRelationship(ctx, execution, prepared, tracker, from, to, mutation.Relationship)
			workspaceRetained += edgeRetained
			if findErr != nil {
				return mutationRunResult{}, findErr
			}
			if existing != nil {
				edge = *existing
			}
		}
		if edge.ID == (graph.EdgeID{}) {
			created, createErr := e.ports.Mutations.CreateEdge(ctx, execution.Transaction, graph.EdgeInput{
				FromNodeID: from.ID.String(), ToNodeID: to.ID.String(), Labels: append([]string(nil), mutation.Relationship.Labels...), Properties: cloneAnyMap(properties),
			})
			if err := mutationDependencyError(ctx, "create relationship", createErr); err != nil {
				return mutationRunResult{}, err
			}
			edge = created
			counters.EdgesInserted++
		}
		if merge {
			beforeBinding := queryexecution.EstimateBindingRow(binding)
			if mutation.Relationship.Alias != "" {
				binding.Edges[mutation.Relationship.Alias] = edge
			}
			afterBinding := queryexecution.EstimateBindingRow(binding)
			if afterBinding > beforeBinding {
				growth := afterBinding - beforeBinding
				if err := tracker.ReserveMemory(growth); err != nil {
					return mutationRunResult{}, err
				}
				retained += growth
			}
			rows = append(rows, binding)
		}
	}
	if merge && mutation.Limit > 0 && int64(len(rows)) > mutation.Limit {
		rows = rows[:mutation.Limit]
	}
	if !merge {
		tracker.ReleaseMemory(retained)
		return mutationRunResult{counters: counters}, nil
	}
	before := mutationRowsSize(rows)
	prepareMutationGraph(rows, mutation.ReturnGraph)
	if err := reserveMutationGrowth(tracker, before, rows, &retained); err != nil {
		return mutationRunResult{}, err
	}
	return mutationRunResult{rows: rows, counters: counters, retained: retained}, nil
}

func (e *Engine) findRelationship(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, from, to graph.Node, relationship *querylogical.RelationshipMutation) (*graph.Edge, int64, error) {
	query := querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "__merge_from", NodeIDs: []string{from.ID.String()}},
		Steps: []querylogical.Step{{
			Edge:  querylogical.EdgePattern{Alias: "__merge_edge", Labels: append([]string(nil), relationship.Labels...), Properties: relationship.Properties, Direction: querylogical.DirectionOutgoing},
			Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "__merge_to", NodeIDs: []string{to.ID.String()}},
		}},
		Page: querylogical.PageSpec{Limit: 1},
	}
	matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
	if err != nil {
		return nil, 0, err
	}
	defer matches.Release()
	if len(matches.Rows) == 0 {
		return nil, 0, nil
	}
	matched := matches.Rows[0].Edges["__merge_edge"]
	retained := mutationEdgeSize(matched)
	if err := tracker.ReserveMemory(retained); err != nil {
		return nil, 0, err
	}
	edge := cloneMutationEdge(matched)
	return &edge, retained, nil
}

func (e *Engine) collectMutationMatches(ctx context.Context, execution ExecutionContext, outer PreparedQuery, query querylogical.Query, tracker *queryexecution.Tracker) (queryexecution.Matches, error) {
	if query.Source == "" {
		query.Source = outer.Statement.Source
	}
	query.AccessMode = querylogical.AccessModeReadOnly
	query.Returns = nil
	query.Aggregates = nil
	query.OrderBy = nil
	query.Distinct = false
	query.Page.Cursor = ""
	statement := querylogical.ReadStatement(query)
	prepared, err := e.Prepare(ctx, outer.Planning, statement)
	if err != nil {
		return queryexecution.Matches{}, err
	}
	if prepared.RequiresLegacy() {
		return queryexecution.Matches{}, unsupportedPlan(prepared)
	}
	view, err := e.readViewIdentity(ctx, execution.Transaction)
	if err != nil {
		return queryexecution.Matches{}, err
	}
	prepared.readViewIdentity = view
	source, err := e.indexedSource(ctx, execution, prepared, tracker, indexedCursor{}, 0)
	if err != nil {
		return queryexecution.Matches{}, err
	}
	if !nativeTraversalFamily(prepared.Physical.Family) {
		source = queryexecution.NewNodePatternFilter(source, query.Start, prepared.Planning.Parameters, execution.Now, tracker)
	}
	query = executionQuery(query, prepared.Physical, indexedCursor{})
	matches, err := queryexecution.CollectMatches(ctx, source, query, tracker, queryexecution.ShapeOptions{Parameters: prepared.Planning.Parameters, Now: execution.Now})
	if err != nil {
		return queryexecution.Matches{}, err
	}
	currentView, err := e.readViewIdentity(ctx, execution.Transaction)
	if err != nil {
		matches.Release()
		return queryexecution.Matches{}, err
	}
	if currentView != view {
		matches.Release()
		return queryexecution.Matches{}, staleCursor("transaction overlay changed during mutation match", nil)
	}
	return matches, nil
}

func shapeMutationResult(ctx context.Context, rows []queryexecution.BindingRow, mutation querylogical.Mutation, prepared PreparedQuery, execution ExecutionContext, tracker *queryexecution.Tracker, budget queryexecution.Budget) (querylogical.Result, error) {
	query := querylogical.Query{
		Source: prepared.Statement.Source, AccessMode: querylogical.AccessModeReadWrite,
		Returns: mutation.Returns, Aggregates: mutation.Aggregates, ReturnGraph: mutation.ReturnGraph, Page: mutation.Page,
	}
	sourceRows := rows
	for index := range sourceRows {
		// Mutation continuations preserve the legacy output-offset contract. A
		// native read-source checkpoint would be rejected by mutation pagination
		// and must never escape as the response cursor.
		sourceRows[index].Cursor = ""
	}
	if mutation.Kind == querylogical.MutationInsertNode || mutation.Kind == querylogical.MutationCreateRelationship {
		sourceRows = nil
	}
	return queryexecution.Shape(ctx, queryexecution.NewLegacySource(sourceRows, budget.BatchSize, tracker), query, tracker, queryexecution.ShapeOptions{Parameters: prepared.Planning.Parameters, Now: execution.Now})
}

func resolveMutationValues(values map[string]querylogical.Value, parameters map[string]querylogical.Value, now time.Time) (map[string]any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(values))
	for name, value := range values {
		resolved, err := queryexecution.StaticValue(value, parameters, now)
		if err != nil {
			return nil, err
		}
		out[name] = resolved
	}
	return out, nil
}

func assignNodeValue(node *graph.Node, field querylogical.FieldRef, value any) error {
	switch field.Namespace {
	case "", querylogical.NamespaceProperties:
		node.Properties = cloneAnyMap(node.Properties)
		if node.Properties == nil {
			node.Properties = map[string]any{}
		}
		node.Properties[field.Name] = value
	case querylogical.NamespacePayload:
		node.Payload = cloneAnyMap(node.Payload)
		if node.Payload == nil {
			node.Payload = map[string]any{}
		}
		node.Payload[field.Name] = value
	default:
		return querylogical.NewError(querylogical.ErrorInvalidQuery, "set node", fmt.Sprintf("unsupported namespace %q", field.Namespace), nil)
	}
	return nil
}

func assignEdgeValue(edge *graph.Edge, field querylogical.FieldRef, value any) error {
	switch field.Namespace {
	case "", querylogical.NamespaceProperties:
		edge.Properties = cloneAnyMap(edge.Properties)
		if edge.Properties == nil {
			edge.Properties = map[string]any{}
		}
		edge.Properties[field.Name] = value
	case querylogical.NamespacePayload:
		edge.Payload = cloneAnyMap(edge.Payload)
		if edge.Payload == nil {
			edge.Payload = map[string]any{}
		}
		edge.Payload[field.Name] = value
	default:
		return querylogical.NewError(querylogical.ErrorInvalidQuery, "set edge", fmt.Sprintf("unsupported namespace %q", field.Namespace), nil)
	}
	return nil
}

func prepareMutationGraph(rows []queryexecution.BindingRow, include bool) {
	if !include {
		return
	}
	for index := range rows {
		nodeIndex := make(map[string]int, len(rows[index].GraphNodes)+len(rows[index].Nodes))
		for nodePosition, node := range rows[index].GraphNodes {
			nodeIndex[node.ID.String()] = nodePosition
		}
		nodeAliases := make([]string, 0, len(rows[index].Nodes))
		for alias := range rows[index].Nodes {
			nodeAliases = append(nodeAliases, alias)
		}
		sort.Strings(nodeAliases)
		for _, alias := range nodeAliases {
			node := rows[index].Nodes[alias]
			if position, ok := nodeIndex[node.ID.String()]; ok {
				rows[index].GraphNodes[position] = node
				continue
			}
			nodeIndex[node.ID.String()] = len(rows[index].GraphNodes)
			rows[index].GraphNodes = append(rows[index].GraphNodes, node)
		}

		edgeIndex := make(map[string]int, len(rows[index].GraphEdges)+len(rows[index].Edges))
		for edgePosition, edge := range rows[index].GraphEdges {
			edgeIndex[edge.ID.String()] = edgePosition
		}
		edgeAliases := make([]string, 0, len(rows[index].Edges))
		for alias := range rows[index].Edges {
			edgeAliases = append(edgeAliases, alias)
		}
		sort.Strings(edgeAliases)
		for _, alias := range edgeAliases {
			edge := rows[index].Edges[alias]
			if position, ok := edgeIndex[edge.ID.String()]; ok {
				rows[index].GraphEdges[position] = edge
				continue
			}
			edgeIndex[edge.ID.String()] = len(rows[index].GraphEdges)
			rows[index].GraphEdges = append(rows[index].GraphEdges, edge)
		}
	}
}

func cloneMutationRowsTracked(rows []queryexecution.BindingRow, tracker *queryexecution.Tracker) ([]queryexecution.BindingRow, int64, error) {
	retained := mutationRowsSize(rows)
	if err := tracker.ReserveMemory(retained); err != nil {
		return nil, 0, err
	}
	out := make([]queryexecution.BindingRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, cloneMutationRow(row))
	}
	return out, retained, nil
}

func mutationRowsSize(rows []queryexecution.BindingRow) int64 {
	var bytes int64
	for _, row := range rows {
		bytes += queryexecution.EstimateBindingRow(row)
	}
	return bytes
}

func mutationNodeSize(node graph.Node) int64 {
	return queryexecution.EstimateBindingRow(queryexecution.BindingRow{Nodes: map[string]graph.Node{"n": node}})
}

func mutationEdgeSize(edge graph.Edge) int64 {
	return queryexecution.EstimateBindingRow(queryexecution.BindingRow{Edges: map[string]graph.Edge{"e": edge}})
}

func reserveMutationGrowth(tracker *queryexecution.Tracker, before int64, rows []queryexecution.BindingRow, retained *int64) error {
	after := mutationRowsSize(rows)
	if after <= before {
		return nil
	}
	growth := after - before
	if err := tracker.ReserveMemory(growth); err != nil {
		return err
	}
	*retained += growth
	return nil
}

func prepareMutationGraphTracked(rows []queryexecution.BindingRow, enabled bool, tracker *queryexecution.Tracker, retained *int64) error {
	before := mutationRowsSize(rows)
	prepareMutationGraph(rows, enabled)
	return reserveMutationGrowth(tracker, before, rows, retained)
}

func sortedMutationKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cloneMutationRow(row queryexecution.BindingRow) queryexecution.BindingRow {
	out := queryexecution.BindingRow{Nodes: map[string]graph.Node{}, Edges: map[string]graph.Edge{}, Paths: row.Paths, Trees: row.Trees, Cursor: row.Cursor, Sequence: row.Sequence}
	for alias, node := range row.Nodes {
		out.Nodes[alias] = cloneMutationNode(node)
	}
	for alias, edge := range row.Edges {
		out.Edges[alias] = cloneMutationEdge(edge)
	}
	out.GraphNodes = append([]graph.Node(nil), row.GraphNodes...)
	out.GraphEdges = append([]graph.Edge(nil), row.GraphEdges...)
	return out
}

func cloneMutationNode(node graph.Node) graph.Node {
	node.Labels = append([]string(nil), node.Labels...)
	node.Properties = cloneAnyMap(node.Properties)
	node.Payload = cloneAnyMap(node.Payload)
	node.Meta = cloneAnyMap(node.Meta)
	node.Props = cloneAnyMap(node.Props)
	return node
}

func cloneMutationEdge(edge graph.Edge) graph.Edge {
	edge.Labels = append([]string(nil), edge.Labels...)
	edge.Properties = cloneAnyMap(edge.Properties)
	edge.Payload = cloneAnyMap(edge.Payload)
	edge.Meta = cloneAnyMap(edge.Meta)
	return edge
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	out := make(map[string]any, len(values))
	for name, value := range values {
		out[name] = value
	}
	return out
}

func mutationDependencyError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextQueryError(operation, contextErr)
	}
	if err == nil {
		return nil
	}
	return querylogical.NewError(querylogical.ErrorDependencyFailure, operation, err.Error(), err)
}
