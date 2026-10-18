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
	savepoint, err := e.ports.Mutations.CaptureMutationState(ctx, execution.Transaction)
	if err := mutationDependencyError(ctx, "capture mutation savepoint", err); err != nil {
		return querylogical.Result{}, err
	}
	rollback := func(statementErr error) (querylogical.Result, error) {
		restoreErr := e.ports.Mutations.RestoreMutationState(context.WithoutCancel(ctx), execution.Transaction, savepoint)
		if restoreErr != nil {
			return querylogical.Result{}, querylogical.NewError(
				querylogical.ErrorDependencyFailure,
				"restore failed mutation statement",
				fmt.Sprintf("statement failed: %v; restore failed: %v", statementErr, restoreErr),
				restoreErr,
			)
		}
		return querylogical.Result{}, statementErr
	}

	mutation := *prepared.Statement.Mutation
	rows, counters, err := e.runMutation(ctx, execution, prepared, mutation, tracker)
	if err != nil {
		return rollback(err)
	}
	result, err := shapeMutationResult(ctx, rows, mutation, prepared, execution, tracker, budget)
	if err != nil {
		return rollback(err)
	}
	result.Counters.NodesInserted = counters.NodesInserted
	result.Counters.NodesUpdated = counters.NodesUpdated
	result.Counters.NodesDeleted = counters.NodesDeleted
	result.Counters.EdgesInserted = counters.EdgesInserted
	result.Counters.EdgesUpdated = counters.EdgesUpdated
	result.Counters.EdgesDeleted = counters.EdgesDeleted
	result.Diagnostics = mergeRuntimeDiagnostics(queryphysical.Explain(prepared.Physical), result.Diagnostics)
	result.Diagnostics.Planner = PlannerName
	result.Diagnostics.PlannerVersion = PlannerVersion
	return result, nil
}

func (e *Engine) runMutation(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) ([]queryexecution.BindingRow, querylogical.Counters, error) {
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
		return nil, querylogical.Counters{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "execute mutation", fmt.Sprintf("unsupported mutation kind %q", mutation.Kind), nil)
	}
}

func (e *Engine) insertNode(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation) ([]queryexecution.BindingRow, querylogical.Counters, error) {
	properties, err := resolveMutationValues(mutation.Node.Properties, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return nil, querylogical.Counters{}, err
	}
	_, err = e.ports.Mutations.CreateNode(ctx, execution.Transaction, graph.NodeInput{Labels: append([]string(nil), mutation.Node.Labels...), Properties: properties})
	if err := mutationDependencyError(ctx, "insert node", err); err != nil {
		return nil, querylogical.Counters{}, err
	}
	return nil, querylogical.Counters{NodesInserted: 1}, nil
}

func (e *Engine) mergeNode(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) ([]queryexecution.BindingRow, querylogical.Counters, error) {
	query := querylogical.Query{
		Source: querylogical.SourceGQL, AccessMode: querylogical.AccessModeReadOnly,
		Start: *mutation.Node, Page: querylogical.PageSpec{Limit: mutation.Limit},
	}
	matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
	if err != nil {
		return nil, querylogical.Counters{}, err
	}
	defer matches.Release()
	rows := cloneMutationRows(matches.Rows)
	counters := querylogical.Counters{}
	if len(rows) == 0 {
		properties, resolveErr := resolveMutationValues(mutation.Node.Properties, prepared.Planning.Parameters, execution.Now)
		if resolveErr != nil {
			return nil, counters, resolveErr
		}
		node, createErr := e.ports.Mutations.CreateNode(ctx, execution.Transaction, graph.NodeInput{Labels: append([]string(nil), mutation.Node.Labels...), Properties: properties})
		if err := mutationDependencyError(ctx, "merge node", createErr); err != nil {
			return nil, counters, err
		}
		rows = []queryexecution.BindingRow{{Nodes: map[string]graph.Node{mutation.Node.Alias: node}}}
		counters.NodesInserted = 1
	}
	prepareMutationGraph(rows, mutation.ReturnGraph)
	return rows, counters, nil
}

func (e *Engine) matchSet(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) ([]queryexecution.BindingRow, querylogical.Counters, error) {
	query := *mutation.Match
	query.Page.Limit = mutation.Limit
	matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
	if err != nil {
		return nil, querylogical.Counters{}, err
	}
	defer matches.Release()
	rows := cloneMutationRows(matches.Rows)
	assignmentValues := make([]any, len(mutation.Assignments))
	for index, assignment := range mutation.Assignments {
		value, resolveErr := queryexecution.StaticValue(assignment.Value, prepared.Planning.Parameters, execution.Now)
		if resolveErr != nil {
			return nil, querylogical.Counters{}, resolveErr
		}
		assignmentValues[index] = value
	}
	updatedNodes := map[string]struct{}{}
	updatedEdges := map[string]struct{}{}
	for index := range rows {
		for assignmentIndex, assignment := range mutation.Assignments {
			value := assignmentValues[assignmentIndex]
			if node, ok := rows[index].Nodes[assignment.Field.Alias]; ok {
				updated := cloneMutationNode(node)
				if err := assignNodeValue(&updated, assignment.Field, value); err != nil {
					return nil, querylogical.Counters{}, err
				}
				written, writeErr := e.ports.Mutations.UpdateNode(ctx, execution.Transaction, graph.UpdateNodeInput{
					NodeID: updated.ID.String(), Labels: append([]string(nil), updated.Labels...),
					Properties: cloneAnyMap(updated.Properties), Payload: cloneAnyMap(updated.Payload),
					UpdateMask: []string{"labels", "properties", "payload"},
				})
				if err := mutationDependencyError(ctx, "set node", writeErr); err != nil {
					return nil, querylogical.Counters{}, err
				}
				rows[index].Nodes[assignment.Field.Alias] = written
				updatedNodes[written.ID.String()] = struct{}{}
				continue
			}
			if edge, ok := rows[index].Edges[assignment.Field.Alias]; ok {
				updated := cloneMutationEdge(edge)
				if err := assignEdgeValue(&updated, assignment.Field, value); err != nil {
					return nil, querylogical.Counters{}, err
				}
				written, writeErr := e.ports.Mutations.UpdateEdge(ctx, execution.Transaction, graph.UpdateEdgeInput{
					EdgeID: updated.ID.String(), Labels: append([]string(nil), updated.Labels...),
					Properties: cloneAnyMap(updated.Properties), Payload: cloneAnyMap(updated.Payload),
					UpdateMask: []string{"labels", "properties", "payload"},
				})
				if err := mutationDependencyError(ctx, "set edge", writeErr); err != nil {
					return nil, querylogical.Counters{}, err
				}
				rows[index].Edges[assignment.Field.Alias] = written
				updatedEdges[written.ID.String()] = struct{}{}
				continue
			}
			return nil, querylogical.Counters{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "set mutation", fmt.Sprintf("alias %q is not bound", assignment.Field.Alias), nil)
		}
	}
	prepareMutationGraph(rows, mutation.ReturnGraph)
	return rows, querylogical.Counters{NodesUpdated: int64(len(updatedNodes)), EdgesUpdated: int64(len(updatedEdges))}, nil
}

func (e *Engine) matchDelete(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker) ([]queryexecution.BindingRow, querylogical.Counters, error) {
	query := *mutation.Match
	query.Page.Limit = mutation.Limit
	matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
	if err != nil {
		return nil, querylogical.Counters{}, err
	}
	defer matches.Release()
	rows := cloneMutationRows(matches.Rows)
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
		return nil, querylogical.Counters{}, err
	}
	for edgeID := range edges {
		_, deleteErr := e.ports.Mutations.DeleteEdge(ctx, execution.Transaction, edgeID)
		if err := mutationDependencyError(ctx, "delete edge", deleteErr); err != nil {
			return nil, querylogical.Counters{}, err
		}
	}
	for nodeID := range nodes {
		_, _, deleteErr := e.ports.Mutations.DeleteNode(ctx, execution.Transaction, nodeID, false)
		if err := mutationDependencyError(ctx, "delete node", deleteErr); err != nil {
			return nil, querylogical.Counters{}, err
		}
	}
	prepareMutationGraph(rows, mutation.ReturnGraph)
	return rows, querylogical.Counters{NodesDeleted: int64(len(nodes)), EdgesDeleted: int64(len(edges))}, nil
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

func (e *Engine) createRelationship(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, mutation querylogical.Mutation, tracker *queryexecution.Tracker, merge bool) ([]queryexecution.BindingRow, querylogical.Counters, error) {
	bindings := []queryexecution.BindingRow{{Nodes: map[string]graph.Node{}, Edges: map[string]graph.Edge{}}}
	var retained int64
	defer tracker.ReleaseMemory(retained)
	for _, pattern := range mutation.Nodes {
		query := querylogical.Query{Source: querylogical.SourceGQL, AccessMode: querylogical.AccessModeReadOnly, Start: pattern}
		matches, err := e.collectMutationMatches(ctx, execution, prepared, query, tracker)
		if err != nil {
			return nil, querylogical.Counters{}, err
		}
		next := make([]queryexecution.BindingRow, 0, len(bindings)*len(matches.Rows))
		for _, binding := range bindings {
			for _, matched := range matches.Rows {
				row := cloneMutationRow(binding)
				row.Nodes[pattern.Alias] = matched.Nodes[pattern.Alias]
				bytes := queryexecution.EstimateBindingRow(row)
				if err := tracker.ProduceIntermediate(1); err != nil {
					matches.Release()
					return nil, querylogical.Counters{}, err
				}
				if err := tracker.ReserveMemory(bytes); err != nil {
					matches.Release()
					return nil, querylogical.Counters{}, err
				}
				retained += bytes
				next = append(next, row)
			}
		}
		matches.Release()
		bindings = next
	}
	if merge && len(bindings) > 1 {
		return nil, querylogical.Counters{}, querylogical.NewError(querylogical.ErrorInvalidQuery, "merge relationship", fmt.Sprintf("matched %d endpoint bindings; refine endpoint predicates", len(bindings)), nil)
	}
	properties, err := resolveMutationValues(mutation.Relationship.Properties, prepared.Planning.Parameters, execution.Now)
	if err != nil {
		return nil, querylogical.Counters{}, err
	}
	counters := querylogical.Counters{}
	rows := make([]queryexecution.BindingRow, 0, len(bindings))
	for _, binding := range bindings {
		from, fromOK := binding.Nodes[mutation.Relationship.FromAlias]
		to, toOK := binding.Nodes[mutation.Relationship.ToAlias]
		if !fromOK || !toOK {
			return nil, counters, querylogical.NewError(querylogical.ErrorInvalidQuery, "create relationship", "relationship endpoint is not bound", nil)
		}
		var edge graph.Edge
		if merge {
			existing, findErr := e.findRelationship(ctx, execution, prepared, tracker, from, to, mutation.Relationship)
			if findErr != nil {
				return nil, counters, findErr
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
				return nil, counters, err
			}
			edge = created
			counters.EdgesInserted++
		}
		if merge {
			row := cloneMutationRow(binding)
			if mutation.Relationship.Alias != "" {
				row.Edges[mutation.Relationship.Alias] = edge
			}
			rows = append(rows, row)
		}
	}
	if merge && mutation.Limit > 0 && int64(len(rows)) > mutation.Limit {
		rows = rows[:mutation.Limit]
	}
	prepareMutationGraph(rows, mutation.ReturnGraph)
	return rows, counters, nil
}

func (e *Engine) findRelationship(ctx context.Context, execution ExecutionContext, prepared PreparedQuery, tracker *queryexecution.Tracker, from, to graph.Node, relationship *querylogical.RelationshipMutation) (*graph.Edge, error) {
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
		return nil, err
	}
	defer matches.Release()
	if len(matches.Rows) == 0 {
		return nil, nil
	}
	edge := cloneMutationEdge(matches.Rows[0].Edges["__merge_edge"])
	return &edge, nil
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

func cloneMutationRows(rows []queryexecution.BindingRow) []queryexecution.BindingRow {
	out := make([]queryexecution.BindingRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, cloneMutationRow(row))
	}
	return out
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
