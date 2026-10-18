package execution

import (
	"context"
	"fmt"
	"io"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

// CollectExpand preserves the structured broad-traversal contract that emits
// one row per root while collecting each step's matched targets. It replaces
// whole-domain edge materialization with finite adjacency pages and endpoint
// reads. GQL and explicit structured path queries use PathExpand instead.
type CollectExpand struct {
	unaryOperator
	startAlias string
	steps      []querylogical.Step
	edgeReader []TraversalEdgePageReader
	nodeReader TraversalNodeReader
	batchSize  int
	tracker    *Tracker
	eval       evaluator

	outputs      []bufferedTraversalRow
	outputOffset int
	retained     int64
}

type collectFrontier struct {
	node  graph.Node
	depth int
}

func NewCollectExpand(input Operator, startAlias string, steps []querylogical.Step, edgeReaders []TraversalEdgePageReader, nodeReader TraversalNodeReader, batchSize int, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *CollectExpand {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &CollectExpand{
		unaryOperator: unaryOperator{input: input}, startAlias: startAlias,
		steps: append([]querylogical.Step(nil), steps...), edgeReader: append([]TraversalEdgePageReader(nil), edgeReaders...),
		nodeReader: nodeReader, batchSize: batchSize, tracker: tracker, eval: newEvaluator(parameters, now),
	}
}

func (o *CollectExpand) Open(ctx context.Context) error {
	if o.nodeReader == nil || len(o.steps) == 0 || len(o.edgeReader) != len(o.steps) {
		return invalidState("open broad traversal collection", "steps and traversal readers are required")
	}
	for index, reader := range o.edgeReader {
		if reader == nil {
			return invalidState("open broad traversal collection", fmt.Sprintf("edge reader %d is required", index))
		}
		step := o.steps[index]
		if len(step.Edge.Labels) != 1 || (step.Edge.Direction != querylogical.DirectionIncoming && step.Edge.Direction != querylogical.DirectionOutgoing) || step.Depth.Max <= 0 || step.Depth.Min < 0 || step.Depth.Min > step.Depth.Max {
			return invalidState("open broad traversal collection", fmt.Sprintf("traversal step %d is invalid", index))
		}
	}
	o.releaseOutputs()
	return o.open(ctx, "open broad traversal collection")
}

func (o *CollectExpand) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "collect broad traversal"); err != nil {
		return Batch{}, err
	}
	for o.outputOffset >= len(o.outputs) {
		o.releaseOutputs()
		batch, err := o.input.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return Batch{}, io.EOF
			}
			return Batch{}, o.fail(err)
		}
		for _, row := range batch.Rows {
			if err := o.expandRow(ctx, row); err != nil {
				return Batch{}, o.fail(err)
			}
		}
	}
	end := o.outputOffset + o.batchSize
	if end > len(o.outputs) {
		end = len(o.outputs)
	}
	rows := make([]BindingRow, 0, end-o.outputOffset)
	for index := o.outputOffset; index < end; index++ {
		item := &o.outputs[index]
		rows = append(rows, item.row)
		o.releaseItem(item)
	}
	o.outputOffset = end
	return Batch{Rows: rows}, nil
}

func (o *CollectExpand) expandRow(ctx context.Context, row BindingRow) (err error) {
	root, ok := row.Nodes[o.startAlias]
	if !ok {
		return executionInvalid("collect broad traversal", fmt.Sprintf("node alias %q is not bound", o.startAlias))
	}
	out := cloneBindingRow(row)
	current := []graph.Node{root}
	reserved := int64(0)
	defer func() {
		if err != nil && o.tracker != nil && reserved > 0 {
			o.tracker.ReleaseMemory(reserved)
		}
	}()
	if err = o.resizeReservation(&reserved, out, current, nil, nil, nil, nil, nil); err != nil {
		return err
	}

	for index, step := range o.steps {
		matched, firstEdge, parents, order, collectErr := o.collectStep(ctx, &out, current, step, o.edgeReader[index], &reserved)
		if collectErr != nil {
			return collectErr
		}
		if len(matched) > 0 {
			out.Nodes[step.Target.Alias] = matched[0]
		} else {
			delete(out.Nodes, step.Target.Alias)
		}
		if step.Edge.Alias != "" && firstEdge != nil {
			out.Edges[step.Edge.Alias] = *firstEdge
		} else if step.Edge.Alias != "" {
			delete(out.Edges, step.Edge.Alias)
		}
		if out.Trees == nil {
			out.Trees = map[string]querylogical.Tree{}
		}
		out.Trees[step.Target.Alias] = buildTree(matched, parents, order)
		current = matched
		if err = o.resizeReservation(&reserved, out, current, nil, nil, nil, nil, nil); err != nil {
			return err
		}
	}
	if o.tracker != nil {
		if err = o.tracker.ProduceIntermediate(1); err != nil {
			return err
		}
	}
	o.retained += reserved
	o.outputs = append(o.outputs, bufferedTraversalRow{row: out, bytes: reserved})
	return nil
}

func (o *CollectExpand) collectStep(ctx context.Context, row *BindingRow, starts []graph.Node, step querylogical.Step, reader TraversalEdgePageReader, reserved *int64) ([]graph.Node, *graph.Edge, map[string]string, map[string]any, error) {
	matched := []graph.Node{}
	matchedIDs := map[string]struct{}{}
	var firstEdge *graph.Edge
	parents := map[string]string{}
	order := map[string]any{}
	for _, start := range starts {
		visited := map[string]struct{}{}
		frontier := []collectFrontier{{node: start}}
		for len(frontier) > 0 {
			current := frontier[0]
			frontier = frontier[1:]
			if current.depth >= step.Depth.Max {
				continue
			}
			cursor := ""
			for {
				limit := o.batchSize
				if o.tracker != nil {
					remaining := o.tracker.RemainingEdgesLoaded()
					if remaining == 0 {
						return nil, nil, nil, nil, o.tracker.LoadEdges(1)
					}
					if remaining < int64(limit) {
						limit = int(remaining)
					}
					if err := o.tracker.UseAdjacency(1); err != nil {
						return nil, nil, nil, nil, err
					}
				}
				page, readErr := reader(ctx, current.node.ID.String(), limit, cursor)
				if err := traversalDependencyError(ctx, "scan broad traversal adjacency", readErr); err != nil {
					return nil, nil, nil, nil, err
				}
				if err := canceled(ctx, "scan broad traversal adjacency"); err != nil {
					return nil, nil, nil, nil, err
				}
				if len(page.Edges) > limit {
					return nil, nil, nil, nil, invalidState("scan broad traversal adjacency", "access method returned an oversized page")
				}
				if len(page.Edges) == 0 && page.NextCursor != "" && page.NextCursor == cursor {
					return nil, nil, nil, nil, querylogical.NewError(querylogical.ErrorDependencyFailure, "scan broad traversal adjacency", "access method returned a non-progressing empty page", nil)
				}
				if err := withTraversalPageMemory(o.tracker, page.Edges, func() error {
					if o.tracker != nil {
						indexEntries := page.IndexEntries
						if indexEntries < int64(len(page.Edges)) {
							indexEntries = int64(len(page.Edges))
						}
						if err := o.tracker.ScanIndexEntries(indexEntries); err != nil {
							return err
						}
						edgesLoaded := page.EdgesLoaded
						if edgesLoaded < int64(len(page.Edges)) {
							edgesLoaded = int64(len(page.Edges))
						}
						if err := o.tracker.LoadEdges(edgesLoaded); err != nil {
							return err
						}
					}
					for _, edge := range page.Edges {
						if err := canceled(ctx, "collect broad traversal"); err != nil {
							return err
						}
						if o.tracker != nil {
							if err := o.tracker.ExamineCandidates(1); err != nil {
								return err
							}
						}
						edgeMatched, err := matchesEdgePattern(edge, step.Edge, *row, o.eval)
						if err != nil {
							return err
						}
						if !edgeMatched {
							continue
						}
						endpointID := traversalEndpoint(edge, step.Edge.Direction)
						if _, seen := visited[endpointID]; seen {
							continue
						}
						if o.tracker != nil && o.tracker.RemainingNodesLoaded() == 0 {
							return o.tracker.LoadNodes(1)
						}
						endpoint, readErr := o.nodeReader(ctx, endpointID)
						if err := traversalDependencyError(ctx, "read broad traversal endpoint", readErr); err != nil {
							return err
						}
						if err := canceled(ctx, "read broad traversal endpoint"); err != nil {
							return err
						}
						if o.tracker != nil {
							if err := o.tracker.LoadNodes(1); err != nil {
								return err
							}
							if err := o.tracker.RecordNodeReads(1); err != nil {
								return err
							}
						}
						childDepth := current.depth + 1
						if o.tracker != nil {
							if err := o.tracker.ObserveTraversalDepth(childDepth); err != nil {
								return err
							}
						}
						visited[endpointID] = struct{}{}
						matchRow := cloneBindingRow(*row)
						matchRow.Nodes[step.Target.Alias] = endpoint
						targetMatched, err := matchesNodePattern(matchRow, endpoint, step.Target, o.eval)
						if err != nil {
							return err
						}
						if childDepth >= step.Depth.Min && targetMatched {
							if _, duplicate := matchedIDs[endpointID]; !duplicate {
								matchedIDs[endpointID] = struct{}{}
								matched = append(matched, endpoint)
								if firstEdge == nil {
									first := edge
									firstEdge = &first
								}
							}
						}
						if step.Edge.Direction == querylogical.DirectionOutgoing && step.Edge.Labels[0] == "contains" {
							parents[endpointID] = current.node.ID.String()
							order[endpointID] = edge.Properties["order"]
						}
						if childDepth < step.Depth.Max {
							frontier = append(frontier, collectFrontier{node: endpoint, depth: childDepth})
						}
						if err := o.resizeReservation(reserved, *row, starts, matched, frontier, visited, parents, order); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					return nil, nil, nil, nil, err
				}
				if page.NextCursor == "" {
					break
				}
				cursor = page.NextCursor
			}
		}
	}
	return matched, firstEdge, parents, order, nil
}

func (o *CollectExpand) resizeReservation(reserved *int64, row BindingRow, current []graph.Node, matched []graph.Node, frontier []collectFrontier, visited map[string]struct{}, parents map[string]string, order map[string]any) error {
	bytes := estimateBindingRow(row)
	for _, node := range current {
		bytes += estimateBindingRow(BindingRow{GraphNodes: []graph.Node{node}})
	}
	for _, node := range matched {
		bytes += estimateBindingRow(BindingRow{GraphNodes: []graph.Node{node}})
	}
	for _, item := range frontier {
		bytes += estimateBindingRow(BindingRow{GraphNodes: []graph.Node{item.node}})
	}
	bytes += int64(len(visited)+len(parents)+len(order)) * 64
	if bytes <= *reserved {
		return nil
	}
	if o.tracker != nil {
		if err := o.tracker.ReserveMemory(bytes - *reserved); err != nil {
			return err
		}
	}
	*reserved = bytes
	return nil
}

func (o *CollectExpand) releaseItem(item *bufferedTraversalRow) {
	if item == nil || item.bytes == 0 {
		return
	}
	if o.tracker != nil {
		o.tracker.ReleaseMemory(item.bytes)
	}
	o.retained -= item.bytes
	item.bytes = 0
}

func (o *CollectExpand) releaseOutputs() {
	for index := range o.outputs {
		o.releaseItem(&o.outputs[index])
	}
	o.outputs = nil
	o.outputOffset = 0
}

func (o *CollectExpand) Close() error {
	o.releaseOutputs()
	o.retained = 0
	return o.close()
}
