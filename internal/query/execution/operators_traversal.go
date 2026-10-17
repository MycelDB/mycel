package execution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

// TraversalEdgePage is one finite adjacency-index page.
type TraversalEdgePage struct {
	Edges        []graph.Edge
	NextCursor   string
	IndexEntries int64
	EdgesLoaded  int64
}

// TraversalEdgePageReader scans one finite adjacency page for a node.
type TraversalEdgePageReader func(context.Context, string, int, string) (TraversalEdgePage, error)

// TraversalNodeReader hydrates one traversal endpoint at the execution snapshot.
type TraversalNodeReader func(context.Context, string) (graph.Node, error)

// AdjacencyCheckpointEncoder binds an adjacency frontier to the prepared plan.
type AdjacencyCheckpointEncoder func(startIndex int, pageCursor string, pageOffset int) string

type bufferedTraversalRow struct {
	row   BindingRow
	bytes int64
}

func withTraversalPageMemory(tracker *Tracker, edges []graph.Edge, visit func() error) error {
	if tracker == nil || len(edges) == 0 {
		return visit()
	}
	var bytes int64
	for _, edge := range edges {
		bytes += estimateBindingRow(BindingRow{GraphEdges: []graph.Edge{edge}})
	}
	if err := tracker.ReserveMemory(bytes); err != nil {
		return err
	}
	defer tracker.ReleaseMemory(bytes)
	return visit()
}

// AdjacencySource streams a one-hop explicit-start traversal without loading a
// domain edge set. Its checkpoint identifies the edge candidate before target
// and residual filtering, so filtered pages cannot skip matches.
type AdjacencySource struct {
	startAlias      string
	startPattern    querylogical.NodePattern
	step            querylogical.Step
	startIDs        []string
	nodeReader      TraversalNodeReader
	edgeReader      TraversalEdgePageReader
	checkpoint      AdjacencyCheckpointEncoder
	resumeStart     int
	resumeCursor    string
	resumeOffset    int
	resumeRequested bool
	batchSize       int
	tracker         *Tracker
	eval            evaluator

	startIndex    int
	startNode     graph.Node
	startLoaded   bool
	startBytes    int64
	pageCursor    string
	nextCursor    string
	edges         []graph.Edge
	edgeSizes     []int64
	edgeOffset    int
	edgeRetained  int64
	pageLoaded    bool
	resuming      bool
	pendingResume int
	sequence      int64
	opened        bool
	closed        bool
}

func NewAdjacencySource(startAlias string, startPattern querylogical.NodePattern, step querylogical.Step, startIDs []string, nodeReader TraversalNodeReader, edgeReader TraversalEdgePageReader, checkpoint AdjacencyCheckpointEncoder, resumeStart int, resumeCursor string, resumeOffset int, resuming bool, batchSize int, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *AdjacencySource {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &AdjacencySource{
		startAlias: startAlias, startPattern: startPattern, step: step,
		startIDs: append([]string(nil), startIDs...), nodeReader: nodeReader, edgeReader: edgeReader,
		checkpoint: checkpoint, resumeStart: resumeStart, resumeCursor: resumeCursor,
		resumeOffset: resumeOffset, resumeRequested: resuming, batchSize: batchSize,
		tracker: tracker, eval: newEvaluator(parameters, now),
	}
}

func (s *AdjacencySource) Open(ctx context.Context) error {
	if err := canceled(ctx, "open adjacency source"); err != nil {
		return err
	}
	if s.opened && !s.closed {
		return invalidState("open adjacency source", "operator is already open")
	}
	if s.nodeReader == nil || s.edgeReader == nil {
		return invalidState("open adjacency source", "node and edge readers are required")
	}
	if s.resumeStart < 0 || s.resumeStart >= len(s.startIDs) {
		return querylogical.NewError(querylogical.ErrorStaleCursor, "resume adjacency source", "start position is outside the explicit start set", nil)
	}
	if s.resumeOffset < 0 || s.resumeOffset >= s.batchSize {
		return querylogical.NewError(querylogical.ErrorStaleCursor, "resume adjacency source", "edge offset is outside the bounded page", nil)
	}
	s.releaseStart()
	s.releaseEdges()
	s.startIndex = s.resumeStart
	s.pageCursor = s.resumeCursor
	s.pendingResume = s.resumeOffset
	s.resuming = s.resumeRequested
	s.sequence = 0
	s.opened = true
	s.closed = false
	return nil
}

func (s *AdjacencySource) Next(ctx context.Context) (Batch, error) {
	if err := s.ready(ctx); err != nil {
		return Batch{}, err
	}
	rows := make([]BindingRow, 0, s.batchSize)
	for len(rows) < s.batchSize {
		row, err := s.nextRow(ctx)
		if err == io.EOF {
			if len(rows) != 0 {
				return Batch{Rows: rows}, nil
			}
			return Batch{}, io.EOF
		}
		if err != nil {
			_ = s.Close()
			return Batch{}, err
		}
		row.Sequence = s.sequence
		s.sequence++
		rows = append(rows, row)
	}
	return Batch{Rows: rows}, nil
}

func (s *AdjacencySource) nextRow(ctx context.Context) (BindingRow, error) {
	for {
		if s.startIndex >= len(s.startIDs) {
			return BindingRow{}, io.EOF
		}
		if !s.startLoaded {
			if err := s.loadStart(ctx); err != nil {
				return BindingRow{}, err
			}
			seed := BindingRow{Nodes: map[string]graph.Node{s.startAlias: s.startNode}}
			matched, err := matchesNodePattern(seed, s.startNode, s.startPattern, s.eval)
			if err != nil {
				return BindingRow{}, err
			}
			if !matched {
				s.advanceStart()
				continue
			}
		}
		if !s.pageLoaded || s.edgeOffset >= len(s.edges) {
			if s.pageLoaded && s.nextCursor == "" {
				s.advanceStart()
				continue
			}
			if err := s.loadEdgePage(ctx); err != nil {
				return BindingRow{}, err
			}
			if len(s.edges) == 0 {
				if s.nextCursor == "" {
					s.advanceStart()
					continue
				}
				return BindingRow{}, querylogical.NewError(querylogical.ErrorDependencyFailure, "scan adjacency", "access method returned an empty non-terminal page", nil)
			}
		}

		index := s.edgeOffset
		edge := s.edges[index]
		checkpoint := ""
		if s.checkpoint != nil {
			checkpoint = s.checkpoint(s.startIndex, s.pageCursor, index)
		}
		s.releaseEdge(index)
		s.edgeOffset++
		if s.tracker != nil {
			if err := s.tracker.ExamineCandidates(1); err != nil {
				return BindingRow{}, err
			}
		}
		seed := BindingRow{
			Nodes: map[string]graph.Node{s.startAlias: s.startNode},
			Edges: map[string]graph.Edge{},
		}
		edgeMatched, err := matchesEdgePattern(edge, s.step.Edge, seed, s.eval)
		if err != nil {
			return BindingRow{}, err
		}
		if !edgeMatched {
			continue
		}
		endpointID := traversalEndpoint(edge, s.step.Edge.Direction)
		endpoint, err := s.readNode(ctx, endpointID)
		if err != nil {
			return BindingRow{}, err
		}
		seed.Nodes[s.step.Target.Alias] = endpoint
		if s.step.Edge.Alias != "" {
			seed.Edges[s.step.Edge.Alias] = edge
		}
		matched, err := matchesNodePattern(seed, endpoint, s.step.Target, s.eval)
		if err != nil {
			return BindingRow{}, err
		}
		if !matched {
			continue
		}
		seed.Cursor = checkpoint
		if s.tracker != nil {
			if err := s.tracker.ProduceIntermediate(1); err != nil {
				return BindingRow{}, err
			}
		}
		return seed, nil
	}
}

func (s *AdjacencySource) loadStart(ctx context.Context) error {
	if s.tracker != nil && s.tracker.RemainingNodesLoaded() == 0 {
		return s.tracker.LoadNodes(1)
	}
	node, err := s.nodeReader(ctx, s.startIDs[s.startIndex])
	if err := traversalDependencyError(ctx, "read adjacency start node", err); err != nil {
		return err
	}
	if err := canceled(ctx, "read adjacency start node"); err != nil {
		return err
	}
	if s.tracker != nil {
		if err := s.tracker.UseStartNodes(1); err != nil {
			return err
		}
		if err := s.tracker.LoadNodes(1); err != nil {
			return err
		}
		if err := s.tracker.RecordNodeReads(1); err != nil {
			return err
		}
	}
	s.startNode = node
	s.startLoaded = true
	s.startBytes = estimateBindingRow(BindingRow{Nodes: map[string]graph.Node{s.startAlias: node}})
	if s.tracker != nil {
		if err := s.tracker.ReserveMemory(s.startBytes); err != nil {
			s.startBytes = 0
			return err
		}
	}
	return nil
}

func (s *AdjacencySource) loadEdgePage(ctx context.Context) error {
	cursor := s.pageCursor
	if s.pageLoaded {
		cursor = s.nextCursor
	}
	limit := s.batchSize
	if s.tracker != nil {
		remaining := s.tracker.RemainingEdgesLoaded()
		if remaining == 0 {
			return s.tracker.LoadEdges(1)
		}
		if remaining < int64(limit) {
			limit = int(remaining)
		}
	}
	if s.tracker != nil {
		if err := s.tracker.UseAdjacency(1); err != nil {
			return err
		}
	}
	page, err := s.edgeReader(ctx, s.startNode.ID.String(), limit, cursor)
	if err := traversalDependencyError(ctx, "scan adjacency", err); err != nil {
		return err
	}
	if err := canceled(ctx, "scan adjacency"); err != nil {
		return err
	}
	if len(page.Edges) > limit {
		return invalidState("scan adjacency", "access method returned an oversized page")
	}
	if s.resuming && s.pendingResume >= len(page.Edges) {
		return querylogical.NewError(querylogical.ErrorStaleCursor, "resume adjacency source", "edge offset exceeds the adjacency page", nil)
	}
	if s.tracker != nil {
		if err := s.tracker.ScanIndexEntries(page.IndexEntries); err != nil {
			return err
		}
		if err := s.tracker.LoadEdges(page.EdgesLoaded); err != nil {
			return err
		}
	}
	s.releaseEdges()
	s.pageCursor = cursor
	s.nextCursor = page.NextCursor
	s.edges = append([]graph.Edge(nil), page.Edges...)
	s.edgeSizes = make([]int64, len(s.edges))
	for i, edge := range s.edges {
		bytes := estimateBindingRow(BindingRow{GraphEdges: []graph.Edge{edge}})
		if s.tracker != nil {
			if err := s.tracker.ReserveMemory(bytes); err != nil {
				s.releaseEdges()
				return err
			}
		}
		s.edgeSizes[i] = bytes
		s.edgeRetained += bytes
	}
	s.edgeOffset = 0
	s.pageLoaded = true
	if s.resuming {
		for i := 0; i < s.pendingResume; i++ {
			s.releaseEdge(i)
		}
		s.edgeOffset = s.pendingResume
		s.pendingResume = 0
		s.resuming = false
	}
	return nil
}

func (s *AdjacencySource) readNode(ctx context.Context, id string) (graph.Node, error) {
	if s.tracker != nil && s.tracker.RemainingNodesLoaded() == 0 {
		return graph.Node{}, s.tracker.LoadNodes(1)
	}
	node, err := s.nodeReader(ctx, id)
	if err := traversalDependencyError(ctx, "read adjacency endpoint", err); err != nil {
		return graph.Node{}, err
	}
	if err := canceled(ctx, "read adjacency endpoint"); err != nil {
		return graph.Node{}, err
	}
	if s.tracker != nil {
		if err := s.tracker.LoadNodes(1); err != nil {
			return graph.Node{}, err
		}
		if err := s.tracker.RecordNodeReads(1); err != nil {
			return graph.Node{}, err
		}
	}
	return node, nil
}

func (s *AdjacencySource) advanceStart() {
	s.releaseStart()
	s.releaseEdges()
	s.startIndex++
	s.startLoaded = false
	s.pageCursor = ""
	s.nextCursor = ""
	s.pageLoaded = false
	s.edgeOffset = 0
	s.resuming = false
	s.pendingResume = 0
}

func (s *AdjacencySource) releaseEdge(index int) {
	if index < 0 || index >= len(s.edgeSizes) || s.edgeSizes[index] == 0 {
		return
	}
	if s.tracker != nil {
		s.tracker.ReleaseMemory(s.edgeSizes[index])
	}
	s.edgeRetained -= s.edgeSizes[index]
	s.edgeSizes[index] = 0
}

func (s *AdjacencySource) releaseEdges() {
	if s.tracker != nil && s.edgeRetained > 0 {
		s.tracker.ReleaseMemory(s.edgeRetained)
	}
	s.edgeRetained = 0
	s.edges = nil
	s.edgeSizes = nil
	s.edgeOffset = 0
}

func (s *AdjacencySource) releaseStart() {
	if s.tracker != nil && s.startBytes > 0 {
		s.tracker.ReleaseMemory(s.startBytes)
	}
	s.startBytes = 0
	s.startNode = graph.Node{}
}

func (s *AdjacencySource) ready(ctx context.Context) error {
	if err := canceled(ctx, "read adjacency source"); err != nil {
		_ = s.Close()
		return err
	}
	if !s.opened {
		return invalidState("read adjacency source", "operator is not open")
	}
	if s.closed {
		return io.EOF
	}
	return nil
}

func (s *AdjacencySource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.releaseEdges()
	s.releaseStart()
	return nil
}

// PathExpand expands one logical traversal step over an input binding stream.
type PathExpand struct {
	unaryOperator
	currentAlias string
	step         querylogical.Step
	pathAlias    string
	edgeReader   TraversalEdgePageReader
	nodeReader   TraversalNodeReader
	batchSize    int
	tracker      *Tracker
	eval         evaluator

	inputs       []bufferedTraversalRow
	inputOffset  int
	outputs      []bufferedTraversalRow
	outputOffset int
	retained     int64
	sequence     int64
}

func NewPathExpand(input Operator, currentAlias string, step querylogical.Step, pathAlias string, edgeReader TraversalEdgePageReader, nodeReader TraversalNodeReader, batchSize int, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *PathExpand {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &PathExpand{
		unaryOperator: unaryOperator{input: input}, currentAlias: currentAlias, step: step,
		pathAlias: pathAlias, edgeReader: edgeReader, nodeReader: nodeReader,
		batchSize: batchSize, tracker: tracker, eval: newEvaluator(parameters, now),
	}
}

func (o *PathExpand) Open(ctx context.Context) error {
	if o.edgeReader == nil || o.nodeReader == nil {
		return invalidState("open path expansion", "node and edge readers are required")
	}
	o.releaseAll()
	o.sequence = 0
	return o.open(ctx, "open path expansion")
}

func (o *PathExpand) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "expand traversal path"); err != nil {
		return Batch{}, err
	}
	for o.outputOffset >= len(o.outputs) {
		o.releaseOutputs()
		row, err := o.nextInput(ctx)
		if err != nil {
			if err == io.EOF {
				return Batch{}, io.EOF
			}
			return Batch{}, o.fail(err)
		}
		rowBytes := estimateBindingRow(row)
		if o.tracker != nil {
			if err := o.tracker.ReserveMemory(rowBytes); err != nil {
				return Batch{}, o.fail(err)
			}
		}
		err = o.expand(ctx, row)
		if o.tracker != nil {
			o.tracker.ReleaseMemory(rowBytes)
		}
		if err != nil {
			return Batch{}, o.fail(err)
		}
	}
	end := o.outputOffset + o.batchSize
	if end > len(o.outputs) {
		end = len(o.outputs)
	}
	rows := make([]BindingRow, 0, end-o.outputOffset)
	for index := o.outputOffset; index < end; index++ {
		item := &o.outputs[index]
		item.row.Sequence = o.sequence
		o.sequence++
		rows = append(rows, item.row)
		o.releaseItem(item)
	}
	o.outputOffset = end
	return Batch{Rows: rows}, nil
}

func (o *PathExpand) nextInput(ctx context.Context) (BindingRow, error) {
	for o.inputOffset >= len(o.inputs) {
		o.releaseInputs()
		batch, err := o.input.Next(ctx)
		if err != nil {
			return BindingRow{}, err
		}
		if len(batch.Rows) == 0 {
			continue
		}
		o.inputs = make([]bufferedTraversalRow, 0, len(batch.Rows))
		for _, row := range batch.Rows {
			bytes := estimateBindingRow(row)
			if o.tracker != nil {
				if err := o.tracker.ReserveMemory(bytes); err != nil {
					o.releaseInputs()
					return BindingRow{}, err
				}
			}
			o.retained += bytes
			o.inputs = append(o.inputs, bufferedTraversalRow{row: row, bytes: bytes})
		}
	}
	item := &o.inputs[o.inputOffset]
	row := item.row
	o.releaseItem(item)
	o.inputOffset++
	return row, nil
}

func (o *PathExpand) expand(ctx context.Context, row BindingRow) error {
	start, ok := row.Nodes[o.currentAlias]
	if !ok {
		return executionInvalid("expand traversal path", fmt.Sprintf("node alias %q is not bound", o.currentAlias))
	}
	if o.pathAlias != "" {
		if row.Paths == nil {
			row.Paths = map[string]querylogical.Path{}
		}
		if _, ok := row.Paths[o.pathAlias]; !ok {
			row.Paths[o.pathAlias] = querylogical.Path{Nodes: []querylogical.Node{neutralNode(start)}}
		}
	}
	seen := map[string]struct{}{}
	if path, ok := row.Paths[o.pathAlias]; ok {
		for _, node := range path.Nodes {
			seen[node.ID] = struct{}{}
		}
	}
	if len(seen) == 0 {
		for _, node := range row.Nodes {
			seen[node.ID.String()] = struct{}{}
		}
	}
	return o.visit(ctx, row, start, 0, seen)
}

func (o *PathExpand) visit(ctx context.Context, base BindingRow, node graph.Node, depth int, seen map[string]struct{}) error {
	if depth >= o.step.Depth.Max {
		return nil
	}
	cursor := ""
	for {
		limit := o.batchSize
		if o.tracker != nil {
			remaining := o.tracker.RemainingEdgesLoaded()
			if remaining == 0 {
				return o.tracker.LoadEdges(1)
			}
			if remaining < int64(limit) {
				limit = int(remaining)
			}
		}
		if o.tracker != nil {
			if err := o.tracker.UseAdjacency(1); err != nil {
				return err
			}
		}
		page, err := o.edgeReader(ctx, node.ID.String(), limit, cursor)
		if err := traversalDependencyError(ctx, "scan path adjacency", err); err != nil {
			return err
		}
		if err := canceled(ctx, "scan path adjacency"); err != nil {
			return err
		}
		if len(page.Edges) > limit {
			return invalidState("scan path adjacency", "access method returned an oversized page")
		}
		if len(page.Edges) == 0 && page.NextCursor != "" {
			return querylogical.NewError(querylogical.ErrorDependencyFailure, "scan path adjacency", "access method returned an empty non-terminal page", nil)
		}
		if o.tracker != nil {
			if err := o.tracker.ScanIndexEntries(page.IndexEntries); err != nil {
				return err
			}
			if err := o.tracker.LoadEdges(page.EdgesLoaded); err != nil {
				return err
			}
		}
		if err := withTraversalPageMemory(o.tracker, page.Edges, func() error {
			for _, edge := range page.Edges {
				if o.tracker != nil {
					if err := o.tracker.ExamineCandidates(1); err != nil {
						return err
					}
				}
				edgeMatched, err := matchesEdgePattern(edge, o.step.Edge, base, o.eval)
				if err != nil {
					return err
				}
				if !edgeMatched {
					continue
				}
				endpointID := traversalEndpoint(edge, o.step.Edge.Direction)
				if _, cycle := seen[endpointID]; cycle {
					continue
				}
				if o.tracker != nil && o.tracker.RemainingNodesLoaded() == 0 {
					return o.tracker.LoadNodes(1)
				}
				endpoint, err := o.nodeReader(ctx, endpointID)
				if err := traversalDependencyError(ctx, "read path endpoint", err); err != nil {
					return err
				}
				if err := canceled(ctx, "read path endpoint"); err != nil {
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
				childDepth := depth + 1
				if o.tracker != nil {
					if err := o.tracker.ObserveTraversalDepth(childDepth); err != nil {
						return err
					}
				}
				child := cloneBindingRow(base)
				if child.Nodes == nil {
					child.Nodes = map[string]graph.Node{}
				}
				child.Nodes[o.step.Target.Alias] = endpoint
				if o.step.Edge.Alias != "" {
					if child.Edges == nil {
						child.Edges = map[string]graph.Edge{}
					}
					child.Edges[o.step.Edge.Alias] = edge
				}
				if o.pathAlias != "" {
					path := child.Paths[o.pathAlias]
					path.Nodes = append(path.Nodes, neutralNode(endpoint))
					path.Edges = append(path.Edges, neutralEdge(edge))
					child.Paths[o.pathAlias] = path
				}
				child.Cursor = ""
				matched, err := matchesNodePattern(child, endpoint, o.step.Target, o.eval)
				if err != nil {
					return err
				}
				if childDepth >= o.step.Depth.Min && matched {
					if err := o.appendOutput(child); err != nil {
						return err
					}
				}
				childBytes := estimateBindingRow(child)
				if o.tracker != nil {
					if err := o.tracker.ReserveMemory(childBytes); err != nil {
						return err
					}
				}
				nextSeen := cloneSeen(seen)
				nextSeen[endpointID] = struct{}{}
				err = o.visit(ctx, child, endpoint, childDepth, nextSeen)
				if o.tracker != nil {
					o.tracker.ReleaseMemory(childBytes)
				}
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if page.NextCursor == "" {
			return nil
		}
		cursor = page.NextCursor
	}
}

func (o *PathExpand) appendOutput(row BindingRow) error {
	bytes := estimateBindingRow(row)
	if o.tracker != nil {
		if err := o.tracker.ProduceIntermediate(1); err != nil {
			return err
		}
		if err := o.tracker.ReserveMemory(bytes); err != nil {
			return err
		}
	}
	o.retained += bytes
	o.outputs = append(o.outputs, bufferedTraversalRow{row: row, bytes: bytes})
	return nil
}

func (o *PathExpand) releaseItem(item *bufferedTraversalRow) {
	if item == nil || item.bytes == 0 {
		return
	}
	if o.tracker != nil {
		o.tracker.ReleaseMemory(item.bytes)
	}
	o.retained -= item.bytes
	item.bytes = 0
}

func (o *PathExpand) releaseInputs() {
	for i := range o.inputs {
		o.releaseItem(&o.inputs[i])
	}
	o.inputs = nil
	o.inputOffset = 0
}

func (o *PathExpand) releaseOutputs() {
	for i := range o.outputs {
		o.releaseItem(&o.outputs[i])
	}
	o.outputs = nil
	o.outputOffset = 0
}

func (o *PathExpand) releaseAll() {
	o.releaseInputs()
	o.releaseOutputs()
	o.retained = 0
}

func (o *PathExpand) Close() error {
	o.releaseAll()
	return o.close()
}

// SubtreeExpand performs a bounded breadth-first hierarchy traversal for each
// indexed root. Adjacency is pulled in finite pages and each endpoint is read
// through the graph transaction owned by the engine adapter.
type SubtreeExpand struct {
	unaryOperator
	startAlias string
	step       querylogical.Step
	edgeReader TraversalEdgePageReader
	nodeReader TraversalNodeReader
	batchSize  int
	tracker    *Tracker
	eval       evaluator

	outputs      []bufferedTraversalRow
	outputOffset int
	retained     int64
	truncated    bool
}

type subtreeFrontier struct {
	node  graph.Node
	depth int
}

func NewSubtreeExpand(input Operator, startAlias string, step querylogical.Step, edgeReader TraversalEdgePageReader, nodeReader TraversalNodeReader, batchSize int, parameters map[string]querylogical.Value, now time.Time, tracker *Tracker) *SubtreeExpand {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &SubtreeExpand{
		unaryOperator: unaryOperator{input: input}, startAlias: startAlias, step: step,
		edgeReader: edgeReader, nodeReader: nodeReader, batchSize: batchSize,
		tracker: tracker, eval: newEvaluator(parameters, now),
	}
}

func (o *SubtreeExpand) Open(ctx context.Context) error {
	if o.edgeReader == nil || o.nodeReader == nil {
		return invalidState("open subtree expansion", "node and edge readers are required")
	}
	o.releaseOutputs()
	o.truncated = false
	return o.open(ctx, "open subtree expansion")
}

func (o *SubtreeExpand) Next(ctx context.Context) (Batch, error) {
	if err := o.ready(ctx, "expand indexed subtree"); err != nil {
		return Batch{}, err
	}
	for o.outputOffset >= len(o.outputs) {
		o.releaseOutputs()
		if o.truncated {
			return Batch{}, io.EOF
		}
		batch, err := o.input.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return Batch{}, io.EOF
			}
			return Batch{}, o.fail(err)
		}
		for _, row := range batch.Rows {
			truncated, err := o.expandRow(ctx, row)
			if err != nil {
				return Batch{}, o.fail(err)
			}
			if truncated {
				o.truncated = true
				break
			}
		}
	}
	end := o.outputOffset + DefaultBatchSize
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

func (o *SubtreeExpand) expandRow(ctx context.Context, row BindingRow) (truncated bool, err error) {
	rowBytes := estimateBindingRow(row)
	if o.tracker != nil {
		if err := o.tracker.ReserveMemory(rowBytes); err != nil {
			return false, err
		}
		defer o.tracker.ReleaseMemory(rowBytes)
	}
	root, ok := row.Nodes[o.startAlias]
	if !ok {
		return false, executionInvalid("expand indexed subtree", fmt.Sprintf("node alias %q is not bound", o.startAlias))
	}

	out := cloneBindingRow(row)
	graphNodeSeen := make(map[string]struct{}, len(out.GraphNodes)+1)
	for _, node := range out.GraphNodes {
		graphNodeSeen[node.ID.String()] = struct{}{}
	}
	graphEdgeSeen := make(map[string]struct{}, len(out.GraphEdges))
	for _, edge := range out.GraphEdges {
		graphEdgeSeen[edge.ID.String()] = struct{}{}
	}
	addGraphNode := func(node graph.Node) {
		if _, exists := graphNodeSeen[node.ID.String()]; exists {
			return
		}
		graphNodeSeen[node.ID.String()] = struct{}{}
		out.GraphNodes = append(out.GraphNodes, node)
	}
	addGraphEdge := func(edge graph.Edge) {
		if _, exists := graphEdgeSeen[edge.ID.String()]; exists {
			return
		}
		graphEdgeSeen[edge.ID.String()] = struct{}{}
		out.GraphEdges = append(out.GraphEdges, edge)
	}
	addGraphNode(root)
	matchedNodes := []graph.Node{}
	parentByChild := map[string]string{}
	orderByChild := map[string]any{}
	if o.step.Depth.Min == 0 {
		matched, matchErr := matchesNodePattern(out, root, o.step.Target, o.eval)
		if matchErr != nil {
			return false, matchErr
		}
		if matched {
			matchedNodes = append(matchedNodes, root)
		}
	}

	visited := map[string]struct{}{root.ID.String(): {}}
	frontier := []subtreeFrontier{{node: root}}
	reserved := int64(0)
	defer func() {
		if err != nil && o.tracker != nil && reserved > 0 {
			o.tracker.ReleaseMemory(reserved)
		}
	}()
	if err = o.resizeSubtreeReservation(&reserved, out, matchedNodes, parentByChild, orderByChild, frontier, visited, graphEdgeSeen); err != nil {
		return false, err
	}

	for len(frontier) > 0 && !truncated {
		current := frontier[0]
		frontier = frontier[1:]
		if current.depth >= o.step.Depth.Max {
			continue
		}
		cursor := ""
		for !truncated {
			limit := o.batchSize
			if o.tracker != nil {
				remaining := o.tracker.RemainingEdgesLoaded()
				if remaining == 0 {
					o.tracker.MarkTruncated("max_edges exceeded")
					truncated = true
					break
				}
				if remaining < int64(limit) {
					limit = int(remaining)
				}
				if err = o.tracker.UseAdjacency(1); err != nil {
					return false, err
				}
			}
			page, readErr := o.edgeReader(ctx, current.node.ID.String(), limit, cursor)
			if err = traversalDependencyError(ctx, "scan subtree adjacency", readErr); err != nil {
				return false, err
			}
			if err = canceled(ctx, "scan subtree adjacency"); err != nil {
				return false, err
			}
			if len(page.Edges) > limit {
				return false, invalidState("scan subtree adjacency", "access method returned an oversized page")
			}
			if len(page.Edges) == 0 && page.NextCursor != "" {
				return false, querylogical.NewError(querylogical.ErrorDependencyFailure, "scan subtree adjacency", "access method returned an empty non-terminal page", nil)
			}
			if o.tracker != nil {
				if err = o.tracker.ScanIndexEntries(page.IndexEntries); err != nil {
					return false, err
				}
				if err = o.tracker.LoadEdges(page.EdgesLoaded); err != nil {
					return false, err
				}
			}
			if err = withTraversalPageMemory(o.tracker, page.Edges, func() error {
				for _, edge := range page.Edges {
					if o.tracker != nil {
						if err = o.tracker.ExamineCandidates(1); err != nil {
							return err
						}
					}
					edgeMatched, matchErr := matchesEdgePattern(edge, o.step.Edge, out, o.eval)
					if matchErr != nil {
						return matchErr
					}
					if !edgeMatched {
						continue
					}
					endpointID := traversalEndpoint(edge, o.step.Edge.Direction)
					if _, seen := visited[endpointID]; seen {
						addGraphEdge(edge)
						if err = o.resizeSubtreeReservation(&reserved, out, matchedNodes, parentByChild, orderByChild, frontier, visited, graphEdgeSeen); err != nil {
							return err
						}
						continue
					}
					if o.tracker != nil && o.tracker.RemainingNodesLoaded() == 0 {
						o.tracker.MarkTruncated("max_nodes exceeded")
						truncated = true
						break
					}
					endpoint, readErr := o.nodeReader(ctx, endpointID)
					if err = traversalDependencyError(ctx, "read subtree endpoint", readErr); err != nil {
						return err
					}
					if err = canceled(ctx, "read subtree endpoint"); err != nil {
						return err
					}
					if o.tracker != nil {
						if err = o.tracker.LoadNodes(1); err != nil {
							return err
						}
						if err = o.tracker.RecordNodeReads(1); err != nil {
							return err
						}
					}
					childDepth := current.depth + 1
					if o.tracker != nil {
						if err = o.tracker.ObserveTraversalDepth(childDepth); err != nil {
							return err
						}
					}
					visited[endpointID] = struct{}{}
					addGraphNode(endpoint)
					addGraphEdge(edge)
					matchRow := cloneBindingRow(out)
					matchRow.Nodes[o.step.Target.Alias] = endpoint
					matched, matchErr := matchesNodePattern(matchRow, endpoint, o.step.Target, o.eval)
					if matchErr != nil {
						return matchErr
					}
					if childDepth >= o.step.Depth.Min && matched {
						matchedNodes = append(matchedNodes, endpoint)
						// Preserve legacy tree semantics: only outgoing containment
						// traversals establish parent/child hierarchy. Other subtree
						// traversals return matched nodes as flat tree roots.
						if o.step.Edge.Direction == querylogical.DirectionOutgoing && o.step.Edge.Labels[0] == "contains" {
							parentByChild[endpointID] = current.node.ID.String()
							orderByChild[endpointID] = edge.Properties["order"]
						}
					}
					if childDepth < o.step.Depth.Max {
						frontier = append(frontier, subtreeFrontier{node: endpoint, depth: childDepth})
					}
					if err = o.resizeSubtreeReservation(&reserved, out, matchedNodes, parentByChild, orderByChild, frontier, visited, graphEdgeSeen); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return false, err
			}
			if truncated || page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
	}

	if len(matchedNodes) > 0 {
		out.Nodes[o.step.Target.Alias] = matchedNodes[0]
	}
	if out.Trees == nil {
		out.Trees = map[string]querylogical.Tree{}
	}
	out.Trees[o.step.Target.Alias] = buildTree(matchedNodes, parentByChild, orderByChild)
	if err = o.resizeSubtreeReservation(&reserved, out, matchedNodes, parentByChild, orderByChild, nil, nil, graphEdgeSeen); err != nil {
		return false, err
	}
	if o.tracker != nil {
		if err = o.tracker.ProduceIntermediate(1); err != nil {
			return false, err
		}
	}
	o.retained += reserved
	o.outputs = append(o.outputs, bufferedTraversalRow{row: out, bytes: reserved})
	reserved = 0
	return truncated, nil
}

func (o *SubtreeExpand) resizeSubtreeReservation(reserved *int64, row BindingRow, matched []graph.Node, parents map[string]string, order map[string]any, frontier []subtreeFrontier, visited map[string]struct{}, seenEdges map[string]struct{}) error {
	bytes := estimateBindingRow(row)
	for _, node := range matched {
		bytes += estimateBindingRow(BindingRow{GraphNodes: []graph.Node{node}})
	}
	bytes += int64(len(parents)+len(order)+len(frontier)+len(visited)+len(seenEdges)) * 64
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

func (o *SubtreeExpand) releaseItem(item *bufferedTraversalRow) {
	if item == nil || item.bytes == 0 {
		return
	}
	if o.tracker != nil {
		o.tracker.ReleaseMemory(item.bytes)
	}
	o.retained -= item.bytes
	item.bytes = 0
}

func (o *SubtreeExpand) releaseOutputs() {
	for i := range o.outputs {
		o.releaseItem(&o.outputs[i])
	}
	o.outputs = nil
	o.outputOffset = 0
	o.retained = 0
}

func (o *SubtreeExpand) Close() error {
	o.releaseOutputs()
	return o.close()
}

func matchesNodePattern(row BindingRow, node graph.Node, pattern querylogical.NodePattern, eval evaluator) (bool, error) {
	if len(pattern.NodeIDs) > 0 && !containsString(pattern.NodeIDs, node.ID.String()) {
		return false, nil
	}
	for _, label := range pattern.Labels {
		if !containsString(node.Labels, label) {
			return false, nil
		}
	}
	for name, expected := range pattern.Properties {
		actual, exists := graph.Property(node, name)
		if !exists {
			return false, nil
		}
		resolved, err := eval.value(row, expected)
		if err != nil {
			return false, err
		}
		if compareValues(querylogical.NativeValue(actual), resolved) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func matchesEdgePattern(edge graph.Edge, pattern querylogical.EdgePattern, row BindingRow, eval evaluator) (bool, error) {
	for _, label := range pattern.Labels {
		if !containsString(edge.Labels, label) {
			return false, nil
		}
	}
	for name, expected := range pattern.Properties {
		actual, exists := edge.Properties[name]
		if !exists {
			return false, nil
		}
		resolved, err := eval.value(row, expected)
		if err != nil {
			return false, err
		}
		if compareValues(querylogical.NativeValue(actual), resolved) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func traversalEndpoint(edge graph.Edge, direction querylogical.Direction) string {
	if direction == querylogical.DirectionIncoming {
		return edge.FromID.String()
	}
	return edge.ToID.String()
}

func traversalDependencyError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return querylogical.NewError(querylogical.ErrorCanceled, operation, "query context canceled", contextErr)
	}
	if err == nil {
		return nil
	}
	var queryErr *querylogical.QueryError
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return querylogical.NewError(querylogical.ErrorCanceled, operation, "query context canceled", err)
	case errors.As(err, &queryErr):
		return err
	default:
		return querylogical.NewError(querylogical.ErrorDependencyFailure, operation, err.Error(), err)
	}
}

func cloneSeen(seen map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(seen)+1)
	for id := range seen {
		out[id] = struct{}{}
	}
	return out
}

func cloneBindingRow(row BindingRow) BindingRow {
	out := row
	out.Nodes = make(map[string]graph.Node, len(row.Nodes))
	for alias, node := range row.Nodes {
		out.Nodes[alias] = node
	}
	out.Edges = make(map[string]graph.Edge, len(row.Edges))
	for alias, edge := range row.Edges {
		out.Edges[alias] = edge
	}
	out.Paths = make(map[string]querylogical.Path, len(row.Paths))
	for alias, path := range row.Paths {
		path.Nodes = append([]querylogical.Node(nil), path.Nodes...)
		path.Edges = append([]querylogical.Edge(nil), path.Edges...)
		out.Paths[alias] = path
	}
	out.Trees = make(map[string]querylogical.Tree, len(row.Trees))
	for alias, tree := range row.Trees {
		out.Trees[alias] = tree
	}
	out.GraphNodes = append([]graph.Node(nil), row.GraphNodes...)
	out.GraphEdges = append([]graph.Edge(nil), row.GraphEdges...)
	return out
}

func buildTree(nodes []graph.Node, parentByChild map[string]string, orderByChild map[string]any) querylogical.Tree {
	byID := make(map[string]graph.Node, len(nodes))
	children := map[string][]graph.Node{}
	for _, node := range nodes {
		byID[node.ID.String()] = node
	}
	roots := []graph.Node{}
	for _, node := range nodes {
		parentID := parentByChild[node.ID.String()]
		if _, exists := byID[parentID]; exists {
			children[parentID] = append(children[parentID], node)
			continue
		}
		roots = append(roots, node)
	}
	sortTraversalNodes(roots, orderByChild)
	for parentID := range children {
		sortTraversalNodes(children[parentID], orderByChild)
	}
	var makeNode func(graph.Node) querylogical.TreeNode
	makeNode = func(node graph.Node) querylogical.TreeNode {
		out := querylogical.TreeNode{Node: neutralNode(node)}
		for _, child := range children[node.ID.String()] {
			out.Children = append(out.Children, makeNode(child))
		}
		return out
	}
	out := querylogical.Tree{}
	for _, root := range roots {
		out.Roots = append(out.Roots, makeNode(root))
	}
	return out
}

func sortTraversalNodes(nodes []graph.Node, order map[string]any) {
	sort.SliceStable(nodes, func(i, j int) bool {
		return traversalNumericOrder(order[nodes[i].ID.String()]) < traversalNumericOrder(order[nodes[j].ID.String()])
	})
}

func traversalNumericOrder(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		parsed, _ := strconv.ParseInt(v, 10, 64)
		return parsed
	default:
		return 0
	}
}
