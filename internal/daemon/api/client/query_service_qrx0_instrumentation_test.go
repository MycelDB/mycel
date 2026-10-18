package client

import (
	"context"
	"sync"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

// qrx0AccessProfile records the graph access paths selected by the legacy query
// implementation. These observations form the baseline for moving one plan
// family at a time to the extracted planner and executor.
type qrx0AccessProfile struct {
	mu       sync.Mutex
	calls    map[string]int
	limits   map[string][]int
	returned map[string]int
	work     qrx0WorkProfile
}

type qrx0WorkProfile struct {
	IndexEntriesScanned int
	CandidatesExamined  int
	NodesLoaded         int
	EdgesLoaded         int
	AdjacencyScanCalls  int
	NodeReadCalls       int
	IntermediateRows    int
	ReturnedRows        int
}

func newQRX0AccessProfile() *qrx0AccessProfile {
	return &qrx0AccessProfile{
		calls:    map[string]int{},
		limits:   map[string][]int{},
		returned: map[string]int{},
	}
}

func (p *qrx0AccessProfile) record(name string, limit int, returned int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[name]++
	if limit >= 0 {
		p.limits[name] = append(p.limits[name], limit)
	}
	p.returned[name] += returned
	switch name {
	case "GetNode":
		p.work.NodesLoaded += returned
		p.work.NodeReadCalls++
	case "ListNodes":
		p.work.NodesLoaded += returned
		p.work.CandidatesExamined += returned
	case "ListEdges":
		p.work.EdgesLoaded += returned
		p.work.CandidatesExamined += returned
	case "ScanAdjacency":
		p.work.AdjacencyScanCalls++
	}
}

func (p *qrx0AccessProfile) recordIndexedStats(stats graphquery.IndexedReadStats) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.work.IndexEntriesScanned += stats.IndexEntriesScanned
	p.work.CandidatesExamined += stats.IndexEntriesScanned
	p.work.NodesLoaded += stats.NodesLoaded
	p.work.EdgesLoaded += stats.EdgesLoaded
	p.work.AdjacencyScanCalls += stats.AdjacencyScanCalls
	p.work.NodeReadCalls += stats.NodeReadCalls
}

func (p *qrx0AccessProfile) recordDiagnostics(diagnostics *clientv1.QueryDiagnostics) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.work.IntermediateRows += int(diagnostics.GetRowsScanned())
	if produced := int(diagnostics.GetRowsProduced()); produced > int(diagnostics.GetRowsReturned()) {
		p.work.IntermediateRows += produced - int(diagnostics.GetRowsReturned())
	}
	p.work.ReturnedRows += int(diagnostics.GetRowsReturned())
}

func (p *qrx0AccessProfile) recordSemantic(limit int, candidates int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls["SemanticSearch"]++
	p.limits["SemanticSearch"] = append(p.limits["SemanticSearch"], limit)
	p.returned["SemanticSearch"] += candidates
	p.work.CandidatesExamined += candidates
}

func (p *qrx0AccessProfile) callCount(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[name]
}

func (p *qrx0AccessProfile) requestedLimits(name string) []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.limits[name]...)
}

func (p *qrx0AccessProfile) returnedCount(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.returned[name]
}

func (p *qrx0AccessProfile) callsSnapshot() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.calls))
	for name, count := range p.calls {
		out[name] = count
	}
	return out
}

func (p *qrx0AccessProfile) workSnapshot() qrx0WorkProfile {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.work
}

func (p *qrx0AccessProfile) dataReadCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, name := range []string{"GetNode", "ListNodes", "ListEdges", "ScanLabel", "ScanTag", "ScanNodePropertyOrdered", "ScanAdjacency", "ScanSubtree"} {
		total += p.calls[name]
	}
	return total
}

// qrx0InstrumentedGraph embeds the production graph manager and intercepts only
// query read/configuration methods. The embedded manager continues to satisfy
// the rest of graphservice.Manager without a test-only duplicate of that broad
// interface.
type qrx0InstrumentedGraph struct {
	graphservice.Manager
	profile *qrx0AccessProfile
}

func instrumentQRX0Graph(manager graphservice.Manager) (*qrx0InstrumentedGraph, *qrx0AccessProfile) {
	profile := newQRX0AccessProfile()
	return &qrx0InstrumentedGraph{Manager: manager, profile: profile}, profile
}

func (g *qrx0InstrumentedGraph) GetNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string) (domaingraph.Node, error) {
	node, err := g.Manager.GetNode(ctx, tx, nodeID)
	returned := 0
	if err == nil {
		returned = 1
	}
	g.profile.record("GetNode", -1, returned)
	return node, err
}

func (g *qrx0InstrumentedGraph) ListNodes(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Node, string, error) {
	nodes, next, err := g.Manager.ListNodes(ctx, tx, pageSize, pageToken)
	g.profile.record("ListNodes", pageSize, len(nodes))
	return nodes, next, err
}

func (g *qrx0InstrumentedGraph) ScanNodes(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.NodeScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	nodes, next, stats, err := g.Manager.ScanNodes(ctx, tx, scan)
	g.profile.record("ListNodes", scan.Limit, len(nodes))
	g.profile.recordIndexedStats(stats)
	return nodes, next, stats, err
}

func (g *qrx0InstrumentedGraph) ListEdges(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Edge, string, error) {
	edges, next, err := g.Manager.ListEdges(ctx, tx, pageSize, pageToken)
	g.profile.record("ListEdges", pageSize, len(edges))
	return edges, next, err
}

func (g *qrx0InstrumentedGraph) ConfigureIndexes(ctx context.Context, tx sessionmodel.GraphTransaction, schemaHash string, indexes []schemamodel.IndexDefinition) error {
	err := g.Manager.ConfigureIndexes(ctx, tx, schemaHash, indexes)
	g.profile.record("ConfigureIndexes", len(indexes), 0)
	return err
}

func (g *qrx0InstrumentedGraph) ScanLabel(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.LabelScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	nodes, next, stats, err := g.Manager.ScanLabel(ctx, tx, scan)
	g.profile.record("ScanLabel", scan.Limit, len(nodes))
	g.profile.recordIndexedStats(stats)
	return nodes, next, stats, err
}

func (g *qrx0InstrumentedGraph) ScanTag(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.TagScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	nodes, next, stats, err := g.Manager.ScanTag(ctx, tx, scan)
	g.profile.record("ScanTag", scan.Limit, len(nodes))
	g.profile.recordIndexedStats(stats)
	return nodes, next, stats, err
}

func (g *qrx0InstrumentedGraph) ScanNodePropertyOrdered(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.OrderedNodePropertyScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	nodes, next, stats, err := g.Manager.ScanNodePropertyOrdered(ctx, tx, scan)
	g.profile.record("ScanNodePropertyOrdered", scan.Limit, len(nodes))
	g.profile.recordIndexedStats(stats)
	return nodes, next, stats, err
}

func (g *qrx0InstrumentedGraph) ScanAdjacency(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]domaingraph.Edge, string, graphquery.IndexedReadStats, error) {
	edges, next, stats, err := g.Manager.ScanAdjacency(ctx, tx, scan)
	g.profile.record("ScanAdjacency", scan.Limit, len(edges))
	g.profile.recordIndexedStats(stats)
	return edges, next, stats, err
}

func (g *qrx0InstrumentedGraph) ScanSubtree(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.SubtreeScan) (graphquery.SubtreeResult, graphquery.IndexedReadStats, error) {
	result, stats, err := g.Manager.ScanSubtree(ctx, tx, scan)
	g.profile.record("ScanSubtree", scan.MaxNodes, len(result.GraphNodes)+len(result.GraphEdges))
	g.profile.recordIndexedStats(stats)
	return result, stats, err
}

type qrx0ListNodesErrorGraph struct {
	graphservice.Manager
	err error
}

func (g *qrx0ListNodesErrorGraph) ListNodes(context.Context, sessionmodel.GraphTransaction, int, string) ([]domaingraph.Node, string, error) {
	return nil, "", g.err
}

func (g *qrx0ListNodesErrorGraph) ScanNodes(context.Context, sessionmodel.GraphTransaction, graphquery.NodeScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	return nil, "", graphquery.IndexedReadStats{}, g.err
}

type qrx0InstrumentedSemantic struct {
	delegate semanticQuerySearcher
	profile  *qrx0AccessProfile
}

func instrumentQRX0Semantic(delegate semanticQuerySearcher, profile *qrx0AccessProfile) *qrx0InstrumentedSemantic {
	return &qrx0InstrumentedSemantic{delegate: delegate, profile: profile}
}

func (s *qrx0InstrumentedSemantic) Search(ctx context.Context, in semanticservice.SearchInput) (semanticsearch.Result, error) {
	result, err := s.delegate.Search(ctx, in)
	s.profile.recordSemantic(in.Limit, len(result.Results))
	return result, err
}

type qrx0SemanticErrorSearcher struct {
	err error
}

func (s *qrx0SemanticErrorSearcher) Search(context.Context, semanticservice.SearchInput) (semanticsearch.Result, error) {
	return semanticsearch.Result{}, s.err
}
