package disrupttest

import (
	"context"
	"fmt"
)

const (
	WorkloadNodes      = "nodes"
	WorkloadEdges      = "edges"
	WorkloadMultiSpace = "multi-space"
)

const defaultMultiSpaceCount = 3

type WorkloadCounts struct {
	Nodes  int64                     `json:"nodes"`
	Edges  int64                     `json:"edges,omitempty"`
	Scopes map[string]WorkloadCounts `json:"scopes,omitempty"`
}

func (c WorkloadCounts) Add(other WorkloadCounts) WorkloadCounts {
	out := WorkloadCounts{Nodes: c.Nodes + other.Nodes, Edges: c.Edges + other.Edges}
	if c.Scopes != nil || other.Scopes != nil {
		out.Scopes = map[string]WorkloadCounts{}
		for k, v := range c.Scopes {
			out.Scopes[k] = v
		}
		for k, v := range other.Scopes {
			out.Scopes[k] = out.Scopes[k].Add(v)
		}
	}
	return out
}

func (c WorkloadCounts) Below(min WorkloadCounts) bool {
	return c.Nodes < min.Nodes || c.Edges < min.Edges
}

func (c WorkloadCounts) Equal(other WorkloadCounts) bool {
	if c.Nodes != other.Nodes || c.Edges != other.Edges || len(c.Scopes) != len(other.Scopes) {
		return false
	}
	for key, left := range c.Scopes {
		right, ok := other.Scopes[key]
		if !ok || !left.Equal(right) {
			return false
		}
	}
	return true
}

type WorkloadClient interface {
	CreateScope(ctx context.Context, runID string) (TestScope, error)
	WriteChaos(ctx context.Context, scope TestScope, workerID string, seq int64) error
	ExecuteGQLScript(ctx context.Context, scope TestScope, script string) error
	CountGQL(ctx context.Context, scope TestScope, gql string) (int64, error)
}

type Workload interface {
	Name() string
	Setup(ctx context.Context, client WorkloadClient, runID string) ([]TestScope, error)
	Write(ctx context.Context, client WorkloadClient, scopes []TestScope, worker string, seq int64) error
	Count(ctx context.Context, client WorkloadClient, scopes []TestScope) (WorkloadCounts, error)
	ExpectedMinimum(successfulWrites int64) WorkloadCounts
	ExpectedWriteCounts(scopes []TestScope, worker string, seq int64) map[string]WorkloadCounts
}

func ResolveWorkload(name string) (Workload, error) {
	switch name {
	case "", WorkloadNodes:
		return nodesWorkload{}, nil
	case WorkloadEdges:
		return edgesWorkload{}, nil
	case WorkloadMultiSpace:
		return multiSpaceWorkload{spaces: defaultMultiSpaceCount}, nil
	default:
		return nil, fmt.Errorf("unsupported workload %q", name)
	}
}

func IsSupportedWorkload(name string) bool {
	_, err := ResolveWorkload(name)
	return err == nil
}

type nodesWorkload struct{}

func (nodesWorkload) Name() string { return WorkloadNodes }

func (nodesWorkload) Setup(ctx context.Context, client WorkloadClient, runID string) ([]TestScope, error) {
	scope, err := client.CreateScope(ctx, runID)
	if err != nil {
		return nil, err
	}
	return []TestScope{scope}, nil
}

func (nodesWorkload) Write(ctx context.Context, client WorkloadClient, scopes []TestScope, worker string, seq int64) error {
	return client.WriteChaos(ctx, scopes[0], worker, seq)
}

func (nodesWorkload) Count(ctx context.Context, client WorkloadClient, scopes []TestScope) (WorkloadCounts, error) {
	count, err := countChaosNodes(ctx, client, scopes[0])
	return WorkloadCounts{Nodes: count, Scopes: map[string]WorkloadCounts{scopes[0].RunID: {Nodes: count}}}, err
}

func (nodesWorkload) ExpectedMinimum(successfulWrites int64) WorkloadCounts {
	return WorkloadCounts{Nodes: successfulWrites}
}

func (nodesWorkload) ExpectedWriteCounts(scopes []TestScope, worker string, seq int64) map[string]WorkloadCounts {
	return map[string]WorkloadCounts{scopes[0].RunID: {Nodes: 1}}
}

type edgesWorkload struct{}

func (edgesWorkload) Name() string { return WorkloadEdges }

func (edgesWorkload) Setup(ctx context.Context, client WorkloadClient, runID string) ([]TestScope, error) {
	scope, err := client.CreateScope(ctx, runID)
	if err != nil {
		return nil, err
	}
	return []TestScope{scope}, nil
}

func (edgesWorkload) Write(ctx context.Context, client WorkloadClient, scopes []TestScope, worker string, seq int64) error {
	scope := scopes[0]
	parentKey := fmt.Sprintf("%s-%s-%d-parent", scope.RunID, worker, seq)
	childKey := fmt.Sprintf("%s-%s-%d-child", scope.RunID, worker, seq)
	script := fmt.Sprintf(`
INSERT (:ChaosParent {run: %q, key: %q, worker: %q, seq: %d});
INSERT (:ChaosChild {run: %q, key: %q, worker: %q, seq: %d});
MATCH (p:ChaosParent {key: %q}), (c:ChaosChild {key: %q}) CREATE (p)-[:CHAOS_EDGE {run: %q, worker: %q, seq: %d}]->(c);
`, scope.RunID, parentKey, worker, seq, scope.RunID, childKey, worker, seq, parentKey, childKey, scope.RunID, worker, seq)
	return client.ExecuteGQLScript(ctx, scope, script)
}

func (edgesWorkload) Count(ctx context.Context, client WorkloadClient, scopes []TestScope) (WorkloadCounts, error) {
	scope := scopes[0]
	parents, err := client.CountGQL(ctx, scope, fmt.Sprintf("MATCH (p:ChaosParent {run: %q}) RETURN count(p) FETCH FIRST 1 ROW ONLY", scope.RunID))
	if err != nil {
		return WorkloadCounts{}, err
	}
	children, err := client.CountGQL(ctx, scope, fmt.Sprintf("MATCH (c:ChaosChild {run: %q}) RETURN count(c) FETCH FIRST 1 ROW ONLY", scope.RunID))
	if err != nil {
		return WorkloadCounts{}, err
	}
	edges, err := client.CountGQL(ctx, scope, fmt.Sprintf("MATCH (p:ChaosParent {run: %q})-[r:CHAOS_EDGE]->(c:ChaosChild) RETURN count(r) FETCH FIRST 1 ROW ONLY", scope.RunID))
	if err != nil {
		return WorkloadCounts{}, err
	}
	counts := WorkloadCounts{Nodes: parents + children, Edges: edges}
	counts.Scopes = map[string]WorkloadCounts{scope.RunID: {Nodes: counts.Nodes, Edges: counts.Edges}}
	return counts, nil
}

func (edgesWorkload) ExpectedMinimum(successfulWrites int64) WorkloadCounts {
	return WorkloadCounts{Nodes: successfulWrites * 2, Edges: successfulWrites}
}

func (edgesWorkload) ExpectedWriteCounts(scopes []TestScope, worker string, seq int64) map[string]WorkloadCounts {
	return map[string]WorkloadCounts{scopes[0].RunID: {Nodes: 2, Edges: 1}}
}

type multiSpaceWorkload struct {
	spaces int
}

func (w multiSpaceWorkload) Name() string { return WorkloadMultiSpace }

func (w multiSpaceWorkload) Setup(ctx context.Context, client WorkloadClient, runID string) ([]TestScope, error) {
	spaces := w.spaces
	if spaces <= 0 {
		spaces = defaultMultiSpaceCount
	}
	scopes := make([]TestScope, 0, spaces)
	for i := 0; i < spaces; i++ {
		scope, err := client.CreateScope(ctx, fmt.Sprintf("%s-s%d", runID, i))
		if err != nil {
			return nil, err
		}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

func (w multiSpaceWorkload) Write(ctx context.Context, client WorkloadClient, scopes []TestScope, worker string, seq int64) error {
	if len(scopes) == 0 {
		return fmt.Errorf("multi-space workload has no scopes")
	}
	idx := int(seq % int64(len(scopes)))
	return client.WriteChaos(ctx, scopes[idx], worker, seq)
}

func (w multiSpaceWorkload) Count(ctx context.Context, client WorkloadClient, scopes []TestScope) (WorkloadCounts, error) {
	var total WorkloadCounts
	total.Scopes = map[string]WorkloadCounts{}
	for _, scope := range scopes {
		count, err := countChaosNodes(ctx, client, scope)
		if err != nil {
			return WorkloadCounts{}, err
		}
		scopeCount := WorkloadCounts{Nodes: count}
		total = total.Add(scopeCount)
		total.Scopes[scope.RunID] = scopeCount
	}
	return total, nil
}

func (w multiSpaceWorkload) ExpectedMinimum(successfulWrites int64) WorkloadCounts {
	return WorkloadCounts{Nodes: successfulWrites}
}

func (w multiSpaceWorkload) ExpectedWriteCounts(scopes []TestScope, worker string, seq int64) map[string]WorkloadCounts {
	idx := int(seq % int64(len(scopes)))
	return map[string]WorkloadCounts{scopes[idx].RunID: {Nodes: 1}}
}

func countChaosNodes(ctx context.Context, client WorkloadClient, scope TestScope) (int64, error) {
	return client.CountGQL(ctx, scope, fmt.Sprintf("MATCH (n:ChaosWrite {run: %q}) RETURN count(n) FETCH FIRST 1 ROW ONLY", scope.RunID))
}
