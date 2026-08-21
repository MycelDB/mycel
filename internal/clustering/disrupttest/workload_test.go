package disrupttest

import (
	"context"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/query/gql"
)

type fakeWorkloadClient struct {
	scopes  []TestScope
	scripts []string
	writes  []string
	counts  map[string]int64
}

func (f *fakeWorkloadClient) CreateScope(ctx context.Context, runID string) (TestScope, error) {
	scope := TestScope{RunID: runID, SpaceID: "space-" + runID, DomainID: "domain-" + runID}
	f.scopes = append(f.scopes, scope)
	return scope, nil
}

func (f *fakeWorkloadClient) WriteChaos(ctx context.Context, scope TestScope, workerID string, seq int64) error {
	f.writes = append(f.writes, scope.RunID)
	f.counts[scope.RunID]++
	return nil
}

func (f *fakeWorkloadClient) ExecuteGQLScript(ctx context.Context, scope TestScope, script string) error {
	f.scripts = append(f.scripts, script)
	f.counts[scope.RunID+":parents"]++
	f.counts[scope.RunID+":children"]++
	f.counts[scope.RunID+":edges"]++
	return nil
}

func (f *fakeWorkloadClient) CountGQL(ctx context.Context, scope TestScope, gql string) (int64, error) {
	switch {
	case strings.Contains(gql, "ChaosParent") && strings.Contains(gql, "count(p)"):
		return f.counts[scope.RunID+":parents"], nil
	case strings.Contains(gql, "ChaosChild") && strings.Contains(gql, "count(c)"):
		return f.counts[scope.RunID+":children"], nil
	case strings.Contains(gql, "CHAOS_EDGE"):
		return f.counts[scope.RunID+":edges"], nil
	default:
		return f.counts[scope.RunID], nil
	}
}

func TestEdgesWorkloadWritesAndCountsNodesAndEdges(t *testing.T) {
	client := &fakeWorkloadClient{counts: map[string]int64{}}
	workload := edgesWorkload{}
	scopes, err := workload.Setup(context.Background(), client, "run-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := workload.Write(context.Background(), client, scopes, "worker", 1); err != nil {
		t.Fatal(err)
	}
	if len(client.scripts) != 1 || !strings.Contains(client.scripts[0], "CHAOS_EDGE") {
		t.Fatalf("edge script = %#v", client.scripts)
	}
	if _, err := gql.CompileScript(client.scripts[0]); err != nil {
		t.Fatalf("edge script does not compile: %v\n%s", err, client.scripts[0])
	}
	counts, err := workload.Count(context.Background(), client, scopes)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Nodes != 2 || counts.Edges != 1 || counts.Scopes[scopes[0].RunID].Nodes != 2 || counts.Scopes[scopes[0].RunID].Edges != 1 {
		t.Fatalf("counts = %+v", counts)
	}
	if min := workload.ExpectedMinimum(3); min.Nodes != 6 || min.Edges != 3 {
		t.Fatalf("minimum = %+v", min)
	}
}

func TestMultiSpaceWorkloadDistributesWrites(t *testing.T) {
	client := &fakeWorkloadClient{counts: map[string]int64{}}
	workload := multiSpaceWorkload{spaces: 3}
	scopes, err := workload.Setup(context.Background(), client, "run-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(scopes) != 3 {
		t.Fatalf("scopes = %d", len(scopes))
	}
	for i := int64(0); i < 6; i++ {
		if err := workload.Write(context.Background(), client, scopes, "worker", i); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := workload.Count(context.Background(), client, scopes)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Nodes != 6 {
		t.Fatalf("nodes = %d", counts.Nodes)
	}
	for _, scope := range scopes {
		if client.counts[scope.RunID] != 2 {
			t.Fatalf("scope %s count = %d", scope.RunID, client.counts[scope.RunID])
		}
	}
}

func TestResolveWorkload(t *testing.T) {
	for _, name := range []string{WorkloadNodes, WorkloadEdges, WorkloadMultiSpace} {
		if _, err := ResolveWorkload(name); err != nil {
			t.Fatalf("ResolveWorkload(%q): %v", name, err)
		}
	}
	if _, err := ResolveWorkload("bad"); err == nil {
		t.Fatal("expected unsupported workload error")
	}
}
