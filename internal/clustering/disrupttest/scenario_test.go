package disrupttest

import (
	"context"
	"errors"
	"strings"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type fakeClusterDriver struct {
	nodes []NodeRef
}

func (f fakeClusterDriver) Name() string                                 { return "fake" }
func (f fakeClusterDriver) ApplyManifests(context.Context, string) error { return nil }
func (f fakeClusterDriver) Nodes(context.Context) ([]NodeRef, error)     { return f.nodes, nil }
func (f fakeClusterDriver) WaitReady(context.Context, NodeRef) error     { return nil }
func (f fakeClusterDriver) WaitAllReady(context.Context) error           { return nil }
func (f fakeClusterDriver) RestartNode(context.Context, NodeRef) error   { return nil }
func (f fakeClusterDriver) PortForward(context.Context, NodeRef, int) (Endpoint, func(), error) {
	return Endpoint{}, func() {}, nil
}
func (f fakeClusterDriver) ServiceEndpoint(context.Context) (Endpoint, func(), error) {
	return Endpoint{}, func() {}, nil
}
func (f fakeClusterDriver) CollectArtifacts(context.Context, string) error { return nil }

func TestResolveRestartNodes(t *testing.T) {
	driver := fakeClusterDriver{nodes: []NodeRef{{Name: "myceld-2"}, {Name: "myceld-0"}, {Name: "myceld-1"}}}
	got, err := ResolveRestartNodes(context.Background(), driver, "")
	if err != nil || len(got) != 1 || got[0] != "myceld-0" {
		t.Fatalf("default restart = %#v, %v", got, err)
	}
	got, err = ResolveRestartNodes(context.Background(), driver, "1")
	if err != nil || len(got) != 1 || got[0] != "myceld-1" {
		t.Fatalf("ordinal restart = %#v, %v", got, err)
	}
	got, err = ResolveRestartNodes(context.Background(), driver, "all")
	if err != nil || strings.Join(got, ",") != "myceld-0,myceld-1,myceld-2" {
		t.Fatalf("all restart = %#v, %v", got, err)
	}
}

func TestAssertCountsConvergedReportsMismatches(t *testing.T) {
	err := AssertCountsConverged(map[string]WorkloadCounts{"service": {Nodes: 5}, "myceld-0": {Nodes: 4}, "myceld-1": {Nodes: 5}}, WorkloadCounts{Nodes: 5}, nil)
	if err == nil || !strings.Contains(err.Error(), "myceld-0 count") || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("AssertCountsConverged() error = %v", err)
	}
	if err := AssertCountsConverged(map[string]WorkloadCounts{"service": {Nodes: 5, Scopes: map[string]WorkloadCounts{"scope-a": {Nodes: 5}}}, "myceld-0": {Nodes: 5, Scopes: map[string]WorkloadCounts{"scope-a": {Nodes: 5}}}}, WorkloadCounts{Nodes: 5}, map[string]WorkloadCounts{"scope-a": {Nodes: 5}}); err != nil {
		t.Fatalf("converged counts error = %v", err)
	}
}

func TestEstimateAmbiguousWrites(t *testing.T) {
	counts := map[string]WorkloadCounts{"client": {Nodes: 616, Edges: 308}}
	if got := EstimateAmbiguousWrites(WorkloadEdges, counts, 306); got != 2 {
		t.Fatalf("edge ambiguous writes = %d, want 2", got)
	}
	counts = map[string]WorkloadCounts{"myceld-0": {Nodes: 12}}
	if got := EstimateAmbiguousWrites(WorkloadNodes, counts, 10); got != 2 {
		t.Fatalf("node ambiguous writes = %d, want 2", got)
	}
	if got := EstimateAmbiguousWrites(WorkloadEdges, map[string]WorkloadCounts{"client": {Nodes: 612, Edges: 306}}, 306); got != 0 {
		t.Fatalf("converged acknowledged writes ambiguous = %d, want 0", got)
	}
}

func TestAssertClusterIdentityConverged(t *testing.T) {
	if err := AssertClusterIdentityConverged([]Diagnostics{{Endpoint: "a", ClusterID: "cluster-1"}, {Endpoint: "b", ClusterID: "cluster-1"}}); err != nil {
		t.Fatalf("matching cluster IDs error = %v", err)
	}
	err := AssertClusterIdentityConverged([]Diagnostics{{Endpoint: "a", ClusterID: "cluster-1"}, {Endpoint: "b", ClusterID: "cluster-2"}})
	if err == nil || !strings.Contains(err.Error(), "cluster identity convergence failed") {
		t.Fatalf("mismatch error = %v", err)
	}
	if err := AssertClusterIdentityConverged([]Diagnostics{{Endpoint: "a", Warning: "unavailable"}}); err != nil {
		t.Fatalf("missing diagnostics should degrade to count assertions: %v", err)
	}
}

func TestIsTransientError(t *testing.T) {
	if !IsTransientError(status.Error(codes.Unavailable, "pod restarting")) {
		t.Fatal("Unavailable should be transient")
	}
	if IsTransientError(status.Error(codes.InvalidArgument, "bad gql")) {
		t.Fatal("InvalidArgument should be permanent")
	}
	if !IsTransientError(errors.New("connection refused")) {
		t.Fatal("connection refused should be transient")
	}
	if !IsTransientError(errors.New("timed out waiting for port-forward 127.0.0.1:49346")) {
		t.Fatal("port-forward timeout should be transient")
	}
}

func TestCountFromResult(t *testing.T) {
	res := &clientv1.QueryResult{Rows: []*clientv1.QueryRow{{Fields: map[string]*clientv1.QueryValue{"count": {Value: &clientv1.QueryValue_Scalar{Scalar: structpb.NewNumberValue(42)}}}}}}
	count, err := CountFromResult(res)
	if err != nil || count != 42 {
		t.Fatalf("CountFromResult() = %d, %v", count, err)
	}
}
