package registration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/topology"
)

type fakeClient struct {
	calls   []string
	results map[string]fakeResult
}

type fakeResult struct {
	res RegisterNodeResult
	err error
}

func (f *fakeClient) RegisterNode(_ context.Context, addr string, _ RegisterNodeInput) (RegisterNodeResult, error) {
	f.calls = append(f.calls, addr)
	if r, ok := f.results[addr]; ok {
		return r.res, r.err
	}
	return RegisterNodeResult{}, errors.New("boom")
}

func newRegistry(t *testing.T) *topology.Registry {
	t.Helper()
	reg, err := topology.NewRegistry(context.Background(), nil, model.Peer{NodeID: "node_a", BackendAdvertiseAddr: "127.0.0.1:9093"})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestTryOnceNoSeedsDoesNothing(t *testing.T) {
	client := &fakeClient{}
	h := Handler{Topology: newRegistry(t), Client: client}
	if h.TryOnce(context.Background()) {
		t.Fatal("expected no success")
	}
	if len(client.calls) != 0 {
		t.Fatalf("calls=%v", client.calls)
	}
}

func TestTryOnceStopsAfterFirstSuccess(t *testing.T) {
	client := &fakeClient{results: map[string]fakeResult{"seed-a": {res: RegisterNodeResult{Accepted: true, Snapshot: model.Snapshot{Peers: []model.Peer{{NodeID: "node_b", BackendAdvertiseAddr: "127.0.0.1:9094", State: model.PeerStateActive, Source: model.PeerSourceDiscovered}}}}}}}
	reg := newRegistry(t)
	h := Handler{Topology: reg, Client: client, Seeds: []string{"seed-a", "seed-b"}}
	if !h.TryOnce(context.Background()) {
		t.Fatal("expected success")
	}
	if len(client.calls) != 1 || client.calls[0] != "seed-a" {
		t.Fatalf("calls=%v", client.calls)
	}
	found := false
	for _, p := range reg.RemotePeers() {
		if p.NodeID == "node_b" {
			found = true
		}
	}
	if !found {
		t.Fatalf("snapshot not merged: %#v", reg.List())
	}
}

func TestTryOnceFailedSeedDoesNotEnterTopologyAndTriesNext(t *testing.T) {
	client := &fakeClient{results: map[string]fakeResult{"seed-a": {err: errors.New("down")}, "seed-b": {res: RegisterNodeResult{Accepted: true, Snapshot: model.Snapshot{Peers: []model.Peer{{NodeID: "node_b", BackendAdvertiseAddr: "127.0.0.1:9094", State: model.PeerStateActive}}}}}}}
	reg := newRegistry(t)
	h := Handler{Topology: reg, Client: client, Seeds: []string{"seed-a", "seed-b"}}
	if !h.TryOnce(context.Background()) {
		t.Fatal("expected success")
	}
	if len(client.calls) != 2 {
		t.Fatalf("calls=%v", client.calls)
	}
	for _, p := range reg.RemotePeers() {
		if p.BackendAdvertiseAddr == "seed-a" {
			t.Fatalf("failed seed should not be in topology: %#v", reg.RemotePeers())
		}
	}
}

func TestRunRetriesUntilSuccess(t *testing.T) {
	client := &flakyClient{}
	reg := newRegistry(t)
	h := Handler{Topology: reg, Client: client, Seeds: []string{"seed-a"}, Interval: 10 * time.Millisecond}
	if err := h.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.calls < 2 {
		t.Fatalf("calls=%d want >=2", client.calls)
	}
}

type flakyClient struct{ calls int }

func (f *flakyClient) RegisterNode(_ context.Context, _ string, _ RegisterNodeInput) (RegisterNodeResult, error) {
	f.calls++
	if f.calls == 1 {
		return RegisterNodeResult{}, errors.New("temporary")
	}
	return RegisterNodeResult{Accepted: true, Snapshot: model.Snapshot{Peers: []model.Peer{{NodeID: "node_b", BackendAdvertiseAddr: "127.0.0.1:9094", State: model.PeerStateActive}}}}, nil
}
