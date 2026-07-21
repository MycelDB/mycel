package consensus

import (
	"context"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestMessageEnvelopeRoundTrip(t *testing.T) {
	msg := raftpb.Message{Type: raftpb.MsgApp, From: 1, To: 2, Term: 3, Index: 4}
	env, err := EncodeMessage("system", 1, msg)
	if err != nil {
		t.Fatalf("EncodeMessage() error = %v", err)
	}
	got, err := DecodeMessage(env)
	if err != nil {
		t.Fatalf("DecodeMessage() error = %v", err)
	}
	if got.Type != msg.Type || got.From != msg.From || got.To != msg.To || got.Term != msg.Term || got.Index != msg.Index {
		t.Fatalf("decoded message mismatch got=%+v want=%+v", got, msg)
	}
}

func TestRoutedTransportWithLocalRouterCommits(t *testing.T) {
	routers := map[NodeID]*LocalMessageRouter{1: NewLocalMessageRouter(), 2: NewLocalMessageRouter(), 3: NewLocalMessageRouter()}
	transport := RoutedTransport{Resolver: ResolverFunc(func(nodeID NodeID) (MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	ctx := context.Background()
	groups := map[NodeID]*Group{}
	sms := map[NodeID]*MemoryStateMachine{}
	peers := []NodeID{1, 2, 3}
	defer func() {
		for _, g := range groups {
			g.Stop()
		}
	}()
	for _, id := range peers {
		sm := &MemoryStateMachine{}
		g, err := StartGroup(ctx, GroupOptions{ID: "system", NodeID: id, Peers: peers, PartitionCount: 64, StateMachine: sm, Transport: transport, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartGroup(%d) error = %v", id, err)
		}
		groups[id] = g
		sms[id] = sm
		for _, router := range routers {
			router.Register(g)
		}
	}
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, g := range groups {
					g.Tick()
				}
			}
		}
	}()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leader := func() *Group {
		counts := map[NodeID]int{}
		for _, g := range groups {
			if l := g.Leader(); l != 0 {
				counts[l]++
			}
		}
		for id, count := range counts {
			if count >= 2 {
				return groups[id]
			}
		}
		return nil
	}
	if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool { return leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.test"), []byte(`{}`), "transport-cmd-1")
	if _, err := leader().Propose(waitCtx, cmd); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		applied := 0
		for _, sm := range sms {
			if sm.AppliedCount() > 0 {
				applied++
			}
		}
		return applied >= 2
	}); err != nil {
		t.Fatalf("command did not apply on quorum: %v", err)
	}
}
