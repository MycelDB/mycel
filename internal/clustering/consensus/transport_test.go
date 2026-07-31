package consensus

import (
	"context"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type failingMessageSender struct{ err error }

func (s failingMessageSender) SendRaftMessage(ctx context.Context, env MessageEnvelope) error {
	return s.err
}

func TestRoutedTransportDiagnosticsMissingSender(t *testing.T) {
	diagnostics := NewTransportDiagnostics(nil)
	transport := RoutedTransport{Diagnostics: diagnostics, Resolver: ResolverFunc(func(nodeID NodeID) (MessageSender, bool) { return nil, false })}
	transport.Send(context.Background(), "system", 1, []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 1, To: 2}})
	snapshot := diagnostics.Snapshot()
	if snapshot.SendAttempts != 1 || snapshot.SendFailures != 1 || snapshot.MissingSenderFailures != 1 {
		t.Fatalf("unexpected diagnostics: %#v", snapshot)
	}
	if snapshot.LastGroupID != "system" || snapshot.LastSourceNodeID != 1 || snapshot.LastTargetNodeID != 2 || snapshot.LastMessageType != raftpb.MsgHeartbeat.String() {
		t.Fatalf("unexpected last failure context: %#v", snapshot)
	}
	if len(snapshot.Targets) != 1 || snapshot.Targets[0].MissingSenderFailures != 1 {
		t.Fatalf("unexpected target diagnostics: %#v", snapshot.Targets)
	}
}

func TestRoutedTransportDiagnosticsAuthFailure(t *testing.T) {
	diagnostics := NewTransportDiagnostics(nil)
	transport := RoutedTransport{Diagnostics: diagnostics, Resolver: ResolverFunc(func(nodeID NodeID) (MessageSender, bool) {
		return failingMessageSender{err: status.Error(codes.Unauthenticated, "cluster backend authentication required")}, true
	})}
	transport.Send(context.Background(), "system", 1, []raftpb.Message{{Type: raftpb.MsgApp, From: 1, To: 2}})
	snapshot := diagnostics.Snapshot()
	if snapshot.SendAttempts != 1 || snapshot.SendFailures != 1 || snapshot.AuthFailures != 1 || snapshot.LastFailureReason != "auth_failure" {
		t.Fatalf("unexpected diagnostics: %#v", snapshot)
	}
	if len(snapshot.Targets) != 1 || snapshot.Targets[0].AuthFailures != 1 {
		t.Fatalf("unexpected target diagnostics: %#v", snapshot.Targets)
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
