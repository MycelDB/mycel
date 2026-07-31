package consensus

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MessageEnvelope struct {
	GroupID GroupID
	From    NodeID
	To      NodeID
	Message []byte
}

func EncodeMessage(groupID GroupID, from NodeID, msg raftpb.Message) (MessageEnvelope, error) {
	if groupID == "" || from == 0 || msg.To == 0 {
		return MessageEnvelope{}, fmt.Errorf("group id, from, and message to are required")
	}
	data, err := msg.Marshal()
	if err != nil {
		return MessageEnvelope{}, err
	}
	return MessageEnvelope{GroupID: groupID, From: from, To: NodeID(msg.To), Message: data}, nil
}

func DecodeMessage(env MessageEnvelope) (raftpb.Message, error) {
	if env.GroupID == "" || env.From == 0 || env.To == 0 || len(env.Message) == 0 {
		return raftpb.Message{}, fmt.Errorf("invalid raft message envelope")
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(env.Message); err != nil {
		return raftpb.Message{}, err
	}
	return msg, nil
}

type MessageSender interface {
	SendRaftMessage(ctx context.Context, env MessageEnvelope) error
}

type SenderResolver interface {
	SenderForNode(nodeID NodeID) (MessageSender, bool)
}

type ResolverFunc func(NodeID) (MessageSender, bool)

func (f ResolverFunc) SenderForNode(nodeID NodeID) (MessageSender, bool) { return f(nodeID) }

type TransportDiagnosticsSnapshot struct {
	SendAttempts          uint64
	SendFailures          uint64
	AuthFailures          uint64
	MissingSenderFailures uint64
	LastErrorAt           time.Time
	LastError             string
	LastFailureReason     string
	LastGroupID           GroupID
	LastSourceNodeID      NodeID
	LastTargetNodeID      NodeID
	LastMessageType       string
	Targets               []TransportTargetDiagnosticsSnapshot
}

type TransportTargetDiagnosticsSnapshot struct {
	GroupID               GroupID
	TargetNodeID          NodeID
	SendAttempts          uint64
	SendFailures          uint64
	AuthFailures          uint64
	MissingSenderFailures uint64
	LastErrorAt           time.Time
	LastError             string
	LastFailureReason     string
	LastMessageType       string
}

type TransportDiagnostics struct {
	mu    sync.Mutex
	log   *slog.Logger
	total TransportDiagnosticsSnapshot
	byKey map[transportDiagnosticsKey]*TransportTargetDiagnosticsSnapshot
}

type transportDiagnosticsKey struct {
	groupID GroupID
	target  NodeID
}

func NewTransportDiagnostics(logger *slog.Logger) *TransportDiagnostics {
	return &TransportDiagnostics{log: logger, byKey: map[transportDiagnosticsKey]*TransportTargetDiagnosticsSnapshot{}}
}

func (d *TransportDiagnostics) Snapshot() TransportDiagnosticsSnapshot {
	if d == nil {
		return TransportDiagnosticsSnapshot{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.total
	out.Targets = make([]TransportTargetDiagnosticsSnapshot, 0, len(d.byKey))
	for _, target := range d.byKey {
		out.Targets = append(out.Targets, *target)
	}
	sort.Slice(out.Targets, func(i, j int) bool {
		if out.Targets[i].GroupID == out.Targets[j].GroupID {
			return out.Targets[i].TargetNodeID < out.Targets[j].TargetNodeID
		}
		return out.Targets[i].GroupID < out.Targets[j].GroupID
	})
	return out
}

func (d *TransportDiagnostics) recordAttempt(groupID GroupID, from NodeID, to NodeID, messageType string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.total.SendAttempts++
	target := d.targetLocked(groupID, to)
	target.SendAttempts++
}

func (d *TransportDiagnostics) recordFailure(groupID GroupID, from NodeID, to NodeID, messageType string, reason string, err error) {
	if d == nil {
		return
	}
	now := time.Now().UTC()
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	d.mu.Lock()
	d.total.SendFailures++
	d.total.LastErrorAt = now
	d.total.LastError = errorText
	d.total.LastFailureReason = reason
	d.total.LastGroupID = groupID
	d.total.LastSourceNodeID = from
	d.total.LastTargetNodeID = to
	d.total.LastMessageType = messageType
	target := d.targetLocked(groupID, to)
	target.SendFailures++
	target.LastErrorAt = now
	target.LastError = errorText
	target.LastFailureReason = reason
	target.LastMessageType = messageType
	if reason == "missing_sender" {
		d.total.MissingSenderFailures++
		target.MissingSenderFailures++
	}
	if isRaftTransportAuthError(err) {
		d.total.AuthFailures++
		target.AuthFailures++
	}
	logger := d.log
	d.mu.Unlock()
	if logger != nil {
		logger.Warn("raft transport send failed", "group", groupID, "from_node_id", from, "target_node_id", to, "message_type", messageType, "reason", reason, "error", errorText)
	}
}

func (d *TransportDiagnostics) targetLocked(groupID GroupID, to NodeID) *TransportTargetDiagnosticsSnapshot {
	if d.byKey == nil {
		d.byKey = map[transportDiagnosticsKey]*TransportTargetDiagnosticsSnapshot{}
	}
	key := transportDiagnosticsKey{groupID: groupID, target: to}
	target := d.byKey[key]
	if target == nil {
		target = &TransportTargetDiagnosticsSnapshot{GroupID: groupID, TargetNodeID: to}
		d.byKey[key] = target
	}
	return target
}

func isRaftTransportAuthError(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	return code == codes.Unauthenticated || code == codes.PermissionDenied
}

type RoutedTransport struct {
	Resolver    SenderResolver
	Diagnostics *TransportDiagnostics
}

func (t RoutedTransport) Send(ctx context.Context, groupID GroupID, from NodeID, messages []raftpb.Message) {
	for _, msg := range messages {
		messageType := msg.Type.String()
		target := NodeID(msg.To)
		t.Diagnostics.recordAttempt(groupID, from, target, messageType)
		env, err := EncodeMessage(groupID, from, msg)
		if err != nil {
			t.Diagnostics.recordFailure(groupID, from, target, messageType, "encode_error", err)
			continue
		}
		if t.Resolver == nil {
			t.Diagnostics.recordFailure(groupID, from, env.To, messageType, "missing_sender", fmt.Errorf("raft sender resolver is not configured"))
			continue
		}
		sender, ok := t.Resolver.SenderForNode(env.To)
		if !ok || sender == nil {
			t.Diagnostics.recordFailure(groupID, from, env.To, messageType, "missing_sender", fmt.Errorf("raft sender for node %d is not configured", env.To))
			continue
		}
		if err := sender.SendRaftMessage(ctx, env); err != nil {
			reason := "send_error"
			if isRaftTransportAuthError(err) {
				reason = "auth_failure"
			}
			t.Diagnostics.recordFailure(groupID, from, env.To, messageType, reason, err)
		}
	}
}

type LocalMessageRouter struct {
	mu     sync.RWMutex
	groups map[GroupID]map[NodeID]*Group
}

func NewLocalMessageRouter() *LocalMessageRouter {
	return &LocalMessageRouter{groups: map[GroupID]map[NodeID]*Group{}}
}

func (r *LocalMessageRouter) Register(g *Group) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.groups[g.id] == nil {
		r.groups[g.id] = map[NodeID]*Group{}
	}
	r.groups[g.id][g.nodeID] = g
}

func (r *LocalMessageRouter) UnregisterNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for groupID := range r.groups {
		delete(r.groups[groupID], nodeID)
	}
}

func (r *LocalMessageRouter) SendRaftMessage(ctx context.Context, env MessageEnvelope) error {
	msg, err := DecodeMessage(env)
	if err != nil {
		return err
	}
	r.mu.RLock()
	g := r.groups[env.GroupID][env.To]
	r.mu.RUnlock()
	if g == nil {
		return fmt.Errorf("raft group %s node %d not found", env.GroupID, env.To)
	}
	return g.Step(ctx, msg)
}
