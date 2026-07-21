package consensus

import (
	"context"
	"fmt"
	"sync"

	raftpb "go.etcd.io/raft/v3/raftpb"
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

type RoutedTransport struct {
	Resolver SenderResolver
}

func (t RoutedTransport) Send(ctx context.Context, groupID GroupID, from NodeID, messages []raftpb.Message) {
	if t.Resolver == nil {
		return
	}
	for _, msg := range messages {
		env, err := EncodeMessage(groupID, from, msg)
		if err != nil {
			continue
		}
		sender, ok := t.Resolver.SenderForNode(env.To)
		if !ok || sender == nil {
			continue
		}
		_ = sender.SendRaftMessage(ctx, env)
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
