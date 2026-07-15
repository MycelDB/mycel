package topology

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
)

type Registry struct {
	mu          sync.RWMutex
	store       Store
	peers       map[string]model.Peer
	subscribers map[uint64]chan model.Event
	nextSubID   uint64
}

func NewRegistry(ctx context.Context, store Store, self model.Peer) (*Registry, error) {
	r := &Registry{store: store, peers: map[string]model.Peer{}, subscribers: map[uint64]chan model.Event{}}
	if store != nil {
		snap, err := store.Load(ctx)
		if err != nil {
			return nil, err
		}
		for _, p := range snap.Peers {
			if key := peerKey(p); key != "" {
				r.peers[key] = p
			}
		}
	}
	if self.BackendAdvertiseAddr != "" || self.NodeID != "" {
		self.State = model.PeerStateSelf
		self.Source = model.PeerSourceSelf
		if self.LastSeenAt == nil {
			now := time.Now().UTC()
			self.LastSeenAt = &now
		}
		r.upsertLocked(self)
	}
	if err := r.persistLocked(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Registry) Self() (model.Peer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.peers {
		if p.State == model.PeerStateSelf {
			return p, true
		}
	}
	return model.Peer{}, false
}

func (r *Registry) Snapshot() model.Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return model.Snapshot{Version: model.PeerStoreVersion, UpdatedAt: time.Now().UTC(), Peers: sortedPeers(r.peers)}
}

func (r *Registry) List() []model.Peer { return r.Snapshot().Peers }

func (r *Registry) RemotePeers() []model.Peer {
	peers := r.List()
	out := make([]model.Peer, 0, len(peers))
	for _, p := range peers {
		if p.State != model.PeerStateSelf {
			out = append(out, p)
		}
	}
	return out
}

func (r *Registry) Upsert(ctx context.Context, peer model.Peer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev, existed, changed := r.upsertLocked(peer)
	if !changed {
		return nil
	}
	if err := r.persistLocked(ctx); err != nil {
		return err
	}
	r.publishLocked(eventForUpsert(peer, prev, existed))
	return nil
}

func (r *Registry) Merge(ctx context.Context, snapshot model.Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var events []model.Event
	for _, peer := range snapshot.Peers {
		if peer.State == model.PeerStateSelf {
			peer.State = model.PeerStateActive
			peer.Source = model.PeerSourceDiscovered
		}
		prev, existed, changed := r.upsertLocked(peer)
		if changed {
			events = append(events, eventForUpsert(peer, prev, existed))
		}
	}
	if len(events) == 0 {
		return nil
	}
	if err := r.persistLocked(ctx); err != nil {
		return err
	}
	for _, ev := range events {
		r.publishLocked(ev)
	}
	r.publishLocked(model.Event{Type: model.EventSnapshotMerged, At: time.Now().UTC()})
	return nil
}

func (r *Registry) MarkUnreachable(ctx context.Context, addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := "addr:" + addr
	peer, ok := r.peers[key]
	if !ok {
		peer = model.Peer{BackendAdvertiseAddr: addr, Source: model.PeerSourceSeed}
	}
	if peer.State == model.PeerStateSelf {
		return nil
	}
	prev := peer
	peer.State = model.PeerStateUnreachable
	if peer.Source == "" {
		peer.Source = model.PeerSourceSeed
	}
	r.peers[peerKey(peer)] = peer
	if err := r.persistLocked(ctx); err != nil {
		return err
	}
	r.publishLocked(model.Event{Type: model.EventPeerStateChanged, Peer: peer, Previous: &prev, At: time.Now().UTC()})
	return nil
}

func (r *Registry) Remove(ctx context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	peer, ok := r.peers[key]
	if !ok {
		return nil
	}
	if peer.State == model.PeerStateSelf {
		return nil
	}
	delete(r.peers, key)
	if err := r.persistLocked(ctx); err != nil {
		return err
	}
	r.publishLocked(model.Event{Type: model.EventPeerRemoved, Peer: peer, At: time.Now().UTC()})
	return nil
}

func (r *Registry) Subscribe(buffer int) (<-chan model.Event, func()) {
	if buffer < 0 {
		buffer = 0
	}
	ch := make(chan model.Event, buffer)
	r.mu.Lock()
	id := r.nextSubID
	r.nextSubID++
	r.subscribers[id] = ch
	r.mu.Unlock()
	return ch, func() {
		r.mu.Lock()
		if c, ok := r.subscribers[id]; ok {
			delete(r.subscribers, id)
			close(c)
		}
		r.mu.Unlock()
	}
}

func (r *Registry) upsertLocked(peer model.Peer) (model.Peer, bool, bool) {
	key := peerKey(peer)
	if key == "" {
		return model.Peer{}, false, false
	}
	if existingSelf, ok := r.selfLocked(); ok && existingSelf.NodeID != "" && peer.NodeID == existingSelf.NodeID && peer.State != model.PeerStateSelf {
		return existingSelf, true, false
	}
	prev, existed := r.peers[key]
	if existed && prev == peer {
		return prev, true, false
	}
	r.peers[key] = peer
	return prev, existed, true
}

func (r *Registry) selfLocked() (model.Peer, bool) {
	for _, p := range r.peers {
		if p.State == model.PeerStateSelf {
			return p, true
		}
	}
	return model.Peer{}, false
}

func (r *Registry) persistLocked(ctx context.Context) error {
	if r.store == nil {
		return nil
	}
	return r.store.Save(ctx, model.Snapshot{Version: model.PeerStoreVersion, UpdatedAt: time.Now().UTC(), Peers: sortedPeers(r.peers)})
}

func (r *Registry) publishLocked(ev model.Event) {
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	for _, ch := range r.subscribers {
		select {
		case ch <- ev:
		default:
		}
	}
}

func eventForUpsert(peer model.Peer, prev model.Peer, existed bool) model.Event {
	if !existed {
		return model.Event{Type: model.EventPeerAdded, Peer: peer, At: time.Now().UTC()}
	}
	typ := model.EventPeerUpdated
	if prev.State != peer.State {
		typ = model.EventPeerStateChanged
	}
	if prev.State == model.PeerStateSelf {
		typ = model.EventSelfUpdated
	}
	return model.Event{Type: typ, Peer: peer, Previous: &prev, At: time.Now().UTC()}
}

func peerKey(p model.Peer) string {
	if strings.TrimSpace(p.NodeID) != "" {
		return "node:" + strings.TrimSpace(p.NodeID)
	}
	if strings.TrimSpace(p.BackendAdvertiseAddr) != "" {
		return "addr:" + strings.TrimSpace(p.BackendAdvertiseAddr)
	}
	return ""
}

func sortedPeers(peers map[string]model.Peer) []model.Peer {
	out := make([]model.Peer, 0, len(peers))
	for _, p := range peers {
		out = append(out, p)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].State == model.PeerStateSelf {
			return true
		}
		if out[j].State == model.PeerStateSelf {
			return false
		}
		return peerKey(out[i]) < peerKey(out[j])
	})
	return out
}
