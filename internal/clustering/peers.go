package clustering

import (
	"context"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/topology"
)

const PeerStoreVersion = model.PeerStoreVersion

type PeerState = model.PeerState
type PeerSource = model.PeerSource
type Peer = model.Peer
type PeerStore = model.Snapshot

const (
	PeerStateSelf        = model.PeerStateSelf
	PeerStateSeed        = model.PeerStateSeed
	PeerStateActive      = model.PeerStateActive
	PeerStateUnreachable = model.PeerStateUnreachable

	PeerSourceSelf       = model.PeerSourceSelf
	PeerSourceSeed       = model.PeerSourceSeed
	PeerSourceDiscovered = model.PeerSourceDiscovered
)

func PeersPath(dataDir string) string { return topology.PeersPath(dataDir) }

func ReadPeers(dataDir string) (PeerStore, error) {
	return topology.NewFileStore(PeersPath(dataDir)).Load(context.Background())
}

func UpsertPeer(dataDir string, peer Peer, now time.Time) error {
	store := topology.NewFileStore(PeersPath(dataDir))
	reg, err := topology.NewRegistry(context.Background(), store, Peer{})
	if err != nil {
		return err
	}
	if peer.LastSeenAt == nil && peer.State == PeerStateActive && peer.NodeID != "" {
		seen := now.UTC()
		peer.LastSeenAt = &seen
	}
	return reg.Upsert(context.Background(), peer)
}

func WritePeers(dataDir string, identity NodeIdentity, seedPeers []string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for _, addr := range seedPeers {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			if err := ValidateBackendAdvertiseAddr(addr); err != nil {
				return err
			}
		}
	}
	var self Peer
	if strings.TrimSpace(identity.BackendAdvertiseAddr) != "" {
		seen := now.UTC()
		self = Peer{NodeID: identity.NodeID, NodeName: identity.NodeName, ClusterID: identity.ClusterID, ClusterName: identity.ClusterName, BackendAdvertiseAddr: identity.BackendAdvertiseAddr, State: PeerStateSelf, Source: PeerSourceSelf, LastSeenAt: &seen}
	}
	store := topology.NewFileStore(PeersPath(dataDir))
	_, err := topology.NewRegistry(context.Background(), store, self)
	return err
}
