package clustering

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const PeerStoreVersion = 1

type PeerState string
type PeerSource string

const (
	PeerStateSelf        PeerState = "self"
	PeerStateSeed        PeerState = "seed"
	PeerStateActive      PeerState = "active"
	PeerStateUnreachable PeerState = "unreachable"

	PeerSourceSelf       PeerSource = "self"
	PeerSourceSeed       PeerSource = "seed"
	PeerSourceDiscovered PeerSource = "discovered"
)

type Peer struct {
	NodeID               string     `json:"node_id,omitempty"`
	NodeName             string     `json:"node_name,omitempty"`
	ClusterID            string     `json:"cluster_id,omitempty"`
	ClusterName          string     `json:"cluster_name,omitempty"`
	BackendAdvertiseAddr string     `json:"backend_advertise_addr"`
	State                PeerState  `json:"state"`
	Source               PeerSource `json:"source"`
	LastSeenAt           *time.Time `json:"last_seen_at,omitempty"`
}

type PeerStore struct {
	Version   int       `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
	Peers     []Peer    `json:"peers"`
}

func PeersPath(dataDir string) string {
	return filepath.Join(dataDir, "meta", "clustering", "peers.json")
}

func ReadPeers(dataDir string) (PeerStore, error) {
	path := PeersPath(dataDir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return PeerStore{Version: PeerStoreVersion, Peers: []Peer{}}, nil
		}
		return PeerStore{}, err
	}
	var store PeerStore
	if err := json.Unmarshal(raw, &store); err != nil {
		return PeerStore{}, err
	}
	if store.Version == 0 {
		store.Version = PeerStoreVersion
	}
	return store, nil
}

func UpsertPeer(dataDir string, peer Peer, now time.Time) error {
	if strings.TrimSpace(peer.BackendAdvertiseAddr) == "" {
		return nil
	}
	if err := ValidateBackendAdvertiseAddr(peer.BackendAdvertiseAddr); err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store, err := ReadPeers(dataDir)
	if err != nil {
		return err
	}
	store.Version = PeerStoreVersion
	store.UpdatedAt = now.UTC()
	for i, existing := range store.Peers {
		if (peer.NodeID != "" && existing.NodeID == peer.NodeID) || existing.BackendAdvertiseAddr == peer.BackendAdvertiseAddr {
			store.Peers[i] = peer
			return writePeerStore(dataDir, store)
		}
	}
	store.Peers = append(store.Peers, peer)
	return writePeerStore(dataDir, store)
}

func WritePeers(dataDir string, identity NodeIdentity, seedPeers []string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	peers := []Peer{}
	if strings.TrimSpace(identity.BackendAdvertiseAddr) != "" {
		seen := now
		peers = append(peers, Peer{NodeID: identity.NodeID, NodeName: identity.NodeName, ClusterID: identity.ClusterID, ClusterName: identity.ClusterName, BackendAdvertiseAddr: identity.BackendAdvertiseAddr, State: PeerStateSelf, Source: PeerSourceSelf, LastSeenAt: &seen})
	}
	seenAddr := map[string]bool{}
	if strings.TrimSpace(identity.BackendAdvertiseAddr) != "" {
		seenAddr[strings.TrimSpace(identity.BackendAdvertiseAddr)] = true
	}
	for _, addr := range seedPeers {
		addr = strings.TrimSpace(addr)
		if addr == "" || seenAddr[addr] {
			continue
		}
		if err := ValidateBackendAdvertiseAddr(addr); err != nil {
			return err
		}
		seenAddr[addr] = true
		peers = append(peers, Peer{BackendAdvertiseAddr: addr, State: PeerStateSeed, Source: PeerSourceSeed})
	}
	store := PeerStore{Version: PeerStoreVersion, UpdatedAt: now, Peers: peers}
	return writePeerStore(dataDir, store)
}

func writePeerStore(dataDir string, store PeerStore) error {
	path := PeersPath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create clustering peers directory: %w", err)
	}
	raw, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
