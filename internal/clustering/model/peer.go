package model

import "time"

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
