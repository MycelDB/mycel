package clustering

import "time"

const NodeIdentityVersion = 1

type NodeIdentity struct {
	Version              int       `json:"version"`
	NodeID               string    `json:"node_id"`
	NodeName             string    `json:"node_name,omitempty"`
	ClusterID            string    `json:"cluster_id"`
	ClusterName          string    `json:"cluster_name,omitempty"`
	BackendAdvertiseAddr string    `json:"backend_advertise_addr,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type Options struct {
	DataDir              string
	NodeName             string
	ClusterName          string
	BackendAdvertiseAddr string
	SeedPeers            []string
	Now                  func() time.Time
}

type LocalNode struct {
	Identity NodeIdentity
	State    NodeState
}
