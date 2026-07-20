package membership

import "time"

const StoreVersion = 1

type MemberState string

const (
	MemberStatePending  MemberState = "pending"
	MemberStateActive   MemberState = "active"
	MemberStateRejected MemberState = "rejected"
	MemberStateRemoved  MemberState = "removed"
)

type StoreData struct {
	Version     int       `json:"version"`
	ClusterID   string    `json:"cluster_id"`
	ClusterName string    `json:"cluster_name,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
	Members     []Member  `json:"members"`
}

type Member struct {
	NodeName                 string      `json:"node_name"`
	NodeID                   string      `json:"node_id,omitempty"`
	State                    MemberState `json:"state"`
	BackendAdvertiseAddr     string      `json:"backend_advertise_addr,omitempty"`
	Role                     string      `json:"role,omitempty"`
	ClusterBootstrap         bool        `json:"cluster_bootstrap,omitempty"`
	NodePublicKeyFingerprint string      `json:"node_public_key_fingerprint,omitempty"`
	CreatedAt                time.Time   `json:"created_at"`
	UpdatedAt                time.Time   `json:"updated_at"`
	JoinedAt                 *time.Time  `json:"joined_at,omitempty"`
}
