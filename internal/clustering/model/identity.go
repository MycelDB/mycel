package model

import "time"

const NodeIdentityVersion = 1

type NodeIdentity struct {
	Version                  int       `json:"version"`
	NodeID                   string    `json:"node_id"`
	NodeName                 string    `json:"node_name,omitempty"`
	ClusterID                string    `json:"cluster_id"`
	ClusterName              string    `json:"cluster_name,omitempty"`
	BackendAdvertiseAddr     string    `json:"backend_advertise_addr,omitempty"`
	ClusterAdmitted          bool      `json:"cluster_admitted"`
	ClusterBootstrap         bool      `json:"cluster_bootstrap,omitempty"`
	NodePublicKeyFingerprint string    `json:"node_public_key_fingerprint,omitempty"`
	CreatedAt                time.Time `json:"created_at"`
	UpdatedAt                time.Time `json:"updated_at"`
}
