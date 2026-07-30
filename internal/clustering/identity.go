package clustering

import (
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
)

const NodeIdentityVersion = model.NodeIdentityVersion

type NodeIdentity = model.NodeIdentity

type Options struct {
	DataDir                  string
	NodeName                 string
	ClusterName              string
	BackendAdvertiseAddr     string
	BackendAuthToken         string
	NodePublicKeyFingerprint string

	// RaftMode makes local identity a cache of authoritative system Raft
	// metadata. Fresh raft-mode identities are pending until system metadata is
	// committed and applied.
	RaftMode        bool
	RaftLocalNodeID uint64
	RaftNodeCount   int

	Now func() time.Time
}

type LocalNode struct {
	Identity NodeIdentity
	State    NodeState
}
