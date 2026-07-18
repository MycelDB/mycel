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
	SeedPeers                []string
	Bootstrap                bool
	NodePublicKeyFingerprint string
	JoinToken                string
	JoinTokenFile            string
	Now                      func() time.Time
}

type LocalNode struct {
	Identity NodeIdentity
	State    NodeState
}
