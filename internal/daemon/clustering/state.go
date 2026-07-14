package clustering

// NodeState describes the local daemon's clustering lifecycle state.
type NodeState string

const (
	NodeStateInitializing  NodeState = "initializing"
	NodeStateStandalone    NodeState = "standalone"
	NodeStateClusterSingle NodeState = "cluster_single"
	NodeStateFailed        NodeState = "failed"
	NodeStateStopped       NodeState = "stopped"
)
