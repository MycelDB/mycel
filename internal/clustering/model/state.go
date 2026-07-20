package model

// NodeState describes the local daemon's clustering lifecycle state.
type NodeState string

const (
	NodeStateInitializing NodeState = "initializing"
	NodeStateStandalone   NodeState = "standalone"
	NodeStateClustered    NodeState = "clustered"
	NodeStateFailed       NodeState = "failed"
	NodeStateStopped      NodeState = "stopped"
)

type ClusterMode string

const (
	ClusterModeStandalone ClusterMode = "standalone"
	ClusterModeClustered  ClusterMode = "clustered"
)
