package clustering

import "github.com/myceldb/mycel/internal/clustering/model"

type NodeState = model.NodeState

const (
	NodeStateInitializing  = model.NodeStateInitializing
	NodeStateStandalone    = model.NodeStateStandalone
	NodeStateClustered     = model.NodeStateClustered
	NodeStateClusterSingle = model.NodeStateClustered // backward-compatible alias
	NodeStateFailed        = model.NodeStateFailed
	NodeStateStopped       = model.NodeStateStopped
)
