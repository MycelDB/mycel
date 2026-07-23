// Package model defines GQL execution results.
package model

// Result is the output of executing a GQL plan.
type Result struct {
	Counters Counters
}

// Counters summarizes mutations performed by a query.
type Counters struct {
	NodesInserted int
}

// NodeRef identifies a graph node created or read during execution.
type NodeRef struct {
	ID string
}
