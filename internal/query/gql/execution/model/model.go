// Package model defines GQL execution results.
package model

// Result is the output of executing a GQL plan.
type Result struct {
	Counters Counters
	Columns  []string
	Rows     []Row
}

// Counters summarizes mutations performed by a query.
type Counters struct {
	NodesInserted int
	EdgesInserted int
}

type Row map[string]Value

type Value struct {
	Node   *Node `json:"node,omitempty"`
	Edge   *Edge `json:"edge,omitempty"`
	Scalar any   `json:"scalar,omitempty"`
}

type Node struct {
	ID         string         `json:"id"`
	DomainID   string         `json:"domainId,omitempty"`
	Labels     []string       `json:"labels,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

type Edge struct {
	ID         string         `json:"id"`
	DomainID   string         `json:"domainId,omitempty"`
	FromID     string         `json:"fromId"`
	ToID       string         `json:"toId"`
	Labels     []string       `json:"labels,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
	Payload    map[string]any `json:"payload,omitempty"`
	Meta       map[string]any `json:"meta,omitempty"`
}

// NodeRef identifies a graph node created or read during execution.
type NodeRef struct {
	ID string
}
