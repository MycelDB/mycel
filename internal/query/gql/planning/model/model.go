// Package model defines execution-oriented GQL plan structures.
package model

import "github.com/myceldb/mycel/internal/query/gql/analysis"

// Plan is the execution-oriented output of GQL planning.
type Plan struct {
	AccessMode analysis.AccessMode
	Operations []Operation
}

// Operation is implemented by all planned operation types.
type Operation interface {
	operation()
}

// InsertNodeOperation inserts one graph node with optional labels/properties.
type InsertNodeOperation struct {
	Variable   string
	Labels     []string
	Properties map[string]any
}

func (InsertNodeOperation) operation() {}
