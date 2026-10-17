// Package binding validates and binds transport-neutral query statements.
package binding

import (
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

// DomainPolicy captures query-relevant domain policy without coupling the
// binder to daemon space services.
type DomainPolicy struct {
	ReadOnly bool
	// BroadSearchable is carried into planning but cannot be enforced until a
	// physical access path distinguishes explicit/indexed reads from broad scans.
	BroadSearchable             bool
	ExplicitSearchable          bool
	SemanticSearchable          bool
	SemanticCandidatesAvailable bool
}

// Limits are semantic safety bounds applied before physical planning.
type Limits struct {
	MaxTraversalDepth  int
	OpenTraversalDepth int
}

// Environment contains immutable request-scoped binding inputs.
type Environment struct {
	Transaction sessionmodel.GraphTransaction
	Schema      *schemamodel.DomainSchema
	Domain      DomainPolicy
	Limits      Limits
	Parameters  map[string]querylogical.Value
}

// VariableKind identifies the entity represented by an alias.
type VariableKind string

const (
	VariableNode VariableKind = "node"
	VariableEdge VariableKind = "edge"
	VariablePath VariableKind = "path"
)

// Variable is one bound alias and its possible schema types.
type Variable struct {
	Name      string
	Kind      VariableKind
	NodeTypes []string
	EdgeTypes []string
}

// BoundStatement is the semantic handoff to logical optimization and physical
// planning. Variables are emitted in deterministic declaration order.
type BoundStatement struct {
	Statement     querylogical.Statement
	Variables     []Variable
	SchemaVersion string
}
