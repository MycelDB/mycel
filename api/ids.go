package api

import "github.com/google/uuid"

// NodeID uniquely identifies a node in the graph.
type NodeID = uuid.UUID

// EdgeID uniquely identifies an edge in the graph.
type EdgeID = uuid.UUID

// TemplateID uniquely identifies a template definition.
type TemplateID = uuid.UUID
