package api

import "github.com/google/uuid"

// NodeID uniquely identifies a node in the graph.
type NodeID = uuid.UUID

// EdgeID uniquely identifies an edge in the graph.
type EdgeID = uuid.UUID

// TemplateID uniquely identifies a template definition.
type TemplateID = uuid.UUID

// UserID uniquely identifies a user internally.
//
// UserID is an immutable UUID used as the stable system key.
type UserID = uuid.UUID

// UserRef uniquely identifies a user externally.
//
// UserRef is an immutable external identifier such as an email, username, or
// identity-provider subject.
type UserRef string
