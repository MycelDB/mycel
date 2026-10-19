package logical

// StatementKind identifies a member of the neutral statement family.
type StatementKind string

const (
	StatementRead     StatementKind = "read"
	StatementMutation StatementKind = "mutation"
)

// Statement is the canonical transport-neutral query-engine input. Exactly one
// of Read or Mutation is populated according to Kind.
type Statement struct {
	Kind       StatementKind `json:"kind"`
	Source     Source        `json:"source,omitempty"`
	AccessMode AccessMode    `json:"access_mode,omitempty"`
	Read       *Query        `json:"read,omitempty"`
	Mutation   *Mutation     `json:"mutation,omitempty"`
}

// ReadStatement wraps a canonical read query in the neutral statement family.
func ReadStatement(query Query) Statement {
	return Statement{
		Kind:       StatementRead,
		Source:     query.Source,
		AccessMode: query.AccessMode,
		Read:       &query,
	}
}

// MutationStatement wraps a canonical mutation in the neutral statement family.
func MutationStatement(source Source, mutation Mutation) Statement {
	return Statement{
		Kind:       StatementMutation,
		Source:     source,
		AccessMode: AccessModeReadWrite,
		Mutation:   &mutation,
	}
}

// Comparable removes adapter provenance and opaque continuation state while
// retaining statement semantics.
func (s Statement) Comparable() Statement {
	s.Source = ""
	if s.Read != nil {
		read := s.Read.Comparable()
		s.Read = &read
	}
	if s.Mutation != nil && s.Mutation.Match != nil {
		mutation := *s.Mutation
		match := mutation.Match.Comparable()
		mutation.Match = &match
		s.Mutation = &mutation
	}
	return s
}

// MutationKind identifies the GQL mutation family represented by a
// neutral statement while execution remains on the existing path.
type MutationKind string

const (
	MutationInsertNode         MutationKind = "insert_node"
	MutationMergeNode          MutationKind = "merge_node"
	MutationMatchSet           MutationKind = "match_set"
	MutationMatchDelete        MutationKind = "match_delete"
	MutationCreateRelationship MutationKind = "create_relationship"
	MutationMergeRelationship  MutationKind = "merge_relationship"
)

// Mutation preserves the semantics required to bind current GQL writes. Match
// is the read pattern used by matched set/delete operations; Nodes contains the
// independently matched endpoints used by relationship mutations.
type Mutation struct {
	Kind         MutationKind          `json:"kind"`
	Node         *NodePattern          `json:"node,omitempty"`
	Match        *Query                `json:"match,omitempty"`
	Nodes        []NodePattern         `json:"nodes,omitempty"`
	Relationship *RelationshipMutation `json:"relationship,omitempty"`
	Assignments  []Assignment          `json:"assignments,omitempty"`
	Targets      []string              `json:"targets,omitempty"`
	Returns      []Projection          `json:"returns,omitempty"`
	Aggregates   []Aggregate           `json:"aggregates,omitempty"`
	ReturnGraph  bool                  `json:"return_graph,omitempty"`
	Limit        int64                 `json:"limit,omitempty"`
	Page         PageSpec              `json:"page,omitempty"`
}

// RelationshipMutation creates or merges one relationship between aliases.
type RelationshipMutation struct {
	Alias      string           `json:"alias,omitempty"`
	FromAlias  string           `json:"from_alias"`
	ToAlias    string           `json:"to_alias"`
	Labels     []string         `json:"labels,omitempty"`
	Properties map[string]Value `json:"properties,omitempty"`
}

// Assignment updates one namespaced field on a matched alias.
type Assignment struct {
	Field FieldRef `json:"field"`
	Value Value    `json:"value"`
}
