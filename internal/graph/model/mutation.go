package graph

// NodeInput describes a graph node creation or upsert mutation.
type NodeInput struct {
	NodeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	BlobID     string
	Content    string
	Props      map[string]any

	// AutomationOutputFence marks this mutation as an active automation-output
	// write whose meta.automation claim must be validated at graph commit apply.
	// Ordinary graph writes may preserve durable Meta but must not set this flag.
	AutomationOutputFence bool
}

// UpdateNodeInput describes a graph node update mutation.
type UpdateNodeInput struct {
	NodeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	Content    *string
	Props      map[string]any
	UpdateMask []string

	AutomationOutputFence bool
}

// EdgeInput describes a graph edge creation mutation.
type EdgeInput struct {
	EdgeID     string
	FromNodeID string
	ToNodeID   string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any

	AutomationOutputFence bool
}

// UpdateEdgeInput describes a graph edge update mutation.
type UpdateEdgeInput struct {
	EdgeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	UpdateMask []string

	AutomationOutputFence bool
}

// ReferenceReplacementMode controls how replacement targets affect existing references.
type ReferenceReplacementMode string

const (
	ReferenceReplacementModeReplace ReferenceReplacementMode = "replace"
	ReferenceReplacementModeAdd     ReferenceReplacementMode = "add"
	ReferenceReplacementModeRemove  ReferenceReplacementMode = "remove"
)

// ReferenceTargetInput describes one desired reference edge target.
type ReferenceTargetInput struct {
	TargetNodeID string
	EdgeID       string
	Properties   map[string]any
	Payload      map[string]any
	Meta         map[string]any
	HasProps     bool
	HasPayload   bool
	HasMeta      bool
}

// ReplaceReferencesInput describes an atomic reference replacement mutation.
type ReplaceReferencesInput struct {
	SourceNodeID string
	Labels       []string
	Targets      []ReferenceTargetInput
	Mode         ReferenceReplacementMode
}

// ReplaceReferencesResult reports reference edges affected by a replacement mutation.
type ReplaceReferencesResult struct {
	AddedEdges     []Edge
	UpdatedEdges   []Edge
	DeletedEdgeIDs []string
}
