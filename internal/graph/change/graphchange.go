package graphchange

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// ChangeType identifies the kind of committed graph mutation.
//
// The V1 internal constants preserve the existing change-stream string values so
// persisted daemon-local replay logs and public stream mappings remain
// compatible while the canonical model is consolidated.
type ChangeType string

const (
	ChangeTypeNodeCreated      ChangeType = "node_created"
	ChangeTypeNodeUpdated      ChangeType = "node_updated"
	ChangeTypeNodeDeleted      ChangeType = "node_deleted"
	ChangeTypeEdgeCreated      ChangeType = "edge_created"
	ChangeTypeEdgeUpdated      ChangeType = "edge_updated"
	ChangeTypeEdgeDeleted      ChangeType = "edge_deleted"
	ChangeTypeRevisionAdvanced ChangeType = "revision_advanced"
)

// OriginMetadata describes advisory client/write origin information. Trusted
// fields such as user, session, and transaction identifiers must be populated by
// the server when API plumbing is added.
type OriginMetadata struct {
	ClientID         string `json:"client_id,omitempty"`
	ClientInstanceID string `json:"client_instance_id,omitempty"`
	OperationID      string `json:"operation_id,omitempty"`
	Label            string `json:"label,omitempty"`
	PrincipalID      string `json:"principal_id,omitempty"`
	SessionID        string `json:"session_id,omitempty"`
	TransactionID    string `json:"transaction_id,omitempty"`
}

// Scope limits a registration or replay request to a portion of a graph.
type Scope struct {
	SpaceID  string   `json:"space_id,omitempty"`
	DomainID string   `json:"domain_id,omitempty"`
	NodeIDs  []string `json:"node_ids,omitempty"`
	EdgeIDs  []string `json:"edge_ids,omitempty"`
}

// Filter selects relevant event kinds inside a scope.
type Filter struct {
	EventTypes []ChangeType `json:"event_types,omitempty"`
	Labels     []string     `json:"labels,omitempty"`
	Fields     []string     `json:"fields,omitempty"`
}

// Projection selects which event and snapshot fields should be delivered to a
// consumer. Full snapshots are opt-in so cache invalidation consumers can prefer
// compact affected-ID payloads.
type Projection struct {
	IncludeRevision        bool     `json:"include_revision,omitempty"`
	IncludeOrigin          bool     `json:"include_origin,omitempty"`
	IncludeAffectedNodeIDs bool     `json:"include_affected_node_ids,omitempty"`
	IncludeAffectedEdgeIDs bool     `json:"include_affected_edge_ids,omitempty"`
	IncludeChangedFields   bool     `json:"include_changed_fields,omitempty"`
	IncludeOldNodeSnapshot bool     `json:"include_old_node_snapshot,omitempty"`
	IncludeNewNodeSnapshot bool     `json:"include_new_node_snapshot,omitempty"`
	IncludeOldEdgeSnapshot bool     `json:"include_old_edge_snapshot,omitempty"`
	IncludeNewEdgeSnapshot bool     `json:"include_new_edge_snapshot,omitempty"`
	NodeFields             []string `json:"node_fields,omitempty"`
	EdgeFields             []string `json:"edge_fields,omitempty"`
}

// Gap reports that a consumer checkpoint is older than retained change history.
type Gap struct {
	SpaceID                 string `json:"space_id,omitempty"`
	DomainID                string `json:"domain_id,omitempty"`
	RequestedAfterRevision  uint64 `json:"requested_after_revision,omitempty"`
	OldestAvailableRevision uint64 `json:"oldest_available_revision,omitempty"`
	CurrentRevision         uint64 `json:"current_revision,omitempty"`
}

// EdgeChange describes a graph edge mutation that may affect semantic source text.
type EdgeChange struct {
	EdgeID graph.EdgeID
	Labels []string
	Change string
	FromID graph.NodeID
	ToID   graph.NodeID
}

// Change describes one object-level graph mutation within a committed event.
type Change struct {
	Type ChangeType `json:"type,omitempty"`

	NodeID string `json:"node_id,omitempty"`
	EdgeID string `json:"edge_id,omitempty"`

	Node    *graph.Node `json:"node,omitempty"`
	OldNode *graph.Node `json:"old_node,omitempty"`
	Edge    *graph.Edge `json:"edge,omitempty"`
	OldEdge *graph.Edge `json:"old_edge,omitempty"`

	ChangedFields   []string `json:"changed_fields,omitempty"`
	AffectedNodeIDs []string `json:"affected_node_ids,omitempty"`
	AffectedEdgeIDs []string `json:"affected_edge_ids,omitempty"`
}

// CommittedEvent describes one committed graph transaction.
// It is intentionally semantic-neutral so graph/session code does not depend on
// semantic maintenance packages.
type CommittedEvent struct {
	ID            uuid.UUID
	CommitID      uuid.UUID
	TxnID         uuid.UUID
	TransactionID uuid.UUID
	GraphRevision uint64
	Revision      uint64
	SpaceID       domainspace.SpaceID
	DomainID      graph.DomainID
	DomainIDs     []graph.DomainID
	Origin        OriginMetadata
	Changes       []Change

	CreatedNodeIDs  []graph.NodeID
	UpdatedNodeIDs  []graph.NodeID
	DeletedNodeIDs  []graph.NodeID
	ChangedEdges    []EdgeChange
	AffectedNodeIDs []graph.NodeID
	AffectedEdgeIDs []graph.EdgeID

	OldParentByNodeID map[graph.NodeID]graph.NodeID
	NewParentByNodeID map[graph.NodeID]graph.NodeID
	OldDomainByNodeID map[graph.NodeID]graph.DomainID
	NewDomainByNodeID map[graph.NodeID]graph.DomainID
	CommittedAt       time.Time
}

// Empty reports whether this event has no graph changes.
func (e CommittedEvent) Empty() bool {
	return len(e.Changes) == 0 && len(e.CreatedNodeIDs) == 0 && len(e.UpdatedNodeIDs) == 0 && len(e.DeletedNodeIDs) == 0 && len(e.ChangedEdges) == 0
}

// Normalize fills compatibility and aggregate fields that can be derived from
// canonical changes and legacy edge summaries. It is safe to call repeatedly.
func (e *CommittedEvent) Normalize() {
	if e == nil {
		return
	}
	if e.Revision == 0 {
		e.Revision = e.GraphRevision
	}
	if e.GraphRevision == 0 {
		e.GraphRevision = e.Revision
	}
	if e.TransactionID == uuid.Nil {
		e.TransactionID = e.TxnID
	}
	if e.TxnID == uuid.Nil {
		e.TxnID = e.TransactionID
	}
	if e.DomainID == uuid.Nil && len(e.DomainIDs) > 0 {
		e.DomainID = e.DomainIDs[0]
	}
	if e.DomainID != uuid.Nil && !domainIDsContain(e.DomainIDs, e.DomainID) {
		e.DomainIDs = append(e.DomainIDs, e.DomainID)
	}

	affectedNodes := map[graph.NodeID]bool{}
	affectedEdges := map[graph.EdgeID]bool{}
	for _, id := range e.AffectedNodeIDs {
		if id != uuid.Nil {
			affectedNodes[id] = true
		}
	}
	for _, id := range e.AffectedEdgeIDs {
		if id != uuid.Nil {
			affectedEdges[id] = true
		}
	}
	for _, id := range e.CreatedNodeIDs {
		if id != uuid.Nil {
			affectedNodes[id] = true
		}
	}
	for _, id := range e.UpdatedNodeIDs {
		if id != uuid.Nil {
			affectedNodes[id] = true
		}
	}
	for _, id := range e.DeletedNodeIDs {
		if id != uuid.Nil {
			affectedNodes[id] = true
		}
	}
	for _, edge := range e.ChangedEdges {
		if edge.EdgeID != uuid.Nil {
			affectedEdges[edge.EdgeID] = true
		}
		if edge.FromID != uuid.Nil {
			affectedNodes[edge.FromID] = true
		}
		if edge.ToID != uuid.Nil {
			affectedNodes[edge.ToID] = true
		}
	}
	for i := range e.Changes {
		change := &e.Changes[i]
		for _, id := range change.AffectedNodeIDs {
			if parsed, err := uuid.Parse(id); err == nil && parsed != uuid.Nil {
				affectedNodes[graph.NodeID(parsed)] = true
			}
		}
		for _, id := range change.AffectedEdgeIDs {
			if parsed, err := uuid.Parse(id); err == nil && parsed != uuid.Nil {
				affectedEdges[graph.EdgeID(parsed)] = true
			}
		}
		if change.NodeID != "" {
			if parsed, err := uuid.Parse(change.NodeID); err == nil && parsed != uuid.Nil {
				affectedNodes[graph.NodeID(parsed)] = true
			}
		}
		if change.EdgeID != "" {
			if parsed, err := uuid.Parse(change.EdgeID); err == nil && parsed != uuid.Nil {
				affectedEdges[graph.EdgeID(parsed)] = true
			}
		}
		if change.Node != nil && change.Node.ID != uuid.Nil {
			affectedNodes[change.Node.ID] = true
			if change.NodeID == "" {
				change.NodeID = change.Node.ID.String()
			}
		}
		if change.OldNode != nil && change.OldNode.ID != uuid.Nil {
			affectedNodes[change.OldNode.ID] = true
			if change.NodeID == "" {
				change.NodeID = change.OldNode.ID.String()
			}
		}
		if change.Edge != nil {
			addEdgeAffected(change, *change.Edge, affectedNodes, affectedEdges)
			if change.EdgeID == "" {
				change.EdgeID = change.Edge.ID.String()
			}
		}
		if change.OldEdge != nil {
			addEdgeAffected(change, *change.OldEdge, affectedNodes, affectedEdges)
			if change.EdgeID == "" {
				change.EdgeID = change.OldEdge.ID.String()
			}
		}
	}
	e.AffectedNodeIDs = sortedGraphNodeIDs(affectedNodes)
	e.AffectedEdgeIDs = sortedGraphEdgeIDs(affectedEdges)
}

// ApplyProjection returns a copy of the event containing only fields selected by
// projection. Scope/filter matching belongs to the notification subsystem; this
// helper is intentionally limited to payload shaping.
func (e CommittedEvent) ApplyProjection(projection Projection) CommittedEvent {
	e.Normalize()
	out := e
	if !projection.IncludeRevision {
		out.GraphRevision = 0
		out.Revision = 0
	}
	if !projection.IncludeOrigin {
		out.Origin = OriginMetadata{}
	}
	if !projection.IncludeAffectedNodeIDs {
		out.AffectedNodeIDs = nil
	}
	if !projection.IncludeAffectedEdgeIDs {
		out.AffectedEdgeIDs = nil
	}
	out.Changes = make([]Change, 0, len(e.Changes))
	for _, change := range e.Changes {
		copy := change
		if !projection.IncludeChangedFields {
			copy.ChangedFields = nil
		}
		if !projection.IncludeAffectedNodeIDs {
			copy.AffectedNodeIDs = nil
		}
		if !projection.IncludeAffectedEdgeIDs {
			copy.AffectedEdgeIDs = nil
		}
		if !projection.IncludeNewNodeSnapshot {
			copy.Node = nil
		}
		if !projection.IncludeOldNodeSnapshot {
			copy.OldNode = nil
		}
		if !projection.IncludeNewEdgeSnapshot {
			copy.Edge = nil
		}
		if !projection.IncludeOldEdgeSnapshot {
			copy.OldEdge = nil
		}
		out.Changes = append(out.Changes, copy)
	}
	return out
}

// Sink receives committed graph events. Implementations must keep this path
// lightweight and durable; expensive semantic analysis belongs in background
// maintenance workers.
type Sink interface {
	OnGraphCommitted(ctx context.Context, event CommittedEvent) error
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(context.Context, CommittedEvent) error

func (f SinkFunc) OnGraphCommitted(ctx context.Context, event CommittedEvent) error {
	if f == nil {
		return nil
	}
	return f(ctx, event)
}

// MultiSink fans out graph events to multiple sinks in order.
type MultiSink []Sink

func (m MultiSink) OnGraphCommitted(ctx context.Context, event CommittedEvent) error {
	var out error
	for _, sink := range m {
		if sink == nil {
			continue
		}
		if err := sink.OnGraphCommitted(ctx, event); err != nil {
			out = errors.Join(out, err)
		}
	}
	return out
}

func addEdgeAffected(change *Change, edge graph.Edge, nodes map[graph.NodeID]bool, edges map[graph.EdgeID]bool) {
	if edge.ID != uuid.Nil {
		edges[edge.ID] = true
		change.AffectedEdgeIDs = appendUniqueString(change.AffectedEdgeIDs, edge.ID.String())
	}
	if edge.FromID != uuid.Nil {
		nodes[edge.FromID] = true
		change.AffectedNodeIDs = appendUniqueString(change.AffectedNodeIDs, edge.FromID.String())
	}
	if edge.ToID != uuid.Nil {
		nodes[edge.ToID] = true
		change.AffectedNodeIDs = appendUniqueString(change.AffectedNodeIDs, edge.ToID.String())
	}
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func domainIDsContain(values []graph.DomainID, value graph.DomainID) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func sortedGraphNodeIDs(values map[graph.NodeID]bool) []graph.NodeID {
	out := make([]graph.NodeID, 0, len(values))
	for id := range values {
		out = append(out, id)
	}
	sortUUIDs(out)
	return out
}

func sortedGraphEdgeIDs(values map[graph.EdgeID]bool) []graph.EdgeID {
	out := make([]graph.EdgeID, 0, len(values))
	for id := range values {
		out = append(out, id)
	}
	sortUUIDs(out)
	return out
}

func sortUUIDs[T ~[16]byte](values []T) {
	for i := 1; i < len(values); i++ {
		value := values[i]
		j := i - 1
		for ; j >= 0 && uuid.UUID(values[j]).String() > uuid.UUID(value).String(); j-- {
			values[j+1] = values[j]
		}
		values[j+1] = value
	}
}
