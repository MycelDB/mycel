package graphchange

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// EdgeChange describes a graph edge mutation that may affect semantic source text.
type EdgeChange struct {
	EdgeID graph.EdgeID
	Kind   graph.EdgeKind
	Change string
	FromID graph.NodeID
	ToID   graph.NodeID
}

// CommittedEvent describes one committed graph transaction.
// It is intentionally semantic-neutral so graph/session code does not depend on
// semantic maintenance packages.
type CommittedEvent struct {
	ID                uuid.UUID
	TxnID             uuid.UUID
	GraphRevision     uint64
	SpaceID           domainspace.SpaceID
	DomainIDs         []graph.DomainID
	CreatedNodeIDs    []graph.NodeID
	UpdatedNodeIDs    []graph.NodeID
	DeletedNodeIDs    []graph.NodeID
	ChangedEdges      []EdgeChange
	OldParentByNodeID map[graph.NodeID]graph.NodeID
	NewParentByNodeID map[graph.NodeID]graph.NodeID
	OldDomainByNodeID map[graph.NodeID]graph.DomainID
	NewDomainByNodeID map[graph.NodeID]graph.DomainID
	CommittedAt       time.Time
}

// Empty reports whether this event has no graph changes.
func (e CommittedEvent) Empty() bool {
	return len(e.CreatedNodeIDs) == 0 && len(e.UpdatedNodeIDs) == 0 && len(e.DeletedNodeIDs) == 0 && len(e.ChangedEdges) == 0
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
