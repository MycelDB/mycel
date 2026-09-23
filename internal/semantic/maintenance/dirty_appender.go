package maintenance

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/change"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

// DirtyEventAppender adapts graph commit notifications into durable semantic
// maintenance dirty events.
type DirtyEventAppender struct {
	MaintenanceManager storesemantic.MaintenanceManager
}

type DirtyEventAppenderTiming struct {
	Total   time.Duration
	Convert time.Duration
	Append  storesemantic.GraphDirtyEventAppendTiming
}

func (a DirtyEventAppender) OnGraphCommitted(ctx context.Context, event graphchange.CommittedEvent) error {
	_, err := a.OnGraphCommittedWithTiming(ctx, event)
	return err
}

func (a DirtyEventAppender) OnGraphCommittedWithTiming(ctx context.Context, event graphchange.CommittedEvent) (DirtyEventAppenderTiming, error) {
	timing := DirtyEventAppenderTiming{}
	traceStart := time.Now()
	if event.Empty() {
		timing.Total = time.Since(traceStart)
		return timing, nil
	}
	if a.MaintenanceManager == nil {
		timing.Total = time.Since(traceStart)
		return timing, ErrMaintenanceManagerRequired
	}
	stepStart := time.Now()
	dirtyEvent := DirtyEventFromGraphCommit(event)
	timing.Convert = time.Since(stepStart)
	if timed, ok := a.MaintenanceManager.(storesemantic.TimedGraphDirtyEventAppender); ok {
		_, appendTiming, err := timed.AppendGraphDirtyEventWithTiming(ctx, dirtyEvent)
		timing.Append = appendTiming
		timing.Total = time.Since(traceStart)
		return timing, err
	}
	stepStart = time.Now()
	_, err := a.MaintenanceManager.AppendGraphDirtyEvent(ctx, dirtyEvent)
	timing.Append.Total = time.Since(stepStart)
	timing.Total = time.Since(traceStart)
	return timing, err
}

// DirtyEventFromGraphCommit converts a semantic-neutral graphchange event into
// the persisted semantic maintenance event shape.
func DirtyEventFromGraphCommit(event graphchange.CommittedEvent) domainsemantic.GraphDirtyEvent {
	out := domainsemantic.GraphDirtyEvent{
		ID:                domainsemantic.GraphDirtyEventID(event.ID),
		TxnID:             event.TxnID,
		GraphRevision:     event.GraphRevision,
		SpaceID:           event.SpaceID,
		DomainIDs:         append([]uuid.UUID(nil), event.DomainIDs...),
		CreatedNodeIDs:    append([]uuid.UUID(nil), event.CreatedNodeIDs...),
		UpdatedNodeIDs:    append([]uuid.UUID(nil), event.UpdatedNodeIDs...),
		DeletedNodeIDs:    append([]uuid.UUID(nil), event.DeletedNodeIDs...),
		OldParentByNodeID: cloneNodeMap(event.OldParentByNodeID),
		NewParentByNodeID: cloneNodeMap(event.NewParentByNodeID),
		OldDomainByNodeID: cloneDomainMap(event.OldDomainByNodeID),
		NewDomainByNodeID: cloneDomainMap(event.NewDomainByNodeID),
		CommittedAt:       event.CommittedAt,
	}
	if out.ID == uuid.Nil {
		out.ID = uuid.New()
	}
	if out.TxnID == uuid.Nil {
		out.TxnID = out.ID
	}
	if out.CommittedAt.IsZero() {
		out.CommittedAt = time.Now().UTC()
	}
	for _, edge := range event.ChangedEdges {
		out.ChangedEdges = append(out.ChangedEdges, domainsemantic.GraphDirtyEdgeChange{EdgeID: edge.EdgeID, Labels: append([]string(nil), edge.Labels...), Change: edge.Change, FromID: edge.FromID, ToID: edge.ToID})
	}
	return out
}

func cloneNodeMap(in map[uuid.UUID]uuid.UUID) map[uuid.UUID]uuid.UUID {
	if len(in) == 0 {
		return map[uuid.UUID]uuid.UUID{}
	}
	out := make(map[uuid.UUID]uuid.UUID, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneDomainMap(in map[uuid.UUID]uuid.UUID) map[uuid.UUID]uuid.UUID {
	if len(in) == 0 {
		return map[uuid.UUID]uuid.UUID{}
	}
	out := make(map[uuid.UUID]uuid.UUID, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
