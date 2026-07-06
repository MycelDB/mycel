package maintenance

import (
	"context"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	"github.com/myceldb/mycel/internal/graphchange"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

// DirtyEventAppender adapts graph commit notifications into durable semantic
// maintenance dirty events.
type DirtyEventAppender struct {
	MaintenanceManager storesemantic.MaintenanceManager
}

func (a DirtyEventAppender) OnGraphCommitted(ctx context.Context, event graphchange.CommittedEvent) error {
	if event.Empty() {
		return nil
	}
	if a.MaintenanceManager == nil {
		return ErrMaintenanceManagerRequired
	}
	_, err := a.MaintenanceManager.AppendGraphDirtyEvent(ctx, DirtyEventFromGraphCommit(event))
	return err
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
	if out.CommittedAt.IsZero() {
		out.CommittedAt = time.Now().UTC()
	}
	for _, edge := range event.ChangedEdges {
		out.ChangedEdges = append(out.ChangedEdges, domainsemantic.GraphDirtyEdgeChange{EdgeID: edge.EdgeID, Kind: edge.Kind, Change: edge.Change, FromID: edge.FromID, ToID: edge.ToID})
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
