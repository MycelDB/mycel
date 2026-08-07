package client

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const graphChangeHeartbeatInterval = 30 * time.Second

type GraphChangeService struct {
	clientv1.UnimplementedGraphChangeServiceServer
	notifications graphnotification.Manager
	spaces        daemonspace.Manager
}

func NewGraphChangeService(notifications graphnotification.Manager, spaces daemonspace.Manager) *GraphChangeService {
	return &GraphChangeService{notifications: notifications, spaces: spaces}
}

func (s *GraphChangeService) WatchGraphChanges(req *clientv1.WatchGraphChangesRequest, stream clientv1.GraphChangeService_WatchGraphChangesServer) error {
	ctx := stream.Context()
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	spaceID := strings.TrimSpace(req.GetSpaceId())
	domainID := strings.TrimSpace(req.GetDomainId())
	if spaceID == "" || domainID == "" {
		return status.Error(codes.InvalidArgument, "space_id and domain_id are required")
	}
	if s.notifications == nil {
		return status.Error(codes.Unavailable, "graph change notifications are unavailable")
	}
	if _, err := s.spaces.GetVisibleDomain(ctx, principal.UserID, spaceID, domainID, ""); err != nil {
		return mapDomainError(err, "watch graph changes")
	}
	var after *uint64
	if req.AfterRevision != nil {
		value := req.GetAfterRevision()
		if value < 0 {
			return status.Error(codes.InvalidArgument, "after_revision must be non-negative")
		}
		converted := uint64(value)
		after = &converted
	}
	consumer := newGraphChangeStreamConsumer(ctx)
	registration, err := s.notifications.RegisterConsumer(ctx, graphnotification.ConsumerSpec{
		ConsumerName: "public-graph-change-stream",
		Scope: graphchange.Scope{
			SpaceID:  spaceID,
			DomainID: domainID,
			NodeIDs:  append([]string(nil), req.GetFilter().GetNodeIds()...),
			EdgeIDs:  append([]string(nil), req.GetFilter().GetEdgeIds()...),
		},
		Filter: graphchange.Filter{
			EventTypes: graphChangeTypesFromProto(req.GetFilter().GetEventTypes()),
			Labels:     append([]string(nil), req.GetFilter().GetLabels()...),
			Fields:     append([]string(nil), req.GetFilter().GetChangedFields()...),
		},
		Projection: graphChangeProjectionFromProto(req.GetProjection()),
		Start:      graphnotification.StartPosition{AfterRevision: after},
	}, consumer)
	if err != nil {
		return mapGraphChangeError(err, "register graph change stream")
	}
	defer registration.Close()
	if req.GetIncludeCurrent() {
		current, err := s.notifications.CurrentRevision(ctx, spaceID, domainID)
		if err != nil {
			return mapGraphChangeError(err, "current graph change revision")
		}
		if err := stream.Send(&clientv1.WatchGraphChangesResponse{Message: &clientv1.WatchGraphChangesResponse_Checkpoint{Checkpoint: &clientv1.GraphChangeCheckpoint{SpaceId: spaceID, DomainId: domainID, CurrentRevision: uint64ToInt64(current), CheckpointTime: timestamppb.Now()}}}); err != nil {
			return err
		}
	}
	filter := graphChangeFilterFromProto(req.GetFilter())
	heartbeats := time.NewTicker(graphChangeHeartbeatInterval)
	defer heartbeats.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case gap := <-consumer.gaps:
			if err := stream.Send(&clientv1.WatchGraphChangesResponse{Message: &clientv1.WatchGraphChangesResponse_Gap{Gap: mapGraphChangeGap(gap)}}); err != nil {
				return err
			}
			return nil
		case event := <-consumer.events:
			protoEvent := mapGraphChangeEvent(event, filter)
			if protoEvent == nil || len(protoEvent.GetChanges()) == 0 {
				continue
			}
			if err := stream.Send(&clientv1.WatchGraphChangesResponse{Message: &clientv1.WatchGraphChangesResponse_Event{Event: protoEvent}}); err != nil {
				return err
			}
		case <-heartbeats.C:
			if err := stream.Send(&clientv1.WatchGraphChangesResponse{Message: &clientv1.WatchGraphChangesResponse_Heartbeat{Heartbeat: &clientv1.GraphChangeHeartbeat{HeartbeatTime: timestamppb.Now()}}}); err != nil {
				return err
			}
		}
	}
}

type graphChangeStreamConsumer struct {
	ctx    context.Context
	events chan graphchange.CommittedEvent
	gaps   chan graphchange.Gap
}

func newGraphChangeStreamConsumer(ctx context.Context) *graphChangeStreamConsumer {
	return &graphChangeStreamConsumer{ctx: ctx, events: make(chan graphchange.CommittedEvent, 64), gaps: make(chan graphchange.Gap, 1)}
}

func (c *graphChangeStreamConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case c.events <- event:
		return nil
	}
}

func (c *graphChangeStreamConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case c.gaps <- gap:
		return nil
	}
}

type graphChangeFilter struct {
	eventTypes    map[clientv1.GraphChangeType]bool
	nodeIDs       map[string]bool
	edgeIDs       map[string]bool
	changedFields map[string]bool
}

func graphChangeFilterFromProto(filter *clientv1.GraphChangeFilter) graphChangeFilter {
	if filter == nil {
		return graphChangeFilter{}
	}
	out := graphChangeFilter{eventTypes: map[clientv1.GraphChangeType]bool{}, nodeIDs: stringBoolMap(filter.GetNodeIds()), edgeIDs: stringBoolMap(filter.GetEdgeIds()), changedFields: stringBoolMap(filter.GetChangedFields())}
	for _, value := range filter.GetEventTypes() {
		if value != clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_UNSPECIFIED {
			out.eventTypes[value] = true
		}
	}
	return out
}

func graphChangeTypesFromProto(values []clientv1.GraphChangeType) []graphchange.ChangeType {
	out := make([]graphchange.ChangeType, 0, len(values))
	for _, value := range values {
		switch value {
		case clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_CREATED:
			out = append(out, graphchange.ChangeTypeNodeCreated)
		case clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_UPDATED:
			out = append(out, graphchange.ChangeTypeNodeUpdated)
		case clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_DELETED:
			out = append(out, graphchange.ChangeTypeNodeDeleted)
		case clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_EDGE_CREATED:
			out = append(out, graphchange.ChangeTypeEdgeCreated)
		case clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_EDGE_UPDATED:
			out = append(out, graphchange.ChangeTypeEdgeUpdated)
		case clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_EDGE_DELETED:
			out = append(out, graphchange.ChangeTypeEdgeDeleted)
		case clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_REVISION_ADVANCED:
			out = append(out, graphchange.ChangeTypeRevisionAdvanced)
		}
	}
	return out
}

func graphChangeProjectionFromProto(projection *clientv1.GraphChangeProjection) graphchange.Projection {
	if projection == nil {
		return graphchange.Projection{IncludeRevision: true, IncludeOrigin: true, IncludeAffectedNodeIDs: true, IncludeAffectedEdgeIDs: true, IncludeChangedFields: true}
	}
	return graphchange.Projection{
		IncludeRevision:        true,
		IncludeOrigin:          projection.GetIncludeOrigin(),
		IncludeAffectedNodeIDs: projection.GetIncludeAffectedNodeIds(),
		IncludeAffectedEdgeIDs: projection.GetIncludeAffectedEdgeIds(),
		IncludeChangedFields:   projection.GetIncludeChangedFields(),
		IncludeOldNodeSnapshot: projection.GetIncludeOldNodeSnapshot(),
		IncludeNewNodeSnapshot: projection.GetIncludeNewNodeSnapshot(),
		IncludeOldEdgeSnapshot: projection.GetIncludeOldEdgeSnapshot(),
		IncludeNewEdgeSnapshot: projection.GetIncludeNewEdgeSnapshot(),
	}
}

func mapGraphChangeEvent(event graphchange.CommittedEvent, filter graphChangeFilter) *clientv1.GraphChangeEvent {
	event.Normalize()
	out := &clientv1.GraphChangeEvent{EventId: event.ID.String(), SpaceId: event.SpaceID.String(), DomainId: event.DomainID.String(), Revision: uint64ToInt64(event.Revision), CommitId: uuidString(event.CommitID), TransactionId: uuidString(event.TransactionID), CommitTime: timestamppb.New(event.CommittedAt), Origin: mapGraphChangeOrigin(event.Origin), AffectedNodeIds: graphNodeIDsToStrings(event.AffectedNodeIDs), AffectedEdgeIds: graphEdgeIDsToStrings(event.AffectedEdgeIDs)}
	for _, change := range event.Changes {
		mappedType := mapGraphChangeType(change.Type)
		if mappedType == clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_UNSPECIFIED || !filter.matchesType(mappedType) || !filter.matchesChange(change) {
			continue
		}
		out.Changes = append(out.Changes, mapGraphObjectChange(change, mappedType))
	}
	return out
}

func mapGraphObjectChange(change graphchange.Change, mappedType clientv1.GraphChangeType) *clientv1.GraphObjectChange {
	out := &clientv1.GraphObjectChange{Type: mappedType, NodeId: change.NodeID, EdgeId: change.EdgeID, AffectedNodeIds: append([]string(nil), change.AffectedNodeIDs...), AffectedEdgeIds: append([]string(nil), change.AffectedEdgeIDs...), ChangedFields: append([]string(nil), change.ChangedFields...)}
	if change.OldNode != nil {
		out.OldNode = mapProtoNode(*change.OldNode)
	}
	if change.Node != nil {
		out.NewNode = mapProtoNode(*change.Node)
	}
	if change.OldEdge != nil {
		out.OldEdge = mapProtoEdge(*change.OldEdge)
	}
	if change.Edge != nil {
		out.NewEdge = mapProtoEdge(*change.Edge)
	}
	return out
}

func mapGraphChangeOrigin(origin graphchange.OriginMetadata) *clientv1.GraphChangeOrigin {
	if strings.TrimSpace(origin.OperationID) == "" {
		return nil
	}
	return &clientv1.GraphChangeOrigin{OperationId: origin.OperationID}
}

func mapGraphChangeGap(gap graphchange.Gap) *clientv1.GraphChangeGap {
	return &clientv1.GraphChangeGap{SpaceId: gap.SpaceID, DomainId: gap.DomainID, RequestedAfterRevision: uint64ToInt64(gap.RequestedAfterRevision), OldestAvailableRevision: uint64ToInt64(gap.OldestAvailableRevision), CurrentRevision: uint64ToInt64(gap.CurrentRevision)}
}

func mapGraphChangeType(value graphchange.ChangeType) clientv1.GraphChangeType {
	switch value {
	case graphchange.ChangeTypeNodeCreated:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_CREATED
	case graphchange.ChangeTypeNodeUpdated:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_UPDATED
	case graphchange.ChangeTypeNodeDeleted:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_DELETED
	case graphchange.ChangeTypeEdgeCreated:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_EDGE_CREATED
	case graphchange.ChangeTypeEdgeUpdated:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_EDGE_UPDATED
	case graphchange.ChangeTypeEdgeDeleted:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_EDGE_DELETED
	case graphchange.ChangeTypeRevisionAdvanced:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_REVISION_ADVANCED
	default:
		return clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_UNSPECIFIED
	}
}

func (f graphChangeFilter) matchesType(value clientv1.GraphChangeType) bool {
	return len(f.eventTypes) == 0 || f.eventTypes[value]
}

func (f graphChangeFilter) matchesChange(change graphchange.Change) bool {
	if len(f.nodeIDs) > 0 && !matchesAnyValue(f.nodeIDs, change.NodeID, change.AffectedNodeIDs) {
		return false
	}
	if len(f.edgeIDs) > 0 && !matchesAnyValue(f.edgeIDs, change.EdgeID, change.AffectedEdgeIDs) {
		return false
	}
	if len(f.changedFields) > 0 && !matchesAnyValue(f.changedFields, "", change.ChangedFields) {
		return false
	}
	return true
}

func matchesAnyValue(want map[string]bool, primary string, values []string) bool {
	if strings.TrimSpace(primary) != "" && want[primary] {
		return true
	}
	for _, value := range values {
		if want[value] {
			return true
		}
	}
	return false
}

func stringBoolMap(values []string) map[string]bool {
	out := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = true
		}
	}
	return out
}

func uuidString(value uuid.UUID) string {
	if value == uuid.Nil {
		return ""
	}
	return value.String()
}

func graphNodeIDsToStrings(values []graph.NodeID) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != uuid.Nil {
			out = append(out, value.String())
		}
	}
	return out
}

func graphEdgeIDsToStrings(values []graph.EdgeID) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != uuid.Nil {
			out = append(out, value.String())
		}
	}
	return out
}

func uint64ToInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

func mapGraphChangeError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, graphnotification.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, graphnotification.ErrOutOfRange) {
		return status.Error(codes.OutOfRange, err.Error())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
