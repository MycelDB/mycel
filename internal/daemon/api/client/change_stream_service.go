package client

import (
	"context"
	"errors"
	"strings"
	"time"

	daemonchange "github.com/myceldb/mycel/internal/daemon/modules/changestream"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const changeStreamHeartbeatInterval = 30 * time.Second

type ChangeStreamService struct {
	clientv1.UnimplementedChangeStreamServiceServer
	changes daemonchange.Manager
	spaces  daemonspace.Manager
}

func NewChangeStreamService(changes daemonchange.Manager, spaces daemonspace.Manager) *ChangeStreamService {
	return &ChangeStreamService{changes: changes, spaces: spaces}
}

func (s *ChangeStreamService) WatchDomainChanges(req *clientv1.WatchDomainChangesRequest, stream clientv1.ChangeStreamService_WatchDomainChangesServer) error {
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
	if _, err := s.spaces.GetVisibleDomain(ctx, principal.UserID, spaceID, domainID, ""); err != nil {
		return mapDomainError(err, "watch domain changes")
	}
	var after *int64
	if req.AfterRevision != nil {
		value := req.GetAfterRevision()
		if value < 0 {
			return status.Error(codes.InvalidArgument, "after_revision must be non-negative")
		}
		after = &value
	}
	sub, err := s.changes.Subscribe(ctx, daemonchange.SubscribeInput{SpaceID: spaceID, DomainID: domainID, AfterRevision: after})
	if err != nil {
		return mapChangeStreamError(err, "subscribe domain changes")
	}
	defer sub.Cancel()
	if req.GetIncludeCurrent() {
		if err := stream.Send(&clientv1.WatchDomainChangesResponse{Message: &clientv1.WatchDomainChangesResponse_Checkpoint{Checkpoint: &clientv1.ChangeCheckpoint{SpaceId: spaceID, DomainId: domainID, CurrentRevision: s.changes.CurrentRevision(spaceID, domainID), CheckpointTime: timestamppb.Now()}}}); err != nil {
			return err
		}
	}
	filter := changeEventFilter(req.GetEventTypes())
	heartbeats := time.NewTicker(changeStreamHeartbeatInterval)
	defer heartbeats.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-sub.Events:
			if !ok {
				return nil
			}
			protoEvent := mapChangeEvent(event, filter)
			if protoEvent == nil || len(protoEvent.GetChanges()) == 0 {
				continue
			}
			if err := stream.Send(&clientv1.WatchDomainChangesResponse{Message: &clientv1.WatchDomainChangesResponse_Event{Event: protoEvent}}); err != nil {
				return err
			}
		case <-heartbeats.C:
			if err := stream.Send(&clientv1.WatchDomainChangesResponse{Message: &clientv1.WatchDomainChangesResponse_Heartbeat{Heartbeat: &clientv1.ChangeStreamHeartbeat{HeartbeatTime: timestamppb.Now()}}}); err != nil {
				return err
			}
		}
	}
}

func mapChangeEvent(event daemonchange.Event, filter map[clientv1.ChangeEventType]bool) *clientv1.ChangeEvent {
	out := &clientv1.ChangeEvent{EventId: event.EventID, SpaceId: event.SpaceID, DomainId: event.DomainID, Revision: event.Revision, CommitId: event.CommitID, EventTime: timestamppb.New(event.EventTime)}
	for _, change := range event.Changes {
		mappedType := mapChangeType(change.Type)
		if mappedType == clientv1.ChangeEventType_CHANGE_EVENT_TYPE_UNSPECIFIED {
			continue
		}
		if len(filter) > 0 && !filter[mappedType] {
			continue
		}
		mapped := &clientv1.GraphChange{Type: mappedType}
		switch mappedType {
		case clientv1.ChangeEventType_CHANGE_EVENT_TYPE_NODE_CREATED, clientv1.ChangeEventType_CHANGE_EVENT_TYPE_NODE_UPDATED:
			if change.Node != nil {
				mapped.Subject = &clientv1.GraphChange_Node{Node: mapProtoNode(*change.Node)}
			} else if change.NodeID != "" {
				mapped.Subject = &clientv1.GraphChange_NodeId{NodeId: change.NodeID}
			}
		case clientv1.ChangeEventType_CHANGE_EVENT_TYPE_NODE_DELETED:
			mapped.Subject = &clientv1.GraphChange_NodeId{NodeId: change.NodeID}
		case clientv1.ChangeEventType_CHANGE_EVENT_TYPE_EDGE_CREATED, clientv1.ChangeEventType_CHANGE_EVENT_TYPE_EDGE_UPDATED:
			if change.Edge != nil {
				mapped.Subject = &clientv1.GraphChange_Edge{Edge: mapProtoEdge(*change.Edge)}
			} else if change.EdgeID != "" {
				mapped.Subject = &clientv1.GraphChange_EdgeId{EdgeId: change.EdgeID}
			}
		case clientv1.ChangeEventType_CHANGE_EVENT_TYPE_EDGE_DELETED:
			mapped.Subject = &clientv1.GraphChange_EdgeId{EdgeId: change.EdgeID}
		}
		out.Changes = append(out.Changes, mapped)
	}
	return out
}

func mapChangeType(value daemonchange.ChangeType) clientv1.ChangeEventType {
	switch value {
	case daemonchange.ChangeTypeNodeCreated:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_NODE_CREATED
	case daemonchange.ChangeTypeNodeUpdated:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_NODE_UPDATED
	case daemonchange.ChangeTypeNodeDeleted:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_NODE_DELETED
	case daemonchange.ChangeTypeEdgeCreated:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_EDGE_CREATED
	case daemonchange.ChangeTypeEdgeUpdated:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_EDGE_UPDATED
	case daemonchange.ChangeTypeEdgeDeleted:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_EDGE_DELETED
	case daemonchange.ChangeTypeRevisionAdvanced:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_REVISION_ADVANCED
	default:
		return clientv1.ChangeEventType_CHANGE_EVENT_TYPE_UNSPECIFIED
	}
}

func changeEventFilter(values []clientv1.ChangeEventType) map[clientv1.ChangeEventType]bool {
	out := map[clientv1.ChangeEventType]bool{}
	for _, value := range values {
		if value != clientv1.ChangeEventType_CHANGE_EVENT_TYPE_UNSPECIFIED {
			out[value] = true
		}
	}
	return out
}

func mapChangeStreamError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daemonchange.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daemonchange.ErrOutOfRange) {
		return status.Error(codes.OutOfRange, err.Error())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
