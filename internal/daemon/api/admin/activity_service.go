package admin

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/activity/model"
	activityservice "github.com/myceldb/mycel/internal/activity/service"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	capAuditRead  = "audit.read"
	capAuditWrite = "audit.write"
)

type AdminActivityService struct {
	adminv1.UnimplementedAdminActivityServiceServer
	manager    activityservice.Manager
	authorizer OperatorAuthorizer
}

func NewAdminActivityService(manager activityservice.Manager, authorizer OperatorAuthorizer) *AdminActivityService {
	return &AdminActivityService{manager: manager, authorizer: authorizer}
}

func (s *AdminActivityService) AppendActivityEvent(ctx context.Context, req *adminv1.AppendActivityEventRequest) (*adminv1.AppendActivityEventResponse, error) {
	if err := s.require(ctx, capAuditWrite); err != nil {
		return nil, err
	}
	if req == nil || req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}
	if req.GetEvent().GetSource() == nil {
		return nil, status.Error(codes.InvalidArgument, "event source is required")
	}
	result, err := s.manager.Append(ctx, eventFromProto(req.GetEvent()))
	if err != nil {
		return nil, mapActivityError(err)
	}
	return &adminv1.AppendActivityEventResponse{Event: eventToProto(result.Event), Duplicate: result.Duplicate}, nil
}

func (s *AdminActivityService) ListActivityEvents(ctx context.Context, req *adminv1.ListActivityEventsRequest) (*adminv1.ListActivityEventsResponse, error) {
	if err := s.require(ctx, capAuditRead); err != nil {
		return nil, err
	}
	result, err := s.manager.List(ctx, listFilterFromProto(req))
	if err != nil {
		return nil, mapActivityError(err)
	}
	events := make([]*adminv1.ActivityEvent, 0, len(result.Events))
	for _, event := range result.Events {
		events = append(events, eventToProto(event))
	}
	return &adminv1.ListActivityEventsResponse{
		Events:        events,
		NextPageToken: result.NextPageToken,
		Summary: &adminv1.ActivityEventSummary{
			TotalCount:   result.Summary.TotalCount,
			WarningCount: result.Summary.WarningCount,
			ErrorCount:   result.Summary.ErrorCount,
		},
	}, nil
}

func (s *AdminActivityService) GetActivityEvent(ctx context.Context, req *adminv1.GetActivityEventRequest) (*adminv1.GetActivityEventResponse, error) {
	if err := s.require(ctx, capAuditRead); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "event_id is required")
	}
	eventID := strings.TrimSpace(req.GetEventId())
	if eventID == "" {
		return nil, status.Error(codes.InvalidArgument, "event_id is required")
	}
	event, err := s.manager.Get(ctx, eventID)
	if err != nil {
		return nil, mapActivityError(err)
	}
	return &adminv1.GetActivityEventResponse{Event: eventToProto(event)}, nil
}

func (s *AdminActivityService) require(ctx context.Context, capability string) error {
	if s.manager == nil {
		return status.Error(codes.FailedPrecondition, "activity manager is not configured")
	}
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	if s.authorizer == nil {
		return status.Error(codes.PermissionDenied, "activity access requires authorization")
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, capability)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if !ok {
		return status.Errorf(codes.PermissionDenied, "principal lacks required capability %s", capability)
	}
	return nil
}

func mapActivityError(err error) error {
	if errors.Is(err, model.ErrNotFound) {
		return status.Error(codes.NotFound, "activity event not found")
	}
	if errors.Is(err, model.ErrInvalidEvent) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func eventFromProto(in *adminv1.ActivityEvent) model.Event {
	if in == nil {
		return model.Event{}
	}
	var occurredAt, ingestedAt time.Time
	if in.GetOccurredAt() != nil {
		occurredAt = in.GetOccurredAt().AsTime()
	}
	if in.GetIngestedAt() != nil {
		ingestedAt = in.GetIngestedAt().AsTime()
	}
	return model.Event{
		EventID:        in.GetEventId(),
		OccurredAt:     occurredAt,
		IngestedAt:     ingestedAt,
		Severity:       in.GetSeverity(),
		Category:       in.GetCategory(),
		Type:           in.GetType(),
		Message:        in.GetMessage(),
		Source:         sourceFromProto(in.GetSource()),
		Actor:          actorFromProto(in.GetActor()),
		Resource:       resourceFromProto(in.GetResource()),
		CorrelationID:  in.GetCorrelationId(),
		IdempotencyKey: in.GetIdempotencyKey(),
		Metadata:       in.GetMetadata(),
	}
}

func eventToProto(event model.Event) *adminv1.ActivityEvent {
	return &adminv1.ActivityEvent{EventId: event.EventID, OccurredAt: timestamppb.New(event.OccurredAt), IngestedAt: timestamppb.New(event.IngestedAt), Severity: event.Severity, Category: event.Category, Type: event.Type, Message: event.Message, Source: sourceToProto(event.Source), Actor: actorToProto(event.Actor), Resource: resourceToProto(event.Resource), CorrelationId: event.CorrelationID, IdempotencyKey: event.IdempotencyKey, Metadata: event.Metadata}
}

func sourceFromProto(in *adminv1.ActivityEventSource) model.Source {
	if in == nil {
		return model.Source{}
	}
	return model.Source{NodeID: in.GetNodeId(), NodeName: in.GetNodeName(), PodName: in.GetPodName(), Component: in.GetComponent(), Service: in.GetService()}
}
func sourceToProto(in model.Source) *adminv1.ActivityEventSource {
	return &adminv1.ActivityEventSource{NodeId: in.NodeID, NodeName: in.NodeName, PodName: in.PodName, Component: in.Component, Service: in.Service}
}
func actorFromProto(in *adminv1.ActivityEventActor) model.Actor {
	if in == nil {
		return model.Actor{}
	}
	return model.Actor{PrincipalID: in.GetPrincipalId(), Username: in.GetUsername()}
}
func actorToProto(in model.Actor) *adminv1.ActivityEventActor {
	if in.PrincipalID == "" && in.Username == "" {
		return nil
	}
	return &adminv1.ActivityEventActor{PrincipalId: in.PrincipalID, Username: in.Username}
}
func resourceFromProto(in *adminv1.ActivityEventResource) model.Resource {
	if in == nil {
		return model.Resource{}
	}
	return model.Resource{Kind: in.GetKind(), ID: in.GetId(), Name: in.GetName()}
}
func resourceToProto(in model.Resource) *adminv1.ActivityEventResource {
	if in.Kind == "" && in.ID == "" && in.Name == "" {
		return nil
	}
	return &adminv1.ActivityEventResource{Kind: in.Kind, Id: in.ID, Name: in.Name}
}

func listFilterFromProto(req *adminv1.ListActivityEventsRequest) model.ListFilter {
	if req == nil {
		return model.ListFilter{}
	}
	var since, until time.Time
	if req.GetSince() != nil {
		since = req.GetSince().AsTime()
	}
	if req.GetUntil() != nil {
		until = req.GetUntil().AsTime()
	}
	return model.ListFilter{Since: since, Until: until, Severities: req.GetSeverities(), Categories: req.GetCategories(), Types: req.GetTypes(), SourceNodeID: strings.TrimSpace(req.GetSourceNodeId()), SourcePodName: strings.TrimSpace(req.GetSourcePodName()), SourceComponent: strings.TrimSpace(req.GetSourceComponent()), SourceService: strings.TrimSpace(req.GetSourceService()), ActorPrincipalID: strings.TrimSpace(req.GetActorPrincipalId()), ResourceKind: strings.TrimSpace(req.GetResourceKind()), ResourceID: strings.TrimSpace(req.GetResourceId()), CorrelationID: strings.TrimSpace(req.GetCorrelationId()), PageSize: int(req.GetPageSize()), PageToken: req.GetPageToken()}
}
