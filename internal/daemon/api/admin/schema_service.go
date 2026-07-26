package admin

import (
	"context"
	"errors"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/schema/dsl"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminSchemaService struct {
	adminv1.UnimplementedAdminSchemaServiceServer
	schemas schemaservice.Manager
}

func NewAdminSchemaService(schemas schemaservice.Manager) *AdminSchemaService {
	return &AdminSchemaService{schemas: schemas}
}

func (s *AdminSchemaService) GetDomainSchema(ctx context.Context, req *adminv1.GetDomainSchemaRequest) (*adminv1.GetDomainSchemaResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := uuid.Parse(req.GetDomainId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "domain_id must be a UUID")
	}
	value, err := s.schemas.GetDomainSchema(ctx, graph.DomainID(domainID))
	if err != nil {
		return nil, mapAdminSchemaError(err)
	}
	return &adminv1.GetDomainSchemaResponse{Gwl: value.SourceGWL}, nil
}

func (s *AdminSchemaService) DeleteDomainSchema(ctx context.Context, req *adminv1.DeleteDomainSchemaRequest) (*adminv1.DeleteDomainSchemaResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := uuid.Parse(req.GetDomainId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "domain_id must be a UUID")
	}
	if err := s.schemas.DeleteDomainSchema(ctx, graph.DomainID(domainID)); err != nil {
		return nil, mapAdminSchemaError(err)
	}
	return &adminv1.DeleteDomainSchemaResponse{}, nil
}

func (s *AdminSchemaService) ValidateSchema(ctx context.Context, req *adminv1.ValidateSchemaRequest) (*adminv1.ValidateSchemaResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	value, err := dsl.Parse(req.GetGwl())
	if err != nil {
		return &adminv1.ValidateSchemaResponse{Valid: false, Issues: []*clientv1.SchemaValidationIssue{{Severity: "error", Message: err.Error()}}}, nil
	}
	if value.DomainID == uuid.Nil {
		value.DomainID = graph.DomainID(uuid.Nil)
		value.DomainID[15] = 1
	}
	if err := schemamodel.Validate(value.Normalize()); err != nil {
		return &adminv1.ValidateSchemaResponse{Valid: false, Issues: []*clientv1.SchemaValidationIssue{{Severity: "error", Message: err.Error()}}}, nil
	}
	return &adminv1.ValidateSchemaResponse{Valid: true}, nil
}

func mapAdminSchemaError(err error) error {
	if errors.Is(err, schemaservice.ErrSchemaNotFound) {
		return status.Error(codes.NotFound, "schema not found")
	}
	return status.Error(codes.Internal, err.Error())
}
