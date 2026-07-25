package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
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
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &adminv1.GetDomainSchemaResponse{SchemaJson: string(data)}, nil
}

func (s *AdminSchemaService) ValidateSchema(ctx context.Context, req *adminv1.ValidateSchemaRequest) (*adminv1.ValidateSchemaResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	var value schemamodel.DomainSchema
	if err := json.Unmarshal([]byte(req.GetSchemaJson()), &value); err != nil {
		return &adminv1.ValidateSchemaResponse{Valid: false, Issues: []*clientv1.SchemaValidationIssue{{Severity: "error", Message: fmt.Sprintf("invalid schema JSON: %v", err)}}}, nil
	}
	if err := schemamodel.Validate(value.Normalize()); err != nil {
		return &adminv1.ValidateSchemaResponse{Valid: false, Issues: []*clientv1.SchemaValidationIssue{{Severity: "error", Message: err.Error()}}}, nil
	}
	return &adminv1.ValidateSchemaResponse{Valid: true}, nil
}

func mapAdminSchemaError(err error) error { return status.Error(codes.Internal, err.Error()) }
