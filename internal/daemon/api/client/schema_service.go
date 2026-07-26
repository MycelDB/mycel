package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/schema/dsl"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SchemaService struct {
	clientv1.UnimplementedSchemaServiceServer
	schemas schemaservice.Manager
}

func NewSchemaService(schemas schemaservice.Manager) *SchemaService {
	return &SchemaService{schemas: schemas}
}

func (s *SchemaService) GetDomainSchema(ctx context.Context, req *clientv1.GetDomainSchemaRequest) (*clientv1.GetDomainSchemaResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	value, err := s.schemas.GetDomainSchema(ctx, domainID)
	if err != nil {
		return nil, mapSchemaError(err)
	}
	return &clientv1.GetDomainSchemaResponse{Gwl: value.SourceGWL}, nil
}

func (s *SchemaService) PutDomainSchema(ctx context.Context, req *clientv1.PutDomainSchemaRequest) (*clientv1.PutDomainSchemaResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.schemas.PutDomainSchemaGWL(ctx, domainID, req.GetGwl()); err != nil {
		return nil, mapSchemaError(err)
	}
	stored, err := s.schemas.GetDomainSchema(ctx, domainID)
	if err != nil {
		return nil, mapSchemaError(err)
	}
	return &clientv1.PutDomainSchemaResponse{Gwl: stored.SourceGWL}, nil
}

func (s *SchemaService) DeleteDomainSchema(ctx context.Context, req *clientv1.DeleteDomainSchemaRequest) (*clientv1.DeleteDomainSchemaResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.schemas.DeleteDomainSchema(ctx, domainID); err != nil {
		return nil, mapSchemaError(err)
	}
	return &clientv1.DeleteDomainSchemaResponse{}, nil
}

func (s *SchemaService) ValidateSchema(ctx context.Context, req *clientv1.ValidateSchemaRequest) (*clientv1.ValidateSchemaResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	value, err := dsl.Parse(req.GetGwl())
	if err != nil {
		return &clientv1.ValidateSchemaResponse{Valid: false, Issues: []*clientv1.SchemaValidationIssue{{Severity: "error", Message: err.Error()}}}, nil
	}
	if value.DomainID == uuid.Nil {
		value.DomainID = graph.DomainID(uuid.Nil)
		value.DomainID[15] = 1
	}
	if err := schemamodel.Validate(value); err != nil {
		return &clientv1.ValidateSchemaResponse{Valid: false, Issues: []*clientv1.SchemaValidationIssue{{Severity: "error", Message: err.Error()}}}, nil
	}
	return &clientv1.ValidateSchemaResponse{Valid: true}, nil
}

func (s *SchemaService) ValidateGraph(ctx context.Context, req *clientv1.ValidateGraphRequest) (*clientv1.ValidateGraphResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	var doc struct {
		Nodes []graph.Node `json:"nodes"`
		Edges []graph.Edge `json:"edges"`
	}
	if err := json.Unmarshal([]byte(req.GetGraphJson()), &doc); err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid graph JSON: %v", err))
	}
	issues := []*clientv1.SchemaValidationIssue{}
	nodesByID := map[graph.NodeID]graph.Node{}
	for _, node := range doc.Nodes {
		if node.DomainID == uuid.Nil {
			node.DomainID = domainID
		}
		nodesByID[node.ID] = node
		result, err := s.schemas.ValidateNode(ctx, domainID, node)
		if err != nil {
			return nil, mapSchemaError(err)
		}
		issues = append(issues, mapIssues(result.Issues)...)
	}
	for _, edge := range doc.Edges {
		if edge.DomainID == uuid.Nil {
			edge.DomainID = domainID
		}
		from, ok := nodesByID[edge.FromID]
		if !ok {
			issues = append(issues, &clientv1.SchemaValidationIssue{Severity: "error", Path: "edges.from", Message: "from node missing from graph document"})
			continue
		}
		to, ok := nodesByID[edge.ToID]
		if !ok {
			issues = append(issues, &clientv1.SchemaValidationIssue{Severity: "error", Path: "edges.to", Message: "to node missing from graph document"})
			continue
		}
		result, err := s.schemas.ValidateEdge(ctx, domainID, edge, from, to)
		if err != nil {
			return nil, mapSchemaError(err)
		}
		issues = append(issues, mapIssues(result.Issues)...)
	}
	return &clientv1.ValidateGraphResponse{Valid: len(issues) == 0, Issues: issues}, nil
}

func parseDomainID(value string) (graph.DomainID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "domain_id must be a UUID")
	}
	return graph.DomainID(id), nil
}
func mapIssues(in []schemaservice.ValidationIssue) []*clientv1.SchemaValidationIssue {
	out := make([]*clientv1.SchemaValidationIssue, 0, len(in))
	for _, issue := range in {
		out = append(out, &clientv1.SchemaValidationIssue{Severity: string(issue.Severity), Path: issue.Path, Message: issue.Message})
	}
	return out
}
func mapSchemaError(err error) error {
	if errors.Is(err, schemaservice.ErrSchemaNotFound) {
		return status.Error(codes.NotFound, "schema not found")
	}
	return status.Error(codes.Internal, err.Error())
}
