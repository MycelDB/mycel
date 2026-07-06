package client

import (
	"context"
	"errors"
	"strconv"
	"strings"

	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	"github.com/myceldb/mycel/internal/graph/model"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type TemplateService struct {
	clientv1.UnimplementedTemplateServiceServer
	spaces daemonspace.Manager
}

func NewTemplateService(spaces daemonspace.Manager) *TemplateService {
	return &TemplateService{spaces: spaces}
}

func (s *TemplateService) ListTemplates(ctx context.Context, req *clientv1.ListTemplatesRequest) (*clientv1.ListTemplatesResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	templates, err := s.spaces.ListVisibleTemplates(ctx, principal.UserID, req.GetSpaceId(), req.GetIncludeSystem(), req.GetIncludeArchived())
	if err != nil {
		return nil, mapTemplateError(err, "list templates")
	}
	if offset > len(templates) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the template list")
	}
	end := offset + pageSize
	if end > len(templates) {
		end = len(templates)
	}
	out := make([]*clientv1.Template, 0, end-offset)
	for _, template := range templates[offset:end] {
		out = append(out, MapTemplate(template))
	}
	var next string
	if end < len(templates) {
		next = strconv.Itoa(end)
	}
	return &clientv1.ListTemplatesResponse{Templates: out, NextPageToken: next}, nil
}

func (s *TemplateService) GetTemplate(ctx context.Context, req *clientv1.GetTemplateRequest) (*clientv1.GetTemplateResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	template, err := s.spaces.GetVisibleTemplate(ctx, principal.UserID, req.GetSpaceId(), req.GetTemplateId())
	if err != nil {
		return nil, mapTemplateError(err, "get template")
	}
	return &clientv1.GetTemplateResponse{Template: MapTemplate(template)}, nil
}

func (s *TemplateService) FindTemplate(ctx context.Context, req *clientv1.FindTemplateRequest) (*clientv1.FindTemplateResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	template, err := s.spaces.FindVisibleTemplate(ctx, principal.UserID, req.GetSpaceId(), req.GetKey(), req.GetVersion())
	if err != nil {
		return nil, mapTemplateError(err, "find template")
	}
	return &clientv1.FindTemplateResponse{Template: MapTemplate(template)}, nil
}

func (s *TemplateService) CreateTemplate(ctx context.Context, req *clientv1.CreateTemplateRequest) (*clientv1.CreateTemplateResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	input, err := TemplateImportFromProto(req.GetTemplate())
	if err != nil {
		return nil, err
	}
	template, err := s.spaces.CreateTemplate(ctx, principal.UserID, req.GetSpaceId(), input)
	if err != nil {
		return nil, mapTemplateError(err, "create template")
	}
	return &clientv1.CreateTemplateResponse{Template: MapTemplate(template)}, nil
}

func (s *TemplateService) UpdateTemplate(ctx context.Context, req *clientv1.UpdateTemplateRequest) (*clientv1.UpdateTemplateResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetUpdateMask() == nil || len(req.GetUpdateMask().GetPaths()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "update_mask is required")
	}
	var displayName *string
	var description *string
	for _, path := range req.GetUpdateMask().GetPaths() {
		switch strings.TrimSpace(path) {
		case "display_name":
			v := req.GetTemplate().GetDisplayName()
			displayName = &v
		case "description":
			v := req.GetTemplate().GetDescription()
			description = &v
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported update_mask path %q", path)
		}
	}
	template, err := s.spaces.UpdateTemplate(ctx, principal.UserID, req.GetSpaceId(), req.GetTemplateId(), displayName, description)
	if err != nil {
		return nil, mapTemplateError(err, "update template")
	}
	return &clientv1.UpdateTemplateResponse{Template: MapTemplate(template)}, nil
}

func (s *TemplateService) ArchiveTemplate(ctx context.Context, req *clientv1.ArchiveTemplateRequest) (*clientv1.ArchiveTemplateResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	template, err := s.spaces.ArchiveTemplate(ctx, principal.UserID, req.GetSpaceId(), req.GetTemplateId())
	if err != nil {
		return nil, mapTemplateError(err, "archive template")
	}
	return &clientv1.ArchiveTemplateResponse{Template: MapTemplate(template)}, nil
}

func (s *TemplateService) DeleteTemplate(ctx context.Context, req *clientv1.DeleteTemplateRequest) (*clientv1.DeleteTemplateResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.spaces.DeleteTemplate(ctx, principal.UserID, req.GetSpaceId(), req.GetTemplateId()); err != nil {
		return nil, mapTemplateError(err, "delete template")
	}
	return &clientv1.DeleteTemplateResponse{DeletedTemplateId: req.GetTemplateId()}, nil
}

func (s *TemplateService) ImportTemplates(ctx context.Context, req *clientv1.ImportTemplatesRequest) (*clientv1.ImportTemplatesResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	inputs := make([]storetemplate.TemplateImport, 0, len(req.GetTemplates()))
	for _, template := range req.GetTemplates() {
		input, err := TemplateImportFromProto(template)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, input)
	}
	templates, err := s.spaces.ImportTemplates(ctx, principal.UserID, req.GetSpaceId(), inputs)
	if err != nil {
		return nil, mapTemplateError(err, "import templates")
	}
	out := make([]*clientv1.Template, 0, len(templates))
	for _, template := range templates {
		out = append(out, MapTemplate(template))
	}
	return &clientv1.ImportTemplatesResponse{Templates: out}, nil
}

func MapTemplate(template graph.Template) *clientv1.Template {
	state := clientv1.TemplateState_TEMPLATE_STATE_ACTIVE
	if template.State == graph.TemplateStateArchived {
		state = clientv1.TemplateState_TEMPLATE_STATE_ARCHIVED
	}
	def := MapTemplateDefinition(template)
	return &clientv1.Template{TemplateId: template.ID.String(), SpaceId: template.SpaceID.String(), Key: def.GetKey(), Version: def.GetVersion(), DisplayName: def.GetDisplayName(), Description: def.GetDescription(), System: def.GetSystem(), State: state, Properties: def.GetProperties(), Children: def.GetChildren()}
}

func MapTemplateDefinition(template graph.Template) *clientv1.TemplateDefinition {
	return &clientv1.TemplateDefinition{Key: template.Key, Version: template.Version, DisplayName: template.DisplayName, Description: template.Description, System: template.System, Properties: propertyPolicyToProto(template.Properties), Children: childPolicyToProto(template.Children)}
}

func TemplateImportFromProto(template *clientv1.TemplateDefinition) (storetemplate.TemplateImport, error) {
	if template == nil {
		return storetemplate.TemplateImport{}, status.Error(codes.InvalidArgument, "template is required")
	}
	return storetemplate.TemplateImport{Key: template.GetKey(), Version: template.GetVersion(), DisplayName: template.GetDisplayName(), Description: template.GetDescription(), System: template.GetSystem(), Properties: propertyPolicyFromProto(template.GetProperties()), Children: childPolicyFromProto(template.GetChildren())}, nil
}

func propertyPolicyToProto(policy graph.PropertyPolicy) *clientv1.PropertyPolicy {
	out := &clientv1.PropertyPolicy{AllowExtra: policy.AllowExtra, Forbidden: append([]string(nil), policy.Forbidden...)}
	for _, prop := range policy.Allowed {
		var defaultValue *structpb.Value
		if prop.Default != nil {
			if v, err := structpb.NewValue(prop.Default); err == nil {
				defaultValue = v
			}
		}
		out.Allowed = append(out.Allowed, &clientv1.TemplateProperty{Name: prop.Name, Type: propertyTypeToProto(prop.Type), Required: prop.Required, DefaultValue: defaultValue, Description: prop.Description})
	}
	return out
}

func propertyPolicyFromProto(policy *clientv1.PropertyPolicy) storetemplate.PropertyPolicyImport {
	if policy == nil {
		return storetemplate.PropertyPolicyImport{}
	}
	out := storetemplate.PropertyPolicyImport{AllowExtra: policy.GetAllowExtra(), Forbidden: append([]string(nil), policy.GetForbidden()...)}
	for _, prop := range policy.GetAllowed() {
		var defaultValue any
		if prop.GetDefaultValue() != nil {
			defaultValue = prop.GetDefaultValue().AsInterface()
		}
		out.Allowed = append(out.Allowed, storetemplate.TemplatePropertyImport{Name: prop.GetName(), Type: propertyTypeFromProto(prop.GetType()), Required: prop.GetRequired(), Default: defaultValue, Description: prop.GetDescription()})
	}
	return out
}

func childPolicyToProto(policy graph.ChildPolicy) *clientv1.ChildPolicy {
	out := &clientv1.ChildPolicy{Allowed: policy.Allowed}
	for _, ref := range policy.AllowedTemplates {
		out.AllowedTemplates = append(out.AllowedTemplates, &clientv1.TemplateRef{Key: ref.Key, Version: ref.Version})
	}
	if policy.Order != nil {
		out.Order = &clientv1.ChildOrderPolicy{Mode: childOrderModeToProto(policy.Order.Mode), Property: policy.Order.Property, Direction: sortDirectionToProto(policy.Order.Direction)}
	}
	return out
}

func childPolicyFromProto(policy *clientv1.ChildPolicy) storetemplate.ChildPolicyImport {
	if policy == nil {
		return storetemplate.ChildPolicyImport{}
	}
	out := storetemplate.ChildPolicyImport{Allowed: policy.GetAllowed()}
	for _, ref := range policy.GetAllowedTemplates() {
		out.AllowedTemplates = append(out.AllowedTemplates, storetemplate.TemplateRefImport{Key: ref.GetKey(), Version: ref.GetVersion()})
	}
	if policy.GetOrder() != nil {
		out.Order = &storetemplate.ChildOrderPolicyImport{Mode: childOrderModeFromProto(policy.GetOrder().GetMode()), Property: policy.GetOrder().GetProperty(), Direction: sortDirectionFromProto(policy.GetOrder().GetDirection())}
	}
	return out
}

func propertyTypeToProto(t graph.PropertyType) clientv1.PropertyType {
	switch t {
	case graph.PropertyTypeString:
		return clientv1.PropertyType_PROPERTY_TYPE_STRING
	case graph.PropertyTypeNumber:
		return clientv1.PropertyType_PROPERTY_TYPE_NUMBER
	case graph.PropertyTypeBool:
		return clientv1.PropertyType_PROPERTY_TYPE_BOOL
	case graph.PropertyTypeObject:
		return clientv1.PropertyType_PROPERTY_TYPE_OBJECT
	case graph.PropertyTypeArray:
		return clientv1.PropertyType_PROPERTY_TYPE_ARRAY
	case graph.PropertyTypeDate:
		return clientv1.PropertyType_PROPERTY_TYPE_DATE
	default:
		return clientv1.PropertyType_PROPERTY_TYPE_UNSPECIFIED
	}
}

func propertyTypeFromProto(t clientv1.PropertyType) graph.PropertyType {
	switch t {
	case clientv1.PropertyType_PROPERTY_TYPE_STRING:
		return graph.PropertyTypeString
	case clientv1.PropertyType_PROPERTY_TYPE_NUMBER:
		return graph.PropertyTypeNumber
	case clientv1.PropertyType_PROPERTY_TYPE_BOOL:
		return graph.PropertyTypeBool
	case clientv1.PropertyType_PROPERTY_TYPE_OBJECT:
		return graph.PropertyTypeObject
	case clientv1.PropertyType_PROPERTY_TYPE_ARRAY:
		return graph.PropertyTypeArray
	case clientv1.PropertyType_PROPERTY_TYPE_DATE:
		return graph.PropertyTypeDate
	default:
		return ""
	}
}

func childOrderModeToProto(mode graph.ChildOrderMode) clientv1.ChildOrderMode {
	if mode == graph.ChildOrderModeEdgeProperty {
		return clientv1.ChildOrderMode_CHILD_ORDER_MODE_EDGE_PROPERTY
	}
	return clientv1.ChildOrderMode_CHILD_ORDER_MODE_NONE
}

func childOrderModeFromProto(mode clientv1.ChildOrderMode) graph.ChildOrderMode {
	if mode == clientv1.ChildOrderMode_CHILD_ORDER_MODE_EDGE_PROPERTY {
		return graph.ChildOrderModeEdgeProperty
	}
	return graph.ChildOrderModeNone
}

func sortDirectionToProto(direction graph.SortDirection) clientv1.TemplateSortDirection {
	if direction == graph.SortDirectionDesc {
		return clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_DESC
	}
	if direction == graph.SortDirectionAsc {
		return clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_ASC
	}
	return clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_UNSPECIFIED
}

func sortDirectionFromProto(direction clientv1.TemplateSortDirection) graph.SortDirection {
	if direction == clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_DESC {
		return graph.SortDirectionDesc
	}
	if direction == clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_ASC {
		return graph.SortDirectionAsc
	}
	return ""
}

func mapTemplateError(err error, action string) error {
	if errors.Is(err, daemonspace.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daemonspace.ErrSpaceNotFound) {
		return status.Error(codes.NotFound, "space or template not found")
	}
	if errors.Is(err, daemonspace.ErrUnauthorized) {
		return status.Error(codes.PermissionDenied, "template access denied")
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
