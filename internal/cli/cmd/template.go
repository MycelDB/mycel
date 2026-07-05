package cmd

import (
	"fmt"

	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/internal/cli/app"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func NewAddTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText, filePath string
	cmd := &cobra.Command{
		Use:   "template",
		Short: "Import templates from a JSON file or stdin through the daemon Client API",
		RunE: func(cmd *cobra.Command, args []string) error {
			if filePath == "" {
				return fmt.Errorf("--file is required; use --file - to read from stdin")
			}
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			doc, err := app.ReadTemplateDocument(filePath)
			if err != nil {
				return err
			}
			defs, err := templateDefinitionsFromDocument(doc)
			if err != nil {
				return err
			}
			conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := clientv1.NewTemplateServiceClient(conn).ImportTemplates(authCtx, &clientv1.ImportTemplatesRequest{SpaceId: spaceID.String(), Templates: defs})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(res.GetTemplates(), "")
			}
			app.RenderClientTemplatesTable(res.GetTemplates())
			return nil
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().StringVar(&filePath, "file", "", "template JSON file path, or - for stdin")
	return cmd
}

func NewListTemplatesCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	var includeArchived, includeSystem bool
	cmd := &cobra.Command{
		Use:   "templates",
		Short: "List templates for a space through the daemon Client API",
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := clientv1.NewTemplateServiceClient(conn).ListTemplates(authCtx, &clientv1.ListTemplatesRequest{SpaceId: spaceID.String(), IncludeArchived: includeArchived, IncludeSystem: includeSystem})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(res.GetTemplates(), "")
			}
			app.RenderClientTemplatesTable(res.GetTemplates())
			return nil
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().BoolVar(&includeArchived, "include-archived", false, "include archived templates")
	cmd.Flags().BoolVar(&includeSystem, "include-system", false, "include system templates")
	return cmd
}

func NewGetTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "get TEMPLATE_ID", Short: "Get a template by ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTemplateServiceClient(conn).GetTemplate(authCtx, &clientv1.GetTemplateRequest{SpaceId: spaceID.String(), TemplateId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetTemplate(), fmt.Sprintf("template: %s@%s (%s)\n", res.GetTemplate().GetKey(), res.GetTemplate().GetVersion(), res.GetTemplate().GetTemplateId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	return cmd
}

func NewFindTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText, version string
	cmd := &cobra.Command{Use: "find KEY", Short: "Find a template by key and version", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if version == "" {
			return fmt.Errorf("--version is required")
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTemplateServiceClient(conn).FindTemplate(authCtx, &clientv1.FindTemplateRequest{SpaceId: spaceID.String(), Key: args[0], Version: version})
		if err != nil {
			return err
		}
		return a.Print(res.GetTemplate(), fmt.Sprintf("template: %s@%s (%s)\n", res.GetTemplate().GetKey(), res.GetTemplate().GetVersion(), res.GetTemplate().GetTemplateId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().StringVar(&version, "version", "", "template semver version")
	return cmd
}

func NewCreateTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText, version, displayName, description string
	var system, allowExtra, childrenAllowed bool
	cmd := &cobra.Command{Use: "create KEY", Short: "Create one basic template", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if version == "" {
			return fmt.Errorf("--version is required")
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		def := &clientv1.TemplateDefinition{Key: args[0], Version: version, DisplayName: displayName, Description: description, System: system, Properties: &clientv1.PropertyPolicy{AllowExtra: allowExtra}, Children: &clientv1.ChildPolicy{Allowed: childrenAllowed}}
		res, err := clientv1.NewTemplateServiceClient(conn).CreateTemplate(authCtx, &clientv1.CreateTemplateRequest{SpaceId: spaceID.String(), Template: def})
		if err != nil {
			return err
		}
		return a.Print(res.GetTemplate(), fmt.Sprintf("template created: %s@%s (%s)\n", res.GetTemplate().GetKey(), res.GetTemplate().GetVersion(), res.GetTemplate().GetTemplateId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().StringVar(&version, "version", "", "template semver version")
	cmd.Flags().StringVar(&displayName, "display-name", "", "display name")
	cmd.Flags().StringVar(&description, "description", "", "description")
	cmd.Flags().BoolVar(&system, "system", false, "mark as system template")
	cmd.Flags().BoolVar(&allowExtra, "allow-extra", false, "allow properties outside the policy")
	cmd.Flags().BoolVar(&childrenAllowed, "children-allowed", false, "allow direct child nodes")
	return cmd
}

func NewUpdateTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText, displayName, description string
	cmd := &cobra.Command{Use: "update TEMPLATE_ID", Short: "Update template display metadata", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		paths := []string{}
		template := &clientv1.Template{TemplateId: args[0]}
		if cmd.Flags().Changed("display-name") {
			template.DisplayName = displayName
			paths = append(paths, "display_name")
		}
		if cmd.Flags().Changed("description") {
			template.Description = description
			paths = append(paths, "description")
		}
		if len(paths) == 0 {
			return fmt.Errorf("at least one of --display-name or --description is required")
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTemplateServiceClient(conn).UpdateTemplate(authCtx, &clientv1.UpdateTemplateRequest{SpaceId: spaceID.String(), TemplateId: args[0], Template: template, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}})
		if err != nil {
			return err
		}
		return a.Print(res.GetTemplate(), fmt.Sprintf("template updated: %s@%s (%s)\n", res.GetTemplate().GetKey(), res.GetTemplate().GetVersion(), res.GetTemplate().GetTemplateId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().StringVar(&displayName, "display-name", "", "display name")
	cmd.Flags().StringVar(&description, "description", "", "description")
	return cmd
}

func NewArchiveTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "archive TEMPLATE_ID", Short: "Archive a template", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTemplateServiceClient(conn).ArchiveTemplate(authCtx, &clientv1.ArchiveTemplateRequest{SpaceId: spaceID.String(), TemplateId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetTemplate(), fmt.Sprintf("template archived: %s\n", args[0]))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	return cmd
}

func NewDeleteTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "delete TEMPLATE_ID", Aliases: []string{"del", "remove", "rm"}, Short: "Delete a template", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTemplateServiceClient(conn).DeleteTemplate(authCtx, &clientv1.DeleteTemplateRequest{SpaceId: spaceID.String(), TemplateId: args[0], Mode: clientv1.TemplateDeleteMode_TEMPLATE_DELETE_MODE_REQUIRE_UNUSED})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("template deleted: %s\n", args[0]))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	return cmd
}

func templateDefinitionsFromDocument(doc sessionapi.ImportDocument) ([]*clientv1.TemplateDefinition, error) {
	out := make([]*clientv1.TemplateDefinition, 0, len(doc.Templates))
	for _, tmpl := range doc.Templates {
		out = append(out, templateDefinitionFromImport(tmpl))
	}
	return out, nil
}

func templateDefinitionFromImport(t sessionapi.TemplateImport) *clientv1.TemplateDefinition {
	return &clientv1.TemplateDefinition{Key: t.Key, Version: t.Version, DisplayName: t.DisplayName, Description: t.Description, System: t.System, Properties: propertyPolicyDefinitionFromImport(t.Properties), Children: childPolicyDefinitionFromImport(t.Children)}
}

func propertyPolicyDefinitionFromImport(policy sessionapi.PropertyPolicyImport) *clientv1.PropertyPolicy {
	out := &clientv1.PropertyPolicy{AllowExtra: policy.AllowExtra, Forbidden: append([]string(nil), policy.Forbidden...)}
	for _, prop := range policy.Allowed {
		var defaultValue *structpb.Value
		if prop.Default != nil {
			if v, err := structpb.NewValue(prop.Default); err == nil {
				defaultValue = v
			}
		}
		out.Allowed = append(out.Allowed, &clientv1.TemplateProperty{Name: prop.Name, Type: propertyTypeDefinitionFromImport(prop.Type), Required: prop.Required, DefaultValue: defaultValue, Description: prop.Description})
	}
	return out
}

func childPolicyDefinitionFromImport(policy sessionapi.ChildPolicyImport) *clientv1.ChildPolicy {
	out := &clientv1.ChildPolicy{Allowed: policy.Allowed}
	for _, ref := range policy.AllowedTemplates {
		out.AllowedTemplates = append(out.AllowedTemplates, &clientv1.TemplateRef{Key: ref.Key, Version: ref.Version})
	}
	if policy.Order != nil {
		out.Order = &clientv1.ChildOrderPolicy{Mode: childOrderModeDefinitionFromImport(policy.Order.Mode), Property: policy.Order.Property, Direction: sortDirectionDefinitionFromImport(policy.Order.Direction)}
	}
	return out
}

func propertyTypeDefinitionFromImport(t graph.PropertyType) clientv1.PropertyType {
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

func childOrderModeDefinitionFromImport(mode graph.ChildOrderMode) clientv1.ChildOrderMode {
	if mode == graph.ChildOrderModeEdgeProperty {
		return clientv1.ChildOrderMode_CHILD_ORDER_MODE_EDGE_PROPERTY
	}
	return clientv1.ChildOrderMode_CHILD_ORDER_MODE_NONE
}

func sortDirectionDefinitionFromImport(direction graph.SortDirection) clientv1.TemplateSortDirection {
	if direction == graph.SortDirectionDesc {
		return clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_DESC
	}
	if direction == graph.SortDirectionAsc {
		return clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_ASC
	}
	return clientv1.TemplateSortDirection_TEMPLATE_SORT_DIRECTION_UNSPECIFIED
}
