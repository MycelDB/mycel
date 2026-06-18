package app

import (
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/myceldb/mycel/domain/access"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
)

func RenderUsersTable(users []identity.User) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"User ID", "Ref", "Email", "Username", "Status"})
	for _, u := range users {
		t.AppendRow(table.Row{u.ID, u.Ref, stringPtrValue(u.Email), stringPtrValue(u.Username), u.Status})
	}
	t.Render()
}

func RenderSpacesTable(spaces []domainspace.Space) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Space ID", "Owner ID", "Name", "Status"})
	for _, sp := range spaces {
		t.AppendRow(table.Row{sp.SpaceID, sp.OwnerID, sp.Name, sp.Status})
	}
	t.Render()
}

func RenderTemplatesTable(templates []graph.Template) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Template ID", "Key", "Version", "Display Name", "System"})
	for _, tmpl := range templates {
		t.AppendRow(table.Row{tmpl.ID, tmpl.Key, tmpl.Version, tmpl.DisplayName, tmpl.System})
	}
	t.Render()
}

func RenderNodesTable(nodes []graph.Node) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Node ID", "Template ID", "Content"})
	for _, node := range nodes {
		t.AppendRow(table.Row{node.ID, templateIDValue(node.TemplateID), previewValue(node.Content, 100)})
	}
	t.Render()
}

func RenderSystemAccessTable(rules []access.SystemAccessRule) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Rule ID", "User ID", "Roles"})
	for _, rule := range rules {
		t.AppendRow(table.Row{rule.ID, rule.UserID, joinSystemRoles(rule.Roles)})
	}
	t.Render()
}

func RenderSpaceAccessTable(rules []access.SpaceAccessRule) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Rule ID", "Space ID", "User ID", "Permissions"})
	for _, rule := range rules {
		t.AppendRow(table.Row{rule.ID, rule.SpaceID, rule.UserID, joinSpacePermissions(rule.Permissions)})
	}
	t.Render()
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func templateIDValue(value *graph.TemplateID) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func previewValue(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit <= 0 || len([]rune(value)) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit]) + "…"
}

func joinSystemRoles(roles []access.SystemRole) string {
	items := make([]string, 0, len(roles))
	for _, role := range roles {
		items = append(items, string(role))
	}
	return strings.Join(items, ",")
}

func joinSpacePermissions(permissions []access.SpacePermission) string {
	items := make([]string, 0, len(permissions))
	for _, permission := range permissions {
		items = append(items, string(permission))
	}
	return strings.Join(items, ",")
}
