package app

import (
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/space/access"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func RenderDaemonOperatorsTable(operators []*adminv1.Operator) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Operator ID", "Username", "State", "Created At"})
	for _, operator := range operators {
		t.AppendRow(table.Row{operator.GetOperatorId(), operator.GetUsername(), operator.GetState().String(), operator.GetCreateTime().AsTime()})
	}
	t.Render()
}

func RenderUsersTable(users []identity.User) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"User ID", "Username", "Status"})
	for _, u := range users {
		t.AppendRow(table.Row{u.ID, u.Username, u.Status})
	}
	t.Render()
}

func RenderDaemonUsersTable(users []*adminv1.User) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"User ID", "Username", "State", "Created At"})
	for _, user := range users {
		t.AppendRow(table.Row{user.GetUserId(), user.GetUsername(), user.GetState().String(), user.GetCreateTime().AsTime()})
	}
	t.Render()
}

func RenderClientSpacesTable(spaces []*clientv1.Space) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Space ID", "Owner", "Name", "State", "Access"})
	for _, sp := range spaces {
		roles := make([]string, 0, len(sp.GetCallerAccess().GetRoles()))
		for _, role := range sp.GetCallerAccess().GetRoles() {
			roles = append(roles, role.String())
		}
		t.AppendRow(table.Row{sp.GetSpaceId(), sp.GetOwner().GetId(), sp.GetName(), sp.GetState().String(), strings.Join(roles, ",")})
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

func RenderClientDomainsTable(domains []*clientv1.Domain) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Domain ID", "Space ID", "Key", "Name", "Default"})
	for _, d := range domains {
		t.AppendRow(table.Row{d.GetDomainId(), d.GetSpaceId(), d.GetKey(), d.GetName(), d.GetDefault()})
	}
	t.Render()
}

func RenderDomainsTable(domains []graph.Domain) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Domain ID", "Space ID", "Key", "Name", "Default"})
	for _, d := range domains {
		t.AppendRow(table.Row{d.ID, d.SpaceID, d.Key, d.Name, d.Default})
	}
	t.Render()
}

func RenderClientTemplatesTable(templates []*clientv1.Template) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Template ID", "Key", "Version", "Display Name", "State", "System"})
	for _, tmpl := range templates {
		t.AppendRow(table.Row{tmpl.GetTemplateId(), tmpl.GetKey(), tmpl.GetVersion(), tmpl.GetDisplayName(), tmpl.GetState().String(), tmpl.GetSystem()})
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

func RenderClientAuthSessionsTable(sessions []*clientv1.AuthSessionSummary) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleDefault)
	t.AppendHeader(table.Row{"Session ID", "State", "Client", "Current", "Last Seen", "Expires At"})
	for _, session := range sessions {
		t.AppendRow(table.Row{session.GetAuthSessionId(), session.GetState().String(), session.GetClient().GetName(), session.GetCurrent(), session.GetLastSeenTime().AsTime(), session.GetExpireTime().AsTime()})
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
