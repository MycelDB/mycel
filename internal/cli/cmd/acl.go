package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/domain/access"
	"github.com/myceldb/mycel/domain/identity"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewACLCommand(a *app.App) *cobra.Command {
	acl := &cobra.Command{Use: "acl", Short: "Manage ACL rules"}
	acl.AddCommand(NewACLGrantCommand(a), NewACLRevokeCommand(a), NewACLListCommand(a))
	return acl
}

func NewACLGrantCommand(a *app.App) *cobra.Command {
	grant := &cobra.Command{Use: "grant", Short: "Grant ACL rules"}
	grant.AddCommand(NewACLGrantSystemCommand(a), NewACLGrantSpaceCommand(a))
	return grant
}

func NewACLGrantSystemCommand(a *app.App) *cobra.Command {
	var userIDText string
	var roles []string
	cmd := &cobra.Command{Use: "system", Short: "Grant system roles to a user", RunE: func(cmd *cobra.Command, args []string) error {
		if userIDText == "" || len(roles) == 0 {
			return fmt.Errorf("--user-id and at least one --role are required")
		}
		userID, err := app.ParseUUID[identity.UserID](userIDText)
		if err != nil {
			return err
		}
		parsed := make([]access.SystemRole, 0, len(roles))
		for _, role := range roles {
			parsed = append(parsed, access.SystemRole(role))
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		rule, err := a.Engine.GrantSystemRole(cmd.Context(), mycelengine.GrantSystemRoleInput{AccessToken: tok, UserID: userID, Roles: parsed})
		if err != nil {
			return err
		}
		return a.Print(rule, fmt.Sprintf("system acl granted: user=%s roles=%v\n", userID, parsed))
	}}
	cmd.Flags().StringVar(&userIDText, "user-id", "", "target user ID")
	cmd.Flags().StringArrayVar(&roles, "role", nil, "system role to grant: superuser, user_admin, operator (repeatable)")
	return cmd
}

func NewACLGrantSpaceCommand(a *app.App) *cobra.Command {
	var userIDText, spaceIDText string
	var permissions []string
	cmd := &cobra.Command{Use: "space", Short: "Grant space permissions to a user", RunE: func(cmd *cobra.Command, args []string) error {
		if userIDText == "" || len(permissions) == 0 {
			return fmt.Errorf("--user-id and at least one --permission are required")
		}
		userID, err := app.ParseUUID[identity.UserID](userIDText)
		if err != nil {
			return err
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		parsed := make([]access.SpacePermission, 0, len(permissions))
		for _, permission := range permissions {
			parsed = append(parsed, access.SpacePermission(permission))
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		rule, err := a.Engine.GrantSpaceAccess(cmd.Context(), mycelengine.GrantSpaceAccessInput{AccessToken: tok, SpaceID: spaceID, UserID: userID, Permissions: parsed})
		if err != nil {
			return err
		}
		return a.Print(rule, fmt.Sprintf("space acl granted: space=%s user=%s permissions=%v\n", spaceID, userID, parsed))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().StringVar(&userIDText, "user-id", "", "target user ID")
	cmd.Flags().StringArrayVar(&permissions, "permission", nil, "space permission to grant: read, write, admin (repeatable)")
	return cmd
}

func NewACLRevokeCommand(a *app.App) *cobra.Command {
	revoke := &cobra.Command{Use: "revoke", Short: "Revoke ACL rules"}
	revoke.AddCommand(NewACLRevokeSystemCommand(a), NewACLRevokeSpaceCommand(a))
	return revoke
}

func NewACLRevokeSystemCommand(a *app.App) *cobra.Command {
	var userIDText string
	cmd := &cobra.Command{Use: "system", Short: "Revoke a user's system roles", RunE: func(cmd *cobra.Command, args []string) error {
		if userIDText == "" {
			return fmt.Errorf("--user-id is required")
		}
		userID, err := app.ParseUUID[identity.UserID](userIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		if err := a.Engine.RevokeSystemRole(cmd.Context(), mycelengine.RevokeSystemRoleInput{AccessToken: tok, UserID: userID}); err != nil {
			return err
		}
		return a.Print(map[string]any{"revoked_system_acl_user_id": userID}, fmt.Sprintf("system acl revoked: user=%s\n", userID))
	}}
	cmd.Flags().StringVar(&userIDText, "user-id", "", "target user ID")
	return cmd
}

func NewACLRevokeSpaceCommand(a *app.App) *cobra.Command {
	var userIDText, spaceIDText string
	cmd := &cobra.Command{Use: "space", Short: "Revoke a user's space permissions", RunE: func(cmd *cobra.Command, args []string) error {
		if userIDText == "" {
			return fmt.Errorf("--user-id is required")
		}
		userID, err := app.ParseUUID[identity.UserID](userIDText)
		if err != nil {
			return err
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		if err := a.Engine.RevokeSpaceAccess(cmd.Context(), mycelengine.RevokeSpaceAccessInput{AccessToken: tok, SpaceID: spaceID, UserID: userID}); err != nil {
			return err
		}
		return a.Print(map[string]any{"revoked_space_acl_space_id": spaceID, "user_id": userID}, fmt.Sprintf("space acl revoked: space=%s user=%s\n", spaceID, userID))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().StringVar(&userIDText, "user-id", "", "target user ID")
	return cmd
}

func NewACLListCommand(a *app.App) *cobra.Command {
	list := &cobra.Command{Use: "list", Short: "List ACL rules"}
	list.AddCommand(NewACLListSystemCommand(a), NewACLListSpaceCommand(a))
	return list
}

func NewACLListSystemCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "system", Short: "List system ACL rules", RunE: func(cmd *cobra.Command, args []string) error {
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		rules, err := a.Engine.ListSystemAccess(cmd.Context(), mycelengine.ListSystemAccessInput{AccessToken: tok})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(rules, "")
		}
		app.RenderSystemAccessTable(rules)
		return nil
	}}
}

func NewACLListSpaceCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "space", Short: "List ACL rules for one space", RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		rules, err := a.Engine.ListSpaceAccess(cmd.Context(), mycelengine.ListSpaceAccessInput{AccessToken: tok, SpaceID: spaceID})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(rules, "")
		}
		app.RenderSpaceAccessTable(rules)
		return nil
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	return cmd
}

func NewAddACLCommand(a *app.App) *cobra.Command {
	var userIDText, spaceIDText string
	var roles, permissions []string
	cmd := &cobra.Command{Use: "acl", Short: "Add/update a system role rule or space ACL rule", RunE: func(cmd *cobra.Command, args []string) error {
		if userIDText == "" {
			return fmt.Errorf("--user-id is required")
		}
		userID, err := app.ParseUUID[identity.UserID](userIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		if len(roles) > 0 {
			parsed := make([]access.SystemRole, 0, len(roles))
			for _, role := range roles {
				parsed = append(parsed, access.SystemRole(role))
			}
			rule, err := a.Engine.GrantSystemRole(cmd.Context(), mycelengine.GrantSystemRoleInput{AccessToken: tok, UserID: userID, Roles: parsed})
			if err != nil {
				return err
			}
			return a.Print(rule, fmt.Sprintf("system acl added: %s roles=%v\n", userID, parsed))
		}
		if len(permissions) == 0 {
			return fmt.Errorf("space ACL requires at least one --permission, or use --role for system ACL")
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		parsed := make([]access.SpacePermission, 0, len(permissions))
		for _, permission := range permissions {
			parsed = append(parsed, access.SpacePermission(permission))
		}
		rule, err := a.Engine.GrantSpaceAccess(cmd.Context(), mycelengine.GrantSpaceAccessInput{AccessToken: tok, SpaceID: spaceID, UserID: userID, Permissions: parsed})
		if err != nil {
			return err
		}
		return a.Print(rule, fmt.Sprintf("space acl added: space=%s user=%s permissions=%v\n", spaceID, userID, parsed))
	}}
	cmd.Flags().StringVar(&userIDText, "user-id", "", "target user ID")
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID for space ACLs")
	cmd.Flags().StringArrayVar(&roles, "role", nil, "system role to grant: superuser, user_admin, operator (repeatable)")
	cmd.Flags().StringArrayVar(&permissions, "permission", nil, "space permission to grant: read, write, admin (repeatable)")
	return cmd
}

func NewDeleteACLCommand(a *app.App) *cobra.Command {
	var userIDText, spaceIDText string
	var systemACL bool
	cmd := &cobra.Command{Use: "acl", Short: "Delete a system role rule or space ACL rule", RunE: func(cmd *cobra.Command, args []string) error {
		if userIDText == "" {
			return fmt.Errorf("--user-id is required")
		}
		userID, err := app.ParseUUID[identity.UserID](userIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		if systemACL {
			if err := a.Engine.RevokeSystemRole(cmd.Context(), mycelengine.RevokeSystemRoleInput{AccessToken: tok, UserID: userID}); err != nil {
				return err
			}
			return a.Print(map[string]any{"deleted_system_acl_user_id": userID}, fmt.Sprintf("system acl deleted: %s\n", userID))
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		if err := a.Engine.RevokeSpaceAccess(cmd.Context(), mycelengine.RevokeSpaceAccessInput{AccessToken: tok, SpaceID: spaceID, UserID: userID}); err != nil {
			return err
		}
		return a.Print(map[string]any{"deleted_space_acl_space_id": spaceID, "user_id": userID}, fmt.Sprintf("space acl deleted: space=%s user=%s\n", spaceID, userID))
	}}
	cmd.Flags().StringVar(&userIDText, "user-id", "", "target user ID")
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID for space ACLs")
	cmd.Flags().BoolVar(&systemACL, "system", false, "delete the target user's system role rule")
	return cmd
}

func NewListACLCommand(a *app.App) *cobra.Command {
	var systemACL bool
	var spaceIDText string
	cmd := &cobra.Command{Use: "acl", Short: "List system ACLs or ACLs for one space", RunE: func(cmd *cobra.Command, args []string) error {
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		if systemACL {
			rules, err := a.Engine.ListSystemAccess(cmd.Context(), mycelengine.ListSystemAccessInput{AccessToken: tok})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(rules, "")
			}
			app.RenderSystemAccessTable(rules)
			return nil
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		rules, err := a.Engine.ListSpaceAccess(cmd.Context(), mycelengine.ListSpaceAccessInput{AccessToken: tok, SpaceID: spaceID})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(rules, "")
		}
		app.RenderSpaceAccessTable(rules)
		return nil
	}}
	cmd.Flags().BoolVar(&systemACL, "system", false, "list system ACL rules")
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "list ACL rules for this space")
	return cmd
}
