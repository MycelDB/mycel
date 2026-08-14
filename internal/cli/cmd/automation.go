package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func NewAutomationCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "automation", Aliases: []string{"automations"}, Short: "Manage graph automations"}
	cmd.AddCommand(newAutomationValidateCommand(), newAutomationCreateCommand(a), newAutomationUpdateCommand(a), newAutomationPutCommand(a), newAutomationListCommand(a), newAutomationGetCommand(a), newAutomationEnableCommand(a), newAutomationDisableCommand(a), newAutomationDeleteCommand(a), newAutomationRunsCommand(a), newAutomationRunGetCommand(a), newAutomationInvocationCommand(a))
	return cmd
}

func newAutomationValidateCommand() *cobra.Command {
	return &cobra.Command{Use: "validate automation.json", Short: "Validate an automation definition locally", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		var def automationmodel.Definition
		if err := json.Unmarshal(data, &def); err != nil {
			return err
		}
		if err := automationmodel.ValidateDefinition(def); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "valid")
		return nil
	}}
}

func newAutomationCreateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: "create automation.json", Aliases: []string{"add"}, Short: "Create an automation definition", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		res, err := clientv1.NewAutomationServiceClient(conn).CreateAutomation(authCtx, &clientv1.CreateAutomationRequest{DomainId: domainID, DefinitionJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Println(res.GetDefinitionJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newAutomationUpdateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: "update <automation-id> automation.json", Short: "Update an automation definition", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		res, err := clientv1.NewAutomationServiceClient(conn).UpdateAutomation(authCtx, &clientv1.UpdateAutomationRequest{DomainId: domainID, AutomationId: args[0], DefinitionJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Println(res.GetDefinitionJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newAutomationPutCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var automationID string
	cmd := &cobra.Command{Use: "put automation.json", Short: "Create or update an automation definition", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		client := clientv1.NewAutomationServiceClient(conn)
		if automationID != "" {
			res, err := client.UpdateAutomation(authCtx, &clientv1.UpdateAutomationRequest{DomainId: domainID, AutomationId: automationID, DefinitionJson: string(data)})
			if err != nil {
				return err
			}
			fmt.Println(res.GetDefinitionJson())
			return nil
		}
		res, err := client.CreateAutomation(authCtx, &clientv1.CreateAutomationRequest{DomainId: domainID, DefinitionJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Println(res.GetDefinitionJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&automationID, "id", "", "existing automation ID to update")
	return cmd
}

func newAutomationListCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var status string
	cmd := &cobra.Command{Use: "list", Short: "List automation definitions", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		res, err := clientv1.NewAutomationServiceClient(conn).ListAutomations(authCtx, &clientv1.ListAutomationsRequest{DomainId: domainID, Status: status})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, item := range res.GetAutomations() {
			fmt.Printf("%s\t%s\tv%d\t%s\n", item.GetId(), item.GetName(), item.GetVersion(), item.GetStatus())
		}
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&status, "status", "", "filter by status")
	return cmd
}

func newAutomationGetCommand(a *app.App) *cobra.Command {
	return automationIDCommand(a, "get", "Get an automation definition", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		res, err := client.GetAutomation(ctx, &clientv1.GetAutomationRequest{DomainId: domainID, AutomationId: id})
		if err != nil {
			return "", err
		}
		return res.GetDefinitionJson(), nil
	})
}
func newAutomationEnableCommand(a *app.App) *cobra.Command {
	return automationIDCommand(a, "enable", "Enable an automation", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		res, err := client.EnableAutomation(ctx, &clientv1.EnableAutomationRequest{DomainId: domainID, AutomationId: id})
		if err != nil {
			return "", err
		}
		return res.GetDefinitionJson(), nil
	})
}
func newAutomationDisableCommand(a *app.App) *cobra.Command {
	return automationIDCommand(a, "disable", "Disable an automation", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		res, err := client.DisableAutomation(ctx, &clientv1.DisableAutomationRequest{DomainId: domainID, AutomationId: id})
		if err != nil {
			return "", err
		}
		return res.GetDefinitionJson(), nil
	})
}
func newAutomationDeleteCommand(a *app.App) *cobra.Command {
	return automationIDCommand(a, "delete", "Delete an automation", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		_, err := client.DeleteAutomation(ctx, &clientv1.DeleteAutomationRequest{DomainId: domainID, AutomationId: id})
		return "deleted", err
	})
}

func automationIDCommand(a *app.App, use, short string, run func(clientv1.AutomationServiceClient, context.Context, string, string) (string, error)) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: use + " <automation-id>", Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		text, err := run(clientv1.NewAutomationServiceClient(conn), authCtx, domainID, args[0])
		if err != nil {
			return err
		}
		fmt.Println(text)
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newAutomationRunGetCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: "run get <run-id>", Short: "Get an automation run detail record", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "get" {
			return fmt.Errorf("unknown run subcommand %q", args[0])
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		res, err := clientv1.NewAutomationServiceClient(conn).GetAutomationRun(authCtx, &clientv1.GetAutomationRunRequest{DomainId: domainID, RunId: args[1]})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		fmt.Println(res.GetRunJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newAutomationInvocationCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "invocation", Aliases: []string{"invocations"}, Short: "Manage automation invocations"}
	cmd.AddCommand(newAutomationInvocationActionCommand(a, "retry"), newAutomationInvocationActionCommand(a, "cancel"))
	return cmd
}

func newAutomationInvocationActionCommand(a *app.App, action string) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: action + " <invocation-id>", Short: action + " an automation invocation", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		client := clientv1.NewAutomationServiceClient(conn)
		var status string
		if action == "retry" {
			res, err := client.RetryAutomationInvocation(authCtx, &clientv1.RetryAutomationInvocationRequest{DomainId: domainID, InvocationId: args[0]})
			if err != nil {
				return err
			}
			status = res.GetInvocation().GetStatus()
		} else {
			res, err := client.CancelAutomationInvocation(authCtx, &clientv1.CancelAutomationInvocationRequest{DomainId: domainID, InvocationId: args[0]})
			if err != nil {
				return err
			}
			status = res.GetInvocation().GetStatus()
		}
		fmt.Println(status)
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newAutomationRunsCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var automationID, status string
	var limit int32
	cmd := &cobra.Command{Use: "runs", Aliases: []string{"run-list", "invocation-list"}, Short: "List automation invocations", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		res, err := clientv1.NewAutomationServiceClient(conn).ListAutomationInvocations(authCtx, &clientv1.ListAutomationInvocationsRequest{DomainId: domainID, AutomationId: automationID, Status: status, Limit: limit})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, item := range res.GetInvocations() {
			fmt.Printf("%s\t%s\t%s\t%s\t%s\n", item.GetId(), item.GetAutomationId(), item.GetEventType(), item.GetChangedElementId(), item.GetStatus())
		}
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&automationID, "automation", "", "filter by automation ID")
	cmd.Flags().StringVar(&status, "status", "", "filter by status")
	cmd.Flags().Int32Var(&limit, "limit", 50, "maximum invocations to list")
	return cmd
}

type automationDomainFlags struct {
	SpaceID  string
	Domain   string
	DomainID string
}

func bindAutomationDomainFlags(cmd *cobra.Command, flags *automationDomainFlags) {
	cmd.Flags().StringVar(&flags.SpaceID, "space-id", "", "space ID for domain lookup")
	cmd.Flags().StringVar(&flags.Domain, "domain", graph.DefaultDomainKey, "domain key or ID")
	cmd.Flags().StringVar(&flags.DomainID, "domain-id", "", "domain UUID (deprecated; prefer --space-id with --domain)")
}

func resolveAutomationDomainID(cmd *cobra.Command, a *app.App, conn grpc.ClientConnInterface, authCtx context.Context, flags automationDomainFlags) (string, error) {
	if strings.TrimSpace(flags.DomainID) != "" {
		return strings.TrimSpace(flags.DomainID), nil
	}
	domainRef := strings.TrimSpace(flags.Domain)
	spaceIDText := strings.TrimSpace(flags.SpaceID)
	if domainRef == "" && a.CurrentDomainID != "" {
		return a.CurrentDomainID, nil
	}
	if domainRef == "" {
		domainRef = graph.DefaultDomainKey
	}
	if spaceIDText == "" {
		if _, err := uuid.Parse(domainRef); err == nil {
			return domainRef, nil
		}
	}
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return "", err
	}
	return daemonResolveDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
}
