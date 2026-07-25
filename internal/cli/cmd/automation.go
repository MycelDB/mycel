package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
)

func NewAutomationCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "automation", Short: "Manage graph automations"}
	cmd.AddCommand(newAutomationPutCommand(a), newAutomationListCommand(a), newAutomationGetCommand(a), newAutomationEnableCommand(a), newAutomationDisableCommand(a), newAutomationDeleteCommand(a), newAutomationRunsCommand(a))
	return cmd
}

func newAutomationPutCommand(a *app.App) *cobra.Command {
	var domainID, automationID string
	cmd := &cobra.Command{Use: "put automation.json", Short: "Create or update an automation definition", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
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
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	cmd.Flags().StringVar(&automationID, "id", "", "existing automation ID to update")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}

func newAutomationListCommand(a *app.App) *cobra.Command {
	var domainID, status string
	cmd := &cobra.Command{Use: "list", Short: "List automation definitions", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAutomationServiceClient(conn).ListAutomations(authCtx, &clientv1.ListAutomationsRequest{DomainId: domainID, Status: status})
		if err != nil {
			return err
		}
		for _, item := range res.GetAutomations() {
			fmt.Printf("%s\t%s\tv%d\t%s\n", item.GetId(), item.GetName(), item.GetVersion(), item.GetStatus())
		}
		return nil
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	cmd.Flags().StringVar(&status, "status", "", "filter by status")
	_ = cmd.MarkFlagRequired("domain")
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
	var domainID string
	cmd := &cobra.Command{Use: use + " <automation-id>", Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		text, err := run(clientv1.NewAutomationServiceClient(conn), authCtx, domainID, args[0])
		if err != nil {
			return err
		}
		fmt.Println(text)
		return nil
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}

func newAutomationRunsCommand(a *app.App) *cobra.Command {
	var domainID, automationID, status string
	var limit int32
	cmd := &cobra.Command{Use: "runs", Short: "List automation invocations", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAutomationServiceClient(conn).ListAutomationInvocations(authCtx, &clientv1.ListAutomationInvocationsRequest{DomainId: domainID, AutomationId: automationID, Status: status, Limit: limit})
		if err != nil {
			return err
		}
		for _, item := range res.GetInvocations() {
			fmt.Printf("%s\t%s\t%s\t%s\t%s\n", item.GetId(), item.GetAutomationId(), item.GetEventType(), item.GetChangedElementId(), item.GetStatus())
		}
		return nil
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	cmd.Flags().StringVar(&automationID, "automation", "", "filter by automation ID")
	cmd.Flags().StringVar(&status, "status", "", "filter by status")
	cmd.Flags().Int32Var(&limit, "limit", 50, "maximum invocations to list")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}
