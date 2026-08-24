package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
)

func NewProcedureCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "procedure", Aliases: []string{"procedures", "graph-procedure", "graph-procedures"}, Short: "Manage graph automation procedures"}
	cmd.AddCommand(newProcedureValidateCommand(), newProcedureCreateCommand(a), newProcedureUpdateCommand(a), newProcedureListCommand(a), newProcedureGetCommand(a), newProcedureDeleteCommand(a))
	return cmd
}

func NewAutomationBindingCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "automation-binding", Aliases: []string{"automation-bindings", "binding", "bindings"}, Short: "Manage graph automation bindings"}
	cmd.AddCommand(newBindingValidateCommand(), newBindingCreateCommand(a), newBindingUpdateCommand(a), newBindingListCommand(a), newBindingGetCommand(a), newBindingEnableCommand(a), newBindingDisableCommand(a), newBindingDeleteCommand(a))
	return cmd
}

func newProcedureValidateCommand() *cobra.Command {
	return &cobra.Command{Use: "validate procedure.json", Short: "Validate a graph procedure locally", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		var procedure automationmodel.Procedure
		if err := json.Unmarshal(data, &procedure); err != nil {
			return err
		}
		if err := automationmodel.ValidateProcedure(procedure); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "valid")
		return nil
	}}
}

func newBindingValidateCommand() *cobra.Command {
	return &cobra.Command{Use: "validate binding.json", Short: "Validate a graph automation binding locally", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		var binding automationmodel.Binding
		if err := json.Unmarshal(data, &binding); err != nil {
			return err
		}
		if err := automationmodel.ValidateBinding(binding, nil); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "valid")
		return nil
	}}
}

func newProcedureCreateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: "create procedure.json", Short: "Create a graph procedure", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
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
		res, err := clientv1.NewAutomationServiceClient(conn).CreateGraphProcedure(authCtx, &clientv1.CreateGraphProcedureRequest{DomainId: domainID, ProcedureJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), res.GetProcedureJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newProcedureUpdateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: "update <procedure-id> procedure.json", Short: "Update a graph procedure", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
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
		res, err := clientv1.NewAutomationServiceClient(conn).UpdateGraphProcedure(authCtx, &clientv1.UpdateGraphProcedureRequest{DomainId: domainID, ProcedureId: args[0], ProcedureJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), res.GetProcedureJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newProcedureListCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var status string
	cmd := &cobra.Command{Use: "list", Short: "List graph procedures", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		res, err := clientv1.NewAutomationServiceClient(conn).ListGraphProcedures(authCtx, &clientv1.ListGraphProceduresRequest{DomainId: domainID, Status: status})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, item := range res.GetProcedures() {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\tv%d\t%s\t%s\n", item.GetId(), item.GetName(), item.GetVersion(), item.GetStatus(), item.GetOperation())
		}
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&status, "status", "", "filter by status")
	return cmd
}

func newProcedureGetCommand(a *app.App) *cobra.Command {
	return procedureIDCommand(a, "get", "Get a graph procedure", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		res, err := client.GetGraphProcedure(ctx, &clientv1.GetGraphProcedureRequest{DomainId: domainID, ProcedureId: id})
		if err != nil {
			return "", err
		}
		return res.GetProcedureJson(), nil
	})
}

func newProcedureDeleteCommand(a *app.App) *cobra.Command {
	return procedureIDCommand(a, "delete", "Delete a graph procedure", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		_, err := client.DeleteGraphProcedure(ctx, &clientv1.DeleteGraphProcedureRequest{DomainId: domainID, ProcedureId: id})
		return "deleted", err
	})
}

func procedureIDCommand(a *app.App, use, short string, run func(clientv1.AutomationServiceClient, context.Context, string, string) (string, error)) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: use + " <procedure-id>", Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
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
		fmt.Fprintln(cmd.OutOrStdout(), text)
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newBindingCreateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: "create binding.json", Short: "Create a graph automation binding", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
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
		res, err := clientv1.NewAutomationServiceClient(conn).CreateGraphAutomationBinding(authCtx, &clientv1.CreateGraphAutomationBindingRequest{DomainId: domainID, BindingJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), res.GetBindingJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newBindingUpdateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: "update <binding-id> binding.json", Short: "Update a graph automation binding", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
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
		res, err := clientv1.NewAutomationServiceClient(conn).UpdateGraphAutomationBinding(authCtx, &clientv1.UpdateGraphAutomationBindingRequest{DomainId: domainID, BindingId: args[0], BindingJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), res.GetBindingJson())
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newBindingListCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var status string
	cmd := &cobra.Command{Use: "list", Short: "List graph automation bindings", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
		if err != nil {
			return err
		}
		res, err := clientv1.NewAutomationServiceClient(conn).ListGraphAutomationBindings(authCtx, &clientv1.ListGraphAutomationBindingsRequest{DomainId: domainID, Status: status})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, item := range res.GetBindings() {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\tv%d\t%s\t%s\t%s\t%s\n", item.GetId(), item.GetProcedureId(), item.GetProcedureVersion(), item.GetStatus(), item.GetTriggerType(), item.GetActorPrincipalId(), item.GetOnBehalfOfPrincipalId())
		}
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&status, "status", "", "filter by status")
	return cmd
}

func newBindingGetCommand(a *app.App) *cobra.Command {
	return bindingIDCommand(a, "get", "Get a graph automation binding", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		res, err := client.GetGraphAutomationBinding(ctx, &clientv1.GetGraphAutomationBindingRequest{DomainId: domainID, BindingId: id})
		if err != nil {
			return "", err
		}
		return res.GetBindingJson(), nil
	})
}
func newBindingEnableCommand(a *app.App) *cobra.Command {
	return bindingIDCommand(a, "enable", "Enable a graph automation binding", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		res, err := client.EnableGraphAutomationBinding(ctx, &clientv1.EnableGraphAutomationBindingRequest{DomainId: domainID, BindingId: id})
		if err != nil {
			return "", err
		}
		return res.GetBindingJson(), nil
	})
}
func newBindingDisableCommand(a *app.App) *cobra.Command {
	return bindingIDCommand(a, "disable", "Disable a graph automation binding", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		res, err := client.DisableGraphAutomationBinding(ctx, &clientv1.DisableGraphAutomationBindingRequest{DomainId: domainID, BindingId: id})
		if err != nil {
			return "", err
		}
		return res.GetBindingJson(), nil
	})
}
func newBindingDeleteCommand(a *app.App) *cobra.Command {
	return bindingIDCommand(a, "delete", "Delete a graph automation binding", func(client clientv1.AutomationServiceClient, ctx context.Context, domainID, id string) (string, error) {
		_, err := client.DeleteGraphAutomationBinding(ctx, &clientv1.DeleteGraphAutomationBindingRequest{DomainId: domainID, BindingId: id})
		return "deleted", err
	})
}

func bindingIDCommand(a *app.App, use, short string, run func(clientv1.AutomationServiceClient, context.Context, string, string) (string, error)) *cobra.Command {
	var flags automationDomainFlags
	cmd := &cobra.Command{Use: use + " <binding-id>", Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
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
		fmt.Fprintln(cmd.OutOrStdout(), text)
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}
