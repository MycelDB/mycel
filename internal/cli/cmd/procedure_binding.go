package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewProcedureCommand(a *app.App) *cobra.Command {
	return newProcedureCommand(a, "procedure", []string{"procedures", "graph-procedure", "graph-procedures"}, "Compatibility alias for 'mycel automation procedure'", "Manage graph automation procedures.\n\nCanonical path: mycel automation procedure\nThis top-level command remains for compatibility with existing scripts.")
}

func newAutomationProcedureCommand(a *app.App) *cobra.Command {
	return newProcedureCommand(a, "procedure", nil, "Manage reusable graph automation procedures", "Manage reusable graph automation procedures.\n\nProcedures define reusable graph work. Pair them with 'mycel automation binding' to attach triggers, scope, and runtime principal context.")
}

func newProcedureCommand(a *app.App, use string, aliases []string, short, long string) *cobra.Command {
	cmd := &cobra.Command{Use: use, Aliases: aliases, Short: short, Long: long}
	cmd.AddCommand(newProcedureValidateCommand(a), newProcedureCreateCommand(a), newProcedureUpdateCommand(a), newProcedurePutCommand(a), newProcedureListCommand(a), newProcedureGetCommand(a), newProcedureDeleteCommand(a))
	return cmd
}

func NewAutomationBindingCommand(a *app.App) *cobra.Command {
	return newBindingCommand(a, "automation-binding", []string{"automation-bindings", "binding", "bindings"}, "Compatibility alias for 'mycel automation binding'", "Manage graph automation bindings.\n\nCanonical path: mycel automation binding\nThis top-level command remains for compatibility with existing scripts. Broad aliases such as 'binding' and 'bindings' are compatibility aliases, not preferred command paths.")
}

func newAutomationBindingCommand(a *app.App) *cobra.Command {
	return newBindingCommand(a, "binding", nil, "Manage graph automation bindings", "Manage graph automation bindings.\n\nBindings attach reusable procedures to triggers, scope, and runtime principal context. Pair them with 'mycel automation procedure'.")
}

func newBindingCommand(a *app.App, use string, aliases []string, short, long string) *cobra.Command {
	cmd := &cobra.Command{Use: use, Aliases: aliases, Short: short, Long: long}
	cmd.AddCommand(newBindingValidateCommand(a), newBindingCreateCommand(a), newBindingUpdateCommand(a), newBindingPutCommand(a), newBindingListCommand(a), newBindingGetCommand(a), newBindingEnableCommand(a), newBindingDisableCommand(a), newBindingDeleteCommand(a))
	return cmd
}

func newProcedureValidateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var server bool
	cmd := &cobra.Command{Use: "validate procedure.json", Short: "Validate a graph procedure", Long: "Validate a graph procedure.\n\nBy default this runs local JSON/model validation only. Use --server to validate against daemon state and print normalized JSON when --output json is set.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		if server {
			return runServerProcedureValidate(cmd, a, flags, string(data))
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
	cmd.Flags().BoolVar(&server, "server", false, "validate against daemon state instead of local-only checks")
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func newBindingValidateCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var server bool
	cmd := &cobra.Command{Use: "validate binding.json", Short: "Validate a graph automation binding", Long: "Validate a graph automation binding.\n\nBy default this runs local JSON/model validation only. Use --server to validate against daemon state, including referenced procedures, and print normalized JSON when --output json is set.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		if server {
			return runServerBindingValidate(cmd, a, flags, string(data))
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
	cmd.Flags().BoolVar(&server, "server", false, "validate against daemon state instead of local-only checks")
	bindAutomationDomainFlags(cmd, &flags)
	return cmd
}

func runServerProcedureValidate(cmd *cobra.Command, a *app.App, flags automationDomainFlags, procedureJSON string) error {
	conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
	if err != nil {
		return err
	}
	res, err := clientv1.NewAutomationServiceClient(conn).ValidateGraphProcedure(authCtx, &clientv1.ValidateGraphProcedureRequest{DomainId: domainID, ProcedureJson: procedureJSON})
	if err != nil {
		return err
	}
	if !res.GetValid() {
		return fmt.Errorf("graph procedure invalid: %s", strings.TrimSpace(res.GetError()))
	}
	if a.Output == "json" {
		fmt.Fprintln(cmd.OutOrStdout(), res.GetNormalizedProcedureJson())
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "valid")
	return nil
}

func runServerBindingValidate(cmd *cobra.Command, a *app.App, flags automationDomainFlags, bindingJSON string) error {
	conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
	if err != nil {
		return err
	}
	res, err := clientv1.NewAutomationServiceClient(conn).ValidateGraphAutomationBinding(authCtx, &clientv1.ValidateGraphAutomationBindingRequest{DomainId: domainID, BindingJson: bindingJSON})
	if err != nil {
		return err
	}
	if !res.GetValid() {
		return fmt.Errorf("graph automation binding invalid: %s", strings.TrimSpace(res.GetError()))
	}
	if a.Output == "json" {
		fmt.Fprintln(cmd.OutOrStdout(), res.GetNormalizedBindingJson())
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "valid")
	return nil
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

func newProcedurePutCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var procedureID string
	cmd := &cobra.Command{Use: "put procedure.json", Aliases: []string{"upsert"}, Short: "Create or update a graph procedure", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, id, err := prepareAutomationPutJSON(args[0], procedureID, "graph procedure")
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
		text, err := putGraphProcedure(authCtx, clientv1.NewAutomationServiceClient(conn), domainID, id, data)
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), text)
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&procedureID, "id", "", "existing procedure ID to update; also overrides the JSON id for create")
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

func newBindingPutCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var bindingID string
	cmd := &cobra.Command{Use: "put binding.json", Aliases: []string{"upsert"}, Short: "Create or update a graph automation binding", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, id, err := prepareAutomationPutJSON(args[0], bindingID, "graph automation binding")
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
		text, err := putGraphAutomationBinding(authCtx, clientv1.NewAutomationServiceClient(conn), domainID, id, data)
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), text)
		return nil
	}}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&bindingID, "id", "", "existing binding ID to update; also overrides the JSON id for create")
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

func prepareAutomationPutJSON(path, overrideID, entityName string) (string, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	var idOnly struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &idOnly); err != nil {
		return "", "", err
	}
	id := strings.TrimSpace(idOnly.ID)
	overrideID = strings.TrimSpace(overrideID)
	if overrideID != "" {
		var object map[string]any
		if err := json.Unmarshal(data, &object); err != nil {
			return "", "", err
		}
		if object == nil {
			return "", "", fmt.Errorf("%s JSON must be an object", entityName)
		}
		object["id"] = overrideID
		updated, err := json.Marshal(object)
		if err != nil {
			return "", "", err
		}
		data = updated
		id = overrideID
	}
	if id == "" {
		return "", "", fmt.Errorf("%s id is required (set JSON id or --id)", entityName)
	}
	return string(data), id, nil
}

func putGraphProcedure(ctx context.Context, client clientv1.AutomationServiceClient, domainID, procedureID, procedureJSON string) (string, error) {
	if _, err := client.GetGraphProcedure(ctx, &clientv1.GetGraphProcedureRequest{DomainId: domainID, ProcedureId: procedureID}); err != nil {
		if status.Code(err) == codes.NotFound {
			res, err := client.CreateGraphProcedure(ctx, &clientv1.CreateGraphProcedureRequest{DomainId: domainID, ProcedureJson: procedureJSON})
			if err != nil {
				return "", err
			}
			return res.GetProcedureJson(), nil
		}
		return "", err
	}
	res, err := client.UpdateGraphProcedure(ctx, &clientv1.UpdateGraphProcedureRequest{DomainId: domainID, ProcedureId: procedureID, ProcedureJson: procedureJSON})
	if err != nil {
		return "", err
	}
	return res.GetProcedureJson(), nil
}

func putGraphAutomationBinding(ctx context.Context, client clientv1.AutomationServiceClient, domainID, bindingID, bindingJSON string) (string, error) {
	if _, err := client.GetGraphAutomationBinding(ctx, &clientv1.GetGraphAutomationBindingRequest{DomainId: domainID, BindingId: bindingID}); err != nil {
		if status.Code(err) == codes.NotFound {
			res, err := client.CreateGraphAutomationBinding(ctx, &clientv1.CreateGraphAutomationBindingRequest{DomainId: domainID, BindingJson: bindingJSON})
			if err != nil {
				return "", err
			}
			return res.GetBindingJson(), nil
		}
		return "", err
	}
	res, err := client.UpdateGraphAutomationBinding(ctx, &clientv1.UpdateGraphAutomationBindingRequest{DomainId: domainID, BindingId: bindingID, BindingJson: bindingJSON})
	if err != nil {
		return "", err
	}
	return res.GetBindingJson(), nil
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
