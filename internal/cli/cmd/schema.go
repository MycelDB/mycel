package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/schema/dsl"
	"github.com/spf13/cobra"
)

func NewSchemaCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "schema", Short: "Manage domain schemas"}
	cmd.AddCommand(newSchemaGetCommand(a), newSchemaPutCommand(a), newSchemaDeleteCommand(a), newSchemaValidateCommand(a), newSchemaCompileCommand(a))
	return cmd
}

func newSchemaGetCommand(a *app.App) *cobra.Command {
	var domainID string
	cmd := &cobra.Command{Use: "get", Short: "Get a domain schema", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSchemaServiceClient(conn).GetDomainSchema(authCtx, &clientv1.GetDomainSchemaRequest{DomainId: domainID})
		if err != nil {
			return err
		}
		fmt.Println(res.GetGwl())
		return nil
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}

func newSchemaPutCommand(a *app.App) *cobra.Command {
	var domainID string
	var format string
	cmd := &cobra.Command{Use: "put schema.gwl", Short: "Put a domain schema GWL document", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := readSchemaSource(args[0], format)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSchemaServiceClient(conn).PutDomainSchema(authCtx, &clientv1.PutDomainSchemaRequest{DomainId: domainID, Gwl: string(data)})
		if err != nil {
			return err
		}
		fmt.Println(res.GetGwl())
		return nil
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	cmd.Flags().StringVar(&format, "format", "auto", "schema input format: auto, gwl")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}

func newSchemaDeleteCommand(a *app.App) *cobra.Command {
	var domainID string
	cmd := &cobra.Command{Use: "delete", Short: "Delete a domain schema", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = clientv1.NewSchemaServiceClient(conn).DeleteDomainSchema(authCtx, &clientv1.DeleteDomainSchemaRequest{DomainId: domainID})
		return err
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}

func newSchemaValidateCommand(a *app.App) *cobra.Command {
	var format string
	cmd := &cobra.Command{Use: "validate schema.gwl", Short: "Validate a schema GWL document", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := readSchemaSource(args[0], format)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSchemaServiceClient(conn).ValidateSchema(authCtx, &clientv1.ValidateSchemaRequest{Gwl: string(data)})
		if err != nil {
			return err
		}
		if res.GetValid() {
			fmt.Println("schema valid")
			return nil
		}
		for _, issue := range res.GetIssues() {
			fmt.Fprintf(os.Stderr, "%s: %s %s\n", issue.GetSeverity(), issue.GetPath(), issue.GetMessage())
		}
		return fmt.Errorf("schema invalid")
	}}
	cmd.Flags().StringVar(&format, "format", "auto", "schema input format: auto, gwl")
	return cmd
}

func newSchemaCompileCommand(a *app.App) *cobra.Command {
	var format string
	cmd := &cobra.Command{Use: "compile schema.gwl", Short: "Compile a GWL schema document to canonical JSON", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := readSchemaInput(args[0], format)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}}
	cmd.Flags().StringVar(&format, "format", "auto", "schema input format: auto, gwl")
	return cmd
}

func readSchemaSource(path string, format string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	selected := strings.ToLower(strings.TrimSpace(format))
	if selected == "" || selected == "auto" {
		selected = "gwl"
	}
	if selected != "gwl" {
		return nil, fmt.Errorf("unsupported schema format %q", format)
	}
	if _, err := dsl.Parse(string(data)); err != nil {
		return nil, err
	}
	return data, nil
}

func readSchemaInput(path string, format string) ([]byte, error) {
	data, err := readSchemaSource(path, format)
	if err != nil {
		return nil, err
	}
	parsed, err := dsl.Parse(string(data))
	if err != nil {
		return nil, err
	}
	out, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return nil, err
	}
	return out, nil
}
