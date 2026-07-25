package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
)

func NewSchemaCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "schema", Short: "Manage domain schemas"}
	cmd.AddCommand(newSchemaGetCommand(a), newSchemaPutCommand(a), newSchemaValidateCommand(a))
	return cmd
}

func newSchemaGetCommand(a *app.App) *cobra.Command {
	var domainID string
	cmd := &cobra.Command{Use: "get", Short: "Get a domain schema", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSchemaServiceClient(conn).GetDomainSchema(authCtx, &clientv1.GetDomainSchemaRequest{DomainId: domainID})
		if err != nil {
			return err
		}
		fmt.Println(res.GetSchemaJson())
		return nil
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}

func newSchemaPutCommand(a *app.App) *cobra.Command {
	var domainID string
	cmd := &cobra.Command{Use: "put schema.json", Short: "Put a domain schema JSON document", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSchemaServiceClient(conn).PutDomainSchema(authCtx, &clientv1.PutDomainSchemaRequest{DomainId: domainID, SchemaJson: string(data)})
		if err != nil {
			return err
		}
		fmt.Println(res.GetSchemaJson())
		return nil
	}}
	cmd.Flags().StringVar(&domainID, "domain", "", "domain UUID")
	_ = cmd.MarkFlagRequired("domain")
	return cmd
}

func newSchemaValidateCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "validate schema.json", Short: "Validate a schema JSON document", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(context.Background(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSchemaServiceClient(conn).ValidateSchema(authCtx, &clientv1.ValidateSchemaRequest{SchemaJson: string(data)})
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
	return cmd
}
