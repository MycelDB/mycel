package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
)

func NewMetadataCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "metadata", Short: "Discover transaction-scoped metadata catalogs"}
	cmd.AddCommand(NewMetadataTagsCommand(a), NewMetadataPropertiesCommand(a))
	return cmd
}

func NewMetadataTagsCommand(a *app.App) *cobra.Command {
	var transactionID, pageToken string
	var pageSize int32
	cmd := &cobra.Command{Use: "tags", Short: "List known tags in a transaction read context", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewMetadataCatalogServiceClient(conn).ListTags(authCtx, &clientv1.ListTagsRequest{TransactionId: transactionID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, tag := range res.GetTags() {
			fmt.Printf("%s\t%d\n", tag.GetName(), tag.GetNodeCount())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func NewMetadataPropertiesCommand(a *app.App) *cobra.Command {
	var transactionID, pageToken string
	var pageSize int32
	cmd := &cobra.Command{Use: "properties", Aliases: []string{"props"}, Short: "List known custom property names in a transaction read context", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewMetadataCatalogServiceClient(conn).ListPropertyNames(authCtx, &clientv1.ListPropertyNamesRequest{TransactionId: transactionID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, property := range res.GetProperties() {
			fmt.Printf("%s\t%d\n", property.GetName(), property.GetNodeCount())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}
