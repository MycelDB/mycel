package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"
)

func NewQueryCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "query", Short: "Run daemon graph queries"}
	cmd.AddCommand(NewQueryNodesCommand(a))
	return cmd
}

func NewQueryNodesCommand(a *app.App) *cobra.Command {
	var transactionID, templateKey, tag, propertyExists, propertyEquals, pageToken string
	var pageSize, limit int32
	cmd := &cobra.Command{Use: "nodes", Short: "Query nodes in a daemon graph transaction", RunE: func(cmd *cobra.Command, args []string) error {
		query, err := buildNodeQuery(templateKey, tag, propertyExists, propertyEquals, limit)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewQueryServiceClient(conn).ExecuteQuery(authCtx, &clientv1.ExecuteQueryRequest{TransactionId: transactionID, Query: query, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, row := range res.GetRows() {
			field := row.GetFields()["node"]
			if field == nil || field.GetNode() == nil {
				continue
			}
			node := field.GetNode()
			fmt.Printf("%s\t%s\n", node.GetNodeId(), previewText(nodePayloadText(node), 120))
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().StringVar(&templateKey, "template-key", "", "restrict to template key")
	cmd.Flags().StringVar(&tag, "tag", "", "match canonical tag")
	cmd.Flags().StringVar(&propertyExists, "property-exists", "", "match canonical custom property name")
	cmd.Flags().StringVar(&propertyEquals, "property-equals", "", "match custom property equality as name=value")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().Int32Var(&limit, "limit", 0, "query-level limit")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func buildNodeQuery(templateKey, tag, propertyExists, propertyEquals string, limit int32) (*clientv1.GraphQuery, error) {
	start := &clientv1.NodePattern{Alias: "n"}
	if strings.TrimSpace(templateKey) != "" {
		value := strings.TrimSpace(templateKey)
		start.TemplateKey = &value
	}
	exprs := []*clientv1.Expr{}
	if strings.TrimSpace(tag) != "" {
		exprs = append(exprs, &clientv1.Expr{Expr: &clientv1.Expr_HasTag{HasTag: &clientv1.HasTagExpr{Alias: "n", Tag: tag}}})
	}
	if strings.TrimSpace(propertyExists) != "" {
		exprs = append(exprs, &clientv1.Expr{Expr: &clientv1.Expr_PropertyExists{PropertyExists: &clientv1.PropertyExistsExpr{Alias: "n", Name: propertyExists}}})
	}
	if strings.TrimSpace(propertyEquals) != "" {
		name, value, ok := strings.Cut(propertyEquals, "=")
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("--property-equals must be name=value")
		}
		protoValue, err := structpb.NewValue(strings.TrimSpace(value))
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: strings.TrimSpace(name), Value: protoValue}}})
	}
	var where *clientv1.Expr
	if len(exprs) == 1 {
		where = exprs[0]
	} else if len(exprs) > 1 {
		where = &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: exprs}}}
	}
	return &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: start}, Where: where, Returns: []*clientv1.ReturnProjection{{Alias: "n", OutputName: "node", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}, Limit: limit}, nil
}
