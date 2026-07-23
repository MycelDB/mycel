package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	"github.com/myceldb/mycel/internal/query/gql/execution"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"
)

func NewQueryCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "query", Short: "Run daemon graph queries"}
	cmd.AddCommand(NewQueryNodesCommand(a), NewQueryGQLCommand(a))
	return cmd
}

func NewQueryGQLCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainID, domainKey string
	cmd := &cobra.Command{Use: "gql QUERY", Short: "Execute a GQL query against a daemon graph domain", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		plan, err := gql.Compile(args[0])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainClient := clientv1.NewDomainServiceClient(conn)
		resolvedDomainID, err := resolveDaemonDomainID(domainClient, authCtx, spaceID.String(), domainID, domainKey)
		if err != nil {
			return err
		}
		sessionClient := clientv1.NewSessionServiceClient(conn)
		txClient := clientv1.NewTransactionServiceClient(conn)
		sessionRes, err := sessionClient.OpenSession(authCtx, &clientv1.OpenSessionRequest{SpaceId: spaceID.String(), DomainId: resolvedDomainID})
		if err != nil {
			return err
		}
		sessionID := sessionRes.GetSession().GetSessionId()
		defer func() {
			_, _ = sessionClient.CloseSession(authCtx, &clientv1.CloseSessionRequest{SessionId: sessionID})
		}()
		mode := clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE
		if plan.AccessMode == analysis.ReadOnly {
			mode = clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY
		}
		txRes, err := txClient.BeginTransaction(authCtx, &clientv1.BeginTransactionRequest{SessionId: sessionID, Mode: mode})
		if err != nil {
			return err
		}
		transactionID := txRes.GetTransaction().GetTransactionId()
		committed := false
		defer func() {
			if !committed {
				_, _ = txClient.RollbackTransaction(authCtx, &clientv1.RollbackTransactionRequest{TransactionId: transactionID})
			}
		}()
		result, err := execution.Execute(authCtx, daemonGraphWriter{graphClient: clientv1.NewGraphServiceClient(conn), queryClient: clientv1.NewQueryServiceClient(conn), transactionID: transactionID}, plan)
		if err != nil {
			return err
		}
		if plan.AccessMode == analysis.ReadOnly {
			if _, err := txClient.CloseTransaction(authCtx, &clientv1.CloseTransactionRequest{TransactionId: transactionID}); err != nil {
				return err
			}
			committed = true
			return a.Print(gqlCLIResult{Result: result, TransactionID: transactionID}, fmt.Sprintf("query executed: rows=%d\n", len(result.Rows)))
		}
		commitRes, err := txClient.CommitTransaction(authCtx, &clientv1.CommitTransactionRequest{TransactionId: transactionID})
		if err != nil {
			return err
		}
		committed = true
		return a.Print(gqlCLIResult{Result: result, TransactionID: transactionID, CommittedRevision: commitRes.GetCommit().GetCommittedRevision()}, fmt.Sprintf("query executed: nodes_inserted=%d revision=%d\n", result.Counters.NodesInserted, commitRes.GetCommit().GetCommittedRevision()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	cmd.Flags().StringVar(&domainID, "domain-id", "", "domain ID")
	cmd.Flags().StringVar(&domainKey, "domain", "", "domain key (defaults to the space default domain)")
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
			fmt.Printf("%s\t%s\n", node.GetNodeId(), previewText(node.GetContent(), 120))
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

type gqlCLIResult struct {
	Result            execmodel.Result `json:"result"`
	TransactionID     string           `json:"transaction_id"`
	CommittedRevision int64            `json:"committed_revision"`
}

type daemonGraphWriter struct {
	graphClient   clientv1.GraphServiceClient
	queryClient   clientv1.QueryServiceClient
	transactionID string
}

func (w daemonGraphWriter) InsertNode(ctx context.Context, node execution.InsertNode) (execmodel.NodeRef, error) {
	props := map[string]any{"properties": copyGQLProperties(node.Properties)}
	if len(node.Labels) > 0 {
		labels := make([]any, len(node.Labels))
		for i, label := range node.Labels {
			labels[i] = label
		}
		props["_gql_labels"] = labels
	}
	protoProps, err := structpb.NewStruct(props)
	if err != nil {
		return execmodel.NodeRef{}, err
	}
	res, err := w.graphClient.CreateNode(ctx, &clientv1.CreateNodeRequest{TransactionId: w.transactionID, Node: &clientv1.NodeCreate{Props: protoProps}})
	if err != nil {
		return execmodel.NodeRef{}, err
	}
	return execmodel.NodeRef{ID: res.GetNode().GetNodeId()}, nil
}

func (w daemonGraphWriter) QueryNodes(ctx context.Context, query execution.QueryNodes) ([]execmodel.Node, error) {
	graphQuery, err := buildGQLNodeQuery(query)
	if err != nil {
		return nil, err
	}
	res, err := w.queryClient.ExecuteQuery(ctx, &clientv1.ExecuteQueryRequest{TransactionId: w.transactionID, Query: graphQuery, PageSize: 100})
	if err != nil {
		return nil, err
	}
	nodes := []execmodel.Node{}
	for _, row := range res.GetRows() {
		field := row.GetFields()["node"]
		if field == nil || field.GetNode() == nil {
			continue
		}
		node := field.GetNode()
		props := node.GetProps().AsMap()
		labels := gqlLabelsFromProps(props)
		if !containsAllLabels(labels, query.Labels) {
			continue
		}
		nodes = append(nodes, execmodel.Node{ID: node.GetNodeId(), Labels: labels, Properties: gqlCustomPropertiesFromProps(props)})
	}
	return nodes, nil
}

func buildGQLNodeQuery(query execution.QueryNodes) (*clientv1.GraphQuery, error) {
	exprs := []*clientv1.Expr{}
	for key, value := range query.Properties {
		protoValue, err := structpb.NewValue(value)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: key, Value: protoValue}}})
	}
	var where *clientv1.Expr
	if len(exprs) == 1 {
		where = exprs[0]
	} else if len(exprs) > 1 {
		where = &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: exprs}}}
	}
	return &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}}, Where: where, Returns: []*clientv1.ReturnProjection{{Alias: "n", OutputName: "node", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}}, nil
}

func gqlCustomPropertiesFromProps(props map[string]any) map[string]any {
	custom, ok := props["properties"].(map[string]any)
	if !ok {
		return nil
	}
	return custom
}

func gqlLabelsFromProps(props map[string]any) []string {
	raw, ok := props["_gql_labels"].([]any)
	if !ok {
		return nil
	}
	labels := make([]string, 0, len(raw))
	for _, value := range raw {
		if label, ok := value.(string); ok {
			labels = append(labels, label)
		}
	}
	return labels
}

func containsAllLabels(labels []string, required []string) bool {
	seen := map[string]struct{}{}
	for _, label := range labels {
		seen[label] = struct{}{}
	}
	for _, label := range required {
		if _, ok := seen[label]; !ok {
			return false
		}
	}
	return true
}

func copyGQLProperties(properties map[string]any) map[string]any {
	out := make(map[string]any, len(properties)+1)
	for key, value := range properties {
		out[key] = value
	}
	return out
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
