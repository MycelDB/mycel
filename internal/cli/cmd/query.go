package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"
)

func NewQueryCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "query", Short: "Run daemon graph queries"}
	cmd.AddCommand(NewQueryNodesCommand(a), NewQueryGQLCommand(a))
	return cmd
}

func NewQueryGQLCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainID, domainKey, paramsJSON string
	var params []string
	var explain bool
	cmd := &cobra.Command{Use: "gql QUERY", Short: "Execute a GQL query against a daemon graph domain", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		parsedParams, err := parseGQLParams(params, paramsJSON)
		if err != nil {
			return err
		}
		return runGQL(cmd.Context(), a, gqlRunOptions{QueryText: args[0], SpaceIDText: spaceIDText, DomainID: domainID, DomainKey: domainKey, Params: parsedParams, Explain: explain})
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	cmd.Flags().StringVar(&domainID, "domain-id", "", "domain ID")
	cmd.Flags().StringVar(&domainKey, "domain", "", "domain key (defaults to the space default domain)")
	cmd.Flags().StringArrayVar(&params, "param", nil, "GQL parameter as name=value; repeatable")
	cmd.Flags().StringVar(&paramsJSON, "params-json", "", "GQL parameters as a JSON object")
	cmd.Flags().BoolVar(&explain, "explain", false, "plan the query and print diagnostics without executing it")
	return cmd
}

type gqlRunOptions struct {
	QueryText     string
	SpaceIDText   string
	DomainID      string
	DomainKey     string
	RequireDomain bool
	Params        map[string]any
	Explain       bool
	Out           io.Writer
}

func runGQL(ctx context.Context, a *app.App, opts gqlRunOptions) error {
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}
	spaceID, err := a.ResolveSpaceID(opts.SpaceIDText)
	if err != nil {
		return err
	}
	domainID := strings.TrimSpace(opts.DomainID)
	domainKey := strings.TrimSpace(opts.DomainKey)
	if domainID == "" && domainKey == "" && strings.TrimSpace(opts.SpaceIDText) == "" && a.CurrentSpaceID != nil && a.CurrentSpaceID.String() == spaceID.String() {
		domainID = a.CurrentDomainID
		domainKey = a.CurrentDomainKey
	}
	plan, err := gql.CompileWithParams(opts.QueryText, opts.Params)
	if err != nil {
		return err
	}
	conn, authCtx, _, err := loginDaemonPrincipal(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	domainClient := clientv1.NewDomainServiceClient(conn)
	resolvedDomainID, err := resolveDaemonDomainID(domainClient, authCtx, spaceID.String(), domainID, domainKey)
	if err != nil {
		if opts.RequireDomain && domainID == "" && domainKey == "" {
			return fmt.Errorf("no domain connected; use connect domain <domain-id-or-key>: %w", err)
		}
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
	queryClient := clientv1.NewQueryServiceClient(conn)
	if opts.Explain {
		explainRes, err := queryClient.ExplainGQL(authCtx, &clientv1.ExplainGQLRequest{TransactionId: transactionID, Query: opts.QueryText, Params: protoParamsFromAny(opts.Params)})
		if err != nil {
			return err
		}
		if _, err := txClient.CloseTransaction(authCtx, &clientv1.CloseTransactionRequest{TransactionId: transactionID}); err != nil {
			return err
		}
		committed = true
		if a.Output == "json" {
			return a.Print(explainRes, "")
		}
		printQueryDiagnostics(out, explainRes.GetDiagnostics())
		return nil
	}
	gqlRes, err := queryClient.ExecuteGQL(authCtx, &clientv1.ExecuteGQLRequest{TransactionId: transactionID, Query: opts.QueryText, Params: protoParamsFromAny(opts.Params)})
	if err != nil {
		return err
	}
	result := execResultFromProto(gqlRes.GetResult(), gqlPlanColumns(plan))
	if plan.AccessMode == analysis.ReadOnly {
		if _, err := txClient.CloseTransaction(authCtx, &clientv1.CloseTransactionRequest{TransactionId: transactionID}); err != nil {
			return err
		}
		committed = true
		if a.Output == "json" {
			return a.Print(gqlCLIResult{Result: result, TransactionID: transactionID}, fmt.Sprintf("query executed: rows=%d\n", len(result.Rows)))
		}
		printGQLRows(out, result)
		fmt.Fprintf(out, "query executed: rows=%d\n", len(result.Rows))
		return nil
	}
	commitRes, err := txClient.CommitTransaction(authCtx, &clientv1.CommitTransactionRequest{TransactionId: transactionID})
	if err != nil {
		return err
	}
	committed = true
	message := fmt.Sprintf("query executed: nodes_inserted=%d edges_inserted=%d nodes_updated=%d nodes_deleted=%d edges_deleted=%d revision=%d\n", result.Counters.NodesInserted, result.Counters.EdgesInserted, result.Counters.NodesUpdated, result.Counters.NodesDeleted, result.Counters.EdgesDeleted, commitRes.GetCommit().GetCommittedRevision())
	if a.Output == "json" {
		return a.Print(gqlCLIResult{Result: result, TransactionID: transactionID, CommittedRevision: commitRes.GetCommit().GetCommittedRevision()}, message)
	}
	printGQLRows(out, result)
	_, err = fmt.Fprint(out, message)
	return err
}

func NewQueryNodesCommand(a *app.App) *cobra.Command {
	var transactionID, tag, propertyExists, propertyEquals, pageToken string
	var labels []string
	var pageSize, limit int32
	cmd := &cobra.Command{Use: "nodes", Short: "Query nodes in a daemon graph transaction", RunE: func(cmd *cobra.Command, args []string) error {
		query, err := buildNodeQuery(labels, tag, propertyExists, propertyEquals, limit)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
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
	cmd.Flags().StringArrayVar(&labels, "label", nil, "restrict to label; repeatable")
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

func parseGQLParams(pairs []string, paramsJSON string) (map[string]any, error) {
	out := map[string]any{}
	if strings.TrimSpace(paramsJSON) != "" {
		if err := json.Unmarshal([]byte(paramsJSON), &out); err != nil {
			return nil, fmt.Errorf("invalid --params-json: %w", err)
		}
	}
	for _, pair := range pairs {
		name, value, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("invalid --param %q; expected name=value", pair)
		}
		out[strings.TrimSpace(name)] = parseGQLParamValue(value)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func parseGQLParamValue(value string) any {
	var decoded any
	if err := json.Unmarshal([]byte(value), &decoded); err == nil {
		return decoded
	}
	return value
}

func protoParamsFromAny(params map[string]any) map[string]*structpb.Value {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]*structpb.Value, len(params))
	for key, value := range params {
		protoValue, err := structpb.NewValue(value)
		if err != nil {
			continue
		}
		out[key] = protoValue
	}
	return out
}

func execResultFromProto(result *clientv1.QueryResult, columns []string) execmodel.Result {
	out := execmodel.Result{Columns: columns}
	if result == nil {
		return out
	}
	counters := result.GetCounters()
	out.Counters = execmodel.Counters{NodesInserted: int(counters.GetNodesInserted()), NodesUpdated: int(counters.GetNodesUpdated()), NodesDeleted: int(counters.GetNodesDeleted()), EdgesInserted: int(counters.GetEdgesInserted()), EdgesDeleted: int(counters.GetEdgesDeleted())}
	for _, row := range result.GetRows() {
		execRow := execmodel.Row{}
		for name, value := range row.GetFields() {
			execRow[name] = execValueFromProto(value)
		}
		out.Rows = append(out.Rows, execRow)
	}
	return out
}

func execValueFromProto(value *clientv1.QueryValue) execmodel.Value {
	if value == nil {
		return execmodel.Value{}
	}
	if node := value.GetNode(); node != nil {
		n := execmodel.Node{ID: node.GetNodeId(), DomainID: node.GetDomainId(), Labels: append([]string(nil), node.GetLabels()...), Properties: structMap(node.GetProperties()), Payload: structMap(node.GetPayload()), Meta: structMap(node.GetMeta())}
		return execmodel.Value{Node: &n}
	}
	if edge := value.GetEdge(); edge != nil {
		e := execmodel.Edge{ID: edge.GetEdgeId(), DomainID: edge.GetDomainId(), FromID: edge.GetFromNodeId(), ToID: edge.GetToNodeId(), Labels: append([]string(nil), edge.GetLabels()...), Properties: structMap(edge.GetProperties()), Payload: structMap(edge.GetPayload()), Meta: structMap(edge.GetMeta())}
		return execmodel.Value{Edge: &e}
	}
	if scalar := value.GetScalar(); scalar != nil {
		return execmodel.Value{Scalar: scalar.AsInterface()}
	}
	return execmodel.Value{}
}

func structMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	return value.AsMap()
}

func printQueryDiagnostics(out io.Writer, diag *clientv1.QueryDiagnostics) {
	if diag == nil {
		fmt.Fprintln(out, "plan: <none>")
		return
	}
	fmt.Fprintf(out, "planner: %s %s\n", diag.GetPlanner(), diag.GetPlannerVersion())
	fmt.Fprintf(out, "plan: %s (%s)\n", diag.GetPlan(), diag.GetPlanKind())
	if diag.GetRejectedReason() != "" {
		fmt.Fprintf(out, "rejected: %s\n", diag.GetRejectedReason())
	}
	if len(diag.GetIndexes()) > 0 {
		fmt.Fprintf(out, "indexes: %s\n", strings.Join(diag.GetIndexes(), ", "))
	}
	if len(diag.GetPushedPredicates()) > 0 {
		fmt.Fprintf(out, "pushed predicates: %s\n", strings.Join(diag.GetPushedPredicates(), ", "))
	}
	if len(diag.GetResidualPredicates()) > 0 {
		fmt.Fprintf(out, "residual predicates: %s\n", strings.Join(diag.GetResidualPredicates(), ", "))
	}
	fmt.Fprintf(out, "full scan: %t\n", diag.GetFullScan())
	if diag.GetFallbackMode() != "" {
		fmt.Fprintf(out, "fallback: %s\n", diag.GetFallbackMode())
	}
	fmt.Fprintf(out, "rows: scanned=%d produced=%d returned=%d\n", diag.GetRowsScanned(), diag.GetRowsProduced(), diag.GetRowsReturned())
	fmt.Fprintf(out, "loaded: candidates=%d index_entries=%d nodes=%d edges=%d\n", diag.GetCandidateCount(), diag.GetIndexEntriesScanned(), diag.GetNodesLoaded(), diag.GetEdgesLoaded())
	if diag.GetTruncated() || diag.GetTruncationReason() != "" {
		fmt.Fprintf(out, "truncated: %t %s\n", diag.GetTruncated(), diag.GetTruncationReason())
	}
}

func gqlPlanColumns(plan planmodel.Plan) []string {
	var columns []string
	for _, op := range plan.Operations {
		switch op := op.(type) {
		case planmodel.QueryNodesOperation:
			columns = appendReturnColumns(columns, op.Returns)
		case planmodel.QueryPatternOperation:
			columns = appendReturnColumns(columns, op.Returns)
		case planmodel.QueryPathOperation:
			columns = appendReturnColumns(columns, op.Returns)
		case planmodel.MatchSetOperation:
			columns = appendReturnColumns(columns, op.Returns)
		case planmodel.MatchDeleteOperation:
			columns = appendReturnColumns(columns, op.Returns)
		case planmodel.MergeNodeOperation:
			columns = appendReturnColumns(columns, op.Returns)
		case planmodel.MatchMergeRelationshipOperation:
			columns = appendReturnColumns(columns, op.Returns)
		}
	}
	return columns
}

func appendReturnColumns(columns []string, returns []planmodel.ReturnItem) []string {
	for _, ret := range returns {
		columns = append(columns, gqlReturnColumn(ret))
	}
	return columns
}

func gqlReturnColumn(ret planmodel.ReturnItem) string {
	if ret.OutputName != "" {
		return ret.OutputName
	}
	if ret.Kind == planmodel.ReturnProperty {
		if ret.Namespace != "" {
			return ret.Variable + "." + ret.Namespace + "." + ret.Property
		}
		return ret.Variable + "." + ret.Property
	}
	return ret.Variable
}

func printGQLRows(out io.Writer, result execmodel.Result) {
	for _, row := range result.Rows {
		columns := result.Columns
		if len(columns) == 0 {
			columns = make([]string, 0, len(row))
			for column := range row {
				columns = append(columns, column)
			}
		}
		parts := make([]string, 0, len(columns))
		for _, column := range columns {
			value, ok := row[column]
			if !ok {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%s", column, formatGQLValue(value)))
		}
		fmt.Fprintln(out, strings.Join(parts, "\t"))
	}
}

func formatGQLValue(value execmodel.Value) string {
	if value.Node != nil {
		encoded, err := json.Marshal(value.Node)
		if err != nil {
			return "<unprintable>"
		}
		return string(encoded)
	}
	if value.Edge != nil {
		encoded, err := json.Marshal(value.Edge)
		if err != nil {
			return "<unprintable>"
		}
		return string(encoded)
	}
	if value.Path != nil {
		encoded, err := json.Marshal(value.Path)
		if err != nil {
			return "<unprintable>"
		}
		return string(encoded)
	}
	encoded, err := json.Marshal(value.Scalar)
	if err != nil {
		return "<unprintable>"
	}
	return string(encoded)
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

func buildNodeQuery(labels []string, tag, propertyExists, propertyEquals string, limit int32) (*clientv1.GraphQuery, error) {
	start := &clientv1.NodePattern{Alias: "n", Labels: append([]string(nil), labels...)}
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
