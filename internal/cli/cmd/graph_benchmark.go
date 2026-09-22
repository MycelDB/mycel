package cmd

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	graphBenchmarkDefaultSmall  = 25
	graphBenchmarkDefaultMedium = 250
	graphBenchmarkDefaultLarge  = 2500
)

type graphWriteBenchmarkOptions struct {
	SpaceIDText     string
	DomainID        string
	DomainKey       string
	GraphSize       string
	SeedNodes       int
	Operations      int
	BatchSize       int
	Operation       string
	Template        string
	ApplyOperations bool
	Cleanup         bool
	PageSize        int32
}

type graphWriteBenchmarkReport struct {
	RunID           string                    `json:"run_id"`
	SpaceID         string                    `json:"space_id"`
	DomainID        string                    `json:"domain_id"`
	GraphSize       string                    `json:"graph_size"`
	SeedTarget      int                       `json:"seed_target"`
	ExistingNodes   int                       `json:"existing_nodes"`
	SeedCreated     int                       `json:"seed_created"`
	SeedDurationMS  float64                   `json:"seed_duration_ms"`
	Operation       string                    `json:"operation"`
	Template        string                    `json:"template"`
	Operations      int                       `json:"operations"`
	BatchSize       int                       `json:"batch_size"`
	ApplyOperations bool                      `json:"apply_operations"`
	StartedAt       time.Time                 `json:"started_at"`
	CompletedAt     time.Time                 `json:"completed_at"`
	DurationMS      float64                   `json:"duration_ms"`
	Summaries       map[string]latencySummary `json:"summaries"`
}

type latencySummary struct {
	Count int     `json:"count"`
	MinMS float64 `json:"min_ms"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
	AvgMS float64 `json:"avg_ms"`
}

type benchmarkSamples struct {
	begin   []time.Duration
	graph   []time.Duration
	commit  []time.Duration
	total   []time.Duration
	cleanup []time.Duration
}

func newGraphBenchmarkCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "benchmark", Short: "Run graph performance benchmark and smoke workloads"}
	cmd.AddCommand(newGraphBenchmarkWritesCommand(a))
	return cmd
}

func newGraphBenchmarkWritesCommand(a *app.App) *cobra.Command {
	opts := graphWriteBenchmarkOptions{GraphSize: "small", Operations: 20, BatchSize: 1, Operation: "create", Template: "minimal", PageSize: 500}
	cmd := &cobra.Command{
		Use:   "writes",
		Short: "Benchmark graph create/update write latency through daemon APIs",
		Long: strings.TrimSpace(`Benchmark graph write latency using normal daemon gRPC APIs.

The command can seed a domain to a target size, then measure CreateNode,
UpdateNode, create+edge, or mixed workloads. Use --batch-size 1 for one
transaction per operation, --batch-size >1 for multiple graph RPCs in one
transaction, or --apply-operations with --batch-size >1 to use the
ApplyGraphOperations RPC.`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGraphWriteBenchmark(cmd.Context(), a, opts)
		},
	}
	cmd.Flags().StringVar(&opts.SpaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&opts.DomainID, "domain-id", "", "domain ID")
	cmd.Flags().StringVar(&opts.DomainKey, "domain", "", "domain key (defaults to the space default domain)")
	cmd.Flags().StringVar(&opts.GraphSize, "graph-size", opts.GraphSize, "target graph size class: small, medium, large, or custom")
	cmd.Flags().IntVar(&opts.SeedNodes, "seed-nodes", 0, "target seed node count; overrides --graph-size when positive")
	cmd.Flags().IntVar(&opts.Operations, "operations", opts.Operations, "measured operation count")
	cmd.Flags().IntVar(&opts.BatchSize, "batch-size", opts.BatchSize, "operations per transaction")
	cmd.Flags().StringVar(&opts.Operation, "operation", opts.Operation, "workload: create, update, create-edge, or mixed")
	cmd.Flags().StringVar(&opts.Template, "template", opts.Template, "node shape: minimal, properties, content, commonfolio-session, or audit")
	cmd.Flags().BoolVar(&opts.ApplyOperations, "apply-operations", opts.ApplyOperations, "use ApplyGraphOperations for create/update batches")
	cmd.Flags().BoolVar(&opts.Cleanup, "cleanup", opts.Cleanup, "delete nodes created by measured create/create-edge workloads after measurement")
	cmd.Flags().Int32Var(&opts.PageSize, "page-size", opts.PageSize, "page size used when listing seed/update nodes")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func runGraphWriteBenchmark(ctx context.Context, a *app.App, opts graphWriteBenchmarkOptions) error {
	if opts.Operations <= 0 {
		return fmt.Errorf("--operations must be positive")
	}
	if opts.BatchSize <= 0 {
		return fmt.Errorf("--batch-size must be positive")
	}
	operation := strings.ToLower(strings.TrimSpace(opts.Operation))
	switch operation {
	case "create", "update", "create-edge", "mixed":
	default:
		return fmt.Errorf("unsupported --operation %q", opts.Operation)
	}
	if _, err := benchmarkNodeShape(opts.Template, "validate", 0, ""); err != nil {
		return err
	}
	spaceID, err := app.ParseUUID[domainspace.SpaceID](opts.SpaceIDText)
	if err != nil {
		return err
	}
	seedTarget, err := graphBenchmarkSeedTarget(opts.GraphSize, opts.SeedNodes)
	if err != nil {
		return err
	}
	conn, authCtx, _, err := loginDaemonPrincipal(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	domainID, err := resolveDaemonDomainID(clientv1.NewDomainServiceClient(conn), authCtx, spaceID.String(), opts.DomainID, opts.DomainKey)
	if err != nil {
		return err
	}
	runner := graphWriteBenchmarkRunner{conn: conn, ctx: authCtx, spaceID: spaceID.String(), domainID: domainID, pageSize: opts.PageSize}
	runID := "graph-writebench-" + time.Now().UTC().Format("20060102T150405Z") + "-" + uuid.NewString()[:8]
	started := time.Now().UTC()
	existing, err := runner.listNodes()
	if err != nil {
		return err
	}
	seedStart := time.Now()
	seedCreated := 0
	if len(existing) < seedTarget {
		created, err := runner.seedNodes(seedTarget-len(existing), opts.Template, runID)
		if err != nil {
			return err
		}
		seedCreated = len(created)
		existing = append(existing, created...)
	}
	if operation == "update" || operation == "mixed" || operation == "create-edge" {
		if len(existing) == 0 {
			created, err := runner.seedNodes(1, opts.Template, runID)
			if err != nil {
				return err
			}
			seedCreated += len(created)
			existing = append(existing, created...)
		}
	}
	samples := &benchmarkSamples{}
	createdMeasured := []string{}
	if err := runner.runMeasured(operation, opts.Template, opts.Operations, opts.BatchSize, opts.ApplyOperations, runID, existing, &createdMeasured, samples); err != nil {
		return err
	}
	if opts.Cleanup && len(createdMeasured) > 0 {
		if err := runner.cleanupNodes(createdMeasured, samples); err != nil {
			return err
		}
	}
	completed := time.Now().UTC()
	report := graphWriteBenchmarkReport{
		RunID:           runID,
		SpaceID:         spaceID.String(),
		DomainID:        domainID,
		GraphSize:       strings.ToLower(strings.TrimSpace(opts.GraphSize)),
		SeedTarget:      seedTarget,
		ExistingNodes:   len(existing) - seedCreated,
		SeedCreated:     seedCreated,
		SeedDurationMS:  durationMS(time.Since(seedStart)),
		Operation:       operation,
		Template:        strings.ToLower(strings.TrimSpace(opts.Template)),
		Operations:      opts.Operations,
		BatchSize:       opts.BatchSize,
		ApplyOperations: opts.ApplyOperations,
		StartedAt:       started,
		CompletedAt:     completed,
		DurationMS:      durationMS(completed.Sub(started)),
		Summaries: map[string]latencySummary{
			"transaction_begin":  summarizeDurations(samples.begin),
			"graph_operation":    summarizeDurations(samples.graph),
			"transaction_commit": summarizeDurations(samples.commit),
			"transaction_total":  summarizeDurations(samples.total),
		},
	}
	if len(samples.cleanup) > 0 {
		report.Summaries["cleanup_delete"] = summarizeDurations(samples.cleanup)
	}
	return printGraphWriteBenchmarkReport(a, report)
}

type graphWriteBenchmarkRunner struct {
	conn     *grpc.ClientConn
	ctx      context.Context
	spaceID  string
	domainID string
	pageSize int32
}

func (r graphWriteBenchmarkRunner) sessionClient() clientv1.SessionServiceClient {
	return clientv1.NewSessionServiceClient(r.conn)
}
func (r graphWriteBenchmarkRunner) txClient() clientv1.TransactionServiceClient {
	return clientv1.NewTransactionServiceClient(r.conn)
}
func (r graphWriteBenchmarkRunner) graphClient() clientv1.GraphServiceClient {
	return clientv1.NewGraphServiceClient(r.conn)
}

func (r graphWriteBenchmarkRunner) openSession() (string, error) {
	res, err := r.sessionClient().OpenSession(r.ctx, &clientv1.OpenSessionRequest{SpaceId: r.spaceID, DomainId: r.domainID})
	if err != nil {
		return "", err
	}
	return res.GetSession().GetSessionId(), nil
}

func (r graphWriteBenchmarkRunner) closeSession(sessionID string) {
	if strings.TrimSpace(sessionID) != "" {
		_, _ = r.sessionClient().CloseSession(r.ctx, &clientv1.CloseSessionRequest{SessionId: sessionID})
	}
}

func (r graphWriteBenchmarkRunner) beginTransaction(sessionID string, samples *benchmarkSamples) (string, error) {
	start := time.Now()
	res, err := r.txClient().BeginTransaction(r.ctx, &clientv1.BeginTransactionRequest{SessionId: sessionID, Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if samples != nil {
		samples.begin = append(samples.begin, time.Since(start))
	}
	if err != nil {
		return "", err
	}
	return res.GetTransaction().GetTransactionId(), nil
}

func (r graphWriteBenchmarkRunner) commitTransaction(txID string, samples *benchmarkSamples) error {
	start := time.Now()
	_, err := r.txClient().CommitTransaction(r.ctx, &clientv1.CommitTransactionRequest{TransactionId: txID})
	if samples != nil {
		samples.commit = append(samples.commit, time.Since(start))
	}
	return err
}

func (r graphWriteBenchmarkRunner) rollbackTransaction(txID string) {
	if strings.TrimSpace(txID) != "" {
		_, _ = r.txClient().RollbackTransaction(r.ctx, &clientv1.RollbackTransactionRequest{TransactionId: txID})
	}
}

func (r graphWriteBenchmarkRunner) listNodes() ([]string, error) {
	sessionID, err := r.openSession()
	if err != nil {
		return nil, err
	}
	defer r.closeSession(sessionID)
	txRes, err := r.txClient().BeginTransaction(r.ctx, &clientv1.BeginTransactionRequest{SessionId: sessionID, Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY})
	if err != nil {
		return nil, err
	}
	txID := txRes.GetTransaction().GetTransactionId()
	defer r.rollbackTransaction(txID)
	pageSize := r.pageSize
	if pageSize <= 0 {
		pageSize = 500
	}
	ids := []string{}
	pageToken := ""
	for {
		res, err := r.graphClient().ListNodes(r.ctx, &clientv1.ListNodesRequest{TransactionId: txID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return nil, err
		}
		for _, node := range res.GetNodes() {
			ids = append(ids, node.GetNodeId())
		}
		pageToken = res.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return ids, nil
}

func (r graphWriteBenchmarkRunner) seedNodes(count int, template string, runID string) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}
	created := []string{}
	batchSize := 100
	for len(created) < count {
		remaining := count - len(created)
		batch := batchSize
		if remaining < batch {
			batch = remaining
		}
		sessionID, err := r.openSession()
		if err != nil {
			return nil, err
		}
		txID, err := r.beginTransaction(sessionID, nil)
		if err != nil {
			r.closeSession(sessionID)
			return nil, err
		}
		ok := false
		for i := 0; i < batch; i++ {
			node, err := benchmarkNodeShape(template, runID, len(created)+i, "seed")
			if err != nil {
				r.rollbackTransaction(txID)
				r.closeSession(sessionID)
				return nil, err
			}
			res, err := r.graphClient().CreateNode(r.ctx, &clientv1.CreateNodeRequest{TransactionId: txID, Node: node})
			if err != nil {
				r.rollbackTransaction(txID)
				r.closeSession(sessionID)
				return nil, err
			}
			created = append(created, res.GetNode().GetNodeId())
		}
		if err := r.commitTransaction(txID, nil); err != nil {
			r.closeSession(sessionID)
			return nil, err
		}
		ok = true
		r.closeSession(sessionID)
		if !ok {
			break
		}
	}
	return created, nil
}

func (r graphWriteBenchmarkRunner) runMeasured(operation, template string, operations, batchSize int, apply bool, runID string, updatePool []string, createdMeasured *[]string, samples *benchmarkSamples) error {
	completed := 0
	for completed < operations {
		batch := batchSize
		if remaining := operations - completed; remaining < batch {
			batch = remaining
		}
		startTotal := time.Now()
		sessionID, err := r.openSession()
		if err != nil {
			return err
		}
		txID, err := r.beginTransaction(sessionID, samples)
		if err != nil {
			r.closeSession(sessionID)
			return err
		}
		batchCreated, err := r.runMeasuredBatch(txID, operation, template, completed, batch, apply, runID, updatePool, samples)
		if err != nil {
			r.rollbackTransaction(txID)
			r.closeSession(sessionID)
			return err
		}
		if err := r.commitTransaction(txID, samples); err != nil {
			r.closeSession(sessionID)
			return err
		}
		samples.total = append(samples.total, time.Since(startTotal))
		*createdMeasured = append(*createdMeasured, batchCreated...)
		updatePool = append(updatePool, batchCreated...)
		completed += batch
		r.closeSession(sessionID)
	}
	return nil
}

func (r graphWriteBenchmarkRunner) runMeasuredBatch(txID, operation, template string, offset, count int, apply bool, runID string, updatePool []string, samples *benchmarkSamples) ([]string, error) {
	if apply && (operation == "create" || operation == "update") {
		return r.runApplyOperationsBatch(txID, operation, template, offset, count, runID, updatePool, samples)
	}
	created := []string{}
	for i := 0; i < count; i++ {
		idx := offset + i
		op := operation
		if operation == "mixed" {
			if idx%2 == 0 {
				op = "create"
			} else {
				op = "update"
			}
		}
		start := time.Now()
		switch op {
		case "create":
			node, err := benchmarkNodeShape(template, runID, idx, "create")
			if err != nil {
				return nil, err
			}
			res, err := r.graphClient().CreateNode(r.ctx, &clientv1.CreateNodeRequest{TransactionId: txID, Node: node})
			if err != nil {
				return nil, err
			}
			created = append(created, res.GetNode().GetNodeId())
		case "update":
			if len(updatePool) == 0 {
				return nil, fmt.Errorf("update workload requires at least one existing node")
			}
			nodeID := updatePool[idx%len(updatePool)]
			if _, err := r.graphClient().UpdateNode(r.ctx, benchmarkUpdateRequest(txID, nodeID, template, runID, idx)); err != nil {
				return nil, err
			}
		case "create-edge":
			if len(updatePool) == 0 {
				return nil, fmt.Errorf("create-edge workload requires at least one existing parent node")
			}
			childID := uuid.NewString()
			node, err := benchmarkNodeShape(template, runID, idx, "edge-child")
			if err != nil {
				return nil, err
			}
			node.NodeId = &childID
			if _, err := r.graphClient().CreateNode(r.ctx, &clientv1.CreateNodeRequest{TransactionId: txID, Node: node}); err != nil {
				return nil, err
			}
			created = append(created, childID)
			_, err = r.graphClient().CreateEdge(r.ctx, &clientv1.CreateEdgeRequest{TransactionId: txID, Edge: &clientv1.EdgeCreate{FromNodeId: updatePool[0], ToNodeId: childID, Labels: []string{"contains"}, Properties: protoStruct(map[string]any{"benchmark_run_id": runID, "benchmark_index": idx})}})
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported operation %q", op)
		}
		samples.graph = append(samples.graph, time.Since(start))
	}
	return created, nil
}

func (r graphWriteBenchmarkRunner) runApplyOperationsBatch(txID, operation, template string, offset, count int, runID string, updatePool []string, samples *benchmarkSamples) ([]string, error) {
	ops := make([]*clientv1.GraphOperation, 0, count)
	created := []string{}
	for i := 0; i < count; i++ {
		idx := offset + i
		switch operation {
		case "create":
			node, err := benchmarkNodeShape(template, runID, idx, "apply-create")
			if err != nil {
				return nil, err
			}
			id := uuid.NewString()
			node.NodeId = &id
			created = append(created, id)
			ops = append(ops, &clientv1.GraphOperation{Operation: &clientv1.GraphOperation_CreateNode{CreateNode: node}})
		case "update":
			if len(updatePool) == 0 {
				return nil, fmt.Errorf("update workload requires at least one existing node")
			}
			nodeID := updatePool[idx%len(updatePool)]
			update := benchmarkUpdateRequest(txID, nodeID, template, runID, idx)
			ops = append(ops, &clientv1.GraphOperation{Operation: &clientv1.GraphOperation_UpdateNode{UpdateNode: &clientv1.NodeUpdate{Node: update.Node, UpdateMask: update.UpdateMask}}})
		}
	}
	start := time.Now()
	_, err := r.graphClient().ApplyGraphOperations(r.ctx, &clientv1.ApplyGraphOperationsRequest{TransactionId: txID, Operations: ops})
	samples.graph = append(samples.graph, time.Since(start))
	if err != nil {
		return nil, err
	}
	return created, nil
}

func (r graphWriteBenchmarkRunner) cleanupNodes(nodeIDs []string, samples *benchmarkSamples) error {
	for _, nodeID := range nodeIDs {
		sessionID, err := r.openSession()
		if err != nil {
			return err
		}
		txID, err := r.beginTransaction(sessionID, nil)
		if err != nil {
			r.closeSession(sessionID)
			return err
		}
		start := time.Now()
		_, err = r.graphClient().DeleteNode(r.ctx, &clientv1.DeleteNodeRequest{TransactionId: txID, NodeId: nodeID, Recursive: true})
		samples.cleanup = append(samples.cleanup, time.Since(start))
		if err != nil {
			r.rollbackTransaction(txID)
			r.closeSession(sessionID)
			return err
		}
		if err := r.commitTransaction(txID, nil); err != nil {
			r.closeSession(sessionID)
			return err
		}
		r.closeSession(sessionID)
	}
	return nil
}

func benchmarkNodeShape(template, runID string, index int, phase string) (*clientv1.NodeCreate, error) {
	template = strings.ToLower(strings.TrimSpace(template))
	if template == "" {
		template = "minimal"
	}
	baseProps := map[string]any{"benchmark_run_id": runID, "benchmark_index": index, "benchmark_phase": phase}
	switch template {
	case "minimal":
		return &clientv1.NodeCreate{Labels: []string{"BenchmarkNode"}, Payload: protoStruct(map[string]any{"text": fmt.Sprintf("benchmark node %d", index)}), Properties: protoStruct(baseProps)}, nil
	case "properties":
		props := copyMapAny(baseProps)
		props["status"] = "active"
		props["ordinal"] = index
		props["bucket"] = index % 10
		props["search_key"] = fmt.Sprintf("bench-%06d", index)
		return &clientv1.NodeCreate{Labels: []string{"BenchmarkNode", "BenchmarkProperties"}, Payload: protoStruct(map[string]any{"text": fmt.Sprintf("benchmark properties node %d", index)}), Properties: protoStruct(props), Meta: protoStruct(map[string]any{"template": template})}, nil
	case "content":
		text := strings.Repeat(fmt.Sprintf("benchmark content node %d ", index), 20)
		return &clientv1.NodeCreate{Labels: []string{"BenchmarkNode", "BenchmarkContent"}, Payload: protoStruct(map[string]any{"text": text}), Properties: protoStruct(baseProps), Meta: protoStruct(map[string]any{"template": template})}, nil
	case "commonfolio-session":
		props := copyMapAny(baseProps)
		props["session_id"] = uuid.NewString()
		props["principal_id"] = "benchmark-principal"
		props["token_hash"] = fmt.Sprintf("bench-token-%06d", index)
		props["state"] = "active"
		props["expires_at"] = time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
		return &clientv1.NodeCreate{Labels: []string{"pkm.browser_session"}, Payload: protoStruct(map[string]any{"text": "benchmark browser session"}), Properties: protoStruct(props), Meta: protoStruct(map[string]any{"template": template})}, nil
	case "audit":
		props := copyMapAny(baseProps)
		props["event_type"] = "auth.login_success"
		props["principal_id"] = "benchmark-principal"
		props["occurred_at"] = time.Now().UTC().Format(time.RFC3339)
		return &clientv1.NodeCreate{Labels: []string{"pkm.auth_audit_event"}, Payload: protoStruct(map[string]any{"text": "benchmark auth audit event"}), Properties: protoStruct(props), Meta: protoStruct(map[string]any{"template": template})}, nil
	default:
		return nil, fmt.Errorf("unsupported --template %q", template)
	}
}

func benchmarkUpdateRequest(txID, nodeID, template, runID string, index int) *clientv1.UpdateNodeRequest {
	props := map[string]any{"benchmark_run_id": runID, "benchmark_update_index": index, "updated_at": time.Now().UTC().Format(time.RFC3339)}
	payload := map[string]any{"text": fmt.Sprintf("updated benchmark node %d", index)}
	if strings.EqualFold(template, "commonfolio-session") {
		props["state"] = "rotated"
		props["token_hash"] = fmt.Sprintf("rotated-token-%06d", index)
	}
	if strings.EqualFold(template, "audit") {
		props["event_type"] = "auth.refresh"
	}
	return &clientv1.UpdateNodeRequest{TransactionId: txID, Node: &clientv1.Node{NodeId: nodeID, Properties: protoStruct(props), Payload: protoStruct(payload), Meta: protoStruct(map[string]any{"benchmark_template": template})}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"properties", "payload", "meta"}}}
}

func graphBenchmarkSeedTarget(size string, override int) (int, error) {
	if override > 0 {
		return override, nil
	}
	switch strings.ToLower(strings.TrimSpace(size)) {
	case "", "small":
		return graphBenchmarkDefaultSmall, nil
	case "medium":
		return graphBenchmarkDefaultMedium, nil
	case "large":
		return graphBenchmarkDefaultLarge, nil
	case "custom":
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported --graph-size %q", size)
	}
}

func summarizeDurations(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, value := range sorted {
		sum += value
	}
	return latencySummary{Count: len(sorted), MinMS: durationMS(sorted[0]), P50MS: percentileDurationMS(sorted, 0.50), P95MS: percentileDurationMS(sorted, 0.95), P99MS: percentileDurationMS(sorted, 0.99), MaxMS: durationMS(sorted[len(sorted)-1]), AvgMS: durationMS(sum / time.Duration(len(sorted)))}
}

func percentileDurationMS(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return durationMS(sorted[0])
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return durationMS(sorted[idx])
}

func durationMS(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func printGraphWriteBenchmarkReport(a *app.App, report graphWriteBenchmarkReport) error {
	if a.Output == "json" {
		return a.Print(report, "")
	}
	fmt.Printf("graph write benchmark: %s\n", report.RunID)
	fmt.Printf("space=%s domain=%s operation=%s template=%s operations=%d batch_size=%d apply_operations=%t\n", report.SpaceID, report.DomainID, report.Operation, report.Template, report.Operations, report.BatchSize, report.ApplyOperations)
	fmt.Printf("graph_size=%s seed_target=%d existing_nodes=%d seed_created=%d seed_duration_ms=%.3f duration_ms=%.3f\n", report.GraphSize, report.SeedTarget, report.ExistingNodes, report.SeedCreated, report.SeedDurationMS, report.DurationMS)
	keys := []string{"transaction_begin", "graph_operation", "transaction_commit", "transaction_total", "cleanup_delete"}
	for _, key := range keys {
		summary, ok := report.Summaries[key]
		if !ok || summary.Count == 0 {
			continue
		}
		fmt.Printf("%s count=%d min=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms avg=%.3fms\n", key, summary.Count, summary.MinMS, summary.P50MS, summary.P95MS, summary.P99MS, summary.MaxMS, summary.AvgMS)
	}
	return nil
}

func copyMapAny(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
