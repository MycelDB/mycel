package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	"github.com/myceldb/mycel/internal/query/gql"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

var (
	reconciliationMatchRE = regexp.MustCompile(`(?i)\bMATCH\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?::\s*([A-Za-z_][A-Za-z0-9_]*))?`)
	reconciliationLimitRE = regexp.MustCompile(`(?i)\bFETCH\s+FIRST\s+([0-9]+)\s+ROWS?\s+ONLY\b`)
)

const (
	reconciliationEventPrefix = "reconciliation:"
	reconciliationFreshReason = "reconciliation_fresh"
)

func (m *AutomationManager) ListReconciliationOperationDomains(ctx context.Context) ([]graph.DomainID, error) {
	return m.store.ListReconciliationOperationDomains(ctx)
}

// ProcessReconciliations advances one bounded page for each active operation.
// Discovery is deliberately separated from invocation execution: target rows
// become ordinary durable invocations and therefore use the same claim,
// retry, authorization, and publication path as graph-triggered work.
func (m *AutomationManager) ProcessReconciliations(ctx context.Context, domainID graph.DomainID, limit int) (int, error) {
	m.reconciliationMu.Lock()
	defer m.reconciliationMu.Unlock()
	if limit <= 0 {
		limit = automation.ReconciliationDefaultBatchSize
	}
	operations, err := m.store.ListReconciliationOperations(ctx, domainID)
	if err != nil {
		return 0, mapStoreError(err)
	}
	processed := 0
	for _, operation := range operations {
		if processed >= limit {
			break
		}
		if operation.Terminal() {
			continue
		}
		if err := m.processReconciliation(ctx, operation); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (m *AutomationManager) processReconciliation(ctx context.Context, operation automation.ReconciliationOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.raftEnabled() {
		if err := m.requireLocalExecutionLeader(ctx, operation.SpaceID); err != nil {
			return err
		}
	} else if err := m.requireWriteAllowed(); err != nil {
		return err
	}
	if operation.CancelRequested {
		return m.refreshReconciliationProgress(ctx, operation)
	}
	if operation.DiscoveryComplete {
		return m.refreshReconciliationProgress(ctx, operation)
	}
	activeReconciliationInvocations := 0
	if operation.MaxConcurrency > 0 {
		active, err := m.reconciliationActiveInvocationCount(ctx, operation)
		if err != nil {
			return err
		}
		if active >= operation.MaxConcurrency {
			return m.refreshReconciliationProgress(ctx, operation)
		}
		activeReconciliationInvocations = active
	}
	binding, err := m.store.GetBinding(ctx, operation.DomainID, operation.BindingID)
	if err != nil {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_BINDING_NOT_FOUND", err)
	}
	procedure, err := m.store.GetProcedure(ctx, operation.DomainID, operation.ProcedureID)
	if err != nil {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_PROCEDURE_NOT_FOUND", err)
	}
	binding = binding.Normalize()
	procedure = procedure.Normalize()
	if binding.Version != operation.BindingVersion || procedure.Version != operation.ProcedureVersion {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_BINDING_VERSION_CHANGED", fmt.Errorf("binding or procedure version changed"))
	}
	if binding.Reconciliation == nil {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_DISCOVERY_MISSING", fmt.Errorf("reconciliation discovery is not configured"))
	}
	if err := automation.ValidateBinding(binding, &procedure); err != nil {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_BINDING_INVALID", err)
	}
	if binding.Status != automation.StatusEnabled || procedure.Status != automation.StatusEnabled {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_BINDING_DISABLED", fmt.Errorf("binding or procedure is disabled"))
	}
	operation.Status = automation.ReconciliationStatusRunning
	operation.UpdatedAt = m.now()
	if err := m.putReconciliationOperationRuntime(ctx, operation); err != nil {
		return err
	}
	if m.sessions == nil || m.graphs == nil {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_GRAPH_RUNTIME_UNAVAILABLE", fmt.Errorf("graph runtime is not configured"))
	}
	pageSize := operation.BatchSize
	queryLimit := reconciliationQueryLimit(binding.Reconciliation.Discovery.GQL)
	if binding.Reconciliation.Discovery.PageSize > 0 && binding.Reconciliation.Discovery.PageSize < pageSize {
		pageSize = binding.Reconciliation.Discovery.PageSize
	}
	if queryLimit > 0 && queryLimit < pageSize {
		pageSize = queryLimit
	}
	if operation.MaxConcurrency > 0 {
		available := operation.MaxConcurrency - activeReconciliationInvocations
		if available < pageSize {
			pageSize = available
		}
	}
	if pageSize < 1 {
		pageSize = 1
	}
	nodes, next, err := m.discoverReconciliationPage(ctx, operation, binding, pageSize)
	if err != nil {
		return m.failReconciliation(ctx, operation, "RECONCILIATION_DISCOVERY_FAILED", err)
	}
	item := runnableAutomation{Procedure: procedure, Binding: binding}
	for _, node := range nodes {
		if queryLimit > 0 && operation.ScannedCount >= int64(queryLimit) {
			break
		}
		operation.ScannedCount++
		invocationID := reconciliationInvocationID(operation.ID, node.ID.String())
		invocation := invocationForRunnable(m.now, operation.DomainID, item, automation.Invocation{
			ID: invocationID, SpaceID: operation.SpaceID, EventID: reconciliationEventPrefix + operation.ID + ":" + node.ID.String(),
			ChangedElementID: node.ID.String(), ChangedElementKind: "node", EventType: automation.EventNodeUpdated,
		}, "")
		created, err := m.putReconciliationInvocation(ctx, invocation)
		if err != nil {
			return m.failReconciliation(ctx, operation, "RECONCILIATION_SCHEDULE_FAILED", err)
		}
		if created {
			operation.ScheduledCount++
		}
	}
	if queryLimit > 0 && operation.ScannedCount >= int64(queryLimit) {
		next = ""
	}
	operation.Cursor = next
	operation.DiscoveryComplete = next == ""
	operation.UpdatedAt = m.now()
	if err := m.putReconciliationOperationRuntime(ctx, operation); err != nil {
		return err
	}
	return m.refreshReconciliationProgress(ctx, operation)
}

func (m *AutomationManager) putReconciliationInvocation(ctx context.Context, invocation automation.Invocation) (bool, error) {
	current, err := m.store.GetInvocation(ctx, invocation.DomainID, invocation.ID)
	if err == nil {
		if !sameInvocationTrigger(current, invocation) {
			return false, fmt.Errorf("reconciliation invocation %q already exists with different trigger metadata", invocation.ID)
		}
		return false, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return false, mapStoreError(err)
	}
	if err := m.putInvocationRuntime(ctx, invocation); err != nil {
		return false, err
	}
	return true, nil
}

func (m *AutomationManager) reconciliationActiveInvocationCount(ctx context.Context, operation automation.ReconciliationOperation) (int, error) {
	invocations, err := m.store.ListInvocations(ctx, operation.DomainID, storage.InvocationFilter{})
	if err != nil {
		return 0, mapStoreError(err)
	}
	prefix := reconciliationEventPrefix + operation.ID + ":"
	active := 0
	for _, invocation := range invocations {
		if strings.HasPrefix(invocation.EventID, prefix) && (invocation.Status == "pending" || invocation.Status == "running" || invocation.Status == "retryable") {
			active++
		}
	}
	return active, nil
}

func (m *AutomationManager) refreshReconciliationProgress(ctx context.Context, operation automation.ReconciliationOperation) error {
	invocations, err := m.store.ListInvocations(ctx, operation.DomainID, storage.InvocationFilter{})
	if err != nil {
		return mapStoreError(err)
	}
	prefix := reconciliationEventPrefix + operation.ID + ":"
	active := false
	failed := int64(0)
	completed := int64(0)
	freshSkipped := int64(0)
	retrying := int64(0)
	for _, invocation := range invocations {
		if !strings.HasPrefix(invocation.EventID, prefix) {
			continue
		}
		switch invocation.Status {
		case "pending", "running":
			active = true
		case "retryable":
			active = true
			retrying++
		case "failed", "cancelled":
			failed++
		default:
			if invocation.Status == "succeeded" || invocation.Status == "skipped" {
				completed++
				if invocation.SkipReason == reconciliationFreshReason {
					freshSkipped++
				}
			}
		}
	}
	operation.CompletedCount = completed
	operation.FreshSkippedCount = freshSkipped
	operation.FailedCount = failed
	operation.RetryingCount = retrying
	operation.UpdatedAt = m.now()
	if operation.CancelRequested && !active {
		operation.Status = automation.ReconciliationStatusCanceled
		operation.CompletedAt = operation.UpdatedAt
	} else if operation.DiscoveryComplete && !active {
		operation.CompletedAt = operation.UpdatedAt
		if failed > 0 {
			operation.Status = automation.ReconciliationStatusCompletedFailures
			operation.FailureCode = "RECONCILIATION_TARGET_FAILURES"
		} else {
			operation.Status = automation.ReconciliationStatusCompleted
		}
	} else if operation.Status == automation.ReconciliationStatusQueued {
		operation.Status = automation.ReconciliationStatusRunning
	}
	return m.putReconciliationOperationRuntime(ctx, operation)
}

func (m *AutomationManager) failReconciliation(ctx context.Context, operation automation.ReconciliationOperation, code string, cause error) error {
	operation.FailureCode = code
	operation.Status = automation.ReconciliationStatusFailed
	operation.CompletedAt = m.now()
	operation.UpdatedAt = operation.CompletedAt
	if err := m.putReconciliationOperationRuntime(ctx, operation); err != nil {
		return err
	}
	return cause
}

func reconciliationInvocationID(operationID, nodeID string) string {
	sum := sha256.Sum256([]byte(operationID + "\x00" + nodeID))
	return "reconciliation-invocation-" + hex.EncodeToString(sum[:])
}

func (m *AutomationManager) discoverReconciliationPage(ctx context.Context, operation automation.ReconciliationOperation, binding automation.Binding, pageSize int) ([]graph.Node, string, error) {
	discoveryPrincipal := firstNonEmptyString(binding.Runtime.OnBehalfOfPrincipalID, binding.Runtime.OwnerPrincipalID, automationActor)
	sess, err := m.sessions.OpenSession(ctx, sessionmodel.OpenSessionInput{PrincipalID: discoveryPrincipal, SpaceID: operation.SpaceID, DomainID: operation.DomainID.String()})
	if err != nil {
		return nil, "", err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionmodel.BeginTransactionInput{PrincipalID: discoveryPrincipal, SessionID: sess.ID, Mode: sessionmodel.TransactionModeReadOnly})
	if err != nil {
		return nil, "", err
	}
	defer func() { _, _ = m.sessions.RollbackTransaction(context.Background(), discoveryPrincipal, tx.ID) }()
	queryText := binding.Reconciliation.Discovery.GQL
	if m.queryEngine != nil {
		plan, err := gql.Compile(queryText)
		if err != nil {
			return nil, "", err
		}
		statement, ok := querylogical.StatementFromGQLPlan(plan, querylogical.NormalizeOptions{PageSize: int32(pageSize), Cursor: operation.Cursor})
		if !ok {
			return nil, "", fmt.Errorf("reconciliation discovery query is not supported by the query engine")
		}
		var schema *schemamodel.DomainSchema
		if m.schemas != nil {
			doc, schemaErr := m.schemas.GetDomainSchema(ctx, operation.DomainID)
			if schemaErr != nil && !errors.Is(schemaErr, schemamodel.ErrNotFound) {
				return nil, "", schemaErr
			}
			if schemaErr == nil {
				schema = &doc
			}
		}
		prepared, err := m.queryEngine.Prepare(ctx, queryengine.PlanningContext{Transaction: tx, Schema: schema, Domain: queryengine.DomainPolicy{BroadSearchable: true, ExplicitSearchable: true}}, statement)
		if err != nil {
			return nil, "", err
		}
		result, err := m.queryEngine.Execute(ctx, queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: pageSize}}, prepared)
		if err != nil {
			return nil, "", err
		}
		nodes := make([]graph.Node, 0, len(result.Rows))
		for _, row := range result.Rows {
			cell, ok := row.Fields[binding.Reconciliation.Target]
			if !ok || cell.Node == nil {
				return nil, "", fmt.Errorf("reconciliation discovery row does not contain target alias")
			}
			nodeID, err := uuid.Parse(cell.Node.ID)
			if err != nil {
				return nil, "", err
			}
			node, err := m.graphs.GetNode(ctx, tx, nodeID.String())
			if err != nil {
				return nil, "", err
			}
			nodes = append(nodes, node)
		}
		return nodes, result.NextCursor, nil
	}
	label, err := reconciliationTargetLabel(queryText, binding.Reconciliation.Target)
	if err != nil {
		return nil, "", err
	}
	nodes, next, err := m.graphs.ListNodes(ctx, tx, pageSize, operation.Cursor)
	if err != nil {
		return nil, "", err
	}
	if label == "" {
		return nodes, next, nil
	}
	filtered := make([]graph.Node, 0, len(nodes))
	for _, node := range nodes {
		if graph.HasLabels(node, []string{label}) {
			filtered = append(filtered, node)
		}
	}
	return filtered, next, nil
}

func reconciliationTargetLabel(queryText, target string) (string, error) {
	upper := strings.ToUpper(queryText)
	if strings.Contains(upper, " WHERE ") || strings.Contains(upper, ")-") || strings.Contains(upper, "-[") || strings.Contains(upper, "->") || strings.Contains(upper, "<-") {
		return "", fmt.Errorf("reconciliation discovery requires the configured query engine")
	}
	match := reconciliationMatchRE.FindStringSubmatch(queryText)
	if len(match) == 0 {
		return "", fmt.Errorf("reconciliation discovery must contain a MATCH node")
	}
	if !strings.EqualFold(match[1], strings.TrimSpace(target)) {
		return "", fmt.Errorf("reconciliation discovery target alias does not match the MATCH alias")
	}
	return strings.TrimSpace(match[2]), nil
}

func reconciliationQueryLimit(queryText string) int {
	match := reconciliationLimitRE.FindStringSubmatch(queryText)
	if len(match) != 2 {
		return 0
	}
	value, _ := strconv.Atoi(match[1])
	return value
}

func reconciliationInputHash(node graph.Node) string {
	value, ok := node.Meta["automation"]
	if !ok {
		return ""
	}
	if metadata, ok := value.(map[string]any); ok {
		return strings.TrimSpace(fmt.Sprint(metadata["input_hash"]))
	}
	if metadata, ok := value.(map[string]string); ok {
		return strings.TrimSpace(metadata["input_hash"])
	}
	return ""
}
