package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	usageservice "github.com/myceldb/mycel/internal/usage/service"
)

type graphUsageDeltaApplier interface {
	ApplyDelta(context.Context, usageservice.Delta) error
	ReserveDeltaForAdmission(context.Context, usageservice.Delta) (func(), error)
}

type graphUsageRebuildMarker interface {
	MarkRebuilding(context.Context, string) error
}

func (m *Module) WithUsageAccounting(usage graphUsageDeltaApplier) *Module {
	m.usageAccounting = usage
	return m
}

func (m *Module) graphUsageChangesForRecord(ctx context.Context, store *graphstorage.LocalStore, record graphCommitRecord) ([]graphchange.Change, error) {
	snapshot := &overlay{putNodes: map[domaingraph.NodeID]domaingraph.Node{}, deleteNodes: map[domaingraph.NodeID]struct{}{}, putEdges: map[domaingraph.EdgeID]domaingraph.Edge{}, deleteEdges: map[domaingraph.EdgeID]struct{}{}, opCount: record.OperationCount}
	for _, node := range record.PutNodes {
		snapshot.putNodes[node.ID] = node
	}
	for _, id := range record.DeleteNodeIDs {
		snapshot.deleteNodes[id] = struct{}{}
	}
	for _, edge := range record.PutEdges {
		snapshot.putEdges[edge.ID] = edge
	}
	for _, id := range record.DeleteEdgeIDs {
		snapshot.deleteEdges[id] = struct{}{}
	}
	return m.overlayChanges(ctx, store, snapshot)
}

func (m *Module) commitGraphStorageWithUsage(ctx context.Context, tx graphstorage.Txn, record graphCommitRecord, changes []graphchange.Change) (graphstorage.CommitInfo, error) {
	m.usageReconciliationMu.RLock()
	defer m.usageReconciliationMu.RUnlock()
	info, err := tx.CommitWithInfo()
	if err != nil || info.IdempotentReplay {
		return info, err
	}
	return info, m.applyGraphUsageDelta(ctx, record, changes, info.NextRevision)
}

func (m *Module) BeginUsageReconciliation(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.usageReconciliationMu.TryLock() {
		return nil, errors.New("graph usage commits or reconciliation are in progress")
	}
	return m.usageReconciliationMu.Unlock, nil
}

func (m *Module) applyGraphUsageDelta(ctx context.Context, record graphCommitRecord, changes []graphchange.Change, revision uint64) error {
	if m.usageAccounting == nil {
		return nil
	}
	delta := m.graphUsageDelta(ctx, record, changes, strconv.FormatUint(revision, 10))
	if len(delta.Metrics) == 0 {
		return nil
	}
	err := m.usageAccounting.ApplyDelta(ctx, delta)
	return m.handleGraphUsageAccountingError(ctx, err)
}

func (m *Module) reserveGraphUsageAdmission(ctx context.Context, record graphCommitRecord, changes []graphchange.Change) (func(), error) {
	if m.usageAccounting == nil {
		return func() {}, nil
	}
	delta := m.graphUsageDelta(ctx, record, changes, "")
	if len(delta.Metrics) == 0 {
		return func() {}, nil
	}
	return m.usageAccounting.ReserveDeltaForAdmission(ctx, delta)
}

func (m *Module) graphUsageDelta(ctx context.Context, record graphCommitRecord, changes []graphchange.Change, revision string) usageservice.Delta {
	return usageservice.Delta{
		MutationID:  graphUsageMutationID(record),
		Source:      graphUsageSource(record),
		Revision:    revision,
		Scope:       usageservice.DomainScope(record.SpaceID, record.DomainID),
		DomainClass: m.graphUsageDomainClass(ctx, record.SpaceID, record.DomainID),
		Metrics:     graphUsageMetricDeltas(changes),
	}
}

func (m *Module) handleGraphUsageAccountingError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	message := "graph usage accounting delta failed; counters require rebuild"
	if marker, ok := m.usageAccounting.(graphUsageRebuildMarker); ok {
		if markErr := marker.MarkRebuilding(context.WithoutCancel(ctx), err.Error()); markErr != nil && m.logger != nil {
			m.logger.Warn("failed to mark usage counters rebuilding after graph accounting failure", "error", markErr, "accounting_error", err)
		}
	}
	if m.logger != nil {
		m.logger.Warn(message, "error", err)
	}
	// Graph commits are already durable by the time usage deltas are materialized.
	// Do not turn a post-commit accounting failure into an ambiguous graph commit;
	// health/read APIs fail closed while counters are marked rebuilding.
	return nil
}

func (m *Module) ReconcileUsage(ctx context.Context, req usageservice.ReconciliationRequest) (usageservice.ReconciliationResult, error) {
	if req.MaxRecords == 0 {
		req.MaxRecords = 100000
	}
	result := usageservice.ReconciliationResult{Source: "graph", Evidence: map[string]string{"max_records": strconv.FormatUint(req.MaxRecords, 10)}}
	scope := req.Scope
	switch scope.Type {
	case usageservice.ScopeTypeDomain:
		if err := scope.Validate(); err != nil {
			return result, fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
		key, err := newDomainStoreKey(scope.SpaceID, scope.DomainID)
		if err != nil {
			return result, err
		}
		store, err := m.existingDomainStore(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				result.ScopesScanned = 1
				result.Deltas = append(result.Deltas, graphUsageReconciliationDelta(usageservice.DomainScope(scope.SpaceID, scope.DomainID), m.graphUsageDomainClass(ctx, scope.SpaceID, scope.DomainID), 0, 0, 0))
				return result, nil
			}
			return result, err
		}
		domainID, err := uuid.Parse(scope.DomainID)
		if err != nil {
			return result, fmt.Errorf("%w: invalid domain_id", ErrInvalidInput)
		}
		nodes, edges, err := listDomainGraphRecords(ctx, store, domaingraph.DomainID(domainID))
		if err != nil {
			return result, err
		}
		result.ScopesScanned = 1
		result.RecordsScanned = uint64(len(nodes) + len(edges))
		if result.RecordsScanned > req.MaxRecords {
			result.Truncated = true
			return result, nil
		}
		result.Deltas = append(result.Deltas, graphUsageReconciliationDelta(usageservice.DomainScope(scope.SpaceID, scope.DomainID), m.graphUsageDomainClass(ctx, scope.SpaceID, scope.DomainID), len(nodes), len(edges), graphNodesPayloadContentBytes(nodes)))
		return result, ctx.Err()
	case usageservice.ScopeTypeSpace:
		if err := scope.Validate(); err != nil {
			return result, fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
		stores, err := m.domainStoresForSpace(ctx, scope.SpaceID)
		if err != nil {
			return result, err
		}
		result.Deltas = append(result.Deltas,
			graphUsageReconciliationDelta(usageservice.SpaceScope(scope.SpaceID), usageservice.DomainClassRetainedNonSystem, 0, 0, 0),
			graphUsageReconciliationDelta(usageservice.SpaceScope(scope.SpaceID), usageservice.DomainClassSystem, 0, 0, 0),
		)
		for _, store := range stores {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			key := storeKeyForUsageReconciliation(m, store)
			if key.SpaceID == "" || key.DomainID == "" {
				continue
			}
			domainID, err := uuid.Parse(key.DomainID)
			if err != nil {
				return result, fmt.Errorf("%w: invalid domain_id", ErrInvalidInput)
			}
			nodes, edges, err := listDomainGraphRecords(ctx, store, domaingraph.DomainID(domainID))
			if err != nil {
				return result, err
			}
			result.ScopesScanned++
			result.RecordsScanned += uint64(len(nodes) + len(edges))
			if result.RecordsScanned > req.MaxRecords {
				result.Truncated = true
				return result, nil
			}
			result.Deltas = append(result.Deltas, graphUsageReconciliationDelta(usageservice.DomainScope(key.SpaceID, key.DomainID), m.graphUsageDomainClass(ctx, key.SpaceID, key.DomainID), len(nodes), len(edges), graphNodesPayloadContentBytes(nodes)))
		}
		return result, nil
	default:
		return result, fmt.Errorf("%w: unsupported usage scope type %q", ErrInvalidInput, scope.Type)
	}
}

func storeKeyForUsageReconciliation(m *Module, store *graphstorage.LocalStore) domainStoreKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, candidate := range m.stores {
		if candidate == store {
			return key
		}
	}
	return domainStoreKey{}
}

func graphUsageReconciliationDelta(scope usageservice.Scope, domainClass usageservice.DomainClass, nodes int, edges int, logicalBytes uint64) usageservice.Delta {
	return usageservice.Delta{Scope: scope, DomainClass: domainClass, Metrics: []usageservice.MetricDelta{
		{Name: usageservice.MetricGraphNodeCount, Version: 1, Delta: int64(nodes)},
		{Name: usageservice.MetricGraphEdgeCount, Version: 1, Delta: int64(edges)},
		{Name: usageservice.MetricGraphLogicalBytes, Version: 1, Delta: int64(logicalBytes)},
	}}
}

func graphUsageMetricDeltas(changes []graphchange.Change) []usageservice.MetricDelta {
	var nodeDelta int64
	var edgeDelta int64
	var logicalBytesDelta int64
	for _, change := range changes {
		switch graphchange.NormalizeChangeType(string(change.Type)) {
		case graphchange.ChangeTypeNodeCreated:
			nodeDelta++
			logicalBytesDelta += int64(graphNodePayloadContentBytes(change.Node))
		case graphchange.ChangeTypeNodeUpdated:
			logicalBytesDelta += graphPayloadContentBytesDelta(change.OldNode, change.Node)
		case graphchange.ChangeTypeNodeDeleted:
			if change.OldNode != nil {
				nodeDelta--
				logicalBytesDelta -= int64(graphNodePayloadContentBytes(change.OldNode))
			}
		case graphchange.ChangeTypeEdgeCreated:
			edgeDelta++
		case graphchange.ChangeTypeEdgeDeleted:
			if change.OldEdge != nil {
				edgeDelta--
			}
		}
	}
	deltas := make([]usageservice.MetricDelta, 0, 3)
	if nodeDelta != 0 {
		deltas = append(deltas, usageservice.MetricDelta{Name: usageservice.MetricGraphNodeCount, Version: 1, Delta: nodeDelta})
	}
	if edgeDelta != 0 {
		deltas = append(deltas, usageservice.MetricDelta{Name: usageservice.MetricGraphEdgeCount, Version: 1, Delta: edgeDelta})
	}
	if logicalBytesDelta != 0 || nodeDelta != 0 {
		// An explicitly materialized zero distinguishes new empty payloads
		// from a scope accumulated before graph.logical_bytes was tracked.
		deltas = append(deltas, usageservice.MetricDelta{Name: usageservice.MetricGraphLogicalBytes, Version: 1, Delta: logicalBytesDelta})
	}
	return deltas
}

func graphPayloadContentBytesDelta(oldNode *domaingraph.Node, newNode *domaingraph.Node) int64 {
	oldBytes := graphNodePayloadContentBytes(oldNode)
	newBytes := graphNodePayloadContentBytes(newNode)
	if newBytes >= oldBytes {
		return int64(newBytes - oldBytes)
	}
	return -int64(oldBytes - newBytes)
}

func graphNodesPayloadContentBytes(nodes []domaingraph.Node) uint64 {
	var total uint64
	for i := range nodes {
		total += graphPayloadContentBytes(nodes[i].Payload)
	}
	return total
}

func graphNodePayloadContentBytes(node *domaingraph.Node) uint64 {
	if node == nil {
		return 0
	}
	return graphPayloadContentBytes(node.Payload)
}

func graphPayloadContentBytes(value any) uint64 {
	switch v := value.(type) {
	case nil:
		return 0
	case string:
		return uint64(len(v))
	case bool:
		if v {
			return uint64(len("true"))
		}
		return uint64(len("false"))
	case int:
		return uint64(len(strconv.FormatInt(int64(v), 10)))
	case int8:
		return uint64(len(strconv.FormatInt(int64(v), 10)))
	case int16:
		return uint64(len(strconv.FormatInt(int64(v), 10)))
	case int32:
		return uint64(len(strconv.FormatInt(int64(v), 10)))
	case int64:
		return uint64(len(strconv.FormatInt(v, 10)))
	case uint:
		return uint64(len(strconv.FormatUint(uint64(v), 10)))
	case uint8:
		return uint64(len(strconv.FormatUint(uint64(v), 10)))
	case uint16:
		return uint64(len(strconv.FormatUint(uint64(v), 10)))
	case uint32:
		return uint64(len(strconv.FormatUint(uint64(v), 10)))
	case uint64:
		return uint64(len(strconv.FormatUint(v, 10)))
	case float32:
		return uint64(len(strconv.FormatFloat(float64(v), 'g', -1, 32)))
	case float64:
		return uint64(len(strconv.FormatFloat(v, 'g', -1, 64)))
	case json.Number:
		return uint64(len(v.String()))
	case []any:
		var total uint64
		for _, item := range v {
			total += graphPayloadContentBytes(item)
		}
		return total
	case []string:
		var total uint64
		for _, item := range v {
			total += uint64(len(item))
		}
		return total
	case map[string]any:
		var total uint64
		for _, item := range v {
			total += graphPayloadContentBytes(item)
		}
		return total
	default:
		return graphJSONPayloadContentBytes(v)
	}
}

func graphJSONPayloadContentBytes(value any) uint64 {
	raw, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return 0
	}
	return graphPayloadContentBytes(decoded)
}

func graphUsageMutationID(record graphCommitRecord) string {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Sprintf("graph-usage:commit:v1:%s:%s:%d", strings.TrimSpace(record.SpaceID), strings.TrimSpace(record.DomainID), record.BaseRevision)
	}
	sum := sha256.Sum256(payload)
	return "graph-usage:commit:v1:" + hex.EncodeToString(sum[:])
}

func graphUsageSource(record graphCommitRecord) string {
	return "graph:" + strings.TrimSpace(record.SpaceID) + ":" + strings.TrimSpace(record.DomainID)
}

func (m *Module) graphUsageDomainClass(_ context.Context, _ string, domainID string) usageservice.DomainClass {
	// Until the space/domain service is wired as a classifier, only clearly
	// internal-looking domains are marked system. Normal domains remain in the
	// retained non-system aggregate used by quota/product snapshots.
	trimmed := strings.TrimSpace(strings.ToLower(domainID))
	if strings.HasPrefix(trimmed, "_") || strings.HasPrefix(trimmed, "system.") || trimmed == "system" {
		return usageservice.DomainClassSystem
	}
	return usageservice.DomainClassRetainedNonSystem
}
