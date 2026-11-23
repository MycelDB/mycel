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
				result.Deltas = append(result.Deltas, graphUsageCountDelta(usageservice.DomainScope(scope.SpaceID, scope.DomainID), m.graphUsageDomainClass(ctx, scope.SpaceID, scope.DomainID), 0, 0))
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
		result.Deltas = append(result.Deltas, graphUsageCountDelta(usageservice.DomainScope(scope.SpaceID, scope.DomainID), m.graphUsageDomainClass(ctx, scope.SpaceID, scope.DomainID), len(nodes), len(edges)))
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
			graphUsageCountDelta(usageservice.SpaceScope(scope.SpaceID), usageservice.DomainClassRetainedNonSystem, 0, 0),
			graphUsageCountDelta(usageservice.SpaceScope(scope.SpaceID), usageservice.DomainClassSystem, 0, 0),
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
			result.Deltas = append(result.Deltas, graphUsageCountDelta(usageservice.DomainScope(key.SpaceID, key.DomainID), m.graphUsageDomainClass(ctx, key.SpaceID, key.DomainID), len(nodes), len(edges)))
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

func graphUsageCountDelta(scope usageservice.Scope, domainClass usageservice.DomainClass, nodes int, edges int) usageservice.Delta {
	return usageservice.Delta{Scope: scope, DomainClass: domainClass, Metrics: []usageservice.MetricDelta{
		{Name: usageservice.MetricGraphNodeCount, Version: 1, Delta: int64(nodes)},
		{Name: usageservice.MetricGraphEdgeCount, Version: 1, Delta: int64(edges)},
	}}
}

func graphUsageMetricDeltas(changes []graphchange.Change) []usageservice.MetricDelta {
	var nodeDelta int64
	var edgeDelta int64
	for _, change := range changes {
		switch graphchange.NormalizeChangeType(string(change.Type)) {
		case graphchange.ChangeTypeNodeCreated:
			nodeDelta++
		case graphchange.ChangeTypeNodeDeleted:
			if change.OldNode != nil {
				nodeDelta--
			}
		case graphchange.ChangeTypeEdgeCreated:
			edgeDelta++
		case graphchange.ChangeTypeEdgeDeleted:
			if change.OldEdge != nil {
				edgeDelta--
			}
		}
	}
	deltas := make([]usageservice.MetricDelta, 0, 2)
	if nodeDelta != 0 {
		deltas = append(deltas, usageservice.MetricDelta{Name: usageservice.MetricGraphNodeCount, Version: 1, Delta: nodeDelta})
	}
	if edgeDelta != 0 {
		deltas = append(deltas, usageservice.MetricDelta{Name: usageservice.MetricGraphEdgeCount, Version: 1, Delta: edgeDelta})
	}
	return deltas
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
